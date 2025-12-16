//! 默认服务端消息观察者
//! 
//! 提供通用的消息观察者实现，处理基础业务逻辑（ping/pong、错误、断开等）

use crate::server::connection::{ConnectionManager, ConnectionManagerTrait};
use crate::server::transports::server_core::ServerCore;
use crate::server::transports::ConnectionHandler;
use crate::common::MessageParser;
use crate::common::protocol::{Frame, pong, frame_with_system_command, Reliability};
use crate::transport::events::{ConnectionEvent, ConnectionObserver};
use crate::server::events::handler::ServerEventHandler;
use crate::common::error::Result;
use std::sync::Arc;
use tracing::{debug, error, info};
use std::convert::TryFrom;

/// 默认服务端消息观察者
/// 
/// 处理基础业务逻辑：
/// - 系统命令（CONNECT、PING、PONG）
/// - 消息命令（路由到 ServerEventHandler）
/// - 通知命令（路由到 ServerEventHandler）
/// - 连接事件（断开、错误）
pub struct DefaultServerMessageObserver {
    /// 连接处理器（用于处理业务逻辑）
    handler: Arc<dyn ConnectionHandler>,
    /// 连接管理器
    manager: Arc<ConnectionManager>,
    /// 消息解析器（用于协商前的消息解析）
    parser: MessageParser,
    /// 连接 ID
    connection_id: String,
    /// ServerCore（用于处理协商等）
    core: Arc<ServerCore>,
    /// 设备管理器（用于连接断开时清理设备）
    device_manager: Option<Arc<crate::server::device::DeviceManager>>,
    /// 事件处理器（可选，用于细化的命令处理）
    event_handler: Option<Arc<dyn ServerEventHandler>>,
}

impl Clone for DefaultServerMessageObserver {
    fn clone(&self) -> Self {
        Self {
            handler: Arc::clone(&self.handler),
            manager: Arc::clone(&self.manager),
            parser: self.parser.clone(),
            connection_id: self.connection_id.clone(),
            core: Arc::clone(&self.core),
            device_manager: self.device_manager.clone(),
            event_handler: self.event_handler.clone(),
        }
    }
}

impl DefaultServerMessageObserver {
    /// 创建新的默认观察者
    pub fn new(
        handler: Arc<dyn ConnectionHandler>,
        manager: Arc<ConnectionManager>,
        parser: MessageParser,
        connection_id: String,
        core: Arc<ServerCore>,
        device_manager: Option<Arc<crate::server::device::DeviceManager>>,
        event_handler: Option<Arc<dyn ServerEventHandler>>,
    ) -> Self {
        Self {
            handler,
            manager,
            parser,
            connection_id,
            core,
            device_manager,
            event_handler,
        }
    }
    
    /// 处理系统命令
    pub async fn handle_system_command(
        &self,
        frame: &Frame,
        sys_type: i32,
        connection_id: &str,
    ) -> Result<()> {
        use crate::common::protocol::flare::core::commands::system_command::Type as SysType;
        
        match SysType::try_from(sys_type) {
            Ok(SysType::Connect) => {
                // CONNECT 消息由 ServerCore 统一处理
                let manager_trait = Arc::clone(&self.manager) as Arc<dyn ConnectionManagerTrait>;
                if let Some((conn, _)) = manager_trait.get_connection(connection_id).await {
                    if let Err(e) = self.core.handle_connect_complete(
                        frame,
                        connection_id,
                        conn,
                        Arc::clone(&self.handler),
                    ).await {
                        error!("[DefaultObserver] 处理 CONNECT 消息失败: {}", e);
                    }
                } else {
                    error!("[DefaultObserver] 连接不存在: {}", connection_id);
                }
            }
            Ok(SysType::Ping) => {
                // 处理 PING：回复 PONG 并更新连接活跃时间
                let manager = Arc::clone(&self.manager) as Arc<dyn ConnectionManagerTrait>;
                let conn_id = connection_id.to_string();
                
                // 更新连接活跃时间
                let manager_update = Arc::clone(&manager);
                let conn_id_update = conn_id.clone();
                tokio::spawn(async move {
                    let _ = manager_update.update_connection_active(&conn_id_update).await;
                });
                
                // 如果有自定义事件处理器，先调用它
                let parser_clone = self.parser.clone();
                if let Some(ref event_handler) = self.event_handler {
                    if let Ok(Some(custom_response)) = event_handler.handle_ping(frame, connection_id).await {
                        // 使用自定义回复
                        let manager_get = Arc::clone(&manager);
                        let parser = parser_clone.clone();
                        tokio::spawn(async move {
                            if let Some((conn, _)) = manager_get.get_connection(&conn_id).await {
                                if let Ok(data) = parser.serialize(&custom_response) {
                                    let conn_clone = Arc::clone(&conn);
                                    let mut c = conn_clone.lock().await;
                                    let _ = c.send(&data).await;
                                }
                            }
                        });
                        return Ok(());
                    }
                }
                
                // 默认处理：回复 PONG
                let pong_cmd = pong();
                let pong_frame = frame_with_system_command(pong_cmd, Reliability::AtLeastOnce);
                if let Ok(pong_data) = parser_clone.serialize(&pong_frame) {
                    let manager_get = Arc::clone(&manager);
                    tokio::spawn(async move {
                        if let Some((conn, _)) = manager_get.get_connection(&conn_id).await {
                            let conn_clone = Arc::clone(&conn);
                            let mut c = conn_clone.lock().await;
                            let _ = c.send(&pong_data).await;
                        }
                    });
                }
            }
            Ok(SysType::Pong) => {
                // 处理 PONG：更新连接活跃时间
                let manager = Arc::clone(&self.manager) as Arc<dyn ConnectionManagerTrait>;
                let conn_id = connection_id.to_string();
                
                // 如果有自定义事件处理器，调用它
                if let Some(ref event_handler) = self.event_handler {
                    let _ = event_handler.handle_pong(frame, connection_id).await;
                }
                
                tokio::spawn(async move {
                    let _ = manager.update_connection_active(&conn_id).await;
                });
            }
            Ok(SysType::Event) => {
                // 将 System::Event 交由 ConnectionHandler 处理（与消息/通知同构）
                let handler = Arc::clone(&self.handler);
                let manager = Arc::clone(&self.manager);
                let parser = self.parser.clone();
                let conn_id = connection_id.to_string();
                let frame_clone = frame.clone();

                // 更新连接活跃时间
                let manager_update = Arc::clone(&manager) as Arc<dyn ConnectionManagerTrait>;
                let conn_id_update = conn_id.clone();
                tokio::spawn(async move {
                    let _ = manager_update.update_connection_active(&conn_id_update).await;
                });

                tokio::spawn(async move {
                    if let Ok(Some(response)) = handler.handle_frame(&frame_clone, &conn_id).await {
                        // 发送回复（如果有）
                        let manager_trait = Arc::clone(&manager) as Arc<dyn ConnectionManagerTrait>;
                        if let Some((conn, _)) = manager_trait.get_connection(&conn_id).await {
                            if let Ok(data) = parser.serialize(&response) {
                                let conn_clone = Arc::clone(&conn);
                                let mut c = conn_clone.lock().await;
                                let _ = c.send(&data).await;
                            }
                        }
                    }
                });
            }
            _ => {
                debug!("[DefaultObserver] 未处理的系统命令类型: {}", sys_type);
            }
        }
        
        Ok(())
    }
    
    /// 处理消息命令
    pub async fn handle_message_command(
        &self,
        frame: &Frame,
        command: &crate::common::protocol::MessageCommand,
        connection_id: &str,
    ) -> Result<()> {
        let message_id = command.message_id.clone();
        debug!(
            "[DefaultObserver] handle_message_command: 开始处理, connection_id={}, message_id={}",
            connection_id, message_id
        );
        
        // 如果有自定义事件处理器，使用它
        if let Some(ref event_handler) = self.event_handler {
            use crate::common::protocol::flare::core::commands::message_command::Type as MsgType;
            if let Ok(msg_type) = MsgType::try_from(command.r#type) {
                debug!(
                    "[DefaultObserver] handle_message_command: 调用 event_handler.handle_message_command_by_type, connection_id={}, message_id={}, msg_type={:?}",
                    connection_id, message_id, msg_type
                );
                if let Ok(Some(response)) = event_handler
                    .handle_message_command_by_type(command, msg_type, connection_id)
                    .await
                {
                    debug!(
                        "[DefaultObserver] handle_message_command: event_handler 返回响应, connection_id={}, message_id={}",
                        connection_id, message_id
                    );
                    // 发送自定义回复
                    let manager_trait = Arc::clone(&self.manager) as Arc<dyn ConnectionManagerTrait>;
                    let conn_id = connection_id.to_string();
                    let parser = self.parser.clone();
                    tokio::spawn(async move {
                        if let Some((conn, _)) = manager_trait.get_connection(&conn_id).await {
                            if let Ok(data) = parser.serialize(&response) {
                                let conn_clone = Arc::clone(&conn);
                                let mut c = conn_clone.lock().await;
                                let _ = c.send(&data).await;
                            }
                        }
                    });
                    return Ok(());
                } else {
                    debug!(
                        "[DefaultObserver] handle_message_command: event_handler 返回 None, 继续默认处理, connection_id={}, message_id={}",
                        connection_id, message_id
                    );
                }
            }
        }
        
        // 默认处理：使用 ConnectionHandler
        let handler = Arc::clone(&self.handler);
        let manager = Arc::clone(&self.manager);
        let parser = self.parser.clone();
        let conn_id = connection_id.to_string();
        let frame_clone = frame.clone();
        
        debug!(
            "[DefaultObserver] handle_message_command: 准备调用 handler.handle_frame, connection_id={}, message_id={}",
            conn_id, message_id
        );
        
        // 更新连接活跃时间
        let manager_update = Arc::clone(&manager) as Arc<dyn ConnectionManagerTrait>;
        let conn_id_update = conn_id.clone();
        tokio::spawn(async move {
            let _ = manager_update.update_connection_active(&conn_id_update).await;
        });
        
        tokio::spawn(async move {
            info!(
                "[DefaultObserver] handle_message_command: 开始调用 handler.handle_frame, connection_id={}, message_id={}",
                conn_id, message_id
            );
            let start_time = std::time::Instant::now();
            
            // 添加超时保护，避免阻塞
            let timeout_duration = std::time::Duration::from_secs(10);
            let result = match tokio::time::timeout(
                timeout_duration,
                handler.handle_frame(&frame_clone, &conn_id)
            ).await {
                Ok(res) => res,
                Err(_) => {
                    error!(
                        "[DefaultObserver] handle_message_command: handler.handle_frame 超时, connection_id={}, message_id={}, timeout={:?}",
                        conn_id, message_id, timeout_duration
                    );
                    return;
                }
            };
            
            info!(
                "[DefaultObserver] handle_message_command: handler.handle_frame 调用完成, connection_id={}, message_id={}, result={:?}",
                conn_id, message_id, result.is_ok()
            );
            match result {
                Ok(Some(response)) => {
                    let duration_ms = start_time.elapsed().as_millis();
                    debug!(
                        "[DefaultObserver] handle_message_command: handler.handle_frame 返回响应, connection_id={}, message_id={}, duration_ms={}",
                        conn_id, message_id, duration_ms
                    );
                // 发送回复
                let manager_trait = Arc::clone(&manager) as Arc<dyn ConnectionManagerTrait>;
                if let Some((conn, _)) = manager_trait.get_connection(&conn_id).await {
                    if let Ok(data) = parser.serialize(&response) {
                        let conn_clone = Arc::clone(&conn);
                        let mut c = conn_clone.lock().await;
                            if let Err(e) = c.send(&data).await {
                                error!(
                                    "[DefaultObserver] handle_message_command: 发送响应失败, connection_id={}, message_id={}, error={}",
                                    conn_id, message_id, e
                                );
                            } else {
                                debug!(
                                    "[DefaultObserver] handle_message_command: 响应已发送, connection_id={}, message_id={}",
                                    conn_id, message_id
                                );
                            }
                        }
                    }
                }
                Ok(None) => {
                    let duration_ms = start_time.elapsed().as_millis();
                    debug!(
                        "[DefaultObserver] handle_message_command: handler.handle_frame 返回 None, connection_id={}, message_id={}, duration_ms={}",
                        conn_id, message_id, duration_ms
                    );
                }
                Err(e) => {
                    let duration_ms = start_time.elapsed().as_millis();
                    error!(
                        "[DefaultObserver] handle_message_command: handler.handle_frame 失败, connection_id={}, message_id={}, error={}, duration_ms={}",
                        conn_id, message_id, e, duration_ms
                    );
                }
            }
        });
        
        Ok(())
    }
    
    /// 处理通知命令
    pub async fn handle_notification_command(
        &self,
        frame: &Frame,
        command: &crate::common::protocol::NotificationCommand,
        connection_id: &str,
    ) -> Result<()> {
        // 如果有自定义事件处理器，使用它
        if let Some(ref event_handler) = self.event_handler {
            use crate::common::protocol::flare::core::commands::notification_command::Type as NotifType;
            if let Ok(notif_type) = NotifType::try_from(command.r#type) {
                if let Ok(Some(response)) = event_handler
                    .handle_notification_command_by_type(command, notif_type, connection_id)
                    .await
                {
                    // 发送自定义回复
                    let manager_trait = Arc::clone(&self.manager) as Arc<dyn ConnectionManagerTrait>;
                    let conn_id = connection_id.to_string();
                    let parser = self.parser.clone();
                    tokio::spawn(async move {
                        if let Some((conn, _)) = manager_trait.get_connection(&conn_id).await {
                            if let Ok(data) = parser.serialize(&response) {
                                let conn_clone = Arc::clone(&conn);
                                let mut c = conn_clone.lock().await;
                                let _ = c.send(&data).await;
                            }
                        }
                    });
                    return Ok(());
                }
            }
        }
        
        // 默认处理：使用 ConnectionHandler
        let handler = Arc::clone(&self.handler);
        let manager = Arc::clone(&self.manager);
        let parser = self.parser.clone();
        let conn_id = connection_id.to_string();
        let frame_clone = frame.clone();
        
        // 更新连接活跃时间
        let manager_update = Arc::clone(&manager) as Arc<dyn ConnectionManagerTrait>;
        let conn_id_update = conn_id.clone();
        tokio::spawn(async move {
            let _ = manager_update.update_connection_active(&conn_id_update).await;
        });
        
        tokio::spawn(async move {
            if let Ok(Some(response)) = handler.handle_frame(&frame_clone, &conn_id).await {
                // 发送回复
                let manager_trait = Arc::clone(&manager) as Arc<dyn ConnectionManagerTrait>;
                if let Some((conn, _)) = manager_trait.get_connection(&conn_id).await {
                    if let Ok(data) = parser.serialize(&response) {
                        let conn_clone = Arc::clone(&conn);
                        let mut c = conn_clone.lock().await;
                        let _ = c.send(&data).await;
                    }
                }
            }
        });
        
        Ok(())
    }
}

impl ConnectionObserver for DefaultServerMessageObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
                debug!(
                    "[DefaultObserver] on_event: 收到消息, connection_id={}, data_len={}",
                    self.connection_id, data.len()
                );
                match self.parser.parse(data) {
                    Ok(frame) => {
                        debug!(
                            "[DefaultObserver] on_event: 解析 Frame 成功, connection_id={}, message_id={}",
                            self.connection_id,
                            frame.message_id
                        );
                    // 先克隆 frame，避免生命周期问题
                    let frame_clone = frame.clone();
                    if let Some(cmd) = &frame.command {
                        match &cmd.r#type {
                            Some(crate::common::protocol::flare::core::commands::command::Type::System(sys_cmd)) => {
                                let sys_type = sys_cmd.r#type;
                                let conn_id = self.connection_id.clone();
                                let observer = self.clone();
                                
                                tokio::spawn(async move {
                                    if let Err(e) = observer.handle_system_command(&frame_clone, sys_type, &conn_id).await {
                                        error!("[DefaultObserver] 处理系统命令失败: {}", e);
                                    }
                                });
                            }
                            Some(crate::common::protocol::flare::core::commands::command::Type::Message(msg_cmd)) => {
                                let conn_id = self.connection_id.clone();
                                let msg_cmd_clone = msg_cmd.clone();
                                let observer = self.clone();
                                            let message_id = msg_cmd_clone.message_id.clone();
                                            
                                            debug!(
                                                "[DefaultObserver] 收到消息命令: connection_id={}, message_id={}, message_type={}",
                                                conn_id, message_id, msg_cmd_clone.r#type
                                            );
                                
                                tokio::spawn(async move {
                                                debug!(
                                                    "[DefaultObserver] 准备调用 handle_message_command: connection_id={}, message_id={}",
                                                    conn_id, message_id
                                                );
                                                match observer.handle_message_command(&frame_clone, &msg_cmd_clone, &conn_id).await {
                                                    Ok(_) => {
                                                        debug!(
                                                            "[DefaultObserver] handle_message_command 成功: connection_id={}, message_id={}",
                                                            conn_id, message_id
                                                        );
                                                    }
                                                    Err(e) => {
                                                        error!(
                                                            "[DefaultObserver] 处理消息命令失败: connection_id={}, message_id={}, error={}",
                                                            conn_id, message_id, e
                                                        );
                                                    }
                                    }
                                });
                            }
                            Some(crate::common::protocol::flare::core::commands::command::Type::Notification(notif_cmd)) => {
                                let conn_id = self.connection_id.clone();
                                let notif_cmd_clone = notif_cmd.clone();
                                let observer = self.clone();
                                
                                tokio::spawn(async move {
                                    if let Err(e) = observer.handle_notification_command(&frame_clone, &notif_cmd_clone, &conn_id).await {
                                        error!("[DefaultObserver] 处理通知命令失败: {}", e);
                                    }
                                });
                            }
                            Some(crate::common::protocol::flare::core::commands::command::Type::Custom(custom_cmd)) => {
                                // 处理自定义命令（如 SyncMessages、ListSessions 等）
                                let handler = Arc::clone(&self.handler);
                                let manager = Arc::clone(&self.manager) as Arc<dyn ConnectionManagerTrait>;
                                let parser = self.parser.clone();
                                let conn_id = self.connection_id.clone();
                                let cmd_name = custom_cmd.name.clone();
                                
                                // 更新连接活跃时间
                                let manager_update = Arc::clone(&manager);
                                let conn_id_update = conn_id.clone();
                                tokio::spawn(async move {
                                    let _ = manager_update.update_connection_active(&conn_id_update).await;
                                });
                                
                                // 处理自定义命令并发送响应
                                tokio::spawn(async move {
                                    if let Ok(Some(response)) = handler.handle_frame(&frame_clone, &conn_id).await {
                                        // 发送响应 Frame 回客户端
                                        if let Some((conn, _)) = manager.get_connection(&conn_id).await {
                                            if let Ok(data) = parser.serialize(&response) {
                                                let conn_clone = Arc::clone(&conn);
                                                let mut c = conn_clone.lock().await;
                                                if let Err(e) = c.send(&data).await {
                                                    error!("[DefaultObserver] 发送自定义命令响应失败: {}", e);
                                                } else {
                                                    debug!("[DefaultObserver] 自定义命令响应已发送: connection_id={}, command={}", conn_id, cmd_name);
                                                }
                                            }
                                        }
                                    }
                                });
                            }
                            _ => {
                                debug!("[DefaultObserver] 未处理的命令类型");
                            }
                        }
                        }
                    }
                    Err(e) => {
                        error!(
                            "[DefaultObserver] on_event: 解析 Frame 失败, connection_id={}, error={}, data_len={}",
                            self.connection_id, e, data.len()
                        );
                    }
                }
            }
            ConnectionEvent::Disconnected(reason) => {
                let handler = Arc::clone(&self.handler);
                let manager = Arc::clone(&self.manager) as Arc<dyn ConnectionManagerTrait>;
                let conn_id = self.connection_id.clone();
                let device_manager = self.device_manager.clone();
                let event_handler = self.event_handler.clone();
                let reason_str = reason.clone();
                
                debug!("[DefaultObserver] Connection disconnected: {}", conn_id);
                tokio::spawn(async move {
                    // 1. 获取连接信息（包括 user_id）
                    let user_id = if let Some((_, conn_info)) = manager.get_connection(&conn_id).await {
                        conn_info.user_id
                    } else {
                        None
                    };
                    
                    // 2. 通知事件处理器
                    if let Some(ref event_handler) = event_handler {
                        let _ = event_handler.on_disconnect(&conn_id, Some(reason_str.as_str())).await;
                    }
                    
                    // 3. 通知连接处理器
                    let _ = handler.on_disconnect(&conn_id).await;
                    
                    // 4. 从连接管理器中移除连接
                    match manager.remove_connection(&conn_id).await {
                        Ok(_) => {
                            debug!("[DefaultObserver] Successfully removed connection: {}", conn_id);
                        }
                        Err(e) => {
                            debug!("[DefaultObserver] Connection {} already removed or not found: {}", conn_id, e);
                        }
                    }
                    
                    // 5. 从设备管理器中移除设备（如果有 user_id）
                    if let (Some(device_mgr), Some(user_id)) = (device_manager, user_id) {
                        if let Err(e) = device_mgr.remove_device(&user_id, &conn_id).await {
                            debug!("[DefaultObserver] Failed to remove device from DeviceManager: {}", e);
                        } else {
                            info!("[DefaultObserver] Successfully removed device from DeviceManager: user_id={}, connection_id={}", user_id, conn_id);
                        }
                    }
                });
            }
            ConnectionEvent::Connected => {
                // 连接已建立（在连接处理函数中已处理）
            }
            ConnectionEvent::Error(e) => {
                error!("[DefaultObserver] Connection error for {}: {:?}", self.connection_id, e);
                let handler = Arc::clone(&self.handler);
                let manager = Arc::clone(&self.manager) as Arc<dyn ConnectionManagerTrait>;
                let conn_id = self.connection_id.clone();
                let device_manager = self.device_manager.clone();
                let event_handler = self.event_handler.clone();
                let error_msg = format!("{:?}", e);
                
                debug!("[DefaultObserver] Connection error detected, removing connection: {}", conn_id);
                tokio::spawn(async move {
                    // 1. 获取连接信息（包括 user_id）
                    let user_id = if let Some((_, conn_info)) = manager.get_connection(&conn_id).await {
                        conn_info.user_id
                    } else {
                        None
                    };
                    
                    // 2. 通知事件处理器
                    if let Some(ref event_handler) = event_handler {
                        let _ = event_handler.on_error(&conn_id, &error_msg).await;
                    }
                    
                    // 3. 通知连接处理器
                    let _ = handler.on_disconnect(&conn_id).await;
                    
                    // 4. 从连接管理器中移除（如果连接存在）
                    match manager.remove_connection(&conn_id).await {
                        Ok(_) => {
                            debug!("[DefaultObserver] Successfully removed connection after error: {}", conn_id);
                        }
                        Err(e) => {
                            debug!("[DefaultObserver] Connection {} already removed or not found after error: {}", conn_id, e);
                        }
                    }
                    
                    // 5. 从设备管理器中移除设备（如果有 user_id）
                    if let (Some(device_mgr), Some(user_id)) = (device_manager, user_id) {
                        if let Err(e) = device_mgr.remove_device(&user_id, &conn_id).await {
                            debug!("[DefaultObserver] Failed to remove device from DeviceManager: {}", e);
                        } else {
                            info!("[DefaultObserver] Successfully removed device from DeviceManager: user_id={}, connection_id={}", user_id, conn_id);
                        }
                    }
                });
            }
        }
    }
}
