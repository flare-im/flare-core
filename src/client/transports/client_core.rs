//! 客户端核心功能
//! 
//! 提供统一的连接状态管理、心跳管理、消息路由等功能，简化客户端实现

use crate::client::connection::ConnectionStateManager;
use crate::client::heartbeat::HeartbeatManager;
use crate::client::router::MessageRouter;
use crate::common::MessageParser;
use crate::client::config::ClientConfig;
use crate::common::protocol::{Frame, connect, frame_with_system_command, Reliability};
use crate::common::protocol::flare::core::commands::command::Type;
use crate::common::error::Result;
use crate::transport::events::{ArcObserver, ConnectionEvent};
use crate::transport::connection::Connection;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex;
use std::collections::HashMap;

/// 客户端核心功能
/// 
/// 统一管理连接状态、心跳、消息路由，简化客户端实现
pub struct ClientCore {
    /// 连接状态管理器
    pub state_manager: Arc<ConnectionStateManager>,
    /// 消息解析器（使用 Arc<Mutex<>> 以支持协商后更新）
    pub parser: Arc<tokio::sync::Mutex<MessageParser>>,
    /// 心跳管理器（可选，通过配置开启）
    /// 使用 Arc<Mutex<>> 以支持并发访问（从同步的观察者中调用）
    heartbeat_manager: Option<Arc<tokio::sync::Mutex<HeartbeatManager>>>,
    /// 消息路由器（可选，通过配置开启）
    message_router: Option<MessageRouter>,
    /// 观察者列表
    pub observers: Arc<StdMutex<Vec<ArcObserver>>>,
    /// 客户端配置
    pub config: ClientConfig,
    /// 事件处理器（可选，用于自定义业务逻辑）
    event_handler: Option<Arc<dyn crate::client::events::handler::ClientEventHandler>>,
    /// 客户端连接（用于断开连接）
    client_connection: Option<Arc<Mutex<Box<dyn Connection>>>>,
}

impl ClientCore {
    /// 创建新的客户端核心
    pub fn new(config: &ClientConfig) -> Self {
        // 如果强制指定了格式，使用强制格式；否则使用默认 JSON（协商时使用首选格式）
        let format = if config.is_force_format() {
            config.get_serialization_format()
        } else {
            // 默认使用 JSON，等待协商后更新
            crate::common::protocol::SerializationFormat::Json
        };
        
        let compression = if config.is_force_format() {
            config.get_compression()
        } else {
            // 默认不压缩，等待协商后更新
            crate::common::compression::CompressionAlgorithm::None
        };
        
        let parser = MessageParser::new(format, compression);
        
        let message_router = if config.enable_router {
            Some(MessageRouter::new())
        } else {
            None
        };
        
        Self {
            state_manager: Arc::new(ConnectionStateManager::new()),
            parser: Arc::new(tokio::sync::Mutex::new(parser)),
            heartbeat_manager: None,
            message_router,
            observers: Arc::new(StdMutex::new(Vec::new())),
            config: config.clone(),
            event_handler: None,
            client_connection: None,
        }
    }
    
    /// 更新消息解析器（协商完成后调用）
    pub async fn update_parser(&self, format: crate::common::protocol::SerializationFormat, compression: crate::common::compression::CompressionAlgorithm) {
        let mut parser = self.parser.lock().await;
        *parser = MessageParser::new(format, compression);
        tracing::debug!(
            "[ClientCore] 协商完成，解析器已更新: 最终序列化方式={:?}, 最终压缩方式={:?}",
            format,
            compression
        );
    }
    
    /// 设置客户端连接（用于断开连接）
    pub fn set_client_connection(&mut self, connection: Arc<Mutex<Box<dyn Connection>>>) {
        self.client_connection = Some(connection);
    }
    
    /// 设置事件处理器
    pub fn set_event_handler(&mut self, handler: Option<Arc<dyn crate::client::events::handler::ClientEventHandler>>) {
        self.event_handler = handler;
    }
    
    /// 启动心跳（如果启用）
    pub async fn start_heartbeat(
        &mut self,
        connection: Arc<Mutex<Box<dyn Connection>>>,
    ) {
        // 保存连接引用（用于断开）
        self.client_connection = Some(Arc::clone(&connection));
        
        if !self.config.heartbeat.enabled {
            return;
        }
        
        let mut heartbeat = HeartbeatManager::new(
            self.config.heartbeat.interval,
            self.config.heartbeat.timeout,
        );
        
        // 获取当前 parser 的副本用于心跳
        let parser = self.parser.lock().await.clone();
        heartbeat.start(connection, parser);
        self.heartbeat_manager = Some(Arc::new(tokio::sync::Mutex::new(heartbeat)));
    }
    
    /// 停止心跳
    pub fn stop_heartbeat(&mut self) {
        if let Some(ref _heartbeat) = self.heartbeat_manager {
            // 需要获取锁来停止心跳
            // 但由于 stop_heartbeat 是 &mut self，我们可以直接 take
            if let Some(mut hb) = self.heartbeat_manager.take() {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::try_current()
                        .map(|handle| handle.block_on(async {
                            let mut hb_guard = hb.lock().await;
                            hb_guard.stop();
                        }))
                        .unwrap_or_else(|_| {
                            tokio::runtime::Runtime::new().unwrap().block_on(async {
                                let mut hb_guard = hb.lock().await;
                                hb_guard.stop();
                            })
                        })
                });
            }
        }
    }
    
    /// 处理接收到的消息
    /// 
    /// 如果启用了路由，使用路由处理；否则直接通知观察者
    pub async fn handle_message(&self, data: Vec<u8>) {
        // 解析消息（使用当前 parser）
        let parser = self.parser.lock().await;
        let frame = match parser.parse(&data) {
            Ok(frame) => frame,
            Err(e) => {
                tracing::warn!("Failed to parse message: {}", e);
                return;
            }
        };
        drop(parser); // 释放锁
        
        // 检查是否是系统命令
        if let Some(cmd) = &frame.command {
            if let Some(crate::common::protocol::flare::core::commands::command::Type::System(sys_cmd)) = &cmd.r#type {
            // 处理 CONNECT_ACK
            if sys_cmd.r#type == crate::common::protocol::flare::core::commands::system_command::Type::ConnectAck as i32 {
                // 如果有事件处理器，先调用它（让用户可以看到 CONNECT_ACK）
                if let Some(ref handler) = self.event_handler {
                    let _ = handler.handle_system_command(
                        crate::common::protocol::flare::core::commands::system_command::Type::ConnectAck,
                        &frame
                    ).await;
                }
                
                match self.handle_connect_ack(&frame) {
                    Ok((format, compression)) => {
                        tracing::info!(
                            "[ClientCore] ✅ 收到 CONNECT_ACK: 服务端确定的序列化方式={:?}, 压缩方式={:?}",
                            format,
                            compression
                        );
                        // 更新 parser 为协商后的格式（如果不是强制模式）
                        if !self.config.is_force_format() {
                            self.update_parser(format, compression).await;
                            tracing::info!(
                                "[ClientCore] ✅ 解析器已更新为协商后的格式: {:?}, 压缩: {:?}",
                                format,
                                compression
                            );
                        } else {
                            tracing::info!(
                                "[ClientCore] ℹ️  强制模式：继续使用客户端强制指定的格式: {:?}, 压缩: {:?}",
                                self.config.get_serialization_format(),
                                self.config.get_compression()
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to handle CONNECT_ACK: {}", e);
                    }
                }
                return; // CONNECT_ACK 不通知观察者
            }
            
            // 处理 KICKED（被踢下线）
            if sys_cmd.r#type == crate::common::protocol::flare::core::commands::system_command::Type::Kicked as i32 {
                let reason = sys_cmd.message.clone();
                tracing::warn!(
                    "[ClientCore] ⚠️  收到被踢消息: {}",
                    reason
                );
                
                // 解析被踢原因（从 metadata 中获取详细信息）
                let mut kick_reason = reason.clone();
                if let Some(reason_bytes) = sys_cmd.metadata.get("reason") {
                    if let Ok(reason_str) = String::from_utf8(reason_bytes.clone()) {
                        if reason_str == "device_conflict" {
                            kick_reason = format!("设备冲突：{}", reason);
                        }
                    }
                }
                
                // 如果有事件处理器，先调用它
                if let Some(ref handler) = self.event_handler {
                    if let Err(e) = handler.handle_system_command(
                        crate::common::protocol::flare::core::commands::system_command::Type::Kicked,
                        &frame
                    ).await {
                        tracing::warn!("[ClientCore] 事件处理器处理 KICKED 失败: {}", e);
                    }
                }
                
                // 更新连接状态为断开（被踢）
                self.state_manager.set_disconnected();
                
                // 主动断开连接
                if let Some(ref client_conn) = self.client_connection {
                    let mut conn = client_conn.lock().await;
                    if let Err(e) = conn.close().await {
                        tracing::error!("[ClientCore] 断开连接失败: {}", e);
                    } else {
                        tracing::info!("[ClientCore] ✅ 已主动断开连接（被踢）");
                    }
                } else {
                    tracing::warn!("[ClientCore] ⚠️  客户端连接未设置，无法主动断开");
                }
                
                // 通知观察者（被踢事件）
                if let Ok(observers) = self.observers.lock() {
                    for observer in observers.iter() {
                        observer.on_event(&crate::transport::events::ConnectionEvent::Disconnected(
                            kick_reason.clone()
                        ));
                    }
                }
                
                tracing::info!(
                    "[ClientCore] 连接已断开（被踢）: {}",
                    kick_reason
                );
                return; // KICKED 消息已处理，不继续通知观察者
            }
                
                // 处理 PONG（心跳响应）
                if sys_cmd.r#type == crate::common::protocol::flare::core::commands::system_command::Type::Pong as i32 {
                    // 如果有事件处理器，先调用它（让用户可以看到 PONG）
                    if let Some(ref handler) = self.event_handler {
                        let _ = handler.handle_system_command(
                            crate::common::protocol::flare::core::commands::system_command::Type::Pong,
                            &frame
                        ).await;
                    }
                    
                    // 记录 PONG，更新心跳
                    self.record_pong();
                    return; // PONG 不通知观察者
                }
            }
        }
        
        // 如果有事件处理器，调用它处理消息命令和通知命令
        // 注意：系统命令（CONNECT_ACK, PONG, KICKED）已经在上面处理时调用了事件处理器
        if let Some(ref handler) = self.event_handler {
            if let Some(cmd) = &frame.command {
                match &cmd.r#type {
                    Some(Type::Message(msg_cmd)) => {
                        if let Ok(cmd_type) = crate::common::protocol::flare::core::commands::message_command::Type::try_from(msg_cmd.r#type) {
                            let _ = handler.handle_message_command(cmd_type, &frame).await;
                        }
                    }
                    Some(Type::Notification(notif_cmd)) => {
                        if let Ok(cmd_type) = crate::common::protocol::flare::core::commands::notification_command::Type::try_from(notif_cmd.r#type) {
                            let _ = handler.handle_notification_command(cmd_type, &frame).await;
                        }
                    }
                    _ => {}
                }
            }
        }
        
        // 如果启用了路由，使用路由处理
        if let Some(ref router) = self.message_router {
            match router.route(&frame).await {
                Ok(replies) => {
                    // 发送回复（如果需要）
                    // 注意：这里需要连接实例来发送，但 ClientCore 不持有连接
                    // 回复应该通过客户端发送，这里只是路由处理
                    tracing::debug!("Router generated {} replies", replies.len());
                }
                Err(e) => {
                    tracing::warn!("Router error: {}", e);
                }
            }
        }
        
        // 通知所有观察者
        self.notify_observers(&ConnectionEvent::Message(data));
    }
    
    /// 处理连接事件
    pub fn handle_connection_event(&self, event: &ConnectionEvent) {
        // 如果有事件处理器，先调用它
        if let Some(ref handler) = self.event_handler {
            let handler_clone = Arc::clone(handler);
            let event_clone = event.clone();
            tokio::spawn(async move {
                let _ = handler_clone.handle_connection_event(&event_clone).await;
            });
        }
        
        match event {
            ConnectionEvent::Connected => {
                self.state_manager.set_connected();
            }
            ConnectionEvent::Disconnected(_) => {
                self.state_manager.set_disconnected();
            }
            ConnectionEvent::Error(_) => {
                self.state_manager.set_failed();
            }
            ConnectionEvent::Message(_) => {
                // 消息处理在 handle_message 中完成
            }
        }
        
        self.notify_observers(event);
    }
    
    /// 添加观察者
    pub fn add_observer(&self, observer: ArcObserver) {
        if let Ok(mut observers) = self.observers.lock() {
            observers.push(observer);
        }
    }
    
    /// 移除观察者
    pub fn remove_observer(&self, observer: ArcObserver) {
        if let Ok(mut observers) = self.observers.lock() {
            observers.retain(|o| !Arc::ptr_eq(o, &observer));
        }
    }
    
    /// 通知所有观察者
    fn notify_observers(&self, event: &ConnectionEvent) {
        if let Ok(observers) = self.observers.lock() {
            for observer in observers.iter() {
                observer.on_event(event);
            }
        }
    }
    
    /// 获取消息路由器（如果启用）
    pub fn router_mut(&mut self) -> Option<&mut MessageRouter> {
        self.message_router.as_mut()
    }
    
    /// 获取消息路由器（只读）
    pub fn router(&self) -> Option<&MessageRouter> {
        self.message_router.as_ref()
    }
    
    /// 获取连接状态
    pub fn state(&self) -> crate::client::connection::ConnectionState {
        self.state_manager.get_state()
    }
    
    /// 检查是否可以发送消息
    pub fn can_send(&self) -> bool {
        self.state_manager.get_state().can_send()
    }
    
    /// 检查是否可以连接
    pub fn can_connect(&self) -> bool {
        self.state_manager.get_state().can_connect()
    }
    
    /// 记录收到 PONG（心跳响应）
    /// 
    /// 由消息观察者调用，用于更新心跳状态
    /// 
    /// 注意：由于观察者是同步的，我们需要异步获取锁
    pub fn record_pong(&self) {
        if let Some(ref heartbeat) = self.heartbeat_manager {
            // HeartbeatManager::record_pong 是 `&self` 方法
            // 但由于我们使用了 Arc<Mutex<>>，需要先获取锁
            // 由于这是从同步上下文调用，使用 block_in_place
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::try_current()
                    .map(|handle| {
                        handle.block_on(async {
                            let hb_guard = heartbeat.lock().await;
                            hb_guard.record_pong();
                        })
                    })
                    .unwrap_or_else(|_| {
                        tokio::runtime::Runtime::new().unwrap().block_on(async {
                            let hb_guard = heartbeat.lock().await;
                            hb_guard.record_pong();
                        })
                    })
            });
        }
    }
    
    /// 发送 CONNECT 消息进行协商
    /// 
    /// # 参数
    /// - `connection`: 连接实例
    /// 
    /// # 返回
    /// 发送成功返回 `Ok(())`，失败返回错误
    pub async fn send_connect_message(&self, connection: Arc<Mutex<Box<dyn Connection>>>) -> Result<()> {
        let mut metadata = HashMap::new();
        
        // 确定要使用的序列化格式和压缩算法
        let (format, compression) = if self.config.is_force_format() {
            // 强制模式：直接使用强制格式
            (self.config.get_serialization_format(), self.config.get_compression())
        } else {
            // 协商模式：CONNECT 消息使用首选格式，但发送时使用默认 JSON
            (self.config.serialization_format, self.config.compression)
        };
        
        tracing::debug!(
            "[ClientCore] 发送 CONNECT 消息: 请求序列化方式={:?}, 请求压缩方式={:?}, 强制模式={}",
            format,
            compression,
            self.config.is_force_format()
        );
        
        // 添加序列化格式（客户端希望使用的格式）
        let format_str = match format {
            crate::common::protocol::SerializationFormat::Protobuf => "protobuf",
            crate::common::protocol::SerializationFormat::Json => "json",
        };
        metadata.insert("format".to_string(), format_str.as_bytes().to_vec());
        
        // 添加压缩算法（客户端希望使用的算法）
        metadata.insert("compression".to_string(), compression.as_str().as_bytes().to_vec());
        
        // 添加是否强制指定格式的标记
        if self.config.is_force_format() {
            metadata.insert("force_format".to_string(), "true".as_bytes().to_vec());
        }
        
        // 添加设备信息（如果提供）
        if let Some(ref device_info) = self.config.device_info {
            metadata.insert("device_id".to_string(), device_info.device_id.as_bytes().to_vec());
            metadata.insert("platform".to_string(), device_info.platform.as_str().as_bytes().to_vec());
            
            if let Some(ref model) = device_info.model {
                metadata.insert("model".to_string(), model.as_bytes().to_vec());
            }
            if let Some(ref app_version) = device_info.app_version {
                metadata.insert("app_version".to_string(), app_version.as_bytes().to_vec());
            }
            if let Some(ref system_version) = device_info.system_version {
                metadata.insert("system_version".to_string(), system_version.as_bytes().to_vec());
            }
            
            // 添加其他元数据
            for (key, value) in &device_info.metadata {
                metadata.insert(key.clone(), value.as_bytes().to_vec());
            }
        }
        
        // 添加用户 ID（如果提供）
        if let Some(ref user_id) = self.config.user_id {
            metadata.insert("user_id".to_string(), user_id.as_bytes().to_vec());
        }
        
        // 添加 token（如果提供，用于认证）
        if let Some(ref token) = self.config.token {
            metadata.insert("token".to_string(), token.as_bytes().to_vec());
            tracing::debug!("[ClientCore] 已添加 token 到 CONNECT 消息元数据");
        }
        
        // 添加其他元数据
        for (key, value) in &self.config.metadata {
            metadata.insert(key.clone(), value.as_bytes().to_vec());
        }
        
        // 创建 CONNECT 命令（使用客户端希望使用的格式）
        let connect_cmd = connect(format, metadata);
        let connect_frame = frame_with_system_command(connect_cmd, Reliability::AtLeastOnce);
        
        // 序列化并发送
        // 如果是强制模式，使用强制格式的 parser；否则使用默认 JSON parser
        let data = if self.config.is_force_format() {
            let parser = self.parser.lock().await;
            parser.serialize(&connect_frame)?
        } else {
            // 协商模式：使用默认 JSON parser 发送 CONNECT 消息
            MessageParser::json().serialize(&connect_frame)?
        };
        let mut conn = connection.lock().await;
        conn.send(&data).await?;
        
        if self.config.is_force_format() {
            tracing::debug!("[ClientCore] CONNECT 消息已发送（强制模式: format={:?}, compression={:?}）", format, compression);
        } else {
            tracing::debug!("[ClientCore] CONNECT 消息已发送（协商模式: 首选 format={:?}, compression={:?}）", format, compression);
        }
        Ok(())
    }
    
    /// 处理 CONNECT_ACK 消息
    /// 
    /// 解析服务器返回的 CONNECT_ACK，确认协商结果
    /// 
    /// # 返回
    /// 协商结果：(序列化格式, 压缩算法)
    pub fn handle_connect_ack(&self, frame: &Frame) -> Result<(crate::common::protocol::SerializationFormat, crate::common::compression::CompressionAlgorithm)> {
        if let Some(cmd) = &frame.command {
            if let Some(crate::common::protocol::flare::core::commands::command::Type::System(sys_cmd)) = &cmd.r#type {
                // 使用 TryFrom 替代已弃用的 from_i32
                use std::convert::TryFrom;
                let cmd_type = crate::common::protocol::flare::core::commands::system_command::Type::try_from(sys_cmd.r#type)
                    .map_err(|_| crate::common::error::FlareError::protocol_error("Invalid system command type".to_string()))?;
                
                if cmd_type == crate::common::protocol::flare::core::commands::system_command::Type::ConnectAck {
                    // 解析协商结果（使用 TryFrom 替代已弃用的 from_i32）
                    let format = crate::common::protocol::SerializationFormat::try_from(sys_cmd.format)
                        .unwrap_or(crate::common::protocol::SerializationFormat::Json);
                    
                    // 从 metadata 中解析压缩算法
                    let compression = if let Some(compression_bytes) = sys_cmd.metadata.get("compression") {
                        if let Ok(compression_str) = String::from_utf8(compression_bytes.clone()) {
                            crate::common::compression::CompressionAlgorithm::from_str(&compression_str)
                                .unwrap_or(crate::common::compression::CompressionAlgorithm::None)
                        } else {
                            crate::common::compression::CompressionAlgorithm::None
                        }
                    } else {
                        crate::common::compression::CompressionAlgorithm::None
                    };
                    
                    tracing::debug!("[ClientCore] 收到 CONNECT_ACK，协商结果: format={:?}, compression={:?}", format, compression);
                    
                    // 检查是否有冲突连接通知
                    if let Some(conflicts_bytes) = sys_cmd.metadata.get("conflict_connections") {
                        if let Ok(conflicts_json) = String::from_utf8(conflicts_bytes.clone()) {
                            if let Ok(conflict_connections) = serde_json::from_str::<Vec<String>>(&conflicts_json) {
                                if !conflict_connections.is_empty() {
                                    tracing::warn!("[ClientCore] 检测到设备冲突，以下连接被踢掉: {:?}", conflict_connections);
                                }
                            }
                        }
                    }
                    
                    return Ok((format, compression));
                }
            }
        }
        
        Err(crate::common::error::FlareError::protocol_error(
            "Not a CONNECT_ACK message".to_string()
        ))
    }
}

// 为 ClientCore 实现 Clone（用于共享状态管理器和观察者）
impl Clone for ClientCore {
    fn clone(&self) -> Self {
        Self {
            state_manager: Arc::clone(&self.state_manager),
            parser: Arc::clone(&self.parser), // Arc 可以安全克隆
            heartbeat_manager: None, // 心跳管理器不克隆，由主实例管理
            message_router: self.message_router.as_ref().map(|_| MessageRouter::new()), // 路由不克隆，创建新的
            observers: Arc::clone(&self.observers),
            config: self.config.clone(),
            event_handler: self.event_handler.clone(), // 事件处理器可以共享
            client_connection: None, // 连接不克隆，每个实例独立
        }
    }
}

