//! WebSocket 服务端实现
//! 
//! 专注于 WebSocket 协议层面的连接处理，连接管理和心跳检测由 ServerCore 统一管理

use crate::server::config::ServerConfig;
use tracing::{debug, error};
use crate::server::connection::{ConnectionManager, ConnectionManagerTrait};
use crate::common::error::Result;
use crate::common::protocol::{Frame, pong};
use crate::server::transports::{Server, ConnectionHandler};
use crate::server::transports::server_core::ServerCore;
use crate::server::handle::ServerHandle;
use crate::common::{generate_id};
use crate::transport::connection::Connection;
use crate::transport::events::ConnectionEvent;
use crate::transport::websocket::WebSocketTransport;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::accept_async;

/// WebSocket 服务端
/// 
/// 专注于 WebSocket 协议层面的连接处理
pub struct WebSocketServer {
    config: ServerConfig,
    core: ServerCore,
    handler: Arc<dyn ConnectionHandler>,
    listener: Option<TcpListener>,
    is_running: Arc<Mutex<bool>>,
}

impl WebSocketServer {
    /// 创建新的 WebSocket 服务端
    pub fn new(config: ServerConfig, handler: Arc<dyn ConnectionHandler>) -> Self {
        Self::with_server_core(config, handler, None)
    }
    
    /// 使用指定的 ServerCore 创建 WebSocket 服务端
    /// 
    /// # 参数
    /// - `config`: 服务端配置
    /// - `handler`: 连接处理器
    /// - `connection_manager`: 可选的连接管理器，如果为 None，则由 ServerCore 创建新的
    pub fn with_connection_manager(
        config: ServerConfig,
        handler: Arc<dyn ConnectionHandler>,
        connection_manager: Option<Arc<ConnectionManager>>,
    ) -> Self {
        Self::with_server_core(config, handler, connection_manager)
    }
    
    /// 使用指定的连接管理器创建 WebSocket 服务端（内部方法）
    fn with_server_core(
        config: ServerConfig,
        handler: Arc<dyn ConnectionHandler>,
        connection_manager: Option<Arc<ConnectionManager>>,
    ) -> Self {
        let core = ServerCore::new(&config, connection_manager);
        
        Self {
            config,
            core,
            handler,
            listener: None,
            is_running: Arc::new(Mutex::new(false)),
        }
    }
    
    /// 获取 ServerCore（用于内部访问）
    fn core(&self) -> &ServerCore {
        &self.core
    }
}

#[async_trait]
impl Server for WebSocketServer {
    async fn start(&mut self) -> Result<()> {
        let addr = self.config.bind_address.parse::<std::net::SocketAddr>()
            .map_err(|e| crate::common::error::FlareError::protocol_error(format!("Invalid address: {}", e)))?;
        
        let listener = TcpListener::bind(addr).await
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("Failed to bind: {}", e)))?;
        
        *self.is_running.lock().await = true;
        
        // 启动心跳检测（由 ServerCore 统一管理）
        self.core.start_heartbeat(&self.config);
        
        // 注意：listener 将被 move 到闭包中，所以不能存储到 self.listener
        // 我们需要在闭包中使用它，但不能同时存储它
        // 解决方案：不存储 listener，只在闭包中使用
        let handler = Arc::clone(&self.handler);
        let manager = Arc::clone(&self.core.connection_manager);
        let parser = self.core.parser.clone();
        let config = self.config.clone();
        let is_running = Arc::clone(&self.is_running);
        
        tokio::spawn(async move {
            debug!("[DEBUG WebSocketServer] 开始监听连接");
            while *is_running.lock().await {
                match listener.accept().await {
                    Ok((stream, _addr)) => {
                        debug!("[DEBUG WebSocketServer] 收到新连接");
                        let handler_clone = Arc::clone(&handler);
                        let manager_clone = Arc::clone(&manager);
                        let parser_clone = parser.clone();
                        let config_clone = config.clone();
                        tokio::spawn(async move {
                            debug!("[DEBUG WebSocketServer] 连接处理任务开始");
                            handle_websocket_connection(
                                stream,
                                handler_clone,
                                manager_clone,
                                parser_clone,
                                config_clone,
                            ).await;
                            debug!("[DEBUG WebSocketServer] 连接处理任务结束");
                        });
                    }
                    Err(e) => {
                        debug!("Failed to accept connection: {}", e);
                    }
                }
            }
            debug!("[DEBUG WebSocketServer] 停止监听连接");
        });
        
        // listener 已被 move，将其存储为 None（实际上不需要存储，但为了保持类型一致）
        self.listener = None;
        
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        *self.is_running.lock().await = false;
        
        // 停止心跳检测（由 ServerCore 统一管理）
        self.core.stop_heartbeat();
        
        // 断开所有连接（通过 ServerHandle）
        let connection_ids = self.core.list_connections().await;
        for conn_id in connection_ids {
            // 先关闭连接
            let manager_trait = self.core.connection_manager_trait();
            if let Some((conn, _)) = manager_trait.get_connection(&conn_id).await {
                let mut c = conn.lock().await;
                let _ = c.close().await;
            }
            // 然后从连接管理器中移除
            let _ = ServerHandle::disconnect(&self.core, &conn_id).await;
        }
        
        Ok(())
    }

    fn is_running(&self) -> bool {
        debug!("[DEBUG WebSocketServer] is_running 开始");
        let result = tokio::task::block_in_place(|| {
            debug!("[DEBUG WebSocketServer] is_running: block_in_place 内部，获取 blocking_lock");
            let guard = self.is_running.blocking_lock();
            debug!("[DEBUG WebSocketServer] is_running: blocking_lock 已获取");
            let result = *guard;
            debug!("[DEBUG WebSocketServer] is_running: 值 = {}", result);
            result
        });
        debug!("[DEBUG WebSocketServer] is_running 返回: {}", result);
        result
    }
}

async fn handle_websocket_connection(
    stream: TcpStream,
    handler: Arc<dyn ConnectionHandler>,
    manager: Arc<ConnectionManager>,
    parser: crate::common::MessageParser,
    config: ServerConfig,
) {
    // WebSocket 服务端：不使用 TLS
    // accept_async 直接接受 TcpStream（无 TLS），返回 WebSocketStream<TcpStream>
    // 使用 WebSocketTransport::from_tcp_stream 来创建 transport
    let ws_stream_plain = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            debug!("WebSocket handshake failed: {}", e);
            return;
        }
    };
    
    let transport = WebSocketTransport::from_tcp_stream(ws_stream_plain);
    let connection: Box<dyn Connection> = Box::new(transport);
    let connection_id = generate_id();
    
    // 检查连接数限制
    debug!("[DEBUG WebSocketServer] handle_websocket_connection: 检查连接数限制, connection_id={}", connection_id);
    if manager.connection_count() >= config.max_connections {
        debug!("Connection limit exceeded: {}", config.max_connections);
        return;
    }
    debug!("[DEBUG WebSocketServer] handle_websocket_connection: 连接数检查通过");
    
    // 添加连接
    debug!("[DEBUG WebSocketServer] handle_websocket_connection: 准备添加连接");
    if let Err(e) = manager.add_connection(connection_id.clone(), connection, None) {
        debug!("Failed to add connection: {}", e);
        return;
    }
    debug!("[DEBUG WebSocketServer] handle_websocket_connection: 连接已添加到管理器");

    // 通知连接建立
    debug!("[DEBUG WebSocketServer] 准备调用 handler.on_connect, connection_id={}", connection_id);
    if let Err(e) = handler.on_connect(&connection_id).await {
        debug!("Handler on_connect error: {}", e);
    } else {
        debug!("[DEBUG WebSocketServer] handler.on_connect 成功返回");
    }

    // 创建消息观察者
    let handler_clone = Arc::clone(&handler);
    let manager_clone = Arc::clone(&manager);
    let parser_clone = parser.clone();
    let conn_id_clone = connection_id.clone();
    let config_clone = config.clone();
    
    let observer = Arc::new(ServerMessageObserver {
        handler: handler_clone,
        manager: manager_clone,
        parser: parser_clone,
        connection_id: conn_id_clone.clone(),
        config: config_clone,
    });

    // 获取连接并添加观察者
    debug!("[DEBUG WebSocketServer] handle_websocket_connection: 准备获取连接并添加观察者");
    if let Some((conn, _)) = manager.get_connection(&connection_id) {
        debug!("[DEBUG WebSocketServer] handle_websocket_connection: 连接已获取");
        {
            debug!("[DEBUG WebSocketServer] handle_websocket_connection: 获取连接锁");
            let mut c = conn.lock().await;
            debug!("[DEBUG WebSocketServer] handle_websocket_connection: 连接锁已获取，添加观察者");
            c.add_observer(observer);
            debug!("[DEBUG WebSocketServer] handle_websocket_connection: 观察者已添加");
            
            // 发送 CONNECT_ACK
            debug!("[DEBUG WebSocketServer] handle_websocket_connection: 准备发送 CONNECT_ACK");
            let mut metadata = HashMap::new();
            let format_bytes = format!("{:?}", config.default_serialization_format).into_bytes();
            metadata.insert("format".to_string(), format_bytes);
            let connect_ack_cmd = crate::common::protocol::connect_ack(config.default_serialization_format, metadata);
            let connect_ack_frame = crate::common::protocol::frame_with_system_command(
                connect_ack_cmd,
                crate::common::protocol::Reliability::AtLeastOnce,
            );
            if let Ok(data) = parser.serialize(&connect_ack_frame) {
                debug!("[DEBUG WebSocketServer] handle_websocket_connection: CONNECT_ACK 序列化成功，准备发送");
                let _ = c.send(&data).await;
                debug!("[DEBUG WebSocketServer] handle_websocket_connection: CONNECT_ACK 已发送");
            } else {
                debug!("[DEBUG WebSocketServer] handle_websocket_connection: CONNECT_ACK 序列化失败");
            }
        }
        debug!("[DEBUG WebSocketServer] handle_websocket_connection: 连接锁已释放");

        // 服务端不需要主动发送心跳，只需要检测超时
        // 心跳检测由 HeartbeatDetector 统一处理
    } else {
        debug!("[DEBUG WebSocketServer] handle_websocket_connection: 警告：无法获取连接，connection_id={}", connection_id);
    }
    debug!("[DEBUG WebSocketServer] handle_websocket_connection: 连接处理完成, connection_id={}", connection_id);
    debug!("[DEBUG WebSocketServer] handle_websocket_connection: 函数即将返回, connection_id={}", connection_id);
    
    // 注意：定期清理任务应该在服务器启动时创建一次，而不是为每个连接创建
    // 这里不再创建清理任务，避免资源泄漏
    // 函数返回后，连接处理任务继续在后台运行
}

struct ServerMessageObserver {
    handler: Arc<dyn ConnectionHandler>,
    manager: Arc<ConnectionManager>,
    parser: crate::common::MessageParser,
    connection_id: String,
    config: ServerConfig,
}

impl crate::transport::events::ConnectionObserver for ServerMessageObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
                if let Ok(frame) = self.parser.parse(data) {
                    // 处理 PING 消息
                    if let Some(cmd) = &frame.command {
                        if let Some(crate::common::protocol::flare::core::commands::command::Type::System(sys_cmd)) = &cmd.r#type {
                            if sys_cmd.r#type == crate::common::protocol::flare::core::commands::system_command::Type::Ping as i32 {
                                // 收到 PING，回复 PONG 并更新连接活跃时间
                                let manager = Arc::clone(&self.manager) as Arc<dyn ConnectionManagerTrait>;
                                let conn_id = self.connection_id.clone();
                                
                                // 更新连接活跃时间（通过 trait 的异步方法）
                                let manager_update = Arc::clone(&manager);
                                let conn_id_update = conn_id.clone();
                                tokio::spawn(async move {
                                    let _ = manager_update.update_connection_active(&conn_id_update).await;
                                });
                                
                                // 回复 PONG
                                let pong_cmd = pong();
                                let pong_frame = crate::common::protocol::frame_with_system_command(
                                    pong_cmd,
                                    crate::common::protocol::Reliability::AtLeastOnce,
                                );
                                if let Ok(pong_data) = self.parser.serialize(&pong_frame) {
                                    let manager_get = Arc::clone(&manager);
                                    tokio::spawn(async move {
                                        if let Some((conn, _)) = manager_get.get_connection(&conn_id).await {
                                            let conn_clone = Arc::clone(&conn);
                                            let mut c = conn_clone.lock().await;
                                            let _ = c.send(&pong_data).await;
                                        }
                                    });
                                }
                                return;
                            }
                            if sys_cmd.r#type == crate::common::protocol::flare::core::commands::system_command::Type::Pong as i32 {
                                // 收到 PONG，更新连接活跃时间
                                let manager = Arc::clone(&self.manager) as Arc<dyn ConnectionManagerTrait>;
                                let conn_id = self.connection_id.clone();
                                tokio::spawn(async move {
                                    let _ = manager.update_connection_active(&conn_id).await;
                                });
                                return;
                            }
                        }
                    }

                    // 处理其他消息 - 更新连接活跃时间
                    let handler = Arc::clone(&self.handler);
                    let manager = Arc::clone(&self.manager);
                    let parser = self.parser.clone();
                    let conn_id = self.connection_id.clone();
                    
                    // 更新连接活跃时间（收到任何消息都算活跃）
                    let manager_update = Arc::clone(&manager) as Arc<dyn ConnectionManagerTrait>;
                    let conn_id_update = conn_id.clone();
                    tokio::spawn(async move {
                        let _ = manager_update.update_connection_active(&conn_id_update).await;
                    });
                    
                    tokio::spawn(async move {
                        if let Ok(Some(response)) = handler.handle_frame(&frame, &conn_id).await {
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
                        // 连接活跃时间已在收到消息时更新
                    });
                }
            }
            ConnectionEvent::Disconnected(_) => {
                let handler = Arc::clone(&self.handler);
                let manager = Arc::clone(&self.manager) as Arc<dyn ConnectionManagerTrait>;
                let conn_id = self.connection_id.clone();
                
                debug!("[WebSocketServer] Connection disconnected: {}", conn_id);
                tokio::spawn(async move {
                    // 通知连接断开
                    let _ = handler.on_disconnect(&conn_id).await;
                    // 立即从连接管理器中移除连接
                    match manager.remove_connection(&conn_id).await {
                        Ok(_) => {
                            debug!("[WebSocketServer] Successfully removed connection: {}", conn_id);
                        }
                        Err(e) => {
                            debug!("[WebSocketServer] Connection {} already removed or not found: {}", conn_id, e);
                        }
                    }
                });
            }
            ConnectionEvent::Connected => {
                // 连接已建立（在 handle_websocket_connection 中已处理）
            }
            ConnectionEvent::Error(e) => {
                error!("Connection error for {}: {:?}", self.connection_id, e);
                // 连接出错时，立即从管理器中移除连接（避免连接一直存在）
                let handler = Arc::clone(&self.handler);
                let manager = Arc::clone(&self.manager) as Arc<dyn ConnectionManagerTrait>;
                let conn_id = self.connection_id.clone();
                
                debug!("[WebSocketServer] Connection error detected, removing connection: {}", conn_id);
                tokio::spawn(async move {
                    // 通知连接断开
                    let _ = handler.on_disconnect(&conn_id).await;
                    // 从连接管理器中移除（如果连接存在）
                    match manager.remove_connection(&conn_id).await {
                        Ok(_) => {
                            debug!("[WebSocketServer] Successfully removed connection after error: {}", conn_id);
                        }
                        Err(e) => {
                            debug!("[WebSocketServer] Connection {} already removed or not found after error: {}", conn_id, e);
                        }
                    }
                });
            }
        }
    }
}
