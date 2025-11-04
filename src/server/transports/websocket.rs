//! WebSocket 服务端实现

use crate::server::config::ServerConfig;
use tracing::{debug, error};
use crate::server::connection::{ConnectionManager, ConnectionManagerTrait};
use crate::common::error::Result;
// 服务端不再使用 HeartbeatManager，改用 HeartbeatDetector 和 ConnectionManager 的更新机制
use crate::common::MessageParser;
use crate::common::protocol::{Frame, pong};
use crate::server::transports::{Server, ConnectionHandler};
use crate::common::{generate_id};
use crate::transport::connection::Connection;
use crate::transport::events::ConnectionEvent;
use crate::transport::websocket::WebSocketTransport;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::accept_async;

/// WebSocket 服务端
pub struct WebSocketServer {
    config: ServerConfig,
    connection_manager: Arc<ConnectionManager>,
    handler: Arc<dyn ConnectionHandler>,
    parser: MessageParser,
    listener: Option<TcpListener>,
    is_running: Arc<Mutex<bool>>,
    heartbeat_detector: Option<crate::server::heartbeat::HeartbeatDetector>,
}

impl WebSocketServer {
    /// 创建新的 WebSocket 服务端
    pub fn new(config: ServerConfig, handler: Arc<dyn ConnectionHandler>) -> Self {
        let parser = MessageParser::new(config.default_serialization_format, config.default_compression);
        
        Self {
            config,
            connection_manager: Arc::new(ConnectionManager::new()),
            handler,
            parser,
            listener: None,
            is_running: Arc::new(Mutex::new(false)),
            heartbeat_detector: None,
        }
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
        
        // 启动心跳检测器
        let manager_trait = Arc::clone(&self.connection_manager) as Arc<dyn ConnectionManagerTrait>;
        let timeout = self.config.connection_timeout;
        let check_interval = Duration::from_secs(timeout.as_secs() / 3).max(Duration::from_secs(10));
        let mut detector = crate::server::heartbeat::HeartbeatDetector::new(
            manager_trait,
            timeout,
            check_interval,
        );
        detector.start();
        self.heartbeat_detector = Some(detector);
        
        // 注意：listener 将被 move 到闭包中，所以不能存储到 self.listener
        // 我们需要在闭包中使用它，但不能同时存储它
        // 解决方案：不存储 listener，只在闭包中使用
        let handler = Arc::clone(&self.handler);
        let manager = Arc::clone(&self.connection_manager);
        let parser = self.parser.clone();
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
        
        // 断开所有连接
        let connection_ids = self.connection_manager.list_connections();
        for conn_id in connection_ids {
            let _ = self.disconnect(&conn_id).await;
        }
        
        Ok(())
    }

    async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        let manager_trait = Arc::clone(&self.connection_manager) as Arc<dyn ConnectionManagerTrait>;
        let (conn, _) = manager_trait.get_connection(connection_id).await
            .ok_or_else(|| crate::common::error::FlareError::protocol_error(format!("Connection {} not found", connection_id)))?;
        
        let data = self.parser.serialize(frame)?;
        
        // 使用 tokio::sync::Mutex，支持跨 await
        let mut c = conn.lock().await;
        c.send(&data).await?;
        
        let _ = manager_trait.update_connection_active(connection_id).await;
        Ok(())
    }

    async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        let connection_ids = self.connection_manager.get_user_connections(user_id);
        for conn_id in connection_ids {
            let _ = self.send_to(&conn_id, frame).await;
        }
        Ok(())
    }

    async fn broadcast(&self, frame: &Frame) -> Result<()> {
        let connection_ids = self.connection_manager.list_connections();
        for conn_id in connection_ids {
            let _ = self.send_to(&conn_id, frame).await;
        }
        Ok(())
    }
    
    async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        let connection_ids = self.connection_manager.list_connections();
        for conn_id in connection_ids {
            if conn_id != exclude_connection_id {
                let _ = self.send_to(&conn_id, frame).await;
            }
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

    fn connection_count(&self) -> usize {
        self.connection_manager.connection_count()
    }

    fn user_count(&self) -> usize {
        self.connection_manager.stats().total_users
    }

    async fn disconnect(&self, connection_id: &str) -> Result<()> {
        // 心跳检测由 HeartbeatDetector 统一管理，不需要手动停止
        let manager_trait = Arc::clone(&self.connection_manager) as Arc<dyn ConnectionManagerTrait>;
        
        if let Some((conn, _)) = manager_trait.get_connection(connection_id).await {
            let mut c = conn.lock().await;
            let _ = c.close().await;
        }
        let _ = manager_trait.remove_connection(connection_id).await;
        Ok(())
    }
}

async fn handle_websocket_connection(
    stream: TcpStream,
    handler: Arc<dyn ConnectionHandler>,
    manager: Arc<ConnectionManager>,
    parser: MessageParser,
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
    parser: MessageParser,
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
                
                tokio::spawn(async move {
                    // 心跳检测由 HeartbeatDetector 统一管理，不需要手动停止
                    let _ = handler.on_disconnect(&conn_id).await;
                    let _ = manager.remove_connection(&conn_id).await;
                });
            }
            ConnectionEvent::Connected => {
                // 连接已建立（在 handle_websocket_connection 中已处理）
            }
            ConnectionEvent::Error(e) => {
                error!("Connection error for {}: {:?}", self.connection_id, e);
            }
        }
    }
}
