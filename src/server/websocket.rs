//! WebSocket 服务端实现

use crate::common::config::ServerConfig;
use tracing::{debug, error};
use crate::common::connection_manager::ConnectionManager;
use crate::common::error::Result;
use crate::common::heartbeat::HeartbeatManager;
use crate::common::message_parser::MessageParser;
use crate::common::protocol::{Frame, pong};
use crate::common::server_trait::{Server, ConnectionHandler};
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
    heartbeat_managers: Arc<Mutex<HashMap<String, HeartbeatManager>>>,
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
            heartbeat_managers: Arc::new(Mutex::new(HashMap::new())),
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
        // 注意：listener 将被 move 到闭包中，所以不能存储到 self.listener
        // 我们需要在闭包中使用它，但不能同时存储它
        // 解决方案：不存储 listener，只在闭包中使用
        let handler = Arc::clone(&self.handler);
        let manager = Arc::clone(&self.connection_manager);
        let parser = self.parser.clone();
        let config = self.config.clone();
        let is_running = Arc::clone(&self.is_running);
        let heartbeat_managers = Arc::clone(&self.heartbeat_managers);
        
        // 启动定期清理任务（只创建一次）
        let manager_for_cleanup = Arc::clone(&manager);
        let config_for_cleanup = config.clone();
        tokio::spawn(async move {
            let mut cleanup_interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                cleanup_interval.tick().await;
                let timeout_conns = manager_for_cleanup.cleanup_timeout_connections(config_for_cleanup.connection_timeout);
                if !timeout_conns.is_empty() {
                    debug!("Cleaned up {} timeout connections", timeout_conns.len());
                }
            }
        });
        
        let heartbeat_managers_for_spawn = Arc::clone(&heartbeat_managers);
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
                        let heartbeat_managers_clone = Arc::clone(&heartbeat_managers_for_spawn);
                        tokio::spawn(async move {
                            debug!("[DEBUG WebSocketServer] 连接处理任务开始");
                            handle_websocket_connection(
                                stream,
                                handler_clone,
                                manager_clone,
                                parser_clone,
                                config_clone,
                                heartbeat_managers_clone,
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
        let (conn, _) = self.connection_manager.get_connection(connection_id)
            .ok_or_else(|| crate::common::error::FlareError::protocol_error(format!("Connection {} not found", connection_id)))?;
        
        let data = self.parser.serialize(frame)?;
        
        // 使用 tokio::sync::Mutex，支持跨 await
        let mut c = conn.lock().await;
        c.send(&data).await?;
        
        self.connection_manager.update_connection_active(connection_id)?;
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
        // 停止心跳
        {
            let mut hb_managers = self.heartbeat_managers.lock().await;
            if let Some(mut hb) = hb_managers.remove(connection_id) {
                hb.stop();
            }
        }

        if let Some((conn, _)) = self.connection_manager.get_connection(connection_id) {
            let mut c = conn.lock().await;
            let _ = c.close().await;
        }
        self.connection_manager.remove_connection(connection_id)?;
        Ok(())
    }
}

async fn handle_websocket_connection(
    stream: TcpStream,
    handler: Arc<dyn ConnectionHandler>,
    manager: Arc<ConnectionManager>,
    parser: MessageParser,
    config: ServerConfig,
    heartbeat_managers: Arc<Mutex<HashMap<String, HeartbeatManager>>>,
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
    let hb_managers_clone = Arc::clone(&heartbeat_managers);
    let config_clone = config.clone();
    
    let observer = Arc::new(ServerMessageObserver {
        handler: handler_clone,
        manager: manager_clone,
        parser: parser_clone,
        connection_id: conn_id_clone.clone(),
        heartbeat_managers: hb_managers_clone,
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

        // 启动心跳
        debug!("[DEBUG WebSocketServer] handle_websocket_connection: 准备启动心跳");
        let mut heartbeat = HeartbeatManager::new(
            config.heartbeat_interval,
            config.heartbeat_interval * 3,
        );
        heartbeat.start(Arc::clone(&conn), parser.clone());
        debug!("[DEBUG WebSocketServer] handle_websocket_connection: 心跳已启动");
        {
            debug!("[DEBUG WebSocketServer] handle_websocket_connection: 获取心跳管理器锁");
            let mut hb_managers = heartbeat_managers.lock().await;
            debug!("[DEBUG WebSocketServer] handle_websocket_connection: 心跳管理器锁已获取");
            hb_managers.insert(connection_id.clone(), heartbeat);
            debug!("[DEBUG WebSocketServer] handle_websocket_connection: 心跳已添加到管理器");
        }
        debug!("[DEBUG WebSocketServer] handle_websocket_connection: 心跳管理器锁已释放");
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
    heartbeat_managers: Arc<Mutex<HashMap<String, HeartbeatManager>>>,
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
                                // 发送 PONG
                                let pong_cmd = pong();
                                let pong_frame = crate::common::protocol::frame_with_system_command(
                                    pong_cmd,
                                    crate::common::protocol::Reliability::AtLeastOnce,
                                );
                                if let Ok(pong_data) = self.parser.serialize(&pong_frame) {
                                    if let Some((conn, _)) = self.manager.get_connection(&self.connection_id) {
                                        let conn_clone = Arc::clone(&conn);
                                        tokio::spawn(async move {
                                            let mut c = conn_clone.lock().await;
                                            let _ = c.send(&pong_data).await;
                                        });
                                    }
                                }
                                return;
                            }
                            if sys_cmd.r#type == crate::common::protocol::flare::core::commands::system_command::Type::Pong as i32 {
                                // 记录 PONG，更新心跳（在同步上下文中，但需要异步访问）
                                let hb_managers = Arc::clone(&self.heartbeat_managers);
                                let conn_id = self.connection_id.clone();
                                tokio::spawn(async move {
                                    let mut hb_managers = hb_managers.lock().await;
                                    if let Some(hb) = hb_managers.get_mut(&conn_id) {
                                        hb.record_pong();
                                    }
                                });
                                return;
                            }
                        }
                    }

                    // 处理其他消息
                    let handler = Arc::clone(&self.handler);
                    let manager = Arc::clone(&self.manager);
                    let parser = self.parser.clone();
                    let conn_id = self.connection_id.clone();
                    
                    tokio::spawn(async move {
                        if let Ok(Some(response)) = handler.handle_frame(&frame, &conn_id).await {
                            // 发送回复
                            if let Some((conn, _)) = manager.get_connection(&conn_id) {
                                if let Ok(data) = parser.serialize(&response) {
                                    let mut c = conn.lock().await;
                                    let _ = c.send(&data).await;
                                }
                            }
                        }
                        // 更新连接活跃时间
                        let _ = manager.update_connection_active(&conn_id);
                    });
                }
            }
            ConnectionEvent::Disconnected(_) => {
                let handler = Arc::clone(&self.handler);
                let manager = Arc::clone(&self.manager);
                let conn_id = self.connection_id.clone();
                let hb_managers = Arc::clone(&self.heartbeat_managers);
                
                tokio::spawn(async move {
                    // 停止心跳
                    {
                        let mut hb_mgrs = hb_managers.lock().await;
                        if let Some(mut hb) = hb_mgrs.remove(&conn_id) {
                            hb.stop();
                        }
                    }
                    
                    // 通知处理器
                    let _ = handler.on_disconnect(&conn_id).await;
                    
                    // 从管理器移除
                    let _ = manager.remove_connection(&conn_id);
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
