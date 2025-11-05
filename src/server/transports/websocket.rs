//! WebSocket 服务端实现
//! 
//! 专注于 WebSocket 协议层面的连接处理，连接管理和心跳检测由 ServerCore 统一管理

use crate::server::config::ServerConfig;
use tracing::debug;
use crate::server::connection::ConnectionManager;
use crate::common::error::Result;
use crate::server::transports::{Server, ConnectionHandler};
use crate::server::transports::server_core::ServerCore;
use crate::server::handle::ServerHandle;
use crate::common::{generate_id};
use crate::transport::connection::Connection;
use crate::transport::websocket::WebSocketTransport;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::accept_async;

/// WebSocket 服务端
/// 
/// 专注于 WebSocket 协议层面的连接处理
pub struct WebSocketServer {
    config: ServerConfig,
    core: Arc<ServerCore>,  // 改为 Arc，便于共享
    handler: Arc<dyn ConnectionHandler>,
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
        let core = Arc::new(ServerCore::new(&config, connection_manager));
        
        Self {
            config,
            core,
            handler,
            is_running: Arc::new(Mutex::new(false)),
        }
    }
    
    /// 使用指定的 ServerCore 创建 WebSocket 服务端（用于共享 ServerCore）
    pub fn with_shared_core(
        config: ServerConfig,
        handler: Arc<dyn ConnectionHandler>,
        core: Arc<ServerCore>,
    ) -> Self {
        Self {
            config,
            core,
            handler,
            is_running: Arc::new(Mutex::new(false)),
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
        
        // 直接使用 self.core 的 Arc，确保 device_manager 等配置正确传递
        let core = Arc::clone(&self.core);
        let core_clone = Arc::clone(&core);
        
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
                        let core_clone = Arc::clone(&core_clone);
                        tokio::spawn(async move {
                            debug!("[DEBUG WebSocketServer] 连接处理任务开始");
                            handle_websocket_connection(
                                stream,
                                handler_clone,
                                manager_clone,
                                parser_clone,
                                config_clone,
                                core_clone,
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
        
        // listener 已在闭包中使用，不需要再存储
        // self.listener = None; // 移除这行，因为 listener 已经在闭包中使用
        
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
            let _ = ServerHandle::disconnect(&*self.core, &conn_id).await;
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
    core: Arc<ServerCore>,
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
    let core_clone = Arc::clone(&core);
    
    let device_manager = core.device_manager();
    let event_handler = core.event_handler();
    let observer = Arc::new(crate::server::events::DefaultServerMessageObserver::new(
        handler_clone,
        manager_clone,
        parser_clone,
        conn_id_clone.clone(),
        core_clone,
        device_manager,
        event_handler, // 从 ServerCore 获取事件处理器
    ));

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
            
            // 注意：CONNECT_ACK 将在收到 CONNECT 消息后发送（在 ServerMessageObserver 中处理）
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

// 旧的 ServerMessageObserver 已移除，现在使用 DefaultServerMessageObserver
