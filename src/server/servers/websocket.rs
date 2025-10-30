use crate::server::config::ServerConfig;
use crate::common::error::FlareError;
use crate::server::manager::traits::ConnectionManager;
use crate::server::traits::ProtocolService;
use std::sync::Arc;
use tracing::{info, error};

/// WebSocket 服务端监听骨架（服务端专有逻辑）
pub struct WebSocketServer {
    pub cfg: ServerConfig,
    /// 连接管理器
    connection_manager: Option<Arc<dyn ConnectionManager>>,
}

impl WebSocketServer {
    pub fn new(cfg: ServerConfig, connection_manager: Arc<dyn ConnectionManager>) -> Self { 
        Self { 
            cfg,
            connection_manager: Some(connection_manager),
        } 
    }
}

#[async_trait::async_trait]
impl ProtocolService for WebSocketServer {
    async fn start(&self, connection_manager: Arc<dyn ConnectionManager>) -> Result<(), FlareError> {
        use std::net::SocketAddr;
        use tokio::net::TcpListener;
        use tokio::spawn;
        use tracing::{info, error};
        
        use crate::common::connections::factory::ConnectionFactory;
use crate::server::connections::websocket::WebSocketServerConnection;
        use std::sync::Arc;

        // 解析监听地址（优先 ServerConfig 中的 WebSocket 配置）
        let listen = if let Some(ws) = self.cfg.get_websocket_config() {
            ws.listen_addr.clone()
        } else {
            "127.0.0.1:4320".to_string()
        };
        let addr: SocketAddr = listen.parse().map_err(|e| FlareError::ConnectionFailed { message: format!("地址解析失败: {}", e), source: None })?;

        let listener = TcpListener::bind(addr).await.map_err(|e| FlareError::ConnectionFailed { message: format!("绑定端口失败: {}", e), source: None })?;
        info!("WebSocket 服务端监听: {}", addr);

        let cfg_clone = self.cfg.clone();
        spawn(async move {
            loop {
                let cfg_clone_inner = cfg_clone.clone();
                let connection_manager_inner = connection_manager.clone();
                
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        let connection_id = format!("ws_{}", peer).replace(":", "_");
                        // 构造连接配置
                        let mut conn_cfg = cfg_clone_inner
                            .to_websocket_connection_config(connection_id.clone())
                            .unwrap_or_default();
                        conn_cfg.transport = crate::common::connections::enums::Transport::WebSocket;
                        conn_cfg.remote_addr = Some(peer.to_string());
                        // 设置连接ID
                        conn_cfg.id = Some(connection_id.clone());
                        // 创建统一服务端连接并触发接受事件
                        let server_conn = Arc::new(WebSocketServerConnection::from_config(conn_cfg));
                        
                        // 添加连接到管理器（无需认证）
                        connection_manager_inner.add_connection(server_conn.clone());
                        
                        // 获取连接引用用于后续处理
                        let conn_arc = server_conn.clone();
                        
                        // 获取连接管理器的事件处理器适配器
                        let handler = Arc::new(connection_manager_inner.get_event_handler_adapter().await);
                        conn_arc.set_event_handler(handler.clone());
                        // 接受连接（启动心跳等）
                        if let Err(e) = conn_arc.accept() {
                            error!("WebSocket 连接接受失败: {}", e);
                            // 从管理器中移除连接
                            connection_manager_inner.remove_connection(&connection_id);
                            continue;
                        }
                        
                        // 升级为 WebSocket 并读取消息，转发到事件处理器
                        tokio::spawn(async move {
                            use tokio_tungstenite::accept_async;
                            use tokio_tungstenite::tungstenite::Message;
                            use futures_util::StreamExt;
                            use crate::common::protocol::factory::FrameFactory;
                            use crate::common::protocol::reliability::Reliability;
                            use crate::common::connections::traits::ConnectionEvent;
                            match accept_async(stream).await {
                                Ok(mut ws) => {
                                    while let Some(msg) = ws.next().await {
                                        match msg {
                                            Ok(Message::Binary(data)) => {
                                                let data_vec: Vec<u8> = data.to_vec();
                                                if let Ok(frame) = FrameFactory::create_data_frame(FrameFactory::generate_message_id(), data_vec, Reliability::BestEffort) {
                                                    // 检查handler是否是EnhancedEventHandler的适配器
                                                    if let Some(enhanced_handler) = handler.as_any().downcast_ref::<crate::server::events::handler::EventHandlerAdapter>() {
                                                        // 使用增强事件处理器，传递连接ID
                                                        enhanced_handler.on_message_received_with_id(connection_id.clone(), frame);
                                                    } else {
                                                        // 使用基础事件处理器
                                                        handler.on_message_received(frame);
                                                    }
                                                }
                                            }
                                            Ok(Message::Text(text)) => {
                                                let data = text.to_string().into_bytes();
                                                if let Ok(frame) = FrameFactory::create_data_frame(FrameFactory::generate_message_id(), data, Reliability::BestEffort) {
                                                    // 检查handler是否是EnhancedEventHandler的适配器
                                                    if let Some(enhanced_handler) = handler.as_any().downcast_ref::<crate::server::events::handler::EventHandlerAdapter>() {
                                                        // 使用增强事件处理器，传递连接ID
                                                        enhanced_handler.on_message_received_with_id(connection_id.clone(), frame);
                                                    } else {
                                                        // 使用基础事件处理器
                                                        handler.on_message_received(frame);
                                                    }
                                                }
                                            }
                                            Ok(Message::Ping(_)) => {
                                                // 检查handler是否是EnhancedEventHandler的适配器
                                                if let Some(enhanced_handler) = handler.as_any().downcast_ref::<crate::server::events::handler::EventHandlerAdapter>() {
                                                    // 使用增强事件处理器，传递连接ID
                                                    enhanced_handler.on_heartbeat_ping_with_id(connection_id.clone());
                                                } else {
                                                    // 使用基础事件处理器
                                                    handler.on_heartbeat_ping();
                                                }
                                            }
                                            Ok(Message::Pong(_)) => {
                                                // 检查handler是否是EnhancedEventHandler的适配器
                                                if let Some(enhanced_handler) = handler.as_any().downcast_ref::<crate::server::events::handler::EventHandlerAdapter>() {
                                                    // 使用增强事件处理器，传递连接ID
                                                    enhanced_handler.on_heartbeat_pong_with_id(connection_id.clone(), 0);
                                                } else {
                                                    // 使用基础事件处理器
                                                    handler.on_heartbeat_pong(0);
                                                }
                                            }
                                            Ok(Message::Close(_)) => {
                                                // 检查handler是否是EnhancedEventHandler的适配器
                                                if let Some(enhanced_handler) = handler.as_any().downcast_ref::<crate::server::events::handler::EventHandlerAdapter>() {
                                                    // 使用增强事件处理器，传递连接ID
                                                    enhanced_handler.on_disconnected_with_id(connection_id.clone(), Some("client closed".into()));
                                                } else {
                                                    // 使用基础事件处理器
                                                    handler.on_disconnected(Some("client closed".into()));
                                                }
                                                break;
                                            }
                                            Ok(other) => { let _ = other; }
                                            Err(e) => {
                                                // 检查handler是否是EnhancedEventHandler的适配器
                                                if let Some(enhanced_handler) = handler.as_any().downcast_ref::<crate::server::events::handler::EventHandlerAdapter>() {
                                                    // 使用增强事件处理器，传递连接ID
                                                    enhanced_handler.on_error_with_id(connection_id.clone(), FlareError::ConnectionFailed { message: format!("WebSocket读取错误: {}", e), source: None });
                                                } else {
                                                    // 使用基础事件处理器
                                                    handler.on_error(FlareError::ConnectionFailed { message: format!("WebSocket读取错误: {}", e), source: None });
                                                }
                                                break;
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    // 检查handler是否是EnhancedEventHandler的适配器
                                    if let Some(enhanced_handler) = handler.as_any().downcast_ref::<crate::server::events::handler::EventHandlerAdapter>() {
                                        // 使用增强事件处理器，传递连接ID
                                        enhanced_handler.on_error_with_id(connection_id.clone(), FlareError::ConnectionFailed { message: format!("WebSocket升级失败: {}", e), source: None });
                                    } else {
                                        // 使用基础事件处理器
                                        handler.on_error(FlareError::ConnectionFailed { message: format!("WebSocket升级失败: {}", e), source: None });
                                    }
                                }
                            }
                            
                            // 连接关闭时从管理器中移除
                            connection_manager_inner.remove_connection(&connection_id);
                        });
                    }
                    Err(e) => {
                        error!("WebSocket accept 错误: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
            }
        });

        Ok(())
    }

    async fn stop(&self) -> Result<(), FlareError> {
        // 最小实现：由外部停止任务或进程
        Ok(())
    }
    
    fn name(&self) -> &str {
        "WebSocket"
    }
}

impl WebSocketServer {
    pub fn connection_manager(&self) -> Option<&Arc<dyn ConnectionManager>> {
        self.connection_manager.as_ref()
    }
}