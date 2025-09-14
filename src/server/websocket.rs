//! WebSocket服务端实现
//!
//! 提供WebSocket协议的服务端支持

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};
use std::net::SocketAddr;

use crate::common::{
    error::Result,
};
use crate::ConnectionEvent;
use super::{manager::traits::ServerConnectionManager, server::{Server, ServerConfig, ServerType, ServerService}, ConnectionEventHandler};

/// WebSocket 服务端实现
///
/// 负责处理 WebSocket 协议的连接和消息
///
/// # 泛型参数
///
/// * `T` - 连接管理器类型，必须实现 [ConnectionManager](../manager/traits/trait.ConnectionManager.html) trait
pub struct WebSocketServer<T: ServerConnectionManager> {
    /// 配置
    config: ServerConfig,
    /// 连接管理器
    connection_manager: Arc<T>,
    /// 服务句柄
    server_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// 服务端事件处理器
    event_handler: Arc<ConnectionEventHandler>,
}

impl<T: ServerConnectionManager + 'static> WebSocketServer<T> {
    /// 创建新的 WebSocket 服务端
    ///
    /// # 参数
    ///
    /// * `config` - 连接配置
    /// * `connection_manager` - 连接管理器
    ///
    /// # 返回值
    ///
    /// 返回新的 [WebSocketServer](struct.WebSocketServer.html) 实例
    pub fn new(
        config: ServerConfig,
        connection_manager: Arc<T>,
        event_handler: Arc<ConnectionEventHandler>,
    ) -> Self {
        Self {
            config,
            connection_manager,
            server_handle: Arc::new(RwLock::new(None)),
            event_handler,
        }
    }
}

#[async_trait::async_trait]
impl<T: ServerConnectionManager + 'static> Server for WebSocketServer<T> {
    /// 启动 WebSocket 服务
    ///
    /// 创建 TCP 监听器并开始监听客户端连接
    ///
    /// # 返回值
    ///
    /// 返回操作结果
    async fn start(&self) -> Result<()> {
        let local_addr = self.config.local_addr.clone().unwrap_or_default();
        info!("启动 WebSocket 服务: {}", local_addr);
        
        // 解析地址
        let addr: SocketAddr = local_addr.parse().map_err(|e| {
            crate::common::error::FlareError::connection_failed(format!("地址解析失败: {}", e))
        })?;
        
        // 创建 TCP 监听器
        let listener = tokio::net::TcpListener::bind(addr).await
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("绑定端口失败: {}", e)))?;
        
        // 克隆必要的组件
        let connection_manager = Arc::clone(&self.connection_manager);
        let config = self.config.clone();
        let event_handler = Arc::clone(&self.event_handler);
        
        // 启动服务任务
        let handle = tokio::spawn(async move {
            info!("WebSocket 服务已启动: {}", local_addr);
            
            // 监听新的客户端连接
            loop {
                match listener.accept().await {
                    Ok((tcp_stream, addr)) => {
                        info!("WebSocket客户端已连接: {}", addr);
                        
                        // 克隆组件
                        let _connection_config = config.clone();
                        let connection_manager = Arc::clone(&connection_manager);
                        let event_handler = Arc::clone(&event_handler);
                        
                        // 为每个连接创建独立的任务
                        tokio::spawn(async move {
                            // 创建事件处理器
                            let connection_event_handler: Arc<dyn ConnectionEvent> = event_handler.clone();
                            
                            // 创建服务端连接配置
                            let connection_config = crate::common::connections::config::ConnectionConfig::server(
                                format!("ws_connection_{}", addr).replace(":", "_"),
                                addr.to_string(),
                            );
                            
                            // 创建服务端连接
                            match crate::common::connections::factory::RawConnectionHandler::from_websocket_with_handler_arc(
                                tcp_stream, 
                                connection_config, 
                                connection_event_handler,
                            ).await {
                                Ok(connection_arc) => {
                                    let connection_id = connection_arc.id().to_string();
                                    debug!("WebSocket 服务端连接已建立: {} (ID: {})", addr, connection_id);
                                    
                                    // 将连接添加到连接管理器
                                    if let Err(e) = connection_manager.add_connection(connection_arc.clone()).await {
                                        error!("添加连接到管理器失败: {}", e);
                                        return;
                                    }
                                    
                                    // 触发连接事件
                                    ConnectionEvent::on_connected(&*event_handler, &connection_id).await;
                                }
                                Err(e) => {
                                    error!("创建WebSocket服务端连接失败: {} - {}", addr, e);
                                }
                            }
                        });
                    }
                    Err(_e) => {
                        // 短暂等待后继续监听
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
            }
        });
        
        // 保存服务句柄
        *self.server_handle.write().await = Some(handle);
        
        Ok(())
    }
    
    /// 停止 WebSocket 服务
    ///
    /// 停止服务任务
    async fn stop(&self) {
        info!("停止 WebSocket 服务");
        
        // 停止服务任务
        if let Some(handle) = self.server_handle.write().await.take() {
            handle.abort();
        }
    }
}

#[async_trait::async_trait]
impl<T: ServerConnectionManager + 'static> ServerService for WebSocketServer<T> {
    /// 获取服务类型
    fn get_type(&self) -> ServerType {
        ServerType::WebSocket
    }
    
    /// 获取本地地址
    fn get_local_addr(&self) -> Option<String> {
        self.config.local_addr.clone()
    }
}