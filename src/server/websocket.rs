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
use crate::server::{
    manager::traits::ServerConnectionManager, 
    Server, 
    ServerService,
    ServerEventAdapter,
};
use crate::{
    ServerConfig,
    ServerType,
};

/// WebSocket 服务端实现
///
/// 负责处理 WebSocket 协议的连接和消息
pub struct WebSocketServer {
    /// 配置
    config: ServerConfig,
    /// 连接管理器
    connection_manager: Arc<dyn ServerConnectionManager>,
    /// 服务句柄
    server_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// 服务端事件处理器
    event_handler: Arc<ServerEventAdapter>,
}

impl WebSocketServer {
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
        connection_manager: Arc<dyn ServerConnectionManager>,
        event_handler: Arc<ServerEventAdapter>,
    ) -> Self {
        Self {
            config,
            connection_manager,
            server_handle: Arc::new(RwLock::new(None)),
            event_handler,
        }
    }
    
    /// 获取WebSocket监听地址
    fn get_listen_addr(&self) -> String {
        if let Some(ws_config) = &self.config.websocket_config {
            ws_config.listen_addr.clone()
        } else {
            "127.0.0.1:0".to_string() // 默认地址
        }
    }
}

#[async_trait::async_trait]
impl Server for WebSocketServer {
    /// 启动 WebSocket 服务
    ///
    /// 创建 TCP 监听器并开始监听客户端连接
    ///
    /// # 返回值
    ///
    /// 返回操作结果
    async fn start(&self) -> Result<()> {
        let local_addr = self.get_listen_addr();
        info!("准备启动 WebSocket 服务: {}", local_addr);
        
        // 解析地址
        let addr: SocketAddr = local_addr.parse().map_err(|e| {
            crate::common::error::FlareError::connection_failed(format!("地址解析失败: {}", e))
        })?;
        
        // 创建 TCP 监听器
        let listener = tokio::net::TcpListener::bind(addr).await
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("绑定端口失败: {}", e)))?;
        
        info!("WebSocket TCP监听器绑定成功: {}", addr);
        
        // 克隆必要的组件
        let connection_manager = Arc::clone(&self.connection_manager);
        let config = self.config.clone();
        let event_handler = Arc::clone(&self.event_handler);
        
        // 启动服务任务
        let handle = tokio::spawn(async move {
            info!("WebSocket 服务任务已启动并监听: {}", local_addr);
            
            // 监听新的客户端连接
            loop {
                match listener.accept().await {
                    Ok((tcp_stream, addr)) => {
                        info!("WebSocket客户端已连接: {}", addr);
                        
                        // 克隆组件
                        let connection_manager = Arc::clone(&connection_manager);
                        let event_handler = Arc::clone(&event_handler);
                        // 克隆序列化配置以避免在循环中移动
                        let serialization_config = config.serialization_config.clone();
                        
                        // 为每个连接创建独立的任务
                        tokio::spawn(async move {
                            // 创建事件处理器
                            let connection_event_handler: Arc<dyn ConnectionEvent> = event_handler.clone();
                            
                            // 创建服务端连接配置
                            let mut connection_config = crate::common::connections::config::ConnectionConfig::server(
                                format!("ws_connection_{}", addr).replace(":", "_"),
                                addr.to_string(),
                            );
                            
                            // 设置远程地址为客户端地址
                            connection_config.remote_addr = addr.to_string();
                            
                            // 使用服务端配置中的序列化配置，如果未设置则默认使用protobuf序列化
                            let serialization_config = if serialization_config.format != crate::common::serialization::SerializationFormat::Json {
                                // 如果已配置了非默认的序列化格式，则使用配置的格式
                                serialization_config
                            } else {
                                // 否则默认使用protobuf序列化
                                crate::common::serialization::SerializationConfig {
                                    format: crate::common::serialization::SerializationFormat::Json,
                                    ..Default::default()
                                }
                            };
                            
                            connection_config = connection_config.with_serialization_config(serialization_config);
                            
                            // 创建服务端连接
                            match crate::common::connections::factory::RawConnectionHandler::from_websocket_with_handler_arc(
                                tcp_stream, 
                                connection_config, 
                                connection_event_handler.clone(),  // 克隆事件处理器
                            ).await {
                                Ok(connection) => {  // 注意：这里改为connection，不再使用mut
                                    let connection_id = connection.id().to_string();
                                    debug!("WebSocket 服务端连接已建立: {} (ID: {})", addr, connection_id);
                                    
                                    // 启动连接任务
                                    if let Err(e) = connection.accept().await {
                                        error!("接受WebSocket连接失败: {} - {}", addr, e);
                                        return;
                                    }
                                    
                                    // 将连接添加到连接管理器
                                    if let Err(e) = connection_manager.add_connection(connection).await {
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
                    Err(e) => {
                        error!("WebSocket监听错误: {}", e);
                        // 短暂等待后继续监听
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
            }
        });
        
        // 保存服务句柄
        *self.server_handle.write().await = Some(handle);
        
        info!("WebSocket服务启动完成");
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
impl ServerService for WebSocketServer {
    /// 获取服务类型
    fn get_type(&self) -> ServerType {
        ServerType::WebSocket
    }
    
    /// 获取本地地址
    fn get_local_addr(&self) -> Option<String> {
        Some(self.get_listen_addr())
    }
}