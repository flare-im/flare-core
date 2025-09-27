//! QUIC服务端实现
//!
//! 提供QUIC协议的服务端支持

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, error};

use crate::common::{
    error::Result,
};
use crate::common::connections::factory::ConnectionFactory;
use crate::ConnectionEvent;
use crate::server::{
    manager::traits::ServerConnectionManager, 
    Server, 
    ServerService,
    ServerEventAdapter,
    ServerConfig,
    ServerType,
};

/// QUIC 服务端实现
///
/// 负责处理 QUIC 协议的连接和消息
pub struct QuicServer {
    /// 配置
    config: ServerConfig,
    /// 连接管理器
    connection_manager: Arc<dyn ServerConnectionManager>,
    /// 服务句柄
    server_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// QUIC端点
    endpoint: Arc<RwLock<Option<quinn::Endpoint>>>,
    /// 服务端事件适配器
    event_handler: Arc<ServerEventAdapter>,
}

impl QuicServer {
    /// 创建新的 QUIC 服务端
    ///
    /// # 参数
    ///
    /// * `config` - 连接配置
    /// * `connection_manager` - 连接管理器
    ///
    /// # 返回值
    ///
    /// 返回新的 [QuicServer](struct.QuicServer.html) 实例
    pub fn new(
        config: ServerConfig,
        connection_manager: Arc<dyn ServerConnectionManager>,
        event_handler: Arc<ServerEventAdapter>,
    ) -> Self {
        Self {
            config,
            connection_manager,
            server_handle: Arc::new(RwLock::new(None)),
            endpoint: Arc::new(RwLock::new(None)),
            event_handler,
        }
    }
    
    /// 获取QUIC监听地址
    fn get_listen_addr(&self) -> String {
        if let Some(quic_config) = &self.config.quic_config {
            quic_config.listen_addr.clone()
        } else {
            "127.0.0.1:0".to_string() // 默认地址
        }
    }
    
    /// 创建 QUIC 端点
    async fn create_endpoint(&self) -> Result<quinn::Endpoint> {
        // 创建连接配置
        let connection_config = self.create_connection_config();
        
        // 使用ConnectionFactory创建QUIC服务端端点
        ConnectionFactory::create_quic_server_endpoint(connection_config).await
    }
    
    /// 创建连接配置
    fn create_connection_config(&self) -> crate::common::connections::config::ConnectionConfig {
        use crate::common::connections::config::ConnectionConfig;
        
        let local_addr = self.get_listen_addr();
        let connection_id = format!("quic_server_{}", local_addr.replace(":", "_"));
        
        // 使用增强的 to_quic_connection_config 方法
        self.config.to_quic_connection_config(connection_id)
            .unwrap_or_else(|| {
                // 如果配置中没有QUIC配置，创建一个基本的连接配置
                ConnectionConfig::server(
                    format!("quic_server_{}", local_addr.replace(":", "_")),
                    local_addr
                )
            })
    }
}

#[async_trait::async_trait]
impl Server for QuicServer {
    /// 启动 QUIC 服务
    ///
    /// 创建 QUIC 端点并开始监听客户端连接
    ///
    /// # 返回值
    ///
    /// 返回操作结果
    async fn start(&self) -> Result<()> {
        let local_addr = self.get_listen_addr();
        info!("启动 QUIC 服务: {}", local_addr);
        
        // 创建 QUIC 端点
        let endpoint = self.create_endpoint().await?;
        *self.endpoint.write().await = Some(endpoint.clone());
        
        // 克隆必要的组件
        let connection_manager = Arc::clone(&self.connection_manager);
        let config = Arc::new(self.config.clone());
        let event_handler = Arc::clone(&self.event_handler);
        
        // 启动服务任务
        let handle = tokio::spawn(async move {
            info!("QUIC 服务已启动: {}", local_addr);
            
            // 监听新的客户端连接
            while let Some(connecting) = endpoint.accept().await {
                let config = Arc::clone(&config);
                let connection_manager = Arc::clone(&connection_manager);
                let event_handler = Arc::clone(&event_handler);
                
                tokio::spawn(async move {
                    match connecting.await {
                        Ok(quic_connection) => {
                            let remote_addr = quic_connection.remote_address();
                            info!("QUIC客户端已连接: {}", remote_addr);
                            
                            // 创建事件处理器
                            let connection_event_handler: Arc<dyn ConnectionEvent> = event_handler.get_server_event_handler();
                            
                            // 使用增强的配置转换方法创建连接配置
                            let connection_id = format!("quic_connection_{}", remote_addr).replace(":", "_");
                            let mut connection_config = config.to_quic_connection_config(connection_id)
                                .unwrap_or_else(|| {
                                    // 如果配置中没有QUIC配置，创建一个基本的连接配置
                                    crate::common::connections::config::ConnectionConfig::server(
                                        format!("quic_connection_{}", remote_addr).replace(":", "_"),
                                        remote_addr.to_string(),
                                    )
                                });
                            
                            // 设置远程地址（从原始连接获取）
                            connection_config.remote_addr = remote_addr.to_string();
                            
                            // 创建服务端连接
                            match ConnectionFactory::from_quic_with_handler_arc(
                                quic_connection, 
                                connection_config, 
                                connection_event_handler,
                            ).await {
                                Ok(connection_arc) => {
                                    let connection_id = connection_arc.id().to_string();
                                    info!("QUIC 服务端连接已建立: {} (ID: {})", remote_addr, connection_id);
                                    
                                    // 调用accept方法完成连接初始化
                                    if let Err(e) = connection_arc.accept().await {
                                        error!("QUIC服务端连接accept失败: {} - {}", remote_addr, e);
                                        return;
                                    }
                                    
                                    // 将连接添加到连接管理器
                                    if let Err(e) = connection_manager.add_connection(connection_arc.clone()).await {
                                        error!("添加连接到管理器失败: {}", e);
                                        return;
                                    }
                                    
                                    // 触发连接事件
                                    ConnectionEvent::on_connected(&*event_handler, &connection_id).await;
                                }
                                Err(e) => {
                                    error!("创建QUIC服务端连接失败: {} - {}", remote_addr, e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("QUIC连接失败: {}", e);
                        }
                    }
                });
            }
        });
        
        // 保存服务句柄
        *self.server_handle.write().await = Some(handle);
        
        Ok(())
    }
    
    /// 停止 QUIC 服务
    ///
    /// 停止服务任务并关闭端点
    async fn stop(&self) {
        info!("停止 QUIC 服务");
        
        // 关闭端点
        if let Some(endpoint) = self.endpoint.write().await.take() {
            endpoint.close(0u32.into(), b"Server shutting down");
        }
        
        // 停止服务任务
        if let Some(handle) = self.server_handle.write().await.take() {
            handle.abort();
        }
    }
}

#[async_trait::async_trait]
impl ServerService for QuicServer {
    /// 获取服务类型
    fn get_type(&self) -> ServerType {
        ServerType::Quic
    }
    
    /// 获取本地地址
    fn get_local_addr(&self) -> Option<String> {
        Some(self.get_listen_addr())
    }
}