//! QUIC服务端实现
//!
//! 提供QUIC协议的服务端支持

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, error};

use crate::common::{
    error::Result,
};
use crate::common::connections::factory::RawConnectionHandler;
use crate::ConnectionEvent;
use super::{manager::traits::ServerConnectionManager, server::{Server, ServerConfig, ServerType, ServerService}, ConnectionEventHandler};

/// QUIC 服务端实现
///
/// 负责处理 QUIC 协议的连接和消息
///
/// # 泛型参数
///
/// * `T` - 连接管理器类型，必须实现 [ConnectionManager](../manager/traits/trait.ConnectionManager.html) trait
pub struct QuicServer<T: ServerConnectionManager> {
    /// 配置
    config: ServerConfig,
    /// 连接管理器
    connection_manager: Arc<T>,
    /// 服务句柄
    server_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// QUIC端点
    endpoint: Arc<RwLock<Option<quinn::Endpoint>>>,
    /// 服务端事件处理器
    event_handler: Arc<ConnectionEventHandler>,
}

impl<T: ServerConnectionManager + 'static> QuicServer<T> {
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
        connection_manager: Arc<T>,
        event_handler: Arc<ConnectionEventHandler>,
    ) -> Self {
        Self {
            config,
            connection_manager,
            server_handle: Arc::new(RwLock::new(None)),
            endpoint: Arc::new(RwLock::new(None)),
            event_handler,
        }
    }
    
    /// 创建 QUIC 端点
    async fn create_endpoint(&self) -> Result<quinn::Endpoint> {
        use quinn::{Endpoint, ServerConfig};
        use rustls::ServerConfig as RustlsServerConfig;
        
        let local_addr = self.config.local_addr.as_ref().ok_or_else(|| {
            crate::common::error::FlareError::connection_failed("未指定本地地址".to_string())
        })?;
        
        let addr = local_addr.parse().map_err(|e| {
            crate::common::error::FlareError::connection_failed(format!("地址解析失败: {}", e))
        })?;
        
        // 创建 TLS 服务器配置（使用自签名证书）
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("生成自签名证书失败: {}", e)))?;
        
        let rustls_config = RustlsServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![rustls::pki_types::CertificateDer::from(cert.cert)],
                rustls::pki_types::PrivateKeyDer::Pkcs8(
                    rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der())
                )
            )
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("TLS 配置失败: {}", e)))?;
        
        let server_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(rustls_config)
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("QUIC 配置失败: {}", e)))?;
        
        let server_config = ServerConfig::with_crypto(Arc::new(server_crypto));
        
        // 绑定端点
        let endpoint = Endpoint::server(server_config, addr)
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("QUIC 端点创建失败: {}", e)))?;
        
        Ok(endpoint)
    }
}

#[async_trait::async_trait]
impl<T: ServerConnectionManager + 'static> Server for QuicServer<T> {
    /// 启动 QUIC 服务
    ///
    /// 创建 QUIC 端点并开始监听客户端连接
    ///
    /// # 返回值
    ///
    /// 返回操作结果
    async fn start(&self) -> Result<()> {
        let local_addr = self.config.local_addr.clone().unwrap_or_default();
        info!("启动 QUIC 服务: {}", local_addr);
        
        // 创建 QUIC 端点
        let endpoint = self.create_endpoint().await?;
        *self.endpoint.write().await = Some(endpoint.clone());
        
        // 克隆必要的组件
        let connection_manager = Arc::clone(&self.connection_manager);
        let config = self.config.clone();
        let event_handler = Arc::clone(&self.event_handler);
        
        // 启动服务任务
        let handle = tokio::spawn(async move {
            info!("QUIC 服务已启动: {}", local_addr);
            
            // 监听新的客户端连接
            while let Some(connecting) = endpoint.accept().await {
                let _connection_config = config.clone();
                let connection_manager = Arc::clone(&connection_manager);
                let event_handler = Arc::clone(&event_handler);
                
                tokio::spawn(async move {
                    match connecting.await {
                        Ok(quic_connection) => {
                            let remote_addr = quic_connection.remote_address();
                            info!("QUIC客户端已连接: {}", remote_addr);
                            
                            // 创建事件处理器
                            let connection_event_handler: Arc<dyn ConnectionEvent> = event_handler.get_server_event_handler();
                            
                            // 创建服务端连接配置
                            let connection_config = crate::common::connections::config::ConnectionConfig::server(
                                format!("quic_connection_{}", remote_addr).replace(":", "_"),
                                remote_addr.to_string(),
                            );
                            
                            // 创建服务端连接
                            match RawConnectionHandler::from_quic_with_handler_arc(
                                quic_connection, 
                                connection_config, 
                                connection_event_handler,
                            ).await {
                                Ok(connection_arc) => {
                                    let connection_id = connection_arc.id().to_string();
                                    info!("QUIC 服务端连接已建立: {} (ID: {})", remote_addr, connection_id);
                                    
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
impl<T: ServerConnectionManager + 'static> ServerService for QuicServer<T> {
    /// 获取服务类型
    fn get_type(&self) -> ServerType {
        ServerType::Quic
    }
    
    /// 获取本地地址
    fn get_local_addr(&self) -> Option<String> {
        self.config.local_addr.clone()
    }
}