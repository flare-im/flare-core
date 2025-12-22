//! QUIC 服务端实现
//!
//! 专注于 QUIC 协议层面的连接处理，连接管理和心跳检测由 ServerCore 统一管理

use crate::common::error::Result;
use crate::server::config::ServerConfig;
use crate::server::connection::ConnectionManager;
use crate::server::transports::Server;
use crate::server::transports::common::ServerConnectionHelper;
use crate::server::transports::server_core::ServerCore;
use crate::transport::connection::Connection;
use crate::transport::quic::QUICTransport;
use async_trait::async_trait;
use quinn::{Endpoint, ServerConfig as QuinnServerConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::debug;

/// QUIC 服务端
///
/// 专注于 QUIC 协议层面的连接处理
pub struct QUICServer {
    config: ServerConfig,
    core: Arc<ServerCore>,
    endpoint: Option<Endpoint>,
    is_running: Arc<Mutex<bool>>,
}

impl QUICServer {
    /// 创建新的 QUIC 服务端
    pub fn new(config: ServerConfig) -> Result<Self> {
        Self::with_connection_manager(config, None)
    }

    /// 使用指定的连接管理器创建 QUIC 服务端
    pub fn with_connection_manager(
        config: ServerConfig,
        connection_manager: Option<Arc<ConnectionManager>>,
    ) -> Result<Self> {
        Self::init_rustls();

        let core = Arc::new(ServerCore::new(&config, connection_manager));
        let endpoint = Self::create_quic_endpoint(&config)?;

        Ok(Self {
            config,
            core,
            endpoint: Some(endpoint),
            is_running: Arc::new(Mutex::new(false)),
        })
    }

    /// 使用指定的 ServerCore 创建 QUIC 服务端（用于共享 ServerCore）
    pub fn with_shared_core(config: ServerConfig, core: Arc<ServerCore>) -> Result<Self> {
        Self::init_rustls();

        let endpoint = Self::create_quic_endpoint(&config)?;

        Ok(Self {
            config,
            core,
            endpoint: Some(endpoint),
            is_running: Arc::new(Mutex::new(false)),
        })
    }

    // ============================================================================
    // 内部辅助方法
    // ============================================================================

    /// 初始化 rustls CryptoProvider（内部辅助方法）
    fn init_rustls() {
        use std::sync::Once;
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
    }

    /// 创建 QUIC 端点（内部辅助方法）
    fn create_quic_endpoint(config: &ServerConfig) -> Result<Endpoint> {
        use crate::common::cert::{get_server_cert_der, get_server_key_der};

        // 加载证书和私钥
        let cert_der = get_server_cert_der().map_err(|e| {
            crate::common::error::FlareError::protocol_error(format!("加载服务器证书失败: {}", e))
        })?;
        let key_der = get_server_key_der().map_err(|e| {
            crate::common::error::FlareError::protocol_error(format!("加载服务器私钥失败: {}", e))
        })?;

        debug!("[QUIC Server] 使用证书: certs/server.crt");

        // 转换证书格式
        let cert = quinn::rustls::pki_types::CertificateDer::from(cert_der);
        let certs = vec![cert];

        // 转换私钥格式
        if key_der.is_empty() {
            return Err(crate::common::error::FlareError::protocol_error(
                "私钥为空".to_string(),
            ));
        }

        let private_key = quinn::rustls::pki_types::PrivateKeyDer::Pkcs8(
            quinn::rustls::pki_types::PrivatePkcs8KeyDer::from(key_der),
        );

        // 构建服务端配置
        let server_config =
            QuinnServerConfig::with_single_cert(certs, private_key).map_err(|e| {
                crate::common::error::FlareError::protocol_error(format!(
                    "创建 QUIC 服务端配置失败: {}",
                    e
                ))
            })?;

        // 解析地址
        let addr = config.bind_address.parse::<SocketAddr>().map_err(|e| {
            crate::common::error::FlareError::protocol_error(format!("无效地址: {}", e))
        })?;

        // 创建端点
        Endpoint::server(server_config, addr).map_err(|e| {
            crate::common::error::FlareError::connection_failed(format!(
                "创建 QUIC 端点失败: {}",
                e
            ))
        })
    }
}

#[async_trait]
impl Server for QUICServer {
    async fn start(&mut self) -> Result<()> {
        *self.is_running.lock().await = true;

        // 启动心跳检测
        self.core.start_heartbeat(&self.config);

        let endpoint = self.endpoint.take().ok_or_else(|| {
            crate::common::error::FlareError::connection_failed("端点未初始化".to_string())
        })?;

        // 准备共享资源
        let manager = Arc::clone(&self.core.connection_manager);
        let config = self.config.clone();
        let is_running = Arc::clone(&self.is_running);
        let core = Arc::clone(&self.core);

        tokio::spawn(async move {
            debug!("[QUIC Server] 开始监听连接");
            while *is_running.lock().await {
                if let Some(conn) = endpoint.accept().await {
                    debug!("[QUIC Server] 收到新连接，等待握手");
                    let manager_clone = Arc::clone(&manager);
                    let config_clone = config.clone();
                    let core_clone = Arc::clone(&core);

                    tokio::spawn(async move {
                        match conn.await {
                            Ok(connecting) => {
                                debug!("[QUIC Server] 握手完成，处理连接");
                                handle_quic_connection(
                                    connecting,
                                    manager_clone,
                                    config_clone,
                                    core_clone,
                                )
                                .await;
                            }
                            Err(e) => {
                                debug!("[QUIC Server] QUIC 连接握手失败: {}", e);
                            }
                        }
                    });
                } else {
                    debug!("[QUIC Server] 没有更多连接，停止");
                    break;
                }
            }
        });

        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        ServerConnectionHelper::stop_server(&self.core, &self.is_running)
            .await
            .map_err(|e| {
                crate::common::error::FlareError::connection_failed(format!(
                    "停止服务器失败: {}",
                    e
                ))
            })
    }

    fn is_running(&self) -> bool {
        tokio::task::block_in_place(|| *self.is_running.blocking_lock())
    }
}

/// 处理 QUIC 连接（内部函数）
async fn handle_quic_connection(
    connection: quinn::Connection,
    manager: Arc<ConnectionManager>,
    config: ServerConfig,
    core: Arc<ServerCore>,
) {
    // 检查连接数限制
    if manager.connection_count() >= config.max_connections {
        debug!("[QUIC Server] 连接数限制已满: {}", config.max_connections);
        connection.close(0u32.into(), b"limit exceeded");
        return;
    }

    // 接受双向流
    debug!("[QUIC Server] 等待客户端打开双向流");
    let (send, recv) = match connection.accept_bi().await {
        Ok(streams) => {
            debug!("[QUIC Server] 双向流已接受");
            streams
        }
        Err(e) => {
            debug!("[QUIC Server] 接受双向流失败: {}", e);
            return;
        }
    };

    // 创建传输层连接
    let transport = QUICTransport::new(send, recv);
    let connection: Box<dyn Connection> = Box::new(transport);

    // 使用公共模块设置连接
    let connection_id = match ServerConnectionHelper::setup_new_connection(
        connection,
        manager.clone(),
        &config,
        core.clone(),
    )
    .await
    {
        Ok(id) => id,
        Err(e) => {
            debug!("[QUIC Server] 设置连接失败: {}", e);
            return;
        }
    };

    // 注意：on_connect 将在收到 CONNECT 消息并完成协商后调用（在观察者中处理）
    debug!("[QUIC Server] 连接已设置: connection_id={}", connection_id);
}
