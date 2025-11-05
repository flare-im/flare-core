//! QUIC 服务端实现
//! 
//! 专注于 QUIC 协议层面的连接处理，连接管理和心跳检测由 ServerCore 统一管理

use crate::server::config::ServerConfig;
use crate::server::connection::ConnectionManager;
use crate::common::error::Result;
use crate::server::transports::{Server, ConnectionHandler};
use crate::server::transports::server_core::ServerCore;
use crate::server::handle::ServerHandle;
use crate::common::{generate_id};
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
    core: Arc<ServerCore>,  // 改为 Arc，便于共享
    handler: Arc<dyn ConnectionHandler>,
    endpoint: Option<Endpoint>,
    is_running: Arc<Mutex<bool>>,
}

impl QUICServer {
    /// 创建新的 QUIC 服务端
    pub fn new(config: ServerConfig, handler: Arc<dyn ConnectionHandler>) -> Result<Self> {
        Self::with_connection_manager(config, handler, None)
    }
    
    /// 使用指定的连接管理器创建 QUIC 服务端
    /// 
    /// # 参数
    /// - `config`: 服务端配置
    /// - `handler`: 连接处理器
    /// - `connection_manager`: 可选的连接管理器，如果为 None，则由 ServerCore 创建新的
    pub fn with_connection_manager(
        config: ServerConfig,
        handler: Arc<dyn ConnectionHandler>,
        connection_manager: Option<Arc<ConnectionManager>>,
    ) -> Result<Self> {
        // 确保 rustls CryptoProvider 已初始化（在服务器端也需要）
        use std::sync::Once;
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
        
        // 创建 ServerCore（统一管理连接和心跳）
        let core = Arc::new(ServerCore::new(&config, connection_manager));
        
        // 创建 QUIC server config（使用共享证书）
        // 使用共享证书工具，确保客户端和服务端使用相同的证书
        use crate::common::cert::{get_server_cert_der, get_server_key_der};
        
        let cert_der = get_server_cert_der()
            .map_err(|e| crate::common::error::FlareError::protocol_error(
                format!("Failed to load server certificate: {}", e)
            ))?;
        let key_der = get_server_key_der()
            .map_err(|e| crate::common::error::FlareError::protocol_error(
                format!("Failed to load server private key: {}", e)
            ))?;
        
        debug!("[QUIC Server] Using certificate from certs/server.crt for localhost, 127.0.0.1, ::1");
        
        // quinn 0.11 使用 rustls，需要转换类型
        // cert.serialize_der() 返回的是 DER 格式的 Vec<u8>
        // rustls 需要 CertificateDer 类型，可以直接从 DER 字节数组创建
        // 不需要使用 rustls_pemfile::certs（那是用于 PEM 格式的）
        
        // 直接将 DER 格式的证书字节数组转换为 CertificateDer
        // CertificateDer 可以从 &[u8] 或 Vec<u8> 创建
        let cert_der_bytes: quinn::rustls::pki_types::CertificateDer<'static> = 
            quinn::rustls::pki_types::CertificateDer::from(cert_der);
        
        // with_single_cert 需要一个证书向量
        let certs = vec![cert_der_bytes];
        
        // 私钥处理：serialize_private_key_der() 返回 DER 格式的 Vec<u8>
        // quinn::rustls::pki_types::PrivateKeyDer::Pkcs8 需要 PrivatePkcs8KeyDer
        // 需要使用 PrivatePkcs8KeyDer::from() 从字节数组创建
        if key_der.is_empty() {
            return Err(crate::common::error::FlareError::protocol_error(
                "Private key is empty".to_string()
            ));
        }
        
        // 从 DER 字节数组创建 PrivateKeyDer
        let private_key = quinn::rustls::pki_types::PrivateKeyDer::Pkcs8(
            quinn::rustls::pki_types::PrivatePkcs8KeyDer::from(key_der)
        );
        
        // 构建服务端配置
        // quinn 0.11 的 API：with_single_cert 直接接受证书和私钥
        let server_config = QuinnServerConfig::with_single_cert(
            certs,
            private_key,
        )
        .map_err(|e| crate::common::error::FlareError::protocol_error(format!("Failed to create server config: {}", e)))?;

        let addr = config.bind_address.parse::<SocketAddr>()
            .map_err(|e| crate::common::error::FlareError::protocol_error(format!("Invalid address: {}", e)))?;

        let endpoint = Endpoint::server(server_config, addr)
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("Failed to create endpoint: {}", e)))?;

        Ok(Self {
            config,
            core,
            handler,
            endpoint: Some(endpoint),
            is_running: Arc::new(Mutex::new(false)),
        })
    }
    
    /// 使用指定的 ServerCore 创建 QUIC 服务端（用于共享 ServerCore）
    pub fn with_shared_core(
        config: ServerConfig,
        handler: Arc<dyn ConnectionHandler>,
        core: Arc<ServerCore>,
    ) -> Result<Self> {
        // 确保 rustls CryptoProvider 已初始化
        use std::sync::Once;
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
        
        // 创建 QUIC server config（使用共享证书）
        use crate::common::cert::{get_server_cert_der, get_server_key_der};
        
        let cert_der = get_server_cert_der()
            .map_err(|e| crate::common::error::FlareError::protocol_error(
                format!("Failed to load server certificate: {}", e)
            ))?;
        let key_der = get_server_key_der()
            .map_err(|e| crate::common::error::FlareError::protocol_error(
                format!("Failed to load server key: {}", e)
            ))?;
        
        let cert = rustls::pki_types::CertificateDer::from(cert_der);
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(key_der)
        );
        
        let server_config = QuinnServerConfig::with_single_cert(
            vec![cert],
            key,
        ).map_err(|e| crate::common::error::FlareError::protocol_error(
            format!("Failed to create QUIC server config: {}", e)
        ))?;
        
        let addr = config.bind_address.parse()
            .map_err(|e| crate::common::error::FlareError::protocol_error(
                format!("Invalid address: {}", e)
            ))?;
        let endpoint = Endpoint::server(server_config, addr)
            .map_err(|e| crate::common::error::FlareError::protocol_error(
                format!("Failed to create QUIC endpoint: {}", e)
            ))?;
        
        Ok(Self {
            config,
            core,
            handler,
            endpoint: Some(endpoint),
            is_running: Arc::new(Mutex::new(false)),
        })
    }
}

#[async_trait]
impl Server for QUICServer {
    async fn start(&mut self) -> Result<()> {
        *self.is_running.lock().await = true;
        
        // 启动心跳检测（由 ServerCore 统一管理）
        self.core.start_heartbeat(&self.config);
        
        let endpoint = self.endpoint.take().ok_or_else(|| {
            crate::common::error::FlareError::connection_failed("Endpoint not initialized".to_string())
        })?;

        let handler = Arc::clone(&self.handler);
        let manager = Arc::clone(&self.core.connection_manager);
        let parser = self.core.parser.clone();
        let config = self.config.clone();
        let is_running = Arc::clone(&self.is_running);
        
        // 直接使用 self.core 的 Arc，确保 device_manager 等配置正确传递
        let core = Arc::clone(&self.core);
        let core_clone = Arc::clone(&core);

        tokio::spawn(async move {
            eprintln!("[QUIC Server] Started listening for connections...");
            while *is_running.lock().await {
                if let Some(conn) = endpoint.accept().await {
                    eprintln!("[QUIC Server] Incoming connection received, waiting for handshake...");
                    let handler_clone = Arc::clone(&handler);
                    let manager_clone = Arc::clone(&manager);
                    let parser_clone = parser.clone();
                    let config_clone = config.clone();
                    let core_clone = Arc::clone(&core_clone);
                    tokio::spawn(async move {
                        // conn 是 Incoming (Future)，await 后得到 Connecting
                        match conn.await {
                            Ok(connecting) => {
                                                                eprintln!("[QUIC Server] Handshake completed, handling connection...");                                                        
                                handle_quic_connection(
                                    connecting,
                                    handler_clone,
                                    manager_clone,
                                    parser_clone,
                                    config_clone,
                                    core_clone,
                                ).await;
                            }
                            Err(e) => {
                                eprintln!("[QUIC Server] Failed to accept QUIC connection: {}", e);
                            }
                        }
                    });
                } else {
                    eprintln!("[QUIC Server] No more connections, stopping...");
                    break;
                }
            }
        });
        
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
        *self.is_running.blocking_lock()
    }
}

async fn handle_quic_connection(
    connection: quinn::Connection,
    handler: Arc<dyn ConnectionHandler>,
    manager: Arc<ConnectionManager>,
    parser: crate::common::MessageParser,
    config: ServerConfig,
    core: Arc<ServerCore>,
) {
    // connection 已经是 quinn::Connection，可以直接使用
    let quinn_connection = connection;

    // 检查连接数限制
    if manager.connection_count() >= config.max_connections {
        eprintln!("Connection limit exceeded: {}", config.max_connections);
        quinn_connection.close(0u32.into(), b"limit exceeded");
        return;
    }

    // 接受双向流
    eprintln!("[QUIC Server] Waiting for client to open bidirectional stream...");
    let (send, recv) = match quinn_connection.accept_bi().await {
        Ok(streams) => {
            eprintln!("[QUIC Server] Bidirectional stream accepted");
            streams
        },
        Err(e) => {
            eprintln!("[QUIC Server] Failed to accept bi stream: {}", e);
            return;
        }
    };

    let connection_id = generate_id();
    let transport = QUICTransport::new(send, recv);
    let connection: Box<dyn Connection> = Box::new(transport);
    
    // 从 ServerCore 获取是否需要认证
    let requires_auth = core.auth_enabled();
    
    if let Err(e) = manager.add_connection(connection_id.clone(), connection, None, requires_auth) {
        eprintln!("Failed to add connection: {}", e);
        return;
    }

    // 注意：on_connect 将在收到 CONNECT 消息并完成协商后调用（在 QUICServerMessageObserver 中处理）

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

    if let Some((conn, _)) = manager.get_connection(&connection_id) {
        {
            let mut c = conn.lock().await;
            c.add_observer(observer);

            // 注意：CONNECT_ACK 将在收到 CONNECT 消息后发送（在 QUICServerMessageObserver 中处理）
        }

        // 服务端不需要主动发送心跳，只需要检测超时
        // 心跳检测由 ServerCore 统一管理
    }
}

// 旧的 QUICServerMessageObserver 已移除，现在使用 DefaultServerMessageObserver

