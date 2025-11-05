//! QUIC 服务端实现
//! 
//! 专注于 QUIC 协议层面的连接处理，连接管理和心跳检测由 ServerCore 统一管理

use crate::server::config::ServerConfig;
use crate::server::connection::{ConnectionManager, ConnectionManagerTrait};
use crate::common::error::Result;
use crate::common::protocol::{Frame, pong};
use crate::server::transports::{Server, ConnectionHandler};
use crate::server::transports::server_core::ServerCore;
use crate::server::handle::ServerHandle;
use crate::common::{generate_id};
use crate::transport::connection::Connection;
use crate::transport::events::ConnectionEvent;
use crate::transport::quic::QUICTransport;
use async_trait::async_trait;
use quinn::{Endpoint, ServerConfig as QuinnServerConfig};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::debug;

/// QUIC 服务端
/// 
/// 专注于 QUIC 协议层面的连接处理
pub struct QUICServer {
    config: ServerConfig,
    core: ServerCore,
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
        let core = ServerCore::new(&config, connection_manager);
        
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

        tokio::spawn(async move {
            eprintln!("[QUIC Server] Started listening for connections...");
            while *is_running.lock().await {
                if let Some(conn) = endpoint.accept().await {
                    eprintln!("[QUIC Server] Incoming connection received, waiting for handshake...");
                    let handler_clone = Arc::clone(&handler);
                    let manager_clone = Arc::clone(&manager);
                    let parser_clone = parser.clone();
                    let config_clone = config.clone();
                    
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
            let _ = ServerHandle::disconnect(&self.core, &conn_id).await;
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
    
    if let Err(e) = manager.add_connection(connection_id.clone(), connection, None) {
        eprintln!("Failed to add connection: {}", e);
        return;
    }

    if let Err(e) = handler.on_connect(&connection_id).await {
        eprintln!("Handler on_connect error: {}", e);
    }

    let handler_clone = Arc::clone(&handler);
    let manager_clone = Arc::clone(&manager);
    let parser_clone = parser.clone();
    let conn_id_clone = connection_id.clone();
    let config_clone = config.clone();

    let observer = Arc::new(QUICServerMessageObserver {
        handler: handler_clone,
        manager: manager_clone,
        parser: parser_clone,
        connection_id: conn_id_clone.clone(),
        config: config_clone,
    });

    if let Some((conn, _)) = manager.get_connection(&connection_id) {
        {
            let mut c = conn.lock().await;
            c.add_observer(observer);

            // 发送 CONNECT_ACK
            let mut metadata = HashMap::new();
            let format_bytes = format!("{:?}", config.default_serialization_format).into_bytes();
            metadata.insert("format".to_string(), format_bytes);
            let connect_ack_cmd = crate::common::protocol::connect_ack(config.default_serialization_format, metadata);
            let connect_ack_frame = crate::common::protocol::frame_with_system_command(
                connect_ack_cmd,
                crate::common::protocol::Reliability::AtLeastOnce,
            );
            if let Ok(data) = parser.serialize(&connect_ack_frame) {
                let _ = c.send(&data).await;
            }
        }

        // 服务端不需要主动发送心跳，只需要检测超时
        // 心跳检测由 ServerCore 统一管理
    }
}

struct QUICServerMessageObserver {
    handler: Arc<dyn ConnectionHandler>,
    manager: Arc<ConnectionManager>,
    parser: crate::common::MessageParser,
    connection_id: String,
    config: ServerConfig,
}

impl crate::transport::events::ConnectionObserver for QUICServerMessageObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
                if let Ok(frame) = self.parser.parse(data) {
                    // 处理 PING/PONG
                    if let Some(cmd) = &frame.command {
                        if let Some(crate::common::protocol::flare::core::commands::command::Type::System(sys_cmd)) = &cmd.r#type {
                            if sys_cmd.r#type == crate::common::protocol::flare::core::commands::system_command::Type::Ping as i32 {
                                // 收到 PING，回复 PONG 并更新连接活跃时间
                                let manager = Arc::clone(&self.manager);
                                let conn_id = self.connection_id.clone();
                                
                                // 更新连接活跃时间（通过 trait 的异步方法）
                                let manager_update = Arc::clone(&manager) as Arc<dyn ConnectionManagerTrait>;
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
                                    let manager_get = Arc::clone(&manager) as Arc<dyn ConnectionManagerTrait>;
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

                    // 处理消息 - 更新连接活跃时间
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
                
                debug!("[QUICServer] Connection disconnected: {}", conn_id);
                tokio::spawn(async move {
                    // 通知连接断开
                    let _ = handler.on_disconnect(&conn_id).await;
                    // 立即从连接管理器中移除连接
                    match manager.remove_connection(&conn_id).await {
                        Ok(_) => {
                            debug!("[QUICServer] Successfully removed connection: {}", conn_id);
                        }
                        Err(e) => {
                            debug!("[QUICServer] Connection {} already removed or not found: {}", conn_id, e);
                        }
                    }
                });
            }
            ConnectionEvent::Connected => {}
            ConnectionEvent::Error(e) => {
                debug!("Connection error for {}: {:?}", self.connection_id, e);
                // 连接出错时，立即从管理器中移除连接（避免连接一直存在）
                let handler = Arc::clone(&self.handler);
                let manager = Arc::clone(&self.manager) as Arc<dyn ConnectionManagerTrait>;
                let conn_id = self.connection_id.clone();
                
                debug!("[QUICServer] Connection error detected, removing connection: {}", conn_id);
                tokio::spawn(async move {
                    // 通知连接断开
                    let _ = handler.on_disconnect(&conn_id).await;
                    // 从连接管理器中移除（如果连接存在）
                    match manager.remove_connection(&conn_id).await {
                        Ok(_) => {
                            debug!("[QUICServer] Successfully removed connection after error: {}", conn_id);
                        }
                        Err(e) => {
                            debug!("[QUICServer] Connection {} already removed or not found after error: {}", conn_id, e);
                        }
                    }
                });
            }
        }
    }
}

