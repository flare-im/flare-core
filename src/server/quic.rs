//! QUIC 服务端实现

use crate::common::config::ServerConfig;
use crate::common::connection_manager::ConnectionManager;
use crate::common::error::Result;
use crate::common::heartbeat::HeartbeatManager;
use crate::common::message_parser::MessageParser;
use crate::common::protocol::{Frame, connect_ack, ping, pong, frame_with_system_command};
use crate::common::server_trait::{Server, ConnectionHandler};
use crate::common::{generate_id};
use crate::transport::connection::Connection;
use crate::transport::events::{ArcObserver, ConnectionEvent};
use crate::transport::quic::QUICTransport;
use async_trait::async_trait;
use quinn::{Endpoint, ServerConfig as QuinnServerConfig};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// QUIC 服务端
pub struct QUICServer {
    config: ServerConfig,
    connection_manager: Arc<ConnectionManager>,
    handler: Arc<dyn ConnectionHandler>,
    parser: MessageParser,
    endpoint: Option<Endpoint>,
    is_running: Arc<Mutex<bool>>,
    heartbeat_managers: Arc<Mutex<HashMap<String, HeartbeatManager>>>,
}

impl QUICServer {
    /// 创建新的 QUIC 服务端
    pub fn new(config: ServerConfig, handler: Arc<dyn ConnectionHandler>) -> Result<Self> {
        let parser = MessageParser::new(config.default_serialization_format, config.default_compression);
        
        // 创建 QUIC server config（简化：使用自签名证书）
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])
            .map_err(|e| crate::common::error::FlareError::protocol_error(format!("Failed to generate cert: {}", e)))?;
        
        // quinn 0.11 的证书类型处理
        // rcgen 返回 Vec<u8>，quinn 需要特定的证书类型
        // 简化处理：直接使用字节数组（需要根据实际 quinn 版本调整）
        let cert_der = cert.serialize_der()
            .map_err(|e| crate::common::error::FlareError::protocol_error(format!("Failed to serialize cert: {}", e)))?;
        let key_der = cert.serialize_private_key_der();
        
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
            connection_manager: Arc::new(ConnectionManager::new()),
            handler,
            parser,
            endpoint: Some(endpoint),
            is_running: Arc::new(Mutex::new(false)),
            heartbeat_managers: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

#[async_trait]
impl Server for QUICServer {
    async fn start(&mut self) -> Result<()> {
        *self.is_running.lock().await = true;
        
        let endpoint = self.endpoint.take().ok_or_else(|| {
            crate::common::error::FlareError::connection_failed("Endpoint not initialized".to_string())
        })?;

        let handler = Arc::clone(&self.handler);
        let manager = Arc::clone(&self.connection_manager);
        let parser = self.parser.clone();
        let config = self.config.clone();
        let is_running = Arc::clone(&self.is_running);
        let heartbeat_managers = Arc::clone(&self.heartbeat_managers);

        tokio::spawn(async move {
            while *is_running.lock().await {
                if let Some(conn) = endpoint.accept().await {
                    let handler_clone = Arc::clone(&handler);
                    let manager_clone = Arc::clone(&manager);
                    let parser_clone = parser.clone();
                    let config_clone = config.clone();
                    let hb_mgrs_clone = Arc::clone(&heartbeat_managers);
                    
                    tokio::spawn(async move {
                        // conn 是 Incoming (Future)，await 后得到 Connecting
                        match conn.await {
                            Ok(connecting) => {
                                handle_quic_connection(
                                    connecting,
                                    handler_clone,
                                    manager_clone,
                                    parser_clone,
                                    config_clone,
                                    hb_mgrs_clone,
                                ).await;
                            }
                            Err(e) => {
                                eprintln!("Failed to accept QUIC connection: {}", e);
                            }
                        }
                    });
                } else {
                    break;
                }
            }
        });
        
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        *self.is_running.lock().await = false;
        
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

    fn is_running(&self) -> bool {
        *self.is_running.blocking_lock()
    }

    fn connection_count(&self) -> usize {
        self.connection_manager.connection_count()
    }

    fn user_count(&self) -> usize {
        self.connection_manager.stats().total_users
    }

    async fn disconnect(&self, connection_id: &str) -> Result<()> {
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

async fn handle_quic_connection(
    connection: quinn::Connection,
    handler: Arc<dyn ConnectionHandler>,
    manager: Arc<ConnectionManager>,
    parser: MessageParser,
    config: ServerConfig,
    heartbeat_managers: Arc<Mutex<HashMap<String, HeartbeatManager>>>,
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
    let (send, recv) = match quinn_connection.accept_bi().await {
        Ok(streams) => streams,
        Err(e) => {
            eprintln!("Failed to accept bi stream: {}", e);
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
    let hb_managers_clone = Arc::clone(&heartbeat_managers);
    let config_clone = config.clone();

    let observer = Arc::new(QUICServerMessageObserver {
        handler: handler_clone,
        manager: manager_clone,
        parser: parser_clone,
        connection_id: conn_id_clone.clone(),
        heartbeat_managers: hb_managers_clone,
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

        // 启动心跳
        let mut heartbeat = HeartbeatManager::new(
            config.heartbeat_interval,
            config.heartbeat_interval * 3,
        );
        heartbeat.start(Arc::clone(&conn), parser.clone());
        {
            let mut hb_managers = heartbeat_managers.lock().await;
            hb_managers.insert(connection_id.clone(), heartbeat);
        }
    }

    // 定期清理超时连接
    let manager_clone = Arc::clone(&manager);
    let config_clone = config.clone();
    tokio::spawn(async move {
        let mut cleanup_interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            cleanup_interval.tick().await;
            let timeout_conns = manager_clone.cleanup_timeout_connections(config_clone.connection_timeout);
            if !timeout_conns.is_empty() {
                eprintln!("Cleaned up {} timeout connections", timeout_conns.len());
            }
        }
    });
}

struct QUICServerMessageObserver {
    handler: Arc<dyn ConnectionHandler>,
    manager: Arc<ConnectionManager>,
    parser: MessageParser,
    connection_id: String,
    heartbeat_managers: Arc<Mutex<HashMap<String, HeartbeatManager>>>,
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

                    let handler = Arc::clone(&self.handler);
                    let manager = Arc::clone(&self.manager);
                    let parser = self.parser.clone();
                    let conn_id = self.connection_id.clone();
                    
                    tokio::spawn(async move {
                        if let Ok(Some(response)) = handler.handle_frame(&frame, &conn_id).await {
                            if let Some((conn, _)) = manager.get_connection(&conn_id) {
                                if let Ok(data) = parser.serialize(&response) {
                                    let mut c = conn.lock().await;
                                    let _ = c.send(&data).await;
                                }
                            }
                        }
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
                    {
                        let mut hb_mgrs = hb_managers.lock().await;
                        if let Some(mut hb) = hb_mgrs.remove(&conn_id) {
                            hb.stop();
                        }
                    }
                    let _ = handler.on_disconnect(&conn_id).await;
                    let _ = manager.remove_connection(&conn_id);
                });
            }
            ConnectionEvent::Connected => {}
            ConnectionEvent::Error(e) => {
                eprintln!("Connection error for {}: {:?}", self.connection_id, e);
            }
        }
    }
}

