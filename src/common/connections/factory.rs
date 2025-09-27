//! 连接工厂实现
//! 
//! 提供创建不同类型连接的工厂模式实现

use std::sync::Arc;
use std::fs;
use std::io::BufReader;
use tracing::{debug, error, warn};

use crate::common::{error::Result, connections::{
    types::{ConnectionConfig, Transport, ConnectionRole},
    traits::{ClientConnection, ServerConnection, ConnectionEvent},
    quic::QuicConnection,
    websocket::WebSocketConnection,
    builder::ConnectionBuilder,
}, FrameSerializer};
use crate::Connection;
use quinn::{Endpoint, ClientConfig, ServerConfig};
use rustls::client::danger::ServerCertVerifier;
use rustls_pemfile::certs;
use tokio::net::TcpStream;
use tokio_tungstenite::accept_async;

/// 连接工厂
#[derive(Clone)]
pub struct ConnectionFactory;

impl ConnectionFactory {
    pub fn new() -> Self {
        Self
    }
    /// 根据配置创建序列化器
    fn create_serializer_from_config(config: &ConnectionConfig) -> Result<Arc<Box<dyn FrameSerializer>>> {
        use crate::common::serialization::{SerializerFactory, SerializationFormat};
        use crate::common::serialization::factory::json_serializer;

        let factory = SerializerFactory::new();
        let format = config.serialization_config.as_ref()
            .map(|c| c.format)
            .unwrap_or(SerializationFormat::Json);

        // 添加调试信息
        debug!("创建序列化器 - 配置格式: {:?}, 序列化配置: {:?}", 
               format, config.serialization_config);

        // 尝试根据配置创建序列化器
        match factory.create_with_config(format, config.get_serialization_config()) {
            Ok(serializer) => {
                debug!("成功创建序列化器: {:?}", format);
                Ok(Arc::from(serializer))
            }
            Err(e) => {
                error!("创建序列化器失败: {}, 使用默认JSON序列化器", e);
                error!("序列化配置: {:?}", config.get_serialization_config());
                // 如果创建失败，使用默认JSON序列化器
                Ok(Arc::from(json_serializer()))
            }
        }
    }
    /// 从构建器创建连接（根据配置自动判断是客户端还是服务端）
    pub async fn create_client_from_builder(builder: ConnectionBuilder) -> Result<Box<dyn ClientConnection>> {
        let (config, custom_serializer) = builder.build();
        
        // 使用自定义序列化器或根据配置创建序列化器
        let serializer = if let Some(serializer) = custom_serializer {
            serializer
        } else {
            Self::create_serializer_from_config(&config)?
        };

        // 根据序列化器创建连接
        Self::create_by_transport(config, Some(serializer))
    }

    /// 从配置创建连接
    pub async fn create_client(config: ConnectionConfig) -> Result<Box<dyn ClientConnection>>{
        // 确保是客户端配置
        if config.role != ConnectionRole::Client {
            return Err(crate::common::error::FlareError::connection_failed(
                "只能为客户端角色创建客户端连接"
            ));
        }
        // 根据配置创建序列化器
        let serializer = Self::create_serializer_from_config(&config)?;
        
        // 根据序列化器创建连接
        Self::create_by_transport(config, Some(serializer))
    }

    /// 根据传输类型创建连接
    fn create_by_transport(
        config: ConnectionConfig,
        serializer: Option<Arc<Box<dyn FrameSerializer>>>
    ) -> Result<Box<dyn ClientConnection>> {
        match config.transport {
            Transport::Quic => {
                if let Some(serializer) = serializer {
                    Ok(Box::new(QuicConnection::with_serializer(config, serializer)))
                } else {
                    Ok(Box::new(QuicConnection::new(config)))
                }
            }
            Transport::WebSocket => {
                if let Some(serializer) = serializer {
                    Ok(Box::new(WebSocketConnection::with_serializer(config, serializer)))
                } else {
                    Ok(Box::new(WebSocketConnection::new(config)))
                }
            }
            Transport::Tcp | Transport::Udp => {
                Err(crate::common::error::FlareError::connection_failed(
                    format!("{:?} 传输暂未实现", config.transport)
                ))
            }
        }
    }
    /// 创建客户端连接（带事件处理器）
    pub async fn create_client_with_handler(
        config: ConnectionConfig,
        handler: Option<Arc<dyn ConnectionEvent>>,
    ) -> Result<Box<dyn ClientConnection>> {


        // 根据配置创建序列化器
        let serializer = Self::create_serializer_from_config(&config)?;

        // 创建连接
        let mut connection = Self::create_by_transport(config, Some(serializer))?;

        // 设置事件处理器（如果提供）
        if let Some(handler) = handler {
            connection.set_event_handler(handler).await;
        }

        Ok(connection)
    }

    /// 创建QUIC客户端连接
    pub async fn create_quic_client_connection(
        config: ConnectionConfig,
    ) -> Result<Box<dyn ClientConnection>> {
        // 确保是客户端配置
        if config.role != crate::common::connections::types::ConnectionRole::Client {
            return Err(crate::common::error::FlareError::connection_failed(
                "只能为客户端角色创建客户端连接"
            ));
        }

        // 创建QUIC连接
        let connection = QuicConnection::new(config);

        Ok(Box::new(connection))
    }


    /// 创建QUIC客户端配置
    pub async fn create_quic_client_config(config: &ConnectionConfig) -> Result<ClientConfig> {
        // 确保 rustls 加密提供者已设置
        if let Err(e) = rustls::crypto::ring::default_provider().install_default() {
            warn!("设置 rustls 加密提供者失败: {:?}", e);
            // 继续执行，因为可能已经设置过了
        }
        
        let quic_config = &config.protocol_config.quic.client;

        // 如果配置为跳过服务器验证，使用跳过验证的配置
        if quic_config.skip_server_verification {
            let client_config_builder = rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(Self::create_skip_server_verification()))
                .with_no_client_auth();

            let quinn_config = ClientConfig::new(Arc::new(
                quinn::crypto::rustls::QuicClientConfig::try_from(client_config_builder)
                    .map_err(|e| crate::common::error::FlareError::connection_failed(format!("QUIC 客户端配置失败: {}", e)))?
            ));

            return Ok(quinn_config);
        }

        // 如果有服务器证书路径，使用证书验证
        if let Some(cert_path) = &quic_config.server_cert_path {
            // 读取服务器证书
            let cert_file = fs::File::open(cert_path)
                .map_err(|e| crate::common::error::FlareError::connection_failed(format!("无法读取服务器证书文件 {}: {}", cert_path, e)))?;
            let cert_reader = &mut BufReader::new(cert_file);

            // 解析证书
            let cert_der = certs(cert_reader)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| crate::common::error::FlareError::connection_failed(format!("解析服务器证书失败: {}", e)))?;

            // 创建根证书存储并添加证书
            let mut root_store = rustls::RootCertStore::empty();
            for cert in cert_der {
                root_store.add(rustls::pki_types::CertificateDer::from(cert))
                    .map_err(|e| crate::common::error::FlareError::connection_failed(format!("添加证书到根证书存储失败: {}", e)))?;
            }

            // 使用quinn的with_root_certificates方法
            let client_config = ClientConfig::with_root_certificates(Arc::new(root_store))
                .map_err(|e| crate::common::error::FlareError::connection_failed(format!("创建客户端配置失败: {}", e)))?;

            Ok(client_config)
        } else {
            // 默认使用跳过验证的配置
            let client_config_builder = rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(Self::create_skip_server_verification()))
                .with_no_client_auth();

            let quinn_config = ClientConfig::new(Arc::new(
                quinn::crypto::rustls::QuicClientConfig::try_from(client_config_builder)
                    .map_err(|e| crate::common::error::FlareError::connection_failed(format!("QUIC 客户端配置失败: {}", e)))?
            ));

            Ok(quinn_config)
        }
    }
    /// 获取支持的类型
    pub fn supported_types(&self) -> Vec<Transport> {
        vec![Transport::WebSocket, Transport::Quic]
    }

    /// 检查配置是否支持
    pub fn supports_config(config: &ConnectionConfig) -> bool {
        config.validate().is_ok()
    }
    /// 创建跳过服务器证书验证的实现
    fn create_skip_server_verification() -> impl rustls::client::danger::ServerCertVerifier {
        #[derive(Debug)]
        struct SkipServerVerification;

        impl ServerCertVerifier for SkipServerVerification {
            fn verify_server_cert(
                &self,
                _end_entity: &rustls::pki_types::CertificateDer,
                _intermediates: &[rustls::pki_types::CertificateDer],
                _server_name: &rustls::pki_types::ServerName,
                _ocsp_response: &[u8],
                _now: rustls::pki_types::UnixTime,
            ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }

            fn verify_tls12_signature(
                &self,
                _message: &[u8],
                _cert: &rustls::pki_types::CertificateDer,
                _dss: &rustls::DigitallySignedStruct,
            ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
                Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
            }

            fn verify_tls13_signature(
                &self,
                _message: &[u8],
                _cert: &rustls::pki_types::CertificateDer,
                _dss: &rustls::DigitallySignedStruct,
            ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
                Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
            }

            fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
                vec![
                    rustls::SignatureScheme::RSA_PKCS1_SHA1,
                    rustls::SignatureScheme::ECDSA_SHA1_Legacy,
                    rustls::SignatureScheme::RSA_PKCS1_SHA256,
                    rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
                    rustls::SignatureScheme::RSA_PKCS1_SHA384,
                    rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
                    rustls::SignatureScheme::RSA_PKCS1_SHA512,
                    rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
                    rustls::SignatureScheme::RSA_PSS_SHA256,
                    rustls::SignatureScheme::RSA_PSS_SHA384,
                    rustls::SignatureScheme::RSA_PSS_SHA512,
                    rustls::SignatureScheme::ED25519,
                    rustls::SignatureScheme::ED448,
                ]
            }
        }

        SkipServerVerification
    }
}

impl Default for ConnectionFactory {
    fn default() -> Self {
        Self::new()
    }
}
/// 服务端应用
impl ConnectionFactory {
    /// 从 WebSocket 原始连接创建服务端连接
    pub async fn from_websocket(tcp_stream: TcpStream, config: ConnectionConfig) -> Result<Box<dyn ServerConnection>> {
        
        // 接受 WebSocket 握手
        let ws_stream = accept_async(tcp_stream).await
            .map_err(|e| crate::common::error::FlareError::connection_failed(
                format!("WebSocket 握手失败: {}", e)
            ))?;
        
        // 创建 WebSocket 连接
        let mut connection = WebSocketConnection::new(config);
        
        // 设置 WebSocket 流到连接中
        connection.set_connection(ws_stream).await;
        
        // 启动消息处理任务
        connection.start_task().await
            .map_err(|e| crate::common::error::FlareError::connection_failed(
                format!("启动消息接收任务失败: {}", e)
            ))?;
        
        Ok(Box::new(connection))
    }
    
    /// 从 WebSocket 原始连接创建服务端连接，并设置事件处理器
    pub async fn from_websocket_with_handler(tcp_stream: TcpStream, config: ConnectionConfig, handler: Arc<dyn ConnectionEvent>, ) -> Result<Box<dyn ServerConnection>> {
        use tokio_tungstenite::accept_async;
        
        // 接受 WebSocket 握手
        let ws_stream = accept_async(tcp_stream).await
            .map_err(|e| crate::common::error::FlareError::connection_failed(
                format!("WebSocket 握手失败: {}", e)
            ))?;
        
        // 创建 WebSocket 连接
        let mut connection = WebSocketConnection::new(config);
        
        // 设置事件处理器
        connection.set_event_handler(handler).await;
        
        // 设置 WebSocket 流到连接中
        connection.set_connection(ws_stream).await;
        
        // 启动消息处理任务
        connection.start_task().await
            .map_err(|e| crate::common::error::FlareError::connection_failed(
                format!("启动消息接收任务失败: {}", e)
            ))?;
        
        Ok(Box::new(connection))
    }
    
    /// 从 WebSocket 原始连接创建服务端连接（使用Arc包装的事件处理器）
    pub async fn from_websocket_with_handler_arc(tcp_stream: TcpStream, config: ConnectionConfig, handler: Arc<dyn ConnectionEvent>, ) -> Result<Arc<dyn ServerConnection>> {
        // 接受 WebSocket 握手
        let ws_stream = accept_async(tcp_stream).await
            .map_err(|e| crate::common::error::FlareError::connection_failed(
                format!("WebSocket 握手失败: {}", e)
            ))?;
        
        // 创建 WebSocket 连接
        let mut connection = WebSocketConnection::new(config);
        
        // 设置事件处理器
        connection.set_event_handler(handler).await;
        
        // 设置 WebSocket 流到连接中
        connection.set_connection(ws_stream).await;
        
        // 注意：不在这里启动任务，而是在accept方法中启动
        // 这样可以确保任务在正确的时机启动
        
        Ok(Arc::new(connection))
    }
    
    /// 从 QUIC 原始连接创建服务端连接
    pub async fn from_quic(quic_connection: quinn::Connection, config: ConnectionConfig) -> Result<Box<dyn ServerConnection>> {
        // 创建 QUIC 连接
        let mut connection = QuicConnection::new(config);
        
        // 设置 QUIC 连接到连接中
        connection.set_connection(quic_connection).await;
        
        Ok(Box::new(connection))
    }
    
    /// 从 QUIC 原始连接创建服务端连接，并设置事件处理器
    pub async fn from_quic_with_handler(
        quic_connection: quinn::Connection,
        config: ConnectionConfig,
        handler: Arc<dyn ConnectionEvent>,
    ) -> Result<Box<dyn ServerConnection>> {
        // 创建 QUIC 连接
        let mut connection = QuicConnection::new(config);
        
        // 设置事件处理器
        connection.set_event_handler(handler).await;
        
        // 设置 QUIC 连接到连接中
        connection.set_connection(quic_connection).await;

        Ok(Box::new(connection))
    }
    
    /// 从 QUIC 原始连接创建服务端连接（使用Arc包装的事件处理器）
    pub async fn from_quic_with_handler_arc(quic_connection: quinn::Connection, config: ConnectionConfig, handler: Arc<dyn ConnectionEvent>) -> Result<Arc<dyn ServerConnection>> {
        // 创建 QUIC 连接
        let mut connection = QuicConnection::new(config);
        
        // 设置事件处理器
        connection.set_event_handler(handler).await;
        
        // 设置 QUIC 连接到连接中
        connection.set_connection(quic_connection).await;

        Ok(Arc::new(connection))
    }
    
    /// 创建QUIC服务端端点
    pub async fn create_quic_server_endpoint(
        config: ConnectionConfig,
    ) -> Result<Endpoint> {
        use std::net::SocketAddr;
        
        // 解析监听地址
        let addr = config.local_addr.as_ref()
            .ok_or_else(|| crate::common::error::FlareError::connection_failed("服务端配置缺少监听地址".to_string()))?
            .parse::<SocketAddr>()
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("无效的监听地址格式: {}", e)))?;
        
        // 创建服务端配置
        let server_config = Self::create_quic_server_config(&config).await?;
        
        // 创建QUIC端点
        let endpoint = quinn::Endpoint::server(server_config, addr)
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("无法创建QUIC服务端端点: {}", e)))?;
        
        Ok(endpoint)
    }
    
    /// 创建QUIC服务端配置
    pub async fn create_quic_server_config(config: &ConnectionConfig) -> Result<ServerConfig> {
        // 确保 rustls 加密提供者已设置
        if let Err(e) = rustls::crypto::ring::default_provider().install_default() {
            warn!("设置 rustls 加密提供者失败: {:?}", e);
            // 继续执行，因为可能已经设置过了
        }
        
        let quic_config = &config.protocol_config.quic.server;
        
        // 读取服务端证书和私钥
        let cert_file = fs::File::open(&quic_config.cert_path)
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("无法读取服务端证书文件 {}: {}", quic_config.cert_path, e)))?;
        let key_file = fs::File::open(&quic_config.key_path)
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("无法读取服务端私钥文件 {}: {}", quic_config.key_path, e)))?;
        
        let cert_reader = &mut BufReader::new(cert_file);
        let key_reader = &mut BufReader::new(key_file);
        
        let cert_chain = rustls_pemfile::certs(cert_reader).collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("解析服务端证书失败: {}", e)))?;
        let key = rustls_pemfile::private_key(key_reader)?.unwrap();
        
        let server_crypto = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("创建服务端TLS配置失败: {}", e)))?;
        
        let server_cfg = ServerConfig::with_crypto(Arc::new(quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("创建QUIC服务端配置失败: {}", e)))?));
        
        Ok(server_cfg)
    }
}
