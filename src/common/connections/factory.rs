use crate::common::connections::config::{ConnectionConfig, QuicClientConfig, QuicServerConfig};
use crate::common::connections::enums::Transport;
use crate::common::connections::traits::{ClientConnection, ServerConnection, ConnectionEvent};
use crate::common::error::FlareError;
use std::sync::Arc;

// 证书/私钥基础校验与加载
use std::fs::File;
use std::io::BufReader;
use rustls::{ClientConfig as TlsClientConfig, ServerConfig as TlsServerConfig, RootCertStore};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;

use crate::common::connections::quic::{QuicClientConn, QuicServerConn};
use crate::common::connections::websocket::{WebSocketClientConn, WebSocketServerConn};

pub struct ConnectionFactory;

impl ConnectionFactory {
    pub fn create_client(config: ConnectionConfig) -> Result<Box<dyn ClientConnection>, FlareError> {
        match config.transport {
            Transport::Quic => Ok(Box::new(QuicClientConn::from_config(config))),
            Transport::WebSocket => Ok(Box::new(WebSocketClientConn::from_config(config))),
        }
    }

    pub fn create_client_with_handler(config: ConnectionConfig, handler: Arc<dyn ConnectionEvent>) -> Result<Box<dyn ClientConnection>, FlareError> {
        let client = Self::create_client(config)?;
        client.set_event_handler(handler);
        Ok(client)
    }

    // QUIC 客户端 TLS/mTLS 配置（返回 rustls::ClientConfig）
    pub fn create_quic_client_config(cfg: &QuicClientConfig) -> Result<TlsClientConfig, FlareError> {
        // RootStore
        let mut root_store = RootCertStore::empty();
        if !cfg.skip_server_verification {
            let server_cert_path = cfg.server_cert_path.as_ref().ok_or_else(|| FlareError::general_error("server_cert_path 缺失且未启用 skip_server_verification"))?;
            let certs = Self::load_cert_chain(server_cert_path)?;
            for c in certs { root_store.add(c).map_err(|e| FlareError::general_error(format!("加入根证书失败: {:?}", e)))?; }
        }
        // Client auth (mTLS)
        let tls = match (&cfg.client_cert_path, &cfg.client_key_path) {
            (Some(cert_path), Some(key_path)) => {
                let client_chain = Self::load_cert_chain(cert_path)?;
                let client_key = Self::load_private_key(key_path)?;
                TlsClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_client_auth_cert(client_chain, client_key)
                    .map_err(|e| FlareError::general_error(format!("构建客户端mTLS失败: {:?}", e)))?
            }
            (None, None) => {
                TlsClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_no_client_auth()
            }
            _ => {
                return Err(FlareError::general_error("启用 mTLS 需同时提供 client_cert_path 与 client_key_path"));
            }
        };
        Ok(tls)
    }

    // QUIC 服务端 TLS/mTLS 配置（返回 rustls::ServerConfig）
    pub fn create_quic_server_config(cfg: &QuicServerConfig) -> Result<TlsServerConfig, FlareError> {
        // 服务端证书与私钥
        let cert_path = cfg.cert_path.as_ref().ok_or_else(|| FlareError::general_error("cert_path 缺失"))?;
        let key_path = cfg.key_path.as_ref().ok_or_else(|| FlareError::general_error("key_path 缺失"))?;
        let cert_chain = Self::load_cert_chain(cert_path)?;
        let private_key = Self::load_private_key(key_path)?;

        let tls_server = if cfg.require_client_auth {
            let ca_path = cfg.client_ca_cert_path.as_ref().ok_or_else(|| FlareError::general_error("require_client_auth=true 需提供 client_ca_cert_path"))?;
            let root_store = Self::load_root_store(ca_path)?;
            let verifier = WebPkiClientVerifier::builder(Arc::new(root_store)).build().map_err(|e| FlareError::general_error(format!("构建客户端验证器失败: {:?}", e)))?;
            TlsServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(cert_chain, private_key)
                .map_err(|e| FlareError::general_error(format!("构建服务端mTLS失败: {:?}", e)))?
        } else {
            TlsServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(cert_chain, private_key)
                .map_err(|e| FlareError::general_error(format!("构建服务端TLS失败: {:?}", e)))?
        };
        Ok(tls_server)
    }

    // 从底层原生 QUIC 连接构建服务端连接（支持传入 quinn::Connection）
    pub fn from_quic_connection(conn: quinn::Connection, config: ConnectionConfig) -> Arc<dyn ServerConnection> {
        Arc::new(QuicServerConn::from_quinn_connection(conn, config))
    }

    // 从底层原生 WebSocket 流构建服务端连接（支持传入 WebSocketStream）
    pub fn from_websocket_stream(
        stream: tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        config: ConnectionConfig,
    ) -> Arc<dyn ServerConnection>
    {
        Arc::new(WebSocketServerConn::from_websocket_stream(stream, config))
    }

    // 从配置构建服务端连接（骨架，不含原生连接）
    pub fn from_quic(config: ConnectionConfig) -> Arc<dyn ServerConnection> {
        Arc::new(QuicServerConn::from_config(config))
    }

    pub fn from_websocket(config: ConnectionConfig) -> Arc<dyn ServerConnection> {
        Arc::new(WebSocketServerConn::from_config(config))
    }

    // 加载证书链
    fn load_cert_chain(path: &str) -> Result<Vec<CertificateDer<'static>>, FlareError> {
        let file = File::open(path).map_err(|e| FlareError::general_error(format!("打开证书文件失败: {} - {}", path, e)))?;
        let mut reader = BufReader::new(file);
        let mut certs_out = Vec::new();
        for item in rustls_pemfile::certs(&mut reader) {
            match item {
                Ok(der) => certs_out.push(der),
                Err(e) => return Err(FlareError::general_error(format!("解析证书失败: {} - {}", path, e))),
            }
        }
        if certs_out.is_empty() {
            Err(FlareError::general_error(format!("证书文件无有效证书: {}", path)))
        } else {
            Ok(certs_out)
        }
    }

    // 加载私钥
    fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, FlareError> {
        let file = File::open(path).map_err(|e| FlareError::general_error(format!("打开私钥文件失败: {} - {}", path, e)))?;
        let mut reader = BufReader::new(file);
        match rustls_pemfile::private_key(&mut reader) {
            Ok(opt) => opt.ok_or_else(|| FlareError::general_error(format!("私钥文件无有效私钥: {}", path))),
            Err(e) => Err(FlareError::general_error(format!("解析私钥失败: {} - {:?}", path, e))),
        }
    }

    // 加载根证书存储
    fn load_root_store(path: &str) -> Result<RootCertStore, FlareError> {
        let file = File::open(path).map_err(|e| FlareError::general_error(format!("打开根证书文件失败: {} - {}", path, e)))?;
        let mut reader = BufReader::new(file);
        let mut store = RootCertStore::empty();
        let mut count = 0usize;
        for item in rustls_pemfile::certs(&mut reader) {
            match item {
                Ok(der) => { store.add(der).map_err(|e| FlareError::general_error(format!("加入根证书失败: {:?}", e)))?; count += 1; }
                Err(e) => return Err(FlareError::general_error(format!("解析根证书失败: {} - {}", path, e))),
            }
        }
        if count == 0 { Err(FlareError::general_error(format!("根证书文件无有效证书: {}", path))) } else { Ok(store) }
    }
}
