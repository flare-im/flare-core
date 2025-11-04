//! 共享配置类型
//! 
//! 定义客户端和服务端共用的配置类型

use std::time::Duration;
use std::path::PathBuf;

/// 传输协议类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TransportProtocol {
    /// WebSocket 协议
    WebSocket,
    /// QUIC 协议
    QUIC,
    /// TCP 协议
    TCP,
}

impl TransportProtocol {
    /// 从字符串转换为协议类型
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "websocket" | "ws" => Some(TransportProtocol::WebSocket),
            "quic" => Some(TransportProtocol::QUIC),
            "tcp" => Some(TransportProtocol::TCP),
            _ => None,
        }
    }
    
    /// 转换为字符串
    pub fn as_str(&self) -> &'static str {
        match self {
            TransportProtocol::WebSocket => "websocket",
            TransportProtocol::QUIC => "quic",
            TransportProtocol::TCP => "tcp",
        }
    }
}

/// TLS/SSL 证书配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TlsConfig {
    /// 证书文件路径（PEM 或 DER 格式）
    pub cert_path: Option<PathBuf>,
    /// 私钥文件路径（PEM 或 DER 格式）
    pub key_path: Option<PathBuf>,
    /// 证书数据（DER 格式，Base64 编码的字符串或直接字节）
    pub cert_data: Option<Vec<u8>>,
    /// 私钥数据（DER 格式，Base64 编码的字符串或直接字节）
    pub key_data: Option<Vec<u8>>,
    /// 是否验证服务器证书（客户端使用）
    pub verify_cert: bool,
    /// CA 证书文件路径（用于验证服务器证书）
    pub ca_cert_path: Option<PathBuf>,
    /// CA 证书数据
    pub ca_cert_data: Option<Vec<u8>>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            cert_path: None,
            key_path: None,
            cert_data: None,
            key_data: None,
            verify_cert: true,
            ca_cert_path: None,
            ca_cert_data: None,
        }
    }
}

impl TlsConfig {
    /// 创建空配置（不使用 TLS）
    pub fn none() -> Self {
        Self::default()
    }

    /// 从文件路径创建配置
    pub fn from_files(cert_path: PathBuf, key_path: PathBuf) -> Self {
        Self {
            cert_path: Some(cert_path),
            key_path: Some(key_path),
            ..Default::default()
        }
    }

    /// 从内存数据创建配置
    pub fn from_data(cert_data: Vec<u8>, key_data: Vec<u8>) -> Self {
        Self {
            cert_data: Some(cert_data),
            key_data: Some(key_data),
            ..Default::default()
        }
    }

    /// 设置 CA 证书路径（用于客户端验证服务器）
    pub fn with_ca_cert(mut self, ca_cert_path: PathBuf) -> Self {
        self.ca_cert_path = Some(ca_cert_path);
        self
    }

    /// 禁用证书验证（仅用于开发/测试）
    pub fn disable_verification(mut self) -> Self {
        self.verify_cert = false;
        self
    }
}

/// 个性化心跳配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HeartbeatConfig {
    /// 心跳发送间隔
    pub interval: Duration,
    /// 心跳超时时间（如果在此时间内未收到响应，认为连接断开）
    pub timeout: Duration,
    /// 是否启用心跳
    pub enabled: bool,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(90),
            enabled: true,
        }
    }
}

impl HeartbeatConfig {
    /// 创建默认配置
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置心跳间隔
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// 设置超时时间
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// 禁用心跳
    pub fn disable(mut self) -> Self {
        self.enabled = false;
        self
    }
}
