//! 服务端配置模块

use crate::common::config_types::{TransportProtocol, TlsConfig, HeartbeatConfig};
use crate::common::protocol::SerializationFormat;
use crate::common::compression::CompressionAlgorithm;
use std::time::Duration;

/// 服务端配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServerConfig {
    /// 监听地址
    pub bind_address: String,
    /// 传输协议（单个）
    pub transport: TransportProtocol,
    /// 传输协议列表（用于同时监听多个协议）
    pub transports: Option<Vec<TransportProtocol>>,
    /// 序列化格式（默认格式）
    pub default_serialization_format: SerializationFormat,
    /// 压缩算法（默认）
    pub default_compression: CompressionAlgorithm,
    /// 最大连接数
    pub max_connections: usize,
    /// 连接超时时间
    pub connection_timeout: Duration,
    /// 默认心跳配置（可以为每个连接配置不同的心跳）
    pub default_heartbeat: HeartbeatConfig,
    /// TLS 配置（用于 HTTPS/WSS/QUIC）
    pub tls: TlsConfig,
    /// 消息大小限制（字节）
    pub max_message_size: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".to_string(),
            transport: TransportProtocol::WebSocket,
            transports: None,
            default_serialization_format: SerializationFormat::Protobuf,
            default_compression: CompressionAlgorithm::None,
            max_connections: 10000,
            connection_timeout: Duration::from_secs(300),
            default_heartbeat: HeartbeatConfig::default(),
            tls: TlsConfig::none(),
            max_message_size: 10 * 1024 * 1024, // 10MB
        }
    }
}

impl ServerConfig {
    /// 创建新的服务端配置
    pub fn new(bind_address: String) -> Self {
        Self {
            bind_address,
            ..Default::default()
        }
    }
    
    /// 使用 WebSocket 协议
    pub fn websocket(mut self) -> Self {
        self.transport = TransportProtocol::WebSocket;
        self
    }
    
    /// 使用 QUIC 协议
    pub fn quic(mut self) -> Self {
        self.transport = TransportProtocol::QUIC;
        self
    }
    
    /// 设置默认序列化格式
    pub fn with_format(mut self, format: SerializationFormat) -> Self {
        self.default_serialization_format = format;
        self
    }
    
    /// 设置默认压缩算法
    pub fn with_compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.default_compression = compression;
        self
    }
    
    /// 设置最大连接数
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }
    
    /// 启用多协议监听
    pub fn with_protocols(mut self, protocols: Vec<TransportProtocol>) -> Self {
        self.transports = Some(protocols);
        self
    }

    /// 设置默认心跳配置
    pub fn with_heartbeat(mut self, heartbeat: HeartbeatConfig) -> Self {
        self.default_heartbeat = heartbeat;
        self
    }

    /// 设置 TLS 配置
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }

    /// 设置连接超时
    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }
    
    /// 获取要使用的协议列表
    pub fn get_protocols(&self) -> Vec<TransportProtocol> {
        if let Some(ref protocols) = self.transports {
            protocols.clone()
        } else {
            vec![self.transport]
        }
    }

    /// 获取心跳间隔（向后兼容）
    pub fn heartbeat_interval(&self) -> Duration {
        self.default_heartbeat.interval
    }
}
