//! 服务端配置模块

use crate::common::compression::CompressionAlgorithm;
use crate::common::config_types::{HeartbeatConfig, TlsConfig, TransportProtocol};
use crate::common::device::DeviceConflictStrategy;
use crate::common::encryption::EncryptionAlgorithm;
use crate::common::protocol::SerializationFormat;
use std::time::Duration;

/// 服务端配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServerConfig {
    /// 监听地址（单个协议时使用，多协议时用作默认地址）
    pub bind_address: String,
    /// 传输协议（单个）
    pub transport: TransportProtocol,
    /// 传输协议列表（用于同时监听多个协议）
    pub transports: Option<Vec<TransportProtocol>>,
    /// 每个协议的独立地址配置（协议 -> 地址映射）
    /// 如果设置了此映射，每个协议将使用对应的地址
    pub protocol_addresses: Option<std::collections::HashMap<TransportProtocol, String>>,
    /// 序列化格式（默认格式）
    pub default_serialization_format: SerializationFormat,
    /// 压缩算法（默认）
    pub default_compression: CompressionAlgorithm,
    /// 加密算法（默认）
    pub default_encryption: EncryptionAlgorithm,
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
    /// 设备冲突策略（用于多端设备管理）
    pub device_conflict_strategy: DeviceConflictStrategy,
    /// 是否启用认证（如果启用，连接必须通过 token 验证才能收发消息）
    pub auth_enabled: bool,
    /// 认证超时时间（连接建立后，如果在此时间内未完成认证，连接将被关闭）
    pub auth_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".to_string(),
            transport: TransportProtocol::WebSocket,
            transports: None,
            protocol_addresses: None,
            default_serialization_format: SerializationFormat::Protobuf,
            default_compression: CompressionAlgorithm::None,
            default_encryption: EncryptionAlgorithm::None,
            max_connections: 10000,
            connection_timeout: Duration::from_secs(300),
            default_heartbeat: HeartbeatConfig::default(),
            tls: TlsConfig::none(),
            max_message_size: 10 * 1024 * 1024, // 10MB
            device_conflict_strategy: DeviceConflictStrategy::default(),
            auth_enabled: false,                   // 默认不启用认证
            auth_timeout: Duration::from_secs(30), // 默认认证超时 30 秒
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

    /// 设置默认加密算法
    pub fn with_encryption(mut self, encryption: EncryptionAlgorithm) -> Self {
        self.default_encryption = encryption;
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

    /// 为特定协议设置监听地址
    pub fn with_protocol_address(mut self, protocol: TransportProtocol, address: String) -> Self {
        if self.protocol_addresses.is_none() {
            self.protocol_addresses = Some(std::collections::HashMap::new());
        }
        if let Some(ref mut addresses) = self.protocol_addresses {
            addresses.insert(protocol, address);
        }
        self
    }

    /// 批量设置协议地址映射
    pub fn with_protocol_addresses(
        mut self,
        addresses: std::collections::HashMap<TransportProtocol, String>,
    ) -> Self {
        self.protocol_addresses = Some(addresses);
        self
    }

    /// 获取指定协议的地址
    pub fn get_protocol_address(&self, protocol: &TransportProtocol) -> String {
        if let Some(ref addresses) = self.protocol_addresses {
            if let Some(addr) = addresses.get(protocol) {
                return addr.clone();
            }
        }
        self.bind_address.clone()
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

    /// 设置设备冲突策略
    pub fn with_device_conflict_strategy(mut self, strategy: DeviceConflictStrategy) -> Self {
        self.device_conflict_strategy = strategy;
        self
    }

    /// 启用认证
    ///
    /// 启用后，所有连接必须通过 token 验证才能收发消息
    pub fn enable_auth(mut self) -> Self {
        self.auth_enabled = true;
        self
    }

    /// 禁用认证（默认）
    pub fn disable_auth(mut self) -> Self {
        self.auth_enabled = false;
        self
    }

    /// 设置认证超时时间
    ///
    /// 连接建立后，如果在此时间内未完成认证，连接将被关闭
    /// 默认值为 30 秒
    pub fn with_auth_timeout(mut self, timeout: Duration) -> Self {
        self.auth_timeout = timeout;
        self
    }
}
