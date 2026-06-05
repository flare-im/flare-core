//! 客户端配置模块

use crate::common::compression::CompressionAlgorithm;
use crate::common::config_types::{HeartbeatConfig, TlsConfig, TransportProtocol};
use crate::common::device::DeviceInfo;
use crate::common::protocol::SerializationFormat;
use std::time::Duration;

/// 客户端配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClientConfig {
    /// 服务器地址（单个协议时使用，多协议时用作默认地址）
    pub server_url: String,
    /// 传输协议（单个）
    pub transport: TransportProtocol,
    /// 传输协议列表（用于竞速，如果设置了此列表，transport 将被忽略）
    /// 列表顺序就是优先级顺序，前面的优先级更高
    pub transports: Option<Vec<TransportProtocol>>,
    /// 每个协议的独立地址配置（协议 -> 地址映射）
    /// 如果设置了此映射，每个协议将使用对应的地址
    pub protocol_urls: Option<std::collections::HashMap<TransportProtocol, String>>,
    /// 协议竞速超时时间（如果启用多协议竞速）
    pub race_timeout: Option<Duration>,
    /// 序列化格式（首选格式，用于协商）
    pub serialization_format: SerializationFormat,
    /// 压缩算法（首选算法，用于协商）
    pub compression: CompressionAlgorithm,
    /// 强制指定的序列化格式（如果设置，则强制使用此格式，不进行协商）
    /// 适用于某些端不支持 protobuf 等格式的场景
    pub force_serialization_format: Option<SerializationFormat>,
    /// 强制指定的压缩算法（如果设置，则强制使用此算法，不进行协商）
    pub force_compression: Option<CompressionAlgorithm>,
    /// 连接超时时间
    pub connect_timeout: Duration,
    /// 重连间隔
    pub reconnect_interval: Duration,
    /// 最大重连次数（None 表示无限重连）
    pub max_reconnect_attempts: Option<u32>,
    /// 心跳配置
    pub heartbeat: HeartbeatConfig,
    /// TLS 配置
    pub tls: TlsConfig,
    /// 自定义连接 ID（如果为 None，则自动生成）
    pub connection_id: Option<String>,
    /// 用户 ID（用于认证）
    pub user_id: Option<String>,
    /// 额外的元数据
    pub metadata: std::collections::HashMap<String, String>,
    /// 是否启用消息路由（默认 false）
    pub enable_router: bool,
    /// 设备信息（用于协商和设备管理）
    pub device_info: Option<DeviceInfo>,
    /// Token（用于认证，如果服务端启用认证，必须提供）
    pub token: Option<String>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_url: "ws://localhost:8080".to_string(),
            transport: TransportProtocol::WebSocket,
            transports: None,
            protocol_urls: None,
            race_timeout: Some(Duration::from_secs(5)),
            // 默认使用 JSON 序列化，不压缩（客户端可以在协商时指定首选格式）
            serialization_format: SerializationFormat::Json,
            compression: CompressionAlgorithm::None,
            force_serialization_format: None,
            force_compression: None,
            connect_timeout: Duration::from_secs(30),
            reconnect_interval: Duration::from_secs(5),
            max_reconnect_attempts: Some(5),
            heartbeat: HeartbeatConfig::default(),
            tls: TlsConfig::none(),
            connection_id: None,
            user_id: None,
            metadata: std::collections::HashMap::new(),
            enable_router: false,
            device_info: None,
            token: None,
        }
    }
}

impl ClientConfig {
    /// 创建新的客户端配置
    pub fn new(server_url: String) -> Self {
        Self {
            server_url,
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

    /// 使用 TCP 协议
    pub fn tcp(mut self) -> Self {
        self.transport = TransportProtocol::TCP;
        self
    }

    /// 设置序列化格式
    pub fn with_format(mut self, format: SerializationFormat) -> Self {
        self.serialization_format = format;
        self
    }

    /// 设置压缩算法
    pub fn with_compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.compression = compression;
        self
    }

    /// 设置用户 ID
    pub fn with_user_id(mut self, user_id: String) -> Self {
        self.user_id = Some(user_id);
        self
    }

    /// 设置 Token（用于认证，如果服务端启用认证，必须提供）
    pub fn with_token(mut self, token: String) -> Self {
        self.token = Some(token);
        self
    }

    /// 启用多协议竞速
    ///
    /// 协议列表的顺序就是优先级顺序，前面的协议优先级更高
    /// 例如：with_protocol_race(vec![QUIC, WebSocket]) 表示 QUIC 优先级高于 WebSocket
    pub fn with_protocol_race(mut self, protocols: Vec<TransportProtocol>) -> Self {
        self.transports = Some(protocols);
        self
    }

    /// 为特定协议设置服务器地址
    pub fn with_protocol_url(mut self, protocol: TransportProtocol, url: String) -> Self {
        if self.protocol_urls.is_none() {
            self.protocol_urls = Some(std::collections::HashMap::new());
        }
        if let Some(ref mut urls) = self.protocol_urls {
            urls.insert(protocol, url);
        }
        self
    }

    /// 批量设置协议地址映射
    pub fn with_protocol_urls(
        mut self,
        urls: std::collections::HashMap<TransportProtocol, String>,
    ) -> Self {
        self.protocol_urls = Some(urls);
        self
    }

    /// 获取指定协议的地址
    pub fn get_protocol_url(&self, protocol: &TransportProtocol) -> String {
        let endpoint = if let Some(ref urls) = self.protocol_urls
            && let Some(url) = urls.get(protocol)
        {
            url.as_str()
        } else {
            self.server_url.as_str()
        };

        protocol.normalize_client_url(endpoint)
    }

    /// 设置竞速超时时间
    pub fn with_race_timeout(mut self, timeout: Duration) -> Self {
        self.race_timeout = Some(timeout);
        self
    }

    /// 设置心跳配置
    pub fn with_heartbeat(mut self, heartbeat: HeartbeatConfig) -> Self {
        self.heartbeat = heartbeat;
        self
    }

    /// 设置 TLS 配置
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }

    /// 设置连接超时
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// 设置重连间隔
    pub fn with_reconnect_interval(mut self, interval: Duration) -> Self {
        self.reconnect_interval = interval;
        self
    }

    /// 设置最大重连次数（None 表示无限重连）
    pub fn with_max_reconnect_attempts(mut self, max: Option<u32>) -> Self {
        self.max_reconnect_attempts = max;
        self
    }

    /// 启用消息路由
    pub fn enable_router(mut self) -> Self {
        self.enable_router = true;
        self
    }

    /// 设置设备信息（用于协商和设备管理）
    pub fn with_device_info(mut self, device_info: DeviceInfo) -> Self {
        self.device_info = Some(device_info);
        self
    }

    /// 强制指定序列化格式（不进行协商，直接使用此格式）
    ///
    /// 适用于某些端不支持 protobuf 等格式的场景
    /// 如果设置了强制格式，客户端将直接使用此格式，服务端必须接受
    pub fn force_format(mut self, format: SerializationFormat) -> Self {
        self.force_serialization_format = Some(format);
        self
    }

    /// 强制指定压缩算法（不进行协商，直接使用此算法）
    ///
    /// 如果设置了强制压缩，客户端将直接使用此算法，服务端必须接受
    pub fn force_compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.force_compression = Some(compression);
        self
    }

    /// 检查是否强制指定了格式（不进行协商）
    pub fn is_force_format(&self) -> bool {
        self.force_serialization_format.is_some() || self.force_compression.is_some()
    }

    /// 获取实际使用的序列化格式（强制格式优先，否则使用首选格式）
    pub fn get_serialization_format(&self) -> SerializationFormat {
        self.force_serialization_format
            .unwrap_or(self.serialization_format)
    }

    /// 获取实际使用的压缩算法（强制算法优先，否则使用首选算法）
    pub fn get_compression(&self) -> CompressionAlgorithm {
        self.force_compression
            .clone()
            .unwrap_or_else(|| self.compression.clone())
    }

    /// 获取要使用的协议列表
    pub fn get_protocols(&self) -> Vec<TransportProtocol> {
        if let Some(ref protocols) = self.transports {
            protocols.clone()
        } else {
            vec![self.transport]
        }
    }

    /// 是否启用协议竞速
    pub fn is_race_mode(&self) -> bool {
        self.transports.is_some() && self.transports.as_ref().unwrap().len() > 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_protocol_specific_urls_from_default_websocket_url() {
        let config = ClientConfig::default();

        assert_eq!(
            config.get_protocol_url(&TransportProtocol::WebSocket),
            "ws://localhost:8080"
        );
        assert_eq!(
            config.get_protocol_url(&TransportProtocol::QUIC),
            "quic://localhost:8080"
        );
        assert_eq!(
            config.get_protocol_url(&TransportProtocol::TCP),
            "tcp://localhost:8080"
        );
    }

    #[test]
    fn normalizes_explicit_bare_protocol_url() {
        let config = ClientConfig::default()
            .with_protocol_url(TransportProtocol::TCP, "127.0.0.1:19090".to_string());

        assert_eq!(
            config.get_protocol_url(&TransportProtocol::TCP),
            "tcp://127.0.0.1:19090"
        );
    }
}
