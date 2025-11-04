//! 客户端配置模块

use crate::common::config_types::{TransportProtocol, TlsConfig, HeartbeatConfig};
use crate::common::protocol::SerializationFormat;
use crate::common::compression::CompressionAlgorithm;
use std::time::Duration;

/// 客户端配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClientConfig {
    /// 服务器地址
    pub server_url: String,
    /// 传输协议（单个）
    pub transport: TransportProtocol,
    /// 传输协议列表（用于竞速，如果设置了此列表，transport 将被忽略）
    pub transports: Option<Vec<TransportProtocol>>,
    /// 协议竞速超时时间（如果启用多协议竞速）
    pub race_timeout: Option<Duration>,
    /// 序列化格式
    pub serialization_format: SerializationFormat,
    /// 压缩算法
    pub compression: CompressionAlgorithm,
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
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_url: "ws://localhost:8080".to_string(),
            transport: TransportProtocol::WebSocket,
            transports: None,
            race_timeout: Some(Duration::from_secs(5)),
            serialization_format: SerializationFormat::Protobuf,
            compression: CompressionAlgorithm::None,
            connect_timeout: Duration::from_secs(30),
            reconnect_interval: Duration::from_secs(5),
            max_reconnect_attempts: Some(5),
            heartbeat: HeartbeatConfig::default(),
            tls: TlsConfig::none(),
            connection_id: None,
            user_id: None,
            metadata: std::collections::HashMap::new(),
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
    
    /// 启用多协议竞速
    pub fn with_protocol_race(mut self, protocols: Vec<TransportProtocol>) -> Self {
        self.transports = Some(protocols);
        self
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

    /// 获取心跳间隔（向后兼容）
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat.interval
    }
}
