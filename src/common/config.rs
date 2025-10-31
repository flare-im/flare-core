//! 配置模块
//! 
//! 定义客户端和服务端的配置结构

use crate::common::protocol::SerializationFormat;
use crate::common::compression::CompressionAlgorithm;
use std::time::Duration;

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
    /// 最大重连次数
    pub max_reconnect_attempts: u32,
    /// 心跳间隔
    pub heartbeat_interval: Duration,
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
            max_reconnect_attempts: 5,
            heartbeat_interval: Duration::from_secs(30),
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
    /// 心跳间隔
    pub heartbeat_interval: Duration,
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
            heartbeat_interval: Duration::from_secs(30),
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
    
    /// 获取要使用的协议列表
    pub fn get_protocols(&self) -> Vec<TransportProtocol> {
        if let Some(ref protocols) = self.transports {
            protocols.clone()
        } else {
            vec![self.transport]
        }
    }
}

