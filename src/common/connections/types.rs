//! 连接类型定义
//! 
//! 定义连接相关的枚举、结构体和配置

use serde::{Deserialize, Serialize};

/// 连接类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConnectionType {
    WebSocket,
    Quic,
    Tcp,
    Udp,
}

/// 连接角色
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionRole {
    Client,
    Server,
}

/// 连接状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionState {
    /// 初始化
    Initializing,
    /// 连接中
    Connecting,
    /// 已连接
    Connected,
    /// 就绪（可以开始通信）
    Ready,
    /// 断开中
    Disconnecting,
    /// 已断开
    Disconnected,
    /// 连接失败
    Failed,
    /// 重连中
    Reconnecting,
    /// 错误状态
    Error,
}

/// 协议特性
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolFeature {
    /// 支持双向通信
    Bidirectional,
    /// 支持流式传输
    Streaming,
    /// 支持可靠传输
    Reliable,
    /// 支持有序传输
    Ordered,
    /// 支持 TLS 加密
    Tls,
    /// 支持心跳
    Heartbeat,
    /// 支持重连
    Reconnection,
}

/// 连接配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    /// 连接ID
    pub id: String,
    /// 连接类型
    pub connection_type: ConnectionType,
    /// 连接角色
    pub role: ConnectionRole,
    /// 远程地址
    pub remote_addr: String,
    /// 本地地址
    pub local_addr: Option<String>,
    /// 连接超时（毫秒）
    pub timeout_ms: u64,
    /// 心跳间隔（毫秒）
    pub heartbeat_interval_ms: u64,
    /// 心跳超时（毫秒）
    pub heartbeat_timeout_ms: u64,
    /// 最大心跳丢失次数
    pub max_missed_heartbeats: u32,
    /// 是否自动重连（仅客户端）
    pub auto_reconnect: bool,
    /// 最大重连次数（仅客户端）
    pub max_reconnect_attempts: u32,
    /// 重连延迟（毫秒，仅客户端）
    pub reconnect_delay_ms: u64,
    /// 是否启用TLS
    pub enable_tls: bool,
    /// 缓冲区大小（字节）
    pub buffer_size: usize,
    /// 最大消息大小（字节）
    pub max_message_size: usize,
    /// 心跳监控超时（毫秒，仅服务端）
    pub heartbeat_monitor_timeout_ms: u64,
    /// 连接清理间隔（毫秒，仅服务端）
    pub cleanup_interval_ms: u64,
    /// 协议特定配置
    pub protocol_config: ProtocolConfig,
    /// 序列化格式（用于配置序列化）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serialization_format: Option<crate::common::serialization::SerializationFormat>,
    /// 序列化配置（用于配置序列化）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serialization_config: Option<crate::common::serialization::SerializationConfig>,
}

/// 协议特定配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolConfig {
    /// WebSocket 配置
    pub websocket: WebSocketConfig,
    /// QUIC 配置
    pub quic: QuicConfig,
    /// TCP 配置
    pub tcp: TcpConfig,
    /// UDP 配置
    pub udp: UdpConfig,
}

/// WebSocket 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketConfig {
    /// 子协议列表
    pub subprotocols: Vec<String>,
    /// 扩展列表
    pub extensions: Vec<String>,
    /// 压缩阈值
    pub compression_threshold: Option<usize>,
}

/// QUIC 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuicConfig {
    /// 最大并发流数
    pub max_concurrent_streams: u32,
    /// 初始流窗口大小
    pub initial_stream_window: u32,
    /// 连接窗口大小
    pub connection_window: u32,
    /// 拥塞控制算法
    pub congestion_control: String,
}

/// TCP 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TcpConfig {
    /// 是否启用 Nagle 算法
    pub nodelay: bool,
    /// 发送缓冲区大小
    pub send_buffer_size: Option<usize>,
    /// 接收缓冲区大小
    pub recv_buffer_size: Option<usize>,
    /// 保活时间
    pub keepalive: Option<u64>,
}

/// UDP 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdpConfig {
    /// 是否启用广播
    pub broadcast: bool,
    /// 是否启用多播
    pub multicast: bool,
    /// 多播 TTL
    pub multicast_ttl: Option<u32>,
    /// 是否启用地址重用
    pub reuse_addr: bool,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            websocket: WebSocketConfig::default(),
            quic: QuicConfig::default(),
            tcp: TcpConfig::default(),
            udp: UdpConfig::default(),
        }
    }
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            subprotocols: vec![],
            extensions: vec![],
            compression_threshold: None,
        }
    }
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            max_concurrent_streams: 100,
            initial_stream_window: 65536,
            connection_window: 262144,
            congestion_control: "bbr".to_string(),
        }
    }
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            nodelay: true,
            send_buffer_size: None,
            recv_buffer_size: None,
            keepalive: None,
        }
    }
}

impl Default for UdpConfig {
    fn default() -> Self {
        Self {
            broadcast: false,
            multicast: false,
            multicast_ttl: None,
            reuse_addr: false,
        }
    }
}

impl ConnectionConfig {
    /// 验证配置是否有效
    pub fn validate(&self) -> Result<(), String> {
        if self.id.is_empty() {
            return Err("连接ID不能为空".to_string());
        }
        
        if self.remote_addr.is_empty() {
            return Err("远程地址不能为空".to_string());
        }
        
        if self.timeout_ms == 0 {
            return Err("连接超时必须大于0".to_string());
        }
        
        if self.heartbeat_interval_ms == 0 {
            return Err("心跳间隔必须大于0".to_string());
        }
        
        if self.heartbeat_timeout_ms >= self.heartbeat_interval_ms {
            return Err("心跳超时必须小于心跳间隔".to_string());
        }
        
        if self.reconnect_delay_ms == 0 {
            return Err("重连延迟必须大于0".to_string());
        }
        
        if self.buffer_size == 0 {
            return Err("缓冲区大小必须大于0".to_string());
        }
        
        if self.max_message_size == 0 {
            return Err("最大消息大小必须大于0".to_string());
        }
        
        Ok(())
    }
    
    /// 创建客户端配置
    pub fn client(id: String, remote_addr: String) -> Self {
        Self {
            id,
            remote_addr,
            role: ConnectionRole::Client,
            ..Default::default()
        }
    }
    
    /// 创建服务端配置
    pub fn server(id: String, local_addr: String) -> Self {
        Self {
            id,
            local_addr: Some(local_addr),
            role: ConnectionRole::Server,
            auto_reconnect: false, // 服务端不需要重连
            max_reconnect_attempts: 0,
            reconnect_delay_ms: 0,
            ..Default::default()
        }
    }
    
    /// 启用TLS
    pub fn with_tls(mut self) -> Self {
        self.enable_tls = true;
        self
    }
    
    /// 设置心跳间隔
    pub fn with_heartbeat(mut self, interval_ms: u64, timeout_ms: u64) -> Self {
        self.heartbeat_interval_ms = interval_ms;
        self.heartbeat_timeout_ms = timeout_ms;
        self
    }
    
    /// 设置重连策略（仅客户端）
    pub fn with_reconnect(mut self, max_attempts: u32, delay_ms: u64) -> Self {
        if self.role == ConnectionRole::Client {
            self.max_reconnect_attempts = max_attempts;
            self.reconnect_delay_ms = delay_ms;
        }
        self
    }
    
    /// 设置缓冲区大小
    pub fn with_buffer(mut self, buffer_size: usize, max_message_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self.max_message_size = max_message_size;
        self
    }
    
    /// 设置连接类型
    pub fn with_type(mut self, connection_type: ConnectionType) -> Self {
        self.connection_type = connection_type;
        self
    }
    
    /// 设置连接超时
    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
    
    /// 设置心跳监控（仅服务端）
    pub fn with_heartbeat_monitoring(mut self, monitor_timeout_ms: u64, cleanup_interval_ms: u64) -> Self {
        if self.role == ConnectionRole::Server {
            self.heartbeat_monitor_timeout_ms = monitor_timeout_ms;
            self.cleanup_interval_ms = cleanup_interval_ms;
        }
        self
    }
    
    /// 设置 WebSocket 配置
    pub fn with_websocket_config(mut self, config: WebSocketConfig) -> Self {
        self.protocol_config.websocket = config;
        self
    }
    
    /// 设置 QUIC 配置
    pub fn with_quic_config(mut self, config: QuicConfig) -> Self {
        self.protocol_config.quic = config;
        self
    }
    
    /// 设置 TCP 配置
    pub fn with_tcp_config(mut self, config: TcpConfig) -> Self {
        self.protocol_config.tcp = config;
        self
    }
    
    /// 设置 UDP 配置
    pub fn with_udp_config(mut self, config: UdpConfig) -> Self {
        self.protocol_config.udp = config;
        self
    }
    
    /// 设置序列化格式
    pub fn with_serialization_format(mut self, format: crate::common::serialization::SerializationFormat) -> Self {
        self.serialization_format = Some(format);
        self
    }
    
    /// 设置序列化配置
    pub fn with_serialization_config(mut self, config: crate::common::serialization::SerializationConfig) -> Self {
        self.serialization_config = Some(config);
        self
    }
    
    /// 使用JSON序列化格式
    pub fn with_json_serialization(mut self) -> Self {
        self.serialization_format = Some(crate::common::serialization::SerializationFormat::Json);
        self
    }
    
    /// 使用Bincode序列化格式（高性能）
    pub fn with_bincode_serialization(mut self) -> Self {
        self.serialization_format = Some(crate::common::serialization::SerializationFormat::Bincode);
        self
    }
    
    /// 使用美化JSON序列化格式（调试友好）
    pub fn with_pretty_json_serialization(mut self) -> Self {
        self.serialization_format = Some(crate::common::serialization::SerializationFormat::Json);
        let mut config = self.serialization_config.unwrap_or_default();
        config.pretty_format = true;
        self.serialization_config = Some(config);
        self
    }
    
    /// 创建高性能配置（适合高吞吐量场景）
    pub fn high_performance(id: String, remote_addr: String) -> Self {
        Self {
            id,
            remote_addr,
            role: ConnectionRole::Client,
            buffer_size: 262144,        // 256KB
            max_message_size: 16777216, // 16MB
            heartbeat_interval_ms: 15000, // 15秒
            ..Default::default()
        }
    }
    
    /// 创建低延迟配置（适合实时通信场景）
    pub fn low_latency(id: String, remote_addr: String) -> Self {
        Self {
            id,
            remote_addr,
            role: ConnectionRole::Client,
            buffer_size: 32768,         // 32KB
            max_message_size: 1048576,  // 1MB
            heartbeat_interval_ms: 10000, // 10秒
            heartbeat_timeout_ms: 5000,   // 5秒
            ..Default::default()
        }
    }
    
    /// 创建稳定连接配置（适合长时间连接场景）
    pub fn stable(id: String, remote_addr: String) -> Self {
        Self {
            id,
            remote_addr,
            role: ConnectionRole::Client,
            auto_reconnect: true,
            max_reconnect_attempts: 10,
            reconnect_delay_ms: 2000,     // 2秒
            heartbeat_interval_ms: 60000,   // 1分钟
            heartbeat_timeout_ms: 20000,    // 20秒
            max_missed_heartbeats: 5,
            ..Default::default()
        }
    }
    
    /// 创建服务端配置（适合高并发场景）
    pub fn server_high_concurrency(id: String, local_addr: String) -> Self {
        Self {
            id,
            local_addr: Some(local_addr),
            role: ConnectionRole::Server,
            buffer_size: 131072,        // 128KB
            max_message_size: 8388608,  // 8MB
            heartbeat_monitor_timeout_ms: 30000, // 30秒
            cleanup_interval_ms: 60000,   // 1分钟
            ..Default::default()
        }
    }
    
    /// 获取序列化格式（带默认值）
    pub fn get_serialization_format(&self) -> crate::common::serialization::SerializationFormat {
        self.serialization_format.unwrap_or(crate::common::serialization::SerializationFormat::Json)
    }
    
    /// 获取序列化配置（带默认值）
    pub fn get_serialization_config(&self) -> crate::common::serialization::SerializationConfig {
        self.serialization_config.clone().unwrap_or_default()
    }
    
    /// 获取协议特性
    pub fn get_protocol_features(&self) -> Vec<ProtocolFeature> {
        match self.connection_type {
            ConnectionType::WebSocket => {
                let mut features = vec![
                    ProtocolFeature::Bidirectional,
                    ProtocolFeature::Streaming,
                    ProtocolFeature::Reliable,
                    ProtocolFeature::Ordered,
                    ProtocolFeature::Heartbeat,
                ];
                if self.enable_tls {
                    features.push(ProtocolFeature::Tls);
                }
                features
            }
            ConnectionType::Quic => {
                let mut features = vec![
                    ProtocolFeature::Bidirectional,
                    ProtocolFeature::Streaming,
                    ProtocolFeature::Reliable,
                    ProtocolFeature::Ordered,
                    ProtocolFeature::Heartbeat,
                ];
                if self.enable_tls {
                    features.push(ProtocolFeature::Tls);
                }
                features
            }
            ConnectionType::Tcp => {
                let mut features = vec![
                    ProtocolFeature::Bidirectional,
                    ProtocolFeature::Streaming,
                    ProtocolFeature::Reliable,
                    ProtocolFeature::Ordered,
                    ProtocolFeature::Heartbeat,
                ];
                if self.enable_tls {
                    features.push(ProtocolFeature::Tls);
                }
                features
            }
            ConnectionType::Udp => {
                vec![
                    ProtocolFeature::Bidirectional,
                    ProtocolFeature::Streaming,
                ]
            }
        }
    }
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            connection_type: ConnectionType::WebSocket,
            role: ConnectionRole::Client,
            remote_addr: String::new(),
            local_addr: None,
            timeout_ms: 30000, // 30秒
            heartbeat_interval_ms: 30000, // 30秒
            heartbeat_timeout_ms: 10000, // 10秒
            max_missed_heartbeats: 3,
            auto_reconnect: true,
            max_reconnect_attempts: 5,
            reconnect_delay_ms: 1000, // 1秒
            enable_tls: false,
            buffer_size: 65536, // 64KB
            max_message_size: 1048576, // 1MB
            heartbeat_monitor_timeout_ms: 60000, // 1分钟
            cleanup_interval_ms: 300000, // 5分钟
            protocol_config: ProtocolConfig::default(),
            serialization_format: Some(crate::common::serialization::SerializationFormat::Json),
            serialization_config: Some(crate::common::serialization::SerializationConfig::default()),
        }
    }
}

/// 连接质量等级
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ConnectionQuality {
    /// 优秀 (90-100)
    Excellent = 100,
    /// 良好 (80-89)
    Good = 89,
    /// 一般 (70-79)
    Fair = 79,
    /// 较差 (60-69)
    Poor = 69,
    /// 很差 (0-59)
    VeryPoor = 59,
}

impl From<u8> for ConnectionQuality {
    fn from(score: u8) -> Self {
        match score {
            90..=100 => ConnectionQuality::Excellent,
            80..=89 => ConnectionQuality::Good,
            70..=79 => ConnectionQuality::Fair,
            60..=69 => ConnectionQuality::Poor,
            _ => ConnectionQuality::VeryPoor,
        }
    }
}

impl ConnectionQuality {
    /// 获取质量描述
    pub fn description(&self) -> &'static str {
        match self {
            ConnectionQuality::Excellent => "优秀",
            ConnectionQuality::Good => "良好",
            ConnectionQuality::Fair => "一般",
            ConnectionQuality::Poor => "较差",
            ConnectionQuality::VeryPoor => "很差",
        }
    }
    
    /// 获取质量评分
    pub fn score(&self) -> u8 {
        *self as u8
    }
}
