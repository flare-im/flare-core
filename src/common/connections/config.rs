//! 连接配置定义
//! 
//! 定义连接相关的配置结构体

use serde::{Deserialize, Serialize};

use crate::common::connections::types::{ConnectionRole, Platform, Transport};

/// 客户端特有配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientSpecificConfig {
     /// 是否启用TLS
    pub enable_tls: bool,
    /// 是否自动重连
    pub auto_reconnect: bool,
    /// 最大重连次数
    pub max_reconnect_attempts: u32,
    /// 重连延迟（毫秒）
    pub reconnect_delay_ms: u64,
    /// 用户id
    pub user_id: Option<String>,
    /// 平台
    pub platform: Option<Platform>,
    /// token
    pub token: Option<String>,
}

impl Default for ClientSpecificConfig {
    fn default() -> Self {
        Self {
            enable_tls: false,
            auto_reconnect: true,
            max_reconnect_attempts: 5,
            reconnect_delay_ms: 1000, // 1秒
            user_id: None,
            platform: Some(Platform::Web),
            token: None,
        }
    }
}

/// 服务端特有配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSpecificConfig {
     /// 是否自动回复心跳响应
    pub auto_heartbeat_response: bool,
    /// 心跳监控超时（毫秒）
    pub heartbeat_monitor_timeout_ms: u64,
    /// 连接清理间隔（毫秒）
    pub cleanup_interval_ms: u64,
}

impl Default for ServerSpecificConfig {
    fn default() -> Self {
        Self {
            auto_heartbeat_response: true,
            heartbeat_monitor_timeout_ms: 60000, // 1分钟
            cleanup_interval_ms: 300000, // 5分钟
        }
    }
}

/// 连接配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    /// 连接ID
    pub id: String,
    /// 连接角色
    pub role: ConnectionRole,
    /// 传输类型
    pub transport: Transport,
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
    /// 缓冲区大小（字节）
    pub buffer_size: usize,
    /// 最大消息大小（字节）
    pub max_message_size: usize,
    /// 客户端特有配置（仅在客户端角色时使用）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_config: Option<ClientSpecificConfig>,
    /// 服务端特有配置（仅在服务端角色时使用）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_config: Option<ServerSpecificConfig>,
    /// 协议特定配置
    pub protocol_config: ProtocolConfig,
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

/// QUIC 客户端配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuicClientConfig {
    /// 最大并发流数
    pub max_concurrent_streams: u32,
    /// 初始流窗口大小
    pub initial_stream_window: u32,
    /// 连接窗口大小
    pub connection_window: u32,
    /// 拥塞控制算法
    pub congestion_control: String,
    /// 服务器证书路径（用于验证服务器证书）
    pub server_cert_path: Option<String>,
    /// 是否跳过服务器证书验证（仅用于测试）
    pub skip_server_verification: bool,
    /// 服务器主机名（用于 SNI 和证书验证）
    pub server_hostname: Option<String>,
}

/// QUIC 服务端配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuicServerConfig {
    /// 最大并发流数
    pub max_concurrent_streams: u32,
    /// 初始流窗口大小
    pub initial_stream_window: u32,
    /// 连接窗口大小
    pub connection_window: u32,
    /// 拥塞控制算法
    pub congestion_control: String,
    /// 服务端证书路径
    pub cert_path: String,
    /// 服务端私钥路径
    pub key_path: String,
    /// 服务端主机名（用于证书验证）
    pub server_hostname: Option<String>,
}

/// QUIC 配置（统一配置，根据角色使用不同部分）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuicConfig {
    /// 客户端配置
    pub client: QuicClientConfig,
    /// 服务端配置
    pub server: QuicServerConfig,
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

impl Default for QuicClientConfig {
    fn default() -> Self {
        Self {
            max_concurrent_streams: 100,
            initial_stream_window: 65536,
            connection_window: 262144,
            congestion_control: "bbr".to_string(),
            server_cert_path: None,
            skip_server_verification: false,
            server_hostname: Some("localhost".to_string()),
        }
    }
}

impl Default for QuicServerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_streams: 100,
            initial_stream_window: 65536,
            connection_window: 262144,
            congestion_control: "bbr".to_string(),
            cert_path: "certs/server.crt".to_string(),
            key_path: "certs/server.key".to_string(),
            server_hostname: Some("localhost".to_string()),
        }
    }
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            client: QuicClientConfig::default(),
            server: QuicServerConfig::default(),
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
        
        if self.remote_addr.is_empty() && self.role == ConnectionRole::Client {
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
        
        if self.buffer_size == 0 {
            return Err("缓冲区大小必须大于0".to_string());
        }
        
        if self.max_message_size == 0 {
            return Err("最大消息大小必须大于0".to_string());
        }
        
        // 验证客户端特有配置
        if self.role == ConnectionRole::Client {
            if let Some(client_config) = &self.client_config {
                if client_config.reconnect_delay_ms == 0 {
                    return Err("重连延迟必须大于0".to_string());
                }
            }
        }
        
        Ok(())
    }
    
    /// 设置是否自动回复心跳响应
    pub fn with_auto_heartbeat_response(mut self, auto_response: bool) -> Self {
        if let Some(server_config) = &mut self.server_config {
            server_config.auto_heartbeat_response = auto_response;
        }
        self
    }
    
    /// 检查是否启用了自动心跳响应
    pub fn is_auto_heartbeat_response_enabled(&self) -> bool {
        if let Some(server_config) = &self.server_config {
            server_config.auto_heartbeat_response
        } else {
            false
        }
    }
    
    /// 创建客户端连接配置
    pub fn client(id: String, remote_addr: String) -> Self {
        Self {
            id,
            role: ConnectionRole::Client,
            transport: Transport::WebSocket,
            remote_addr,
            local_addr: None,
            timeout_ms: 30000,
            heartbeat_interval_ms: 30000,
            heartbeat_timeout_ms: 10000,
            max_missed_heartbeats: 3,
            buffer_size: 65536,
            max_message_size: 1048576,
            client_config: Some(ClientSpecificConfig {
                auto_reconnect: true,
                max_reconnect_attempts: 3,
                reconnect_delay_ms: 1000,
                ..Default::default()
            }),
            server_config: None,
            protocol_config: ProtocolConfig::default(),
            serialization_config: None,
        }
    }
    
    /// 创建服务端连接配置
    pub fn server(id: String, local_addr: String) -> Self {
        Self {
            id,
            role: ConnectionRole::Server,
            transport: Transport::WebSocket,
            remote_addr: String::new(),
            local_addr: Some(local_addr),
            timeout_ms: 30000,
            heartbeat_interval_ms: 30000,
            heartbeat_timeout_ms: 10000,
            max_missed_heartbeats: 3,
            buffer_size: 65536,
            max_message_size: 1048576,
            client_config: None,
            server_config: Some(ServerSpecificConfig::default()),
            protocol_config: ProtocolConfig::default(),
            serialization_config: None,
        }
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
            if let Some(client_config) = &mut self.client_config {
                client_config.max_reconnect_attempts = max_attempts;
                client_config.reconnect_delay_ms = delay_ms;
            } else {
                self.client_config = Some(ClientSpecificConfig {
                    auto_reconnect: true,
                    max_reconnect_attempts: max_attempts,
                    reconnect_delay_ms: delay_ms,
                    ..Default::default()
                });
            }
        }
        self
    }
    
    /// 设置缓冲区大小
    pub fn with_buffer(mut self, buffer_size: usize, max_message_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self.max_message_size = max_message_size;
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
            if let Some(server_config) = &mut self.server_config {
                server_config.heartbeat_monitor_timeout_ms = monitor_timeout_ms;
                server_config.cleanup_interval_ms = cleanup_interval_ms;
            } else {
                self.server_config = Some(ServerSpecificConfig {
                    auto_heartbeat_response: true,  // 添加缺失的字段
                    heartbeat_monitor_timeout_ms: monitor_timeout_ms,
                    cleanup_interval_ms,
                });
            }
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
    
    /// 设置序列化配置
    pub fn with_serialization_config(mut self, config: crate::common::serialization::SerializationConfig) -> Self {
        self.serialization_config = Some(config);
        self
    }
    
    /// 设置远程地址
    pub fn with_remote_addr(mut self, remote_addr: String) -> Self {
        self.remote_addr = remote_addr;
        self
    }
    
    /// 创建高性能配置（适合高吞吐量场景）
    pub fn high_performance(id: String, remote_addr: String) -> Self {
        Self {
            id,
            remote_addr,
            role: ConnectionRole::Client,
            transport: Transport::Quic,
            buffer_size: 262144,        // 256KB
            max_message_size: 16777216, // 16MB
            heartbeat_interval_ms: 15000, // 15秒
            client_config: Some(ClientSpecificConfig {
                auto_reconnect: true,
                max_reconnect_attempts: 5,
                reconnect_delay_ms: 1000,
                ..Default::default()
            }),
            server_config: None,
            ..Default::default()
        }
    }
    
    /// 创建低延迟配置（适合实时通信场景）
    pub fn low_latency(id: String, remote_addr: String) -> Self {
        Self {
            id,
            remote_addr,
            role: ConnectionRole::Client,
            transport: Transport::WebSocket,
            buffer_size: 32768,         // 32KB
            max_message_size: 1048576,  // 1MB
            heartbeat_interval_ms: 10000, // 10秒
            heartbeat_timeout_ms: 5000,   // 5秒
            client_config: Some(ClientSpecificConfig {
                auto_reconnect: true,
                max_reconnect_attempts: 5,
                reconnect_delay_ms: 1000,
                ..Default::default()
            }),
            server_config: None,
            ..Default::default()
        }
    }
    
    /// 创建稳定连接配置（适合长时间连接场景）
    pub fn stable(id: String, remote_addr: String) -> Self {
        Self {
            id,
            remote_addr,
            role: ConnectionRole::Client,
            transport: Transport::WebSocket,
            client_config: Some(ClientSpecificConfig {
                auto_reconnect: true,
                max_reconnect_attempts: 10,
                reconnect_delay_ms: 2000,     // 2秒
                ..Default::default()
            }),
            server_config: None,
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
            server_config: Some(ServerSpecificConfig::default()),
            client_config: None,
            ..Default::default()
        }
    }
    
    /// 获取序列化配置（带默认值）
    pub fn get_serialization_config(&self) -> crate::common::serialization::SerializationConfig {
        self.serialization_config.clone().unwrap_or_default()
    }
    
    /// 获取协议特性
    pub fn get_protocol_features(&self) -> Vec<crate::common::connections::enums::ProtocolFeature> {
        // 注意：这里需要根据实际的连接类型实现获取协议特性
        vec![] // 简化实现，实际应该根据连接类型返回对应的特性
    }
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            role: ConnectionRole::Client,
            transport: Transport::WebSocket,
            remote_addr: String::new(),
            local_addr: None,
            timeout_ms: 30000, // 30秒
            heartbeat_interval_ms: 30000, // 30秒
            heartbeat_timeout_ms: 10000, // 10秒
            max_missed_heartbeats: 3,
            buffer_size: 65536, // 64KB
            max_message_size: 1048576, // 1MB
            client_config: Some(ClientSpecificConfig::default()),
            server_config: None,
            protocol_config: ProtocolConfig::default(),
            serialization_config: Some(crate::common::serialization::SerializationConfig::default()),
        }
    }
}