use crate::common::connections::enums::Transport;
use crate::common::parsing::PayloadCodec;

/// WebSocket配置
#[derive(Debug, Clone, Default)]
pub struct WebSocketConfig {
    /// 子协议
    pub subprotocols: Vec<String>,
    /// 扩展
    pub extensions: Vec<String>,
    /// 压缩阈值
    pub compression_threshold: Option<usize>,
}

/// QUIC客户端配置
#[derive(Debug, Clone, Default)]
pub struct QuicClientConfig {
    /// 服务端证书路径
    pub server_cert_path: Option<String>,
    /// 跳过服务端验证
    pub skip_server_verification: bool,
    /// 客户端证书路径
    pub client_cert_path: Option<String>,
    /// 客户端密钥路径
    pub client_key_path: Option<String>,
}

/// QUIC服务端配置
#[derive(Debug, Clone, Default)]
pub struct QuicServerConfig {
    /// 证书路径
    pub cert_path: Option<String>,
    /// 密钥路径
    pub key_path: Option<String>,
    /// 要求客户端认证
    pub require_client_auth: bool,
    /// 客户端CA证书路径
    pub client_ca_cert_path: Option<String>,
}

/// QUIC配置
#[derive(Debug, Clone, Default)]
pub struct QuicConfig {
    /// 客户端配置
    pub client: Option<QuicClientConfig>,
    /// 服务端配置
    pub server: Option<QuicServerConfig>,
}

/// 协议配置
#[derive(Debug, Clone, Default)]
pub struct ProtocolConfig {
    /// WebSocket配置
    pub websocket: Option<WebSocketConfig>,
    /// QUIC配置
    pub quic: Option<QuicConfig>,
}

/// 连接配置
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// 连接ID
    pub id: Option<String>,
    /// 传输协议
    pub transport: Transport,
    /// 远程地址
    pub remote_addr: Option<String>,
    /// 心跳间隔（毫秒）
    pub heartbeat_interval_ms: Option<u64>,
    /// 心跳超时（毫秒）
    pub heartbeat_timeout_ms: Option<u64>,
    /// 最大丢失心跳数
    pub max_missed_heartbeats: Option<u32>,
    /// 序列化编解码器
    pub serialization_codec: Option<PayloadCodec>,
    /// 协议子配置
    pub protocol_config: Option<ProtocolConfig>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            id: None,
            transport: Transport::Quic,
            remote_addr: None,
            heartbeat_interval_ms: None,
            heartbeat_timeout_ms: None,
            max_missed_heartbeats: None,
            serialization_codec: None,
            protocol_config: None,
        }
    }
}