//! 常量定义模块
//! 
//! 定义项目中使用的各种常量，包括默认值、超时时间、限制等

/// 默认连接超时时间（秒）
pub const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 300;

/// 默认心跳间隔（秒）
pub const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 30;

/// 默认消息大小限制（字节）
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024; // 10MB

/// 默认连接数限制
pub const DEFAULT_MAX_CONNECTIONS: usize = 10000;

/// 默认重连间隔（秒）
pub const DEFAULT_RECONNECT_INTERVAL_SECS: u64 = 5;

/// 最大重连次数
pub const DEFAULT_MAX_RECONNECT_ATTEMPTS: u32 = 5;

/// 默认缓冲区大小（字节）
pub const DEFAULT_BUFFER_SIZE: usize = 8192;

/// 协议版本
pub const PROTOCOL_VERSION: &str = "1.0.0";

/// 支持的协议版本列表
pub const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &["1.0.0"];

/// 默认压缩算法
pub const DEFAULT_COMPRESSION: &str = "none";

/// 默认序列化格式
pub const DEFAULT_SERIALIZATION_FORMAT: &str = "protobuf";

/// 元数据键名常量
pub mod metadata_keys {
    /// 会话 ID
    pub const SESSION_ID: &str = "session_id";
    /// 用户 ID
    pub const USER_ID: &str = "user_id";
    /// 压缩算法
    pub const COMPRESSION: &str = "compression";
    /// 序列化格式
    pub const FORMAT: &str = "format";
    /// 优先级
    pub const PRIORITY: &str = "priority";
    /// 是否加密
    pub const ENCRYPTED: &str = "encrypted";
    /// 客户端版本
    pub const CLIENT_VERSION: &str = "client_version";
    /// 服务器版本
    pub const SERVER_VERSION: &str = "server_version";
}

/// 可靠性等级常量
pub mod reliability {
    use crate::common::protocol::Reliability;
    
    /// 默认可靠性等级
    pub const DEFAULT: Reliability = Reliability::BestEffort;
    
    /// 关键消息推荐使用的可靠性等级
    pub const CRITICAL: Reliability = Reliability::ExactlyOnce;
}

/// 错误消息常量
pub mod error_messages {
    /// 连接失败
    pub const CONNECTION_FAILED: &str = "Connection failed";
    /// 连接超时
    pub const CONNECTION_TIMEOUT: &str = "Connection timeout";
    /// 认证失败
    pub const AUTHENTICATION_FAILED: &str = "Authentication failed";
    /// 协议错误
    pub const PROTOCOL_ERROR: &str = "Protocol error";
    /// 消息格式错误
    pub const MESSAGE_FORMAT_ERROR: &str = "Message format error";
    /// 服务不可用
    pub const SERVICE_UNAVAILABLE: &str = "Service unavailable";
}

