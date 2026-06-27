//! Flare Core 公共模块
//!
//! 提供核心功能的公共实现，包括：
//! - 错误处理：统一的错误类型和处理机制
//! - 协议定义：消息协议和命令定义
//! - 压缩/序列化：可扩展的压缩和序列化框架
//! - 消息处理：消息解析和处理机制
//! - 连接管理：连接存储和查询
//! - 会话ID生成：客户端和服务端通用的会话ID生成和验证
//! - 工具函数：常用工具和常量

#[cfg(not(target_arch = "wasm32"))]
pub mod cert;
pub mod compression;
pub mod config_types;
pub mod constants;
pub mod conversation;
pub mod device;
pub mod encryption;
pub mod error;
pub mod features;
pub mod message;
pub mod message_observer;
pub mod platform;
pub mod protobuf_decoder;
pub mod protocol;
pub mod serializer;
pub mod utils;

// 重新导出常用类型和函数，方便使用

pub use compression::{CompressionAlgorithm, CompressionUtil, Compressor};
pub use config_types::{HeartbeatAppState, HeartbeatConfig, TlsConfig, TransportProtocol};
pub use constants::*;
pub use conversation::*;
pub use device::{
    DeviceConflictStrategy, DeviceConflictStrategyBuilder, DeviceInfo, DevicePlatform,
};
pub use encryption::{EncryptionAlgorithm, EncryptionUtil, Encryptor};
pub use error::{ClientError, ErrorBuilder, ErrorCode, FlareError, Result, ServerError};
pub use features::FeatureSet;
pub use message::{
    ArcMessageMiddleware, ArcMessageProcessor, DelegateProcessor, FunctionProcessor, LogLevel,
    LoggingMiddleware, MessageContext, MessageEvent, MessageHandler, MessageMiddleware,
    MessageParser, MessagePipeline, MessageProcessor, MetricsMiddleware, ValidationMiddleware,
};
pub use message_observer::{ArcMessageObserver, MessageObserver};
pub use platform::{
    AES256_KEY_LEN, MonotonicInstant, clear_runtime_encryption_key, default_local_ws_url,
    format_now_rfc3339, has_runtime_encryption_key, interval, monotonic_now,
    parse_encryption_key_hex, parse_encryption_key_utf8, register_aes256_encryption,
    resolve_encryption_key_bytes, runtime_instance_id, set_runtime_encryption_key, sleep, timeout,
    wall_clock_ms, web_device_info,
};
pub use protocol::{
    Command, CustomCommand, Frame, NotificationCommand, PayloadCommand, Reliability,
    SerializationFormat, SystemCommand,
};
pub use serializer::{SerializationUtil, Serializer};
pub use utils::*;
