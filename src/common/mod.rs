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
pub mod device;
pub mod encryption;
pub mod error;
pub mod message;
pub mod message_observer;
pub mod protocol;
pub mod serializer;
pub mod session_id;
pub mod utils;

// 重新导出常用类型和函数，方便使用

pub use compression::{Compressor, CompressionAlgorithm, CompressionUtil};
pub use config_types::{TransportProtocol, TlsConfig, HeartbeatConfig};
pub use constants::*;
pub use device::{DevicePlatform, DeviceInfo, DeviceConflictStrategy, DeviceConflictStrategyBuilder};
pub use encryption::{Encryptor, EncryptionAlgorithm, EncryptionUtil};
pub use error::{FlareError, Result, ClientError, ServerError, ErrorCode, ErrorBuilder};
pub use message::{
    MessageParser, MessageHandler, MessageEvent,
    MessagePipeline, MessageContext, MessageMiddleware, MessageProcessor,
    ArcMessageMiddleware, ArcMessageProcessor,
    FunctionProcessor, DelegateProcessor,
    LoggingMiddleware, MetricsMiddleware, ValidationMiddleware, LogLevel,
};
pub use message_observer::{MessageObserver, ArcMessageObserver};
pub use protocol::{Frame, Command, SystemCommand, MessageCommand, NotificationCommand, CustomCommand, Reliability, SerializationFormat};
pub use serializer::{Serializer, SerializationUtil};
pub use session_id::*;
pub use utils::*;
