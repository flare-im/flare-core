//! 消息处理模块
//! 
//! 提供完整的消息处理功能，包括：
//! - 消息解析：序列化/反序列化、压缩/解压缩
//! - 消息处理：观察者模式的消息分发和处理
//! - 消息管道：统一的消息处理流程，支持中间件
//! - 中间件：日志、监控、验证等常用中间件

pub mod parser;
pub mod handler;
pub mod pipeline;
pub mod processor;
pub mod middleware;

// 重新导出常用类型，方便使用
pub use parser::MessageParser;
pub use handler::{MessageHandler, MessageObserver, MessageEvent, ArcMessageObserver};
pub use pipeline::{MessagePipeline, MessageContext, MessageMiddleware, MessageProcessor, ArcMessageMiddleware, ArcMessageProcessor};
pub use processor::{FunctionProcessor, DelegateProcessor};
pub use middleware::{LoggingMiddleware, MetricsMiddleware, ValidationMiddleware, LogLevel};
