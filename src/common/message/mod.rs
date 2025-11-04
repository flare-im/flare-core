//! 消息处理模块
//! 
//! 提供完整的消息处理功能，包括：
//! - 消息解析：序列化/反序列化、压缩/解压缩
//! - 消息处理：观察者模式的消息分发和处理

pub mod parser;
pub mod handler;

// 重新导出常用类型，方便使用
pub use parser::MessageParser;
pub use handler::{MessageHandler, MessageObserver, MessageEvent, ArcMessageObserver};
