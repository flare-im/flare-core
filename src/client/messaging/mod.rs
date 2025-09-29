//! 客户端消息处理模块
//!
//! 提供统一的消息发送和响应处理功能

pub mod message_handler;

// 重新导出主要类型
pub use message_handler::{MessageHandler, SendFunction};
