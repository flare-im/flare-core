//! Messaging 模块
//!
//! 提供消息构建、优先级队列、可靠性保证和统一的消息处理
//! 
//! 核心组件：
//! - `MessageProcessor`: 统一的消息处理（编码、压缩、解析）
//! - `FrameBuilder`: Frame 构建器
//! - `PriorityMessageQueue`: 优先级消息队列
//! - `ReliabilityManager`: 可靠性管理器

pub mod builder;
pub mod priority_queue;
pub mod reliability;
pub mod processor;

// 重新导出便捷使用
pub use crate::common::parsing::parser::MessageParser;
pub use builder::FrameBuilder;
pub use priority_queue::PriorityMessageQueue;
pub use reliability::ReliabilityManager;
pub use processor::{MessageProcessor, MessageProcessorConfig};
