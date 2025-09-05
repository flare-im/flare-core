//! 消息处理模块
//! 
//! 提供消息处理相关的功能，包括消息队列和消息解析器

pub mod priority_queue;
pub mod message_parser;

// 重新导出常用的类型
pub use priority_queue::PriorityMessageQueue;
pub use message_parser::MessageParser;