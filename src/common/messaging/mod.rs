//! 消息处理模块
//!
//! 提供优先级队列、消息调度等高级消息处理功能

pub mod priority_queue;

pub use priority_queue::{
    PriorityMessageQueue, PriorityMessage, MessagePriority, QueueStats,
    create_system_message, create_realtime_message, create_high_priority_message,
    create_normal_message, create_low_priority_message,
};