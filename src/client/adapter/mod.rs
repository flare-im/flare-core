//! 客户端适配器模块
//!
//! 提供客户端事件适配器等适配器功能

pub mod client_event_adapter;

// 重新导出主要类型
pub use client_event_adapter::ClientEventAdapter;