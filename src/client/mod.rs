//! 客户端模块
//! 
//! 提供完整的客户端实现，支持WebSocket和QUIC协议竞速

pub mod client;
pub mod config;
pub mod protocol_racing;

// 重新导出主要类型
pub use client::Client;
pub use config::{ClientConfig, ProtocolSelection};
pub use crate::common::connections::types::Transport;