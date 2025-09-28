//! 客户端模块
//! 
//! 提供完整的客户端实现，支持WebSocket和QUIC协议竞速

pub mod client;
pub mod config;
pub mod protocol_racing;
// auth 模块已移至 fast 模块
pub mod fast;
pub mod event;
pub mod messaging;

// 重新导出主要类型
pub use client::{Client, ClientBuilder};
pub use config::{ClientConfig, ProtocolSelection};
pub use fast::{FastClient, FastClientBuilder, FastEvent, DefFastEventHandler};
pub use event::{ClientEvent, DefClientEventHandler};
pub use messaging::MessageHandler;
pub use crate::common::connections::types::Transport;