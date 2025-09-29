//! FastClient 模块
//!
//! 提供开箱即用的高级客户端功能，基于基础 Client 构建

pub mod event;
pub mod auth;
pub mod client;
pub mod builder;
pub mod event_adapter;

// 重新导出主要类型
pub use event::{FastEvent, DefFastEventHandler};
pub use auth::{FastAuthManager, AuthConfig};
pub use client::FastClient;
pub use builder::FastClientBuilder;
pub use event_adapter::FastClientEventAdapter;
