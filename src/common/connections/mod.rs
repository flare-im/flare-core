//! 通用连接模块
//!
//! 该模块提供了统一的连接抽象层，屏蔽底层协议（WebSocket、QUIC等）的差异，
//! 提供一致的API和功能。

pub mod config;
pub mod enums;
pub mod traits;
pub mod types;
pub mod stats;
pub mod monitor;
pub mod heartbeat;
pub mod factory;
pub mod enhanced;
pub mod manager;
pub mod base;
pub mod websocket;
pub mod quic;
pub mod reconnect;
pub mod ratelimit;
pub mod reliable;