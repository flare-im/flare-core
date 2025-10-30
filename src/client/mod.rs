//! 客户端模块
//!
//! # 职责
//! - 客户端连接实现（WebSocket, QUIC）
//! - 协议竞速（Protocol Racing）
//! - 自动重连（Auto Reconnect）
//! - 客户端认证
//!
//! # 模块组织
//! - `protocol_racer`: 协议竞速器
//! - `reconnect`: 重连逻辑
//! - `auth`: 认证模块
//! - `fast`: 高性能客户端实现
//! - `enhanced_client`: 增强型客户端
//! - `connections`: 客户端连接实现

pub mod protocol_racer;   // 协议竞速
pub mod reconnect;        // 重连逻辑
pub mod auth;             // 认证
pub mod fast;             // 高性能客户端
pub mod enhanced_client;  // 增强型客户端
pub mod connections;      // 客户端连接实现