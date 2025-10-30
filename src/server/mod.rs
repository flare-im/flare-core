//! 服务端模块
//!
//! # 职责
//! - 服务端监听与连接管理
//! - WebSocket/QUIC 服务器实现
//! - 连接池与负载均衡
//! - 服务端配置
//!
//! # 模块组织
//! - `manager`: 连接管理器
//! - `config`: 服务端配置
//! - `servers`: 各种协议的服务端实现（WebSocket、QUIC等）
//! - `server`: 聚合型服务端
//! - `fast`: 高性能轻量级服务端
//! - `events`: 事件处理机制
//! - `traits`: 服务端 trait 定义
//! - `connections`: 服务端连接实现

pub mod manager;      // 连接管理
pub mod config;       // 配置
pub mod servers;     // 各种协议的服务端实现
pub mod server;       // 聚合型服务端
pub mod fast;         // 高性能轻量级服务端
pub mod events;       // 事件处理机制
pub mod traits;       // trait 定义
pub mod connections;  // 服务端连接实现