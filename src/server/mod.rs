//! 服务端模块
//!
//! 提供服务端核心功能实现
//!
//! # 模块结构
//!
//! - [server](server/index.html): 服务端主模块
//! - [service](service/index.html): 服务接口定义
//! - [manager](manager/index.html): 连接管理器实现
//! - [auth](auth/index.html): 认证管理器实现
//! - [websocket](websocket/index.html): WebSocket 服务端实现
//! - [quic](quic/index.html): QUIC 服务端实现

pub mod server;
pub mod service;
pub mod manager;
pub mod auth;
pub mod websocket;
pub mod quic;

// 重新导出常用类型
pub use server::{Server, ServerConfig, ServerType, ServerStats};
pub use service::{MessageHandler, EchoMessageHandler};
pub use manager::{
    traits::ConnectionManager,
    ConnectionBasedManager,
    UserBasedManager,
    message_handler::{EnhancedMessageHandler, LoggingMessageHandler, BroadcastMessageHandler},
};
pub use auth::{AuthManager, AuthHandler, SimpleAuthHandler, AuthStatus, AuthInfo};
