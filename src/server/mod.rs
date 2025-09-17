//! 服务端模块
//!
//! 提供服务端核心功能实现

pub mod manager;
pub mod server;
pub mod websocket;
pub mod quic;
pub mod event;
mod handlers;

// 重新导出常用的类型，方便外部使用
pub use manager::{
    traits::ServerConnectionManager,
    ConnectionManager,
    UserConnectionManager,
    Platform,
    HeartbeatConfig,
};
pub use event::{
    ServerEvent,
    DefServerEventHandler,
};
// 服务端处理器
pub use handlers::{
    ConnectionEventHandler,
};