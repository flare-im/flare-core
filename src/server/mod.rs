//! 服务端模块
//!
//! 提供服务端核心功能实现

pub mod manager;
pub mod server;
pub mod websocket;
pub mod quic;
pub mod event;
mod adapter;
pub mod fast;
pub mod config;

// 重新导出常用的类型，方便外部使用
pub use manager::{
    traits::ServerConnectionManager,
    ConnectionManager,
    UserConnectionManager,
    HeartbeatConfig,
};
pub use event::{
    ServerEvent,
    DefServerEventHandler,
};
// 服务端处理器
pub use adapter::{
    ServerEventAdapter,
};
// 服务端代理
pub use fast::{
    server::FastServer,
};
// 配置相关
pub use config::{
    ServerConfig,
    ServerType,
    ProtocolConfig,
    TlsConfig,
};
// 服务相关
pub use server::{
    Server,
    ServerService,
};