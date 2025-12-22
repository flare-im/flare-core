//! 服务端模块
//!
//! 提供服务端实现，支持 WebSocket 和 QUIC 协议

pub mod auth;
pub mod builder;
pub mod config;
pub mod connection;
pub mod device;
pub mod events;
pub mod handle;
pub mod heartbeat;
pub mod transports;

pub use auth::{AuthResult, Authenticator, DefaultAuthenticator};
pub use builder::{
    FlareServer, FlareServerBuilder, MessageContext, ObserverServer, ObserverServerBuilder,
    ServerBuilder, SimpleServer,
};
pub use config::ServerConfig;
pub use connection::{ConnectionInfo, ConnectionManager, ConnectionManagerTrait, ConnectionStats};
pub use device::{DeviceConflictStrategy, DeviceConflictStrategyBuilder, DeviceManager};
pub use events::ServerEventHandler;
pub use handle::{DefaultServerHandle, ServerHandle};
pub use heartbeat::HeartbeatDetector;
pub use transports::{ConnectionHandler, HybridServer, QUICServer, Server, WebSocketServer};

// 重新导出错误类型，服务端使用 ServerError
pub use crate::common::error::Result;
pub use crate::common::error::ServerError;

pub use crate::common::config_types::TransportProtocol;
