//! 服务端模块
//!
//! 提供服务端实现，支持 WebSocket 和 QUIC 协议

#[cfg(not(any(feature = "websocket", feature = "quic", feature = "tcp")))]
compile_error!(
    "feature `server` requires at least one transport feature: `websocket`, `quic`, or `tcp`"
);

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
#[cfg(feature = "quic")]
pub use transports::QUICServer;
#[cfg(feature = "tcp")]
pub use transports::TCPServer;
#[cfg(feature = "websocket")]
pub use transports::WebSocketServer;
pub use transports::{ConnectionHandler, HybridServer, Server};

// 重新导出错误类型，服务端使用 ServerError
pub use crate::common::error::Result;
pub use crate::common::error::ServerError;

pub use crate::common::config_types::TransportProtocol;
