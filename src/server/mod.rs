//! 服务端模块
//! 
//! 提供服务端实现，支持 WebSocket 和 QUIC 协议

pub mod config;
pub mod connection;
pub mod handle;
pub mod heartbeat;
pub mod transports;
pub mod builder;

pub use config::ServerConfig;
pub use connection::{ConnectionManager, ConnectionManagerTrait, ConnectionInfo, ConnectionStats};
pub use handle::{ServerHandle, DefaultServerHandle};
pub use heartbeat::HeartbeatDetector;
pub use transports::{Server, ConnectionHandler, QUICServer, HybridServer, WebSocketServer};
pub use builder::{ServerBuilder, SimpleServer, MessageContext, ObserverServerBuilder, ObserverServer};

// 重新导出错误类型，服务端使用 ServerError
pub use crate::common::error::ServerError;
pub use crate::common::error::Result;

pub use crate::common::config_types::TransportProtocol;
