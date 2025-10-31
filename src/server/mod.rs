//! 服务端模块
//! 
//! 提供服务端实现，支持 WebSocket 和 QUIC 协议

pub mod cluster;
pub mod gateway;
pub mod quic;
pub mod router;
pub mod unified;
pub mod websocket;

// 重新导出错误类型，服务端使用 ServerError
pub use crate::common::error::ServerError;
pub use crate::common::error::Result;
pub use crate::common::{Server, ConnectionHandler, ServerConfig, TransportProtocol};
pub use quic::QUICServer;
pub use unified::UnifiedServer;
pub use websocket::WebSocketServer;
