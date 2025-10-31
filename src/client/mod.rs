//! 客户端模块
//! 
//! 提供客户端实现，支持 WebSocket 和 QUIC 协议

pub mod manager;
pub mod quic;
pub mod router;
pub mod unified;
pub mod websocket;

// 重新导出错误类型，客户端使用 ClientError
pub use crate::common::error::ClientError;
pub use crate::common::error::Result;
pub use crate::common::{Client, ClientConfig, TransportProtocol};
pub use quic::QUICClient;
pub use unified::UnifiedClient;
pub use websocket::WebSocketClient;
