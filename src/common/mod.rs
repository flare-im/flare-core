//! 通用模块
//! 
//! 提供核心功能、错误处理、协议定义等

pub mod connections;
pub mod error;
pub mod protocol;

// 重新导出主要类型
pub use connections::{
    Connection, ClientConnection, ServerConnection, 
    ConnectionFactory, ConnectionManager,
    ConnectionType, ConnectionRole, ConnectionState, ConnectionConfig, 
    ConnectionEventHandler, DefaultConnectionEventHandler,
    QuicConnection, WebSocketConnection,
};
pub use error::{Result, FlareError};
pub use protocol::{UnifiedProtocolMessage, Frame, MessageType, Reliability, ProtocolSelection}; 