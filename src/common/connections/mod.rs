//! 连接模块
//! 
//! 提供统一的连接抽象和实现，支持客户端和服务端的差异化需求

pub mod traits;
pub mod types;
pub mod quic;
pub mod websocket;
pub mod factory;
pub mod manager;
pub mod event;

// 重新导出主要类型
pub use traits::{
    Connection, ClientConnection, ServerConnection, 
    ConnectionFactory as ConnectionFactoryTrait, 
    ConnectionEventHandler, DefaultConnectionEventHandler,
    ServerConnectionManager, ServerStats,
};
// 事件处理模块通过 traits 统一对外导出
pub use types::{
    ConnectionType, ConnectionRole, ConnectionState, ConnectionConfig, 
    ConnectionQuality, ProtocolFeature, ProtocolConfig, WebSocketConfig, 
    QuicConfig, TcpConfig, UdpConfig,
};
pub use quic::QuicConnection;
pub use websocket::WebSocketConnection;
pub use factory::{ConnectionFactory, RawConnectionHandler};
pub use manager::ConnectionManager;
