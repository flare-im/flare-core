//! Flare Core - 高性能、可靠的即时通讯长连接工具包
//! 
//! 专注于两个核心功能：
//! 1. 长连接可靠性：QUIC + WebSocket 连接管理
//! 2. 客户端协议竞速：智能协议选择和动态切换

pub mod common;
pub mod client;
pub mod server;

// 重新导出核心类型
pub use common::{
    error::FlareError,
    protocol::{Frame, MessageType, ProtocolSelection, ProtobufFrame, ProtobufMessageType, ProtobufReliability},
    connections::{
        Connection, ClientConnection, ServerConnection, ConnectionFactory, ConnectionManager,
        ConnectionConfig, ConnectionType, ConnectionRole, ConnectionState,
        ConnectionEvent, DefConnectionEventHandler,
    },
};

// 重新导出客户端相关类型
pub use client::{
    Client,
    ClientConfig,
    ProtocolSelection as ClientProtocolSelection,
};

// 重新导出服务端相关类型
pub use server::{
    Server,
    ServerConfig,
    ServerType,
    service::{MessageHandler, ServerService},
    service::EchoMessageHandler,
    quic::QuicServer,
    websocket::WebSocketServer,
    ConnectionBasedManager,
    UserBasedManager,
    auth::{AuthManager, AuthHandler, SimpleAuthHandler, AuthStatus, AuthInfo},
};

/// 版本信息
pub const VERSION: &str = env!("CARGO_PKG_VERSION");