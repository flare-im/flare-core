//! 客户端模块
//! 
//! 提供客户端实现，支持 WebSocket 和 QUIC 协议

pub mod config;
pub mod connection;
pub mod events;
pub mod heartbeat;
pub mod manager;
pub mod router;
pub mod transports;
pub mod builder;

pub use config::ClientConfig;
pub use connection::{ConnectionState, ConnectionStateManager};
pub use events::{ClientEventHandler, DefaultClientMessageObserver};
pub use heartbeat::HeartbeatManager;
pub use manager::ClientConnectionManager;
pub use router::{MessageRouter, MessageHandler, SimpleHandler, AsyncHandler};
pub use transports::{Client, QUICClient, HybridClient, WebSocketClient};
pub use builder::{ClientBuilder, SimpleClient, ObserverClientBuilder, ObserverClient, FlareClientBuilder, FlareClient, MessageListener};

// 重新导出错误类型，客户端使用 ClientError
pub use crate::common::error::ClientError;
pub use crate::common::error::Result;
pub use crate::common::config_types::TransportProtocol;
