//! 连接模块
//! 
//! 提供统一的连接抽象和多种协议实现

pub mod traits;
pub mod types;
pub mod event;
pub mod factory;
pub mod builder;
pub mod manager;
pub mod pool;
pub mod websocket;
pub mod quic;
pub mod enums;
pub mod config;

// 重新导出常用的类型，保持对外接口稳定
pub use traits::{Connection, ClientConnection, ServerConnection, ConnectionEvent};
pub use types::{ConnectionConfig, ConnectionState, Transport, ConnectionRole};
pub use event::{DefConnectionEventHandler};
pub use factory::ConnectionFactory;
pub use builder::ConnectionBuilder;
pub use manager::ConnectionManager;
pub use pool::ConnectionPool;