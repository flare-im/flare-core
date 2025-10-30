// 声明工作区成员
pub mod common;
pub mod client;
pub mod server;

// 重新导出常用的类型，方便外部使用
pub use common::{
    error::{Result, FlareError},
    protocol::{Frame, Reliability, ProtocolSelection},
    connections::{
        traits::{Connection, ClientConnection, ServerConnection, ConnectionEvent, DefConnectionEventHandler},
        factory::ConnectionFactory,
        types::{ConnectionRole, Transport, ConnectionConfig, ConnectionState, ConnectionQuality, QuicConfig},
    },
};

// 重新导出服务端模块
pub use server::{
    ServerConfig,
    ServerType,
    ProtocolConfig,
};

// 重新导出客户端模块
pub use client::{
    Client,
    ClientConfig,
};

// 重新导出连接工厂
// 移除重复的ConnectionFactory导入

// 重新导出连接实现
pub use common::connections::{
    quic::QuicConnection,
    websocket::WebSocketConnection,
};