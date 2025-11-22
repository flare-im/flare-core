pub mod client;
pub mod common;
pub mod server;
pub mod transport;

// 重新导出混合接口
pub use client::HybridClient;
pub use server::HybridServer;

// 重新导出 Builder API（观察者模式和简单模式）
pub use client::{ClientBuilder, SimpleClient, ObserverClientBuilder, ObserverClient};
pub use server::{ServerBuilder, SimpleServer, MessageContext, ObserverServerBuilder, ObserverServer, ServerHandle, DefaultServerHandle};

// 重新导出会话ID相关功能
pub use common::session_id::*;