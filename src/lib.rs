#[cfg(not(target_arch = "wasm32"))]
pub mod client;
pub mod common;
#[cfg(not(target_arch = "wasm32"))]
pub mod server;
#[cfg(not(target_arch = "wasm32"))]
pub mod transport;

// 重新导出混合接口
#[cfg(not(target_arch = "wasm32"))]
pub use client::HybridClient;
#[cfg(not(target_arch = "wasm32"))]
pub use server::HybridServer;

// 重新导出 Builder API（观察者模式和简单模式）
#[cfg(not(target_arch = "wasm32"))]
pub use client::{ClientBuilder, SimpleClient, ObserverClientBuilder, ObserverClient};
#[cfg(not(target_arch = "wasm32"))]
pub use server::{ServerBuilder, SimpleServer, MessageContext, ObserverServerBuilder, ObserverServer, ServerHandle, DefaultServerHandle};

// 重新导出会话ID相关功能
pub use common::session_id::*;
