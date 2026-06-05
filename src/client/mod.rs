//! 客户端模块
//!
//! Native: WebSocket + QUIC + Hybrid
//! WASM: WebSocket-only client stack
//!
//! WASM integrators: read [`crate::transport::connection`] for `Send`/`Sync` and LocalSet rules,
//! and use [`wasm_tokio::run_async`](crate::client::wasm_tokio::run_async) for all async entry points.

#[cfg(not(any(
    feature = "websocket",
    all(feature = "quic", not(target_arch = "wasm32")),
    all(feature = "tcp", not(target_arch = "wasm32"))
)))]
compile_error!(
    "feature `client` requires at least one transport feature: `websocket`, native `quic`, or native `tcp`"
);

pub mod builder;
pub mod config;
pub mod connection;
pub mod events;
pub mod heartbeat;
pub mod router;
pub mod runtime;
pub mod transports;
#[cfg(target_arch = "wasm32")]
pub mod wasm_tokio;

#[cfg(not(target_arch = "wasm32"))]
pub mod manager;

pub use builder::{ClientBuilder, FlareClient, FlareClientBuilder, MessageListener, SimpleClient};
pub use config::ClientConfig;
pub use connection::{ConnectionState, ConnectionStateManager};
pub use events::{ClientEventHandler, DefaultClientMessageObserver};
pub use heartbeat::HeartbeatManager;
pub use router::{AsyncHandler, MessageHandler, MessageRouter, SimpleHandler};
pub use transports::Client;
#[cfg(feature = "websocket")]
pub use transports::WebSocketClient;

#[cfg(target_arch = "wasm32")]
pub use wasm_tokio::{
    ensure_initialized as ensure_wasm_tokio, run_async as run_wasm_async,
    spawn_detached as spawn_wasm_detached,
};

#[cfg(not(target_arch = "wasm32"))]
pub use builder::{ObserverClient, ObserverClientBuilder};
#[cfg(not(target_arch = "wasm32"))]
pub use manager::ClientConnectionManager;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "websocket", feature = "quic", feature = "tcp")
))]
pub use transports::HybridClient;
#[cfg(all(not(target_arch = "wasm32"), feature = "quic"))]
pub use transports::QUICClient;
#[cfg(all(not(target_arch = "wasm32"), feature = "tcp"))]
pub use transports::TCPClient;

pub use crate::common::config_types::TransportProtocol;
pub use crate::common::error::ClientError;
pub use crate::common::error::Result;
