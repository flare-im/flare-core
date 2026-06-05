pub mod common;

#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
pub mod server;

#[cfg(feature = "client")]
pub mod client;
pub mod transport;

#[cfg(all(
    feature = "client",
    not(target_arch = "wasm32"),
    any(feature = "websocket", feature = "quic", feature = "tcp")
))]
pub use client::HybridClient;
#[cfg(all(
    feature = "server",
    not(target_arch = "wasm32"),
    any(feature = "websocket", feature = "quic", feature = "tcp")
))]
pub use server::HybridServer;

#[cfg(all(feature = "client", not(target_arch = "wasm32")))]
pub use client::{ClientBuilder, ObserverClient, ObserverClientBuilder, SimpleClient};
#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
pub use server::{
    DefaultServerHandle, MessageContext, ObserverServer, ObserverServerBuilder, ServerBuilder,
    ServerHandle, SimpleServer,
};

#[cfg(all(feature = "client", target_arch = "wasm32"))]
pub use client::{
    ClientBuilder, FlareClient, FlareClientBuilder, MessageListener, SimpleClient, WebSocketClient,
};

pub use common::conversation::*;
