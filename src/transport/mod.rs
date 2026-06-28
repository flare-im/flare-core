//! Transport layer: WebSocket / QUIC / TCP connections and frame I/O.
//!
//! This module is intentionally lower level than the public client and server
//! builders. Use it when implementing custom transports, browser adapters,
//! protocol-racing infrastructure, or transport-level tests.
//!
//! Platform split:
//!
//! - **Native**: `websocket`, `quic`, `tcp`, `framing`, and `factory`.
//! - **WASM**: `websocket_wasm` only.
//!
//! [`connection::Connection`] documents the cross-platform `Send + Sync` contract and WASM
//! LocalSet / JS event-loop rules — read it before adding SDK or custom transports.

pub mod connection;
pub mod events;

#[cfg(all(not(target_arch = "wasm32"), any(feature = "quic", feature = "tcp")))]
pub mod framing;

#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "websocket", feature = "quic", feature = "tcp")
))]
pub mod factory;
#[cfg(all(not(target_arch = "wasm32"), feature = "quic"))]
pub mod quic;
#[cfg(all(not(target_arch = "wasm32"), feature = "tcp"))]
pub mod tcp;
#[cfg(all(not(target_arch = "wasm32"), feature = "websocket"))]
pub mod websocket;

#[cfg(all(target_arch = "wasm32", feature = "websocket"))]
pub mod websocket_wasm;

#[cfg(all(not(target_arch = "wasm32"), feature = "websocket"))]
pub use websocket::WebSocketTransport;

#[cfg(all(not(target_arch = "wasm32"), feature = "tcp"))]
pub use tcp::TCPTransport;

#[cfg(all(target_arch = "wasm32", feature = "websocket"))]
pub use websocket_wasm::WebSocketTransport;
