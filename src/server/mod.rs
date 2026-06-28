//! Native server runtime for WebSocket, QUIC, TCP, and hybrid listeners.
//!
//! The server module is available on native targets when the `server` feature is
//! enabled. It provides:
//!
//! - [`FlareServerBuilder`] for production integrations built around
//!   [`ServerEventHandler`], authentication, message pipelines, and connection
//!   lifecycle hooks.
//! - [`ServerBuilder`] for closure-based demos and compact prototypes.
//! - [`ObserverServerBuilder`] for integrations that need explicit connection
//!   observation.
//! - [`ConnectionManager`] and [`ServerHandle`] for sending frames, broadcasting,
//!   disconnecting clients, and inspecting connection snapshots.
//!
//! The server stack owns transport admission, negotiation, heartbeat detection,
//! backpressure-aware fanout, and cleanup. Business-specific message semantics
//! should remain in application handlers or higher-level services.

#[cfg(not(any(feature = "websocket", feature = "quic", feature = "tcp")))]
compile_error!(
    "feature `server` requires at least one transport feature: `websocket`, `quic`, or `tcp`"
);

pub mod auth;
pub mod builder;
pub mod config;
pub mod connection;
pub mod device;
pub mod events;
pub mod handle;
pub mod heartbeat;
pub mod transports;

pub use auth::{AuthResult, Authenticator, DefaultAuthenticator};
pub use builder::{
    FlareServer, FlareServerBuilder, MessageContext, ObserverServer, ObserverServerBuilder,
    ServerBuilder, SimpleServer,
};
pub use config::ServerConfig;
pub use connection::{ConnectionInfo, ConnectionManager, ConnectionManagerTrait, ConnectionStats};
pub use device::{DeviceConflictStrategy, DeviceConflictStrategyBuilder, DeviceManager};
pub use events::ServerEventHandler;
pub use handle::{DefaultServerHandle, ServerHandle};
pub use heartbeat::HeartbeatDetector;
#[cfg(feature = "quic")]
pub use transports::QUICServer;
#[cfg(feature = "tcp")]
pub use transports::TCPServer;
#[cfg(feature = "websocket")]
pub use transports::WebSocketServer;
pub use transports::{ConnectionHandler, HybridServer, Server};

// Re-export server-oriented error types for ergonomic imports.
pub use crate::common::error::Result;
pub use crate::common::error::ServerError;

pub use crate::common::config_types::TransportProtocol;
