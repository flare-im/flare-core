use crate::common::error::FlareError;
use std::sync::Arc;

/// Represents events that occur on a connection.
///
/// This enum is used by the `ConnectionObserver` to react to various
/// states and data received on a connection.
#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    /// Emitted when a connection is successfully established.
    Connected,
    /// Emitted when a connection is closed.
    /// The string parameter provides a reason for the disconnection.
    Disconnected(String),
    /// Emitted when a new message is received.
    /// The payload is a byte vector.
    Message(Vec<u8>),
    /// Emitted when a non-fatal error occurs on the connection.
    Error(FlareError),
}

/// An observer for connection events.
///
/// Implement this trait to react to events like connection establishment,
/// disconnection, and incoming messages. Observers are registered with
/// a `Connection` instance.
pub trait ConnectionObserver: Send + Sync {
    /// Called by the `Connection` when an event occurs.
    fn on_event(&self, event: &ConnectionEvent);
}

/// A type alias for a thread-safe, reference-counted observer.
pub type ArcObserver = Arc<dyn ConnectionObserver>;