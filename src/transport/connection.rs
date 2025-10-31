use crate::transport::events::ArcObserver;
use async_trait::async_trait;
use std::error::Error;

/// A unified transport layer connection interface.
///
/// This trait abstracts the underlying transport protocol (e.g., WebSocket, QUIC)
/// and provides a common set of methods for interacting with a connection.
/// It uses an observer pattern to notify interested parties of connection events.
#[async_trait]
pub trait Connection: Send + Sync {
    /// Adds an observer to the connection.
    ///
    /// The observer will be notified of connection events.
    fn add_observer(&mut self, observer: ArcObserver);

    /// Removes an observer from the connection.
    fn remove_observer(&mut self, observer: ArcObserver);

    /// Sends data over the connection.
    async fn send(&mut self, data: &[u8]) -> Result<(), Box<dyn Error>>;

    /// Closes the connection.
    async fn close(&mut self) -> Result<(), Box<dyn Error>>;
}