//! Transport connection trait and platform `Send` contract.
//!
//! # Native (`not(wasm32)`)
//!
//! - `Connection: Send + Sync` so `Box<dyn Connection>` can move across Tokio worker tasks.
//! - Implementations: `WebSocketTransport`, `TCPTransport`, `QuicTransport`, etc.
//! - Observers may spawn async work via [`crate::client::runtime::spawn_client_task`].
//!
//! # WASM (`wasm32-unknown-unknown`)
//!
//! Browser WebSocket runs on a **single JS thread**. The trait still requires `Send + Sync`
//! so client code shares one object-safe API with Native; WASM transports use targeted
//! `unsafe impl Send/Sync` (see `websocket_wasm.rs`) because `web_sys` handles are not
//! `Send` by default but are never accessed off the browser thread.
//!
//! ## WASM async rules (do not violate)
//!
//! 1. **Never** `Runtime::block_on` — use [`crate::client::wasm_tokio::run_async`].
//! 2. **Never** `async_trait(?Send)` on this trait — it breaks `Box<dyn Connection + Send + Sync>`.
//! 3. **Browser callbacks are sync** (`onmessage`, `onopen`): enqueue bytes with
//!    [`ClientCore::push_wasm_inbound`](crate::client::transports::ClientCore::push_wasm_inbound)
//!    and drain inside `wait_for_negotiation` / `run_async` LocalSet (see `ClientMessageObserver`).
//! 4. **Yield to the JS event loop** during long waits (`yield_to_event_loop`) so WebSocket
//!    I/O callbacks can run while Rust awaits CONNECT_ACK.
//!
//! # Implementing `Connection`
//!
//! - Keep `send` / `close` non-blocking; delegate to the transport driver.
//! - Notify observers synchronously from I/O callbacks; do not queue through unbounded
//!   channels that the Tokio driver might not poll promptly on WASM.
//! - Update `last_active_time` on send and receive.

use crate::common::error::Result;
use crate::common::platform::MonotonicInstant;
use crate::transport::events::ArcObserver;
use async_trait::async_trait;

/// Unified transport connection interface (Native + WASM).
///
/// See [module-level documentation](self) for `Send`/`Sync` and WASM LocalSet requirements.
#[async_trait]
pub trait Connection: Send + Sync {
    /// Adds an observer to the connection.
    ///
    /// The observer will be notified of connection events.
    fn add_observer(&mut self, observer: ArcObserver);

    /// Removes an observer from the connection.
    fn remove_observer(&mut self, observer: ArcObserver);

    /// Sends data over the connection.
    async fn send(&mut self, data: &[u8]) -> Result<()>;

    /// Closes the connection.
    async fn close(&mut self) -> Result<()>;

    /// Returns last activity time (send/receive). Used for heartbeat and idle detection.
    fn last_active_time(&self) -> MonotonicInstant;

    /// Updates last activity time (also called automatically on send/receive in most impls).
    fn update_active_time(&mut self);
}
