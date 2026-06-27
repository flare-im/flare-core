//! Native TCP transport with length-prefixed Flare frames.
//!
//! # Wire format
//!
//! Same as QUIC bi-stream: `u32 BE length` + protobuf/JSON Frame bytes.
//!
//! # Role in the stack
//!
//! - **Transport only**: no CONNECT/negotiation semantics here.
//! - **Native only**: browser clients use WebSocket (`websocket_wasm`).
//! - Use cases: embedded gateways, internal mesh, custom load balancers, protocol tests.

use crate::common::error::{FlareError, Result};
use crate::transport::connection::Connection;
use crate::transport::events::{
    ArcObserver, ConnectionEvent, notify_observers as notify_connection_observers,
    notify_observers_and_clear as notify_connection_observers_and_clear,
};
use crate::transport::framing::{read_length_prefixed, write_length_prefixed};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncWriteExt, split};
use tokio::net::TcpStream;
use tokio::sync::Mutex as TokioMutex;
use tracing::debug;

pub struct TCPTransport {
    write_half: Arc<TokioMutex<tokio::io::WriteHalf<TcpStream>>>,
    observers: Arc<Mutex<Vec<ArcObserver>>>,
    last_active: Arc<Mutex<std::time::Instant>>,
    is_closed: Arc<Mutex<bool>>,
}

impl TCPTransport {
    pub fn new(stream: TcpStream) -> Self {
        let (read_half, write_half) = split(stream);
        let observers = Arc::new(Mutex::new(Vec::new()));
        let last_active = Arc::new(Mutex::new(std::time::Instant::now()));
        let is_closed = Arc::new(Mutex::new(false));
        let write_half = Arc::new(TokioMutex::new(write_half));

        let task_observers = Arc::clone(&observers);
        let task_last_active = Arc::clone(&last_active);
        let task_is_closed = Arc::clone(&is_closed);
        tokio::spawn(Self::receiver_task(
            read_half,
            task_observers,
            task_last_active,
            task_is_closed,
        ));

        Self {
            write_half,
            observers,
            last_active,
            is_closed,
        }
    }

    async fn receiver_task(
        mut read_half: tokio::io::ReadHalf<TcpStream>,
        observers_arc: Arc<Mutex<Vec<ArcObserver>>>,
        last_active: Arc<Mutex<std::time::Instant>>,
        is_closed: Arc<Mutex<bool>>,
    ) {
        loop {
            if is_closed.lock().map(|closed| *closed).unwrap_or(true) {
                debug!("[TCP Transport] Receiver task: connection closed");
                break;
            }

            match read_length_prefixed(&mut read_half).await {
                Ok(data) => {
                    if data.is_empty() {
                        debug!("[TCP Transport] EOF, peer closed connection");
                        Self::notify_observers_and_clear(
                            &observers_arc,
                            &ConnectionEvent::Disconnected(
                                "TCP connection closed by peer".to_string(),
                            ),
                        );
                        break;
                    }

                    if let Ok(mut active) = last_active.lock() {
                        *active = std::time::Instant::now();
                    }
                    debug!("[TCP Transport] Received frame: {} bytes", data.len());
                    Self::notify_observers(&observers_arc, &ConnectionEvent::Message(data));
                }
                Err(e) => {
                    let err_str = e.to_string();
                    let event = if err_str.contains("peer disconnected")
                        || err_str.contains("Connection reset")
                        || err_str.contains("Broken pipe")
                    {
                        ConnectionEvent::Disconnected(err_str)
                    } else {
                        ConnectionEvent::Error(e)
                    };
                    Self::notify_observers_and_clear(&observers_arc, &event);
                    break;
                }
            }
        }

        debug!("[TCP Transport] Receiver task ended");
    }

    fn notify_observers(observers_arc: &Arc<Mutex<Vec<ArcObserver>>>, event: &ConnectionEvent) {
        notify_connection_observers(observers_arc, event, "tcp observers");
    }

    fn notify_observers_and_clear(
        observers_arc: &Arc<Mutex<Vec<ArcObserver>>>,
        event: &ConnectionEvent,
    ) {
        notify_connection_observers_and_clear(observers_arc, event, "tcp observers");
    }
}

#[async_trait]
impl Connection for TCPTransport {
    fn add_observer(&mut self, observer: ArcObserver) {
        observer.on_event(&ConnectionEvent::Connected);
        if let Ok(mut observers) = self.observers.lock() {
            observers.push(observer);
        }
    }

    fn remove_observer(&mut self, observer: ArcObserver) {
        if let Ok(mut observers) = self.observers.lock() {
            observers.retain(|o| !Arc::ptr_eq(o, &observer));
        }
    }

    async fn send(&mut self, data: &[u8]) -> Result<()> {
        if let Ok(mut active) = self.last_active.lock() {
            *active = std::time::Instant::now();
        }

        let mut writer = self.write_half.lock().await;
        write_length_prefixed(&mut *writer, data).await?;
        writer
            .flush()
            .await
            .map_err(|e| FlareError::io(e.to_string()))?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let Ok(mut closed) = self.is_closed.lock() {
            *closed = true;
        }

        let close_result = self
            .write_half
            .lock()
            .await
            .shutdown()
            .await
            .map_err(|e| FlareError::connection_closed(e.to_string()));

        Self::notify_observers_and_clear(
            &self.observers,
            &ConnectionEvent::Disconnected("Closed by local endpoint".to_string()),
        );
        close_result
    }

    fn last_active_time(&self) -> std::time::Instant {
        self.last_active
            .lock()
            .map(|guard| *guard)
            .unwrap_or_else(|_| std::time::Instant::now() - std::time::Duration::from_secs(3600))
    }

    fn update_active_time(&mut self) {
        if let Ok(mut active) = self.last_active.lock() {
            *active = std::time::Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::events::ConnectionObserver;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;

    struct CountingObserver {
        messages: Arc<AtomicUsize>,
    }

    impl ConnectionObserver for CountingObserver {
        fn on_event(&self, event: &ConnectionEvent) {
            if matches!(event, ConnectionEvent::Message(_)) {
                self.messages.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    #[tokio::test]
    async fn tcp_transport_delivers_inbound_frames() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");

        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut transport = TCPTransport::new(stream);
            let observer = Arc::new(CountingObserver {
                messages: Arc::new(AtomicUsize::new(0)),
            });
            let messages = Arc::clone(&observer.messages);
            transport.add_observer(observer);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            messages
        });

        let stream = TcpStream::connect(addr).await.expect("connect");
        let mut client = TCPTransport::new(stream);
        client.send(b"hello-tcp-frame").await.expect("client send");

        let messages = server_task.await.expect("server task");
        assert_eq!(messages.load(Ordering::SeqCst), 1);
    }
}
