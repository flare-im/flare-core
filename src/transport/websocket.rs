use crate::transport::connection::Connection;
use crate::transport::events::{ArcObserver, ConnectionEvent};
use async_trait::async_trait;
use futures_util::stream::{SplitSink, SplitStream, StreamExt};
use futures_util::SinkExt;
use std::error::Error;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub struct WebSocketTransport {
    sink: Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>,
    observers: Arc<std::sync::Mutex<Vec<ArcObserver>>>,
}

impl WebSocketTransport {
    pub fn new(stream: WebSocketStream<MaybeTlsStream<TcpStream>>) -> Self {
        let (sink, receiver) = stream.split();
        let observers = Arc::new(std::sync::Mutex::new(Vec::new()));

        let task_observers = Arc::clone(&observers);
        tokio::spawn(Self::receiver_task(receiver, task_observers));

        Self {
            sink: Mutex::new(sink),
            observers,
        }
    }

    async fn receiver_task(
        mut receiver: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        observers_arc: Arc<std::sync::Mutex<Vec<ArcObserver>>>,
    ) {
        while let Some(message) = receiver.next().await {
            let event = match message {
                Ok(msg) => match msg {
                    Message::Text(text) => Some(ConnectionEvent::Message(text.into_bytes())),
                    Message::Binary(data) => Some(ConnectionEvent::Message(data)),
                    Message::Close(frame) => {
                        let reason = frame
                            .map(|f| f.reason.to_string())
                            .unwrap_or_else(|| "Connection closed by peer".to_string());
                        Some(ConnectionEvent::Disconnected(reason))
                    }
                    _ => None, // Ignore Ping/Pong/etc.
                },
                Err(e) => Some(ConnectionEvent::Error(e.to_string())),
            };

            if let Some(event) = event {
                let is_terminal =
                    matches!(event, ConnectionEvent::Disconnected(_) | ConnectionEvent::Error(_));

                Self::_notify_observers(&observers_arc, &event);

                if is_terminal {
                    break;
                }
            }
        }
    }

    // 私有辅助方法，用于通知所有观察者
    fn _notify_observers(observers_arc: &Arc<std::sync::Mutex<Vec<ArcObserver>>>, event: &ConnectionEvent) {
        let observers = observers_arc.lock().unwrap();
        for observer in observers.iter() {
            observer.on_event(event);
        }
    }

    fn notify_observers(&self, event: &ConnectionEvent) {
        Self::_notify_observers(&self.observers, event);
    }
}

#[async_trait]
impl Connection for WebSocketTransport {
    fn add_observer(&mut self, observer: ArcObserver) {
        observer.on_event(&ConnectionEvent::Connected);
        self.observers.lock().unwrap().push(observer);
    }

    fn remove_observer(&mut self, observer: ArcObserver) {
        self.observers
            .lock()
            .unwrap()
            .retain(|o| !Arc::ptr_eq(o, &observer));
    }

    async fn send(&mut self, data: &[u8]) -> Result<(), Box<dyn Error>> {
        let message = Message::Binary(data.to_vec());
        self.sink.lock().await.send(message).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.sink.lock().await.close().await?;
        self.notify_observers(&ConnectionEvent::Disconnected("Closed by client".to_string()));
        Ok(())
    }
}