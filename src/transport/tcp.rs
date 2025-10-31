use crate::transport::connection::Connection;
use crate::transport::events::{ArcObserver, ConnectionEvent};
use async_trait::async_trait;
use std::error::Error;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub struct TCPTransport {
    stream: TcpStream,
    observers: Mutex<Vec<ArcObserver>>,
}

impl TCPTransport {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            observers: Mutex::new(Vec::new()),
        }
    }

    fn notify_observers(&self, event: &ConnectionEvent) {
        let observers = self.observers.lock().unwrap();
        for observer in observers.iter() {
            observer.on_event(event);
        }
    }
}

#[async_trait]
impl Connection for TCPTransport {
    fn add_observer(&mut self, observer: ArcObserver) {
        self.observers.lock().unwrap().push(observer);
    }

    fn remove_observer(&mut self, observer: ArcObserver) {
        self.observers
            .lock()
            .unwrap()
            .retain(|o| !Arc::ptr_eq(o, &observer));
    }

    async fn send(&mut self, data: &[u8]) -> Result<(), Box<dyn Error>> {
        self.stream.write_all(data).await?;
        self.notify_observers(&ConnectionEvent::Message(data.to_vec()));
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.stream.shutdown().await?;
        self.notify_observers(&ConnectionEvent::Disconnected("Closed by client".to_string()));
        Ok(())
    }
}