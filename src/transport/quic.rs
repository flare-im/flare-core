use crate::transport::connection::Connection;
use crate::transport::events::{ArcObserver, ConnectionEvent};
use async_trait::async_trait;
use std::error::Error;
use std::sync::{Arc, Mutex};

pub struct QUICTransport {
    connection: quinn::Connection,
    observers: Mutex<Vec<ArcObserver>>,
}

impl QUICTransport {
    pub fn new(connection: quinn::Connection) -> Self {
        Self {
            connection,
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
impl Connection for QUICTransport {
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
        let mut send = self.connection.open_uni().await?;
        send.write_all(data).await?;
        send.finish()?;
        self.notify_observers(&ConnectionEvent::Message(data.to_vec()));
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Box<dyn Error>> {
        self.connection.close(0u32.into(), b"done");
        self.notify_observers(&ConnectionEvent::Disconnected("Closed by client".to_string()));
        Ok(())
    }
}