use crate::common::error::{FlareError, Result};
use crate::transport::connection::Connection;
use crate::transport::events::{ArcObserver, ConnectionEvent};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub struct TCPTransport {
    stream: TcpStream,
    observers: Mutex<Vec<ArcObserver>>,
    last_active: Mutex<std::time::Instant>,
}

impl TCPTransport {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            observers: Mutex::new(Vec::new()),
            last_active: Mutex::new(std::time::Instant::now()),
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

    async fn send(&mut self, data: &[u8]) -> Result<()> {
        // 发送消息时更新活跃时间
        if let Ok(mut active) = self.last_active.lock() {
            *active = std::time::Instant::now();
        }

        self.stream
            .write_all(data)
            .await
            .map_err(|e| FlareError::io(e.to_string()))?;
        self.notify_observers(&ConnectionEvent::Message(data.to_vec()));
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.stream
            .shutdown()
            .await
            .map_err(|e| FlareError::connection_closed(e.to_string()))?;
        self.notify_observers(&ConnectionEvent::Disconnected(
            "Closed by client".to_string(),
        ));
        Ok(())
    }

    fn last_active_time(&self) -> std::time::Instant {
        self.last_active
            .lock()
            .map(|guard| *guard)
            .unwrap_or_else(|_| {
                // 如果锁被 poison，返回当前时间减去一个较大值，表示连接可能有问题
                std::time::Instant::now() - std::time::Duration::from_secs(3600)
            })
    }

    fn update_active_time(&mut self) {
        if let Ok(mut active) = self.last_active.lock() {
            *active = std::time::Instant::now();
        }
        // 如果锁被 poison，忽略更新（连接可能已经出问题）
    }
}
