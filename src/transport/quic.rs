use crate::common::error::{FlareError, Result};
use crate::transport::connection::Connection;
use crate::transport::events::{ArcObserver, ConnectionEvent};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as TokioMutex;

pub struct QUICTransport {
    send_stream: Arc<TokioMutex<quinn::SendStream>>,
    observers: Arc<Mutex<Vec<ArcObserver>>>,
    last_active: Arc<Mutex<std::time::Instant>>,
    is_closed: Arc<Mutex<bool>>,
}

impl QUICTransport {
    /// 从 SendStream 和 RecvStream 创建 QUICTransport
    /// 
    /// 直接使用已经打开的双向流的发送端和接收端：
    /// - 发送：使用提供的 SendStream 发送数据
    /// - 接收：使用提供的 RecvStream 接收数据
    /// 
    /// 这样设计的好处：
    /// 1. 流的创建由调用者管理，更灵活
    /// 2. 不需要维护 Connection，只关注流的使用
    /// 3. 适用于从 open_bi() 或 accept_bi() 获得的流
    pub fn new(send_stream: quinn::SendStream, recv_stream: quinn::RecvStream) -> Self {
        let observers = Arc::new(Mutex::new(Vec::new()));
        let last_active = Arc::new(Mutex::new(std::time::Instant::now()));
        let is_closed = Arc::new(Mutex::new(false));
        let send_stream = Arc::new(TokioMutex::new(send_stream));

        // 启动接收任务（使用提供的 RecvStream）
        let task_recv = Arc::new(TokioMutex::new(recv_stream));
        let task_observers = Arc::clone(&observers);
        let task_last_active = Arc::clone(&last_active);
        let task_is_closed = Arc::clone(&is_closed);
        tokio::spawn(Self::receiver_task(
            task_recv,
            task_observers,
            task_last_active,
            task_is_closed,
        ));

        Self {
            send_stream,
            observers,
            last_active,
            is_closed,
        }
    }

    /// 接收任务：使用提供的 RecvStream 接收消息
    async fn receiver_task(
        recv_stream: Arc<TokioMutex<quinn::RecvStream>>,
        observers_arc: Arc<Mutex<Vec<ArcObserver>>>,
        last_active: Arc<Mutex<std::time::Instant>>,
        is_closed: Arc<Mutex<bool>>,
    ) {
        loop {
            // 检查是否已关闭
            if let Ok(closed) = is_closed.lock() {
                if *closed {
                    break;
                }
            }

            let mut recv = recv_stream.lock().await;
            
            // 收到消息时更新活跃时间
            if let Ok(mut active) = last_active.lock() {
                *active = std::time::Instant::now();
            }

            // 读取完整消息
            match Self::read_stream(&mut *recv).await {
                Ok(data) => {
                    if !data.is_empty() {
                        Self::_notify_observers(&observers_arc, &ConnectionEvent::Message(data));
                    } else {
                        // EOF，流结束
                        Self::_notify_observers(
                            &observers_arc,
                            &ConnectionEvent::Disconnected("Stream closed by peer".to_string()),
                        );
                        break;
                    }
                }
                Err(e) => {
                    // 读取失败，发送错误事件
                    Self::_notify_observers(
                        &observers_arc,
                        &ConnectionEvent::Error(FlareError::io(e.to_string())),
                    );
                    break;
                }
            }
        }
    }

    /// 从流中读取完整消息
    async fn read_stream(recv: &mut quinn::RecvStream) -> Result<Vec<u8>> {
        let mut buf = Vec::<u8>::new();
        let mut temp_buf = vec![0u8; 4096];
        
        loop {
            match recv.read(&mut temp_buf).await {
                Ok(Some(0)) | Ok(None) => {
                    // EOF，读取完成
                    break;
                }
                Ok(Some(n)) => {
                    buf.extend_from_slice(&temp_buf[..n]);
                }
                Err(e) => {
                    return Err(FlareError::io(e.to_string()));
                }
            }
        }
        
        Ok(buf)
    }

    // 私有辅助方法，用于通知所有观察者
    fn _notify_observers(
        observers_arc: &Arc<Mutex<Vec<ArcObserver>>>,
        event: &ConnectionEvent,
    ) {
        if let Ok(observers) = observers_arc.lock() {
            for observer in observers.iter() {
                observer.on_event(event);
            }
        }
    }

    fn notify_observers(&self, event: &ConnectionEvent) {
        Self::_notify_observers(&self.observers, event);
    }
}

#[async_trait]
impl Connection for QUICTransport {
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
        // 发送消息时更新活跃时间
        if let Ok(mut active) = self.last_active.lock() {
            *active = std::time::Instant::now();
        }

        // 使用已有的 SendStream 发送数据
        let mut send = self.send_stream.lock().await;
        send.write_all(data)
            .await
            .map_err(|e| FlareError::io(e.to_string()))?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        // 标记为已关闭
        if let Ok(mut closed) = self.is_closed.lock() {
            *closed = true;
        }
        
        // 关闭发送流
        let mut send = self.send_stream.lock().await;
        send.finish()
            .map_err(|e| FlareError::io(e.to_string()))?;
        
        self.notify_observers(&ConnectionEvent::Disconnected("Closed by client".to_string()));
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