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
        use tracing::debug;
        
        loop {
            // 检查是否已关闭
            if let Ok(closed) = is_closed.lock() {
                if *closed {
                    debug!("[QUIC Transport] Receiver task: connection closed");
                    break;
                }
            }

            let mut recv = recv_stream.lock().await;
            
            // 收到消息时更新活跃时间
            if let Ok(mut active) = last_active.lock() {
                *active = std::time::Instant::now();
            }

            // 读取完整消息（带长度前缀）
            match Self::read_stream(&mut *recv).await {
                Ok(data) => {
                    if !data.is_empty() {
                        debug!("[QUIC Transport] Received message: {} bytes", data.len());
                        Self::_notify_observers(&observers_arc, &ConnectionEvent::Message(data));
                    } else {
                        // EOF，流结束
                        debug!("[QUIC Transport] Stream EOF, closing");
                        Self::_notify_observers(
                            &observers_arc,
                            &ConnectionEvent::Disconnected("Stream closed by peer".to_string()),
                        );
                        break;
                    }
                }
                Err(e) => {
                    // 读取失败，发送错误事件
                    debug!("[QUIC Transport] Read error: {}", e);
                    Self::_notify_observers(
                        &observers_arc,
                        &ConnectionEvent::Error(FlareError::io(e.to_string())),
                    );
                    break;
                }
            }
            
            // 释放锁，允许其他任务使用流
            drop(recv);
        }
        
        debug!("[QUIC Transport] Receiver task ended");
    }

    /// 从流中读取完整消息（使用长度前缀）
    /// 
    /// 消息格式：4字节长度前缀（u32，网络字节序）+ 消息数据
    async fn read_stream(recv: &mut quinn::RecvStream) -> Result<Vec<u8>> {
        // 首先读取长度前缀（4字节）
        let mut length_buf = [0u8; 4];
        let mut length_bytes_read = 0;
        
        // 读取长度前缀的4个字节
        while length_bytes_read < 4 {
            match recv.read(&mut length_buf[length_bytes_read..]).await {
                Ok(Some(0)) | Ok(None) => {
                    // EOF，流结束
                    if length_bytes_read == 0 {
                        // 没有读取到任何数据，表示流正常结束
                        return Ok(Vec::new());
                    } else {
                        // 部分读取了长度前缀，这是错误
                        return Err(FlareError::io("Stream closed while reading length prefix".to_string()));
                    }
                }
                Ok(Some(n)) => {
                    length_bytes_read += n;
                }
                Err(e) => {
                    return Err(FlareError::io(e.to_string()));
                }
            }
        }
        
        // 解析长度（u32，网络字节序/大端序）
        let length = u32::from_be_bytes(length_buf) as usize;
        
        if length == 0 {
            return Ok(Vec::new());
        }
        
        if length > 10 * 1024 * 1024 {
            // 限制最大消息大小为 10MB
            return Err(FlareError::io(format!("Message too large: {} bytes", length)));
        }
        
        // 读取完整的消息数据
        let mut buf = vec![0u8; length];
        let mut bytes_read = 0;
        
        while bytes_read < length {
            match recv.read(&mut buf[bytes_read..]).await {
                Ok(Some(0)) | Ok(None) => {
                    // EOF，但还没读完所有数据
                    return Err(FlareError::io(format!("Stream closed while reading message: expected {} bytes, got {}", length, bytes_read)));
                }
                Ok(Some(n)) => {
                    bytes_read += n;
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
        // 先发送长度前缀（4字节，u32，网络字节序）
        let mut send = self.send_stream.lock().await;
        let length = data.len() as u32;
        let length_bytes = length.to_be_bytes();
        
        // 先发送长度前缀
        send.write_all(&length_bytes)
            .await
            .map_err(|e| FlareError::io(e.to_string()))?;
        
        // 再发送消息数据
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