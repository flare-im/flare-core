use crate::common::error::{FlareError, Result};
use crate::transport::connection::Connection;
use crate::transport::events::{
    ArcObserver, ConnectionEvent, notify_observers as notify_connection_observers,
    notify_observers_and_clear as notify_connection_observers_and_clear,
};
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as TokioMutex, Notify};

/// 流已关闭（对端结束了它那一侧、读出错，或本端已关闭）之后，发送与挂观察者都报这个原因。
const STREAM_CLOSED_REASON: &str = "QUIC stream is closed";

pub struct QUICTransport {
    send_stream: Arc<TokioMutex<quinn::SendStream>>,
    observers: Arc<Mutex<Vec<ArcObserver>>>,
    last_active: Arc<Mutex<std::time::Instant>>,
    /// 流是否已经不可用。对端 EOF、读出错、本端关闭都会置位：此后 `send` 立即失败，
    /// 新挂的观察者只会收到 Disconnected，不会被告知 Connected。
    is_closed: Arc<AtomicBool>,
    /// 读任务等第一个观察者挂上才开始读（本端关闭或传输对象释放时也放行，让它退出）。
    reader_gate: Arc<Notify>,
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
        let is_closed = Arc::new(AtomicBool::new(false));
        let reader_gate = Arc::new(Notify::new());
        let send_stream = Arc::new(TokioMutex::new(send_stream));

        // 启动接收任务（使用提供的 RecvStream）
        tokio::spawn(Self::receiver_task(
            recv_stream,
            Arc::clone(&send_stream),
            Arc::clone(&observers),
            Arc::clone(&last_active),
            Arc::clone(&is_closed),
            Arc::clone(&reader_gate),
        ));

        Self {
            send_stream,
            observers,
            last_active,
            is_closed,
            reader_gate,
        }
    }

    /// 接收任务：使用提供的 RecvStream 接收消息
    ///
    /// 对端结束它那一侧（EOF）或读出错时，这条流就不能再用了：先标记关闭，本端也结束发送，
    /// 再上报。只上报不标记的话，传输对象还会被当成活的——在它上面再发 CONNECT，
    /// 写入会成功，但对端早已不读这条流，客户端只能干等 CONNECT_ACK。
    async fn receiver_task(
        mut recv: quinn::RecvStream,
        send_stream: Arc<TokioMutex<quinn::SendStream>>,
        observers_arc: Arc<Mutex<Vec<ArcObserver>>>,
        last_active: Arc<Mutex<std::time::Instant>>,
        is_closed: Arc<AtomicBool>,
        reader_gate: Arc<Notify>,
    ) {
        use tracing::debug;

        // 第一个观察者挂上之前不读：此前读出的帧没人接收，会被直接丢掉。服务端 accept_bi 返回时
        // 客户端的 CONNECT 往往已经到了，而观察者要等连接登记完才挂上——CONNECT 丢了，
        // 客户端只能干等 CONNECT_ACK。
        reader_gate.notified().await;

        loop {
            if is_closed.load(Ordering::SeqCst) {
                debug!("[QUIC Transport] Receiver task: connection closed");
                break;
            }

            // 读取完整消息（带长度前缀）
            let read = Self::read_stream(&mut recv).await;
            if is_closed.load(Ordering::SeqCst) {
                debug!("[QUIC Transport] Receiver task: closed locally");
                break;
            }

            // 收到消息时更新活跃时间
            if let Ok(mut active) = last_active.lock() {
                *active = std::time::Instant::now();
            }

            match read {
                Ok(data) if !data.is_empty() => {
                    debug!("[QUIC Transport] Received message: {} bytes", data.len());
                    Self::notify_observers(&observers_arc, &ConnectionEvent::Message(data));
                }
                Ok(_) => {
                    // EOF：对端结束了它那一侧（被踢、被服务端移除）
                    debug!("[QUIC Transport] Stream EOF, closing");
                    Self::mark_closed_by_peer(&is_closed, &send_stream);
                    Self::notify_observers_and_clear(
                        &observers_arc,
                        &ConnectionEvent::Disconnected("Stream closed by peer".to_string()),
                    );
                    break;
                }
                Err(e) => {
                    // 读取失败（含连接级关闭，如对端关连接、空闲超时），发送错误事件
                    debug!("[QUIC Transport] Read error: {}", e);
                    Self::mark_closed_by_peer(&is_closed, &send_stream);
                    Self::notify_observers_and_clear(
                        &observers_arc,
                        &ConnectionEvent::Error(FlareError::io(e.to_string())),
                    );
                    break;
                }
            }
        }

        debug!("[QUIC Transport] Receiver task ended");
    }

    /// 对端那一侧已经结束：标记关闭，并结束本端发送，让对端的读循环也能尽快收尾。
    /// 发送端正被占用（例如一次写被流控卡住）时不等它，关闭或释放时自会结束。
    fn mark_closed_by_peer(
        is_closed: &AtomicBool,
        send_stream: &Arc<TokioMutex<quinn::SendStream>>,
    ) {
        is_closed.store(true, Ordering::SeqCst);
        if let Ok(mut send) = send_stream.try_lock() {
            let _ = send.finish();
        }
    }

    fn closed_error() -> FlareError {
        FlareError::connection_closed(STREAM_CLOSED_REASON.to_string())
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
                        return Err(FlareError::io(
                            "Stream closed while reading length prefix".to_string(),
                        ));
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
            return Err(FlareError::io(format!(
                "Message too large: {} bytes",
                length
            )));
        }

        // 读取完整的消息数据
        let mut buf = vec![0u8; length];
        let mut bytes_read = 0;

        while bytes_read < length {
            match recv.read(&mut buf[bytes_read..]).await {
                Ok(Some(0)) | Ok(None) => {
                    // EOF，但还没读完所有数据
                    return Err(FlareError::io(format!(
                        "Stream closed while reading message: expected {} bytes, got {}",
                        length, bytes_read
                    )));
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

    fn notify_observers(observers_arc: &Arc<Mutex<Vec<ArcObserver>>>, event: &ConnectionEvent) {
        notify_connection_observers(observers_arc, event, "quic observers");
    }

    fn notify_observers_and_clear(
        observers_arc: &Arc<Mutex<Vec<ArcObserver>>>,
        event: &ConnectionEvent,
    ) {
        notify_connection_observers_and_clear(observers_arc, event, "quic observers");
    }

    fn notify_self_observers_and_clear(&self, event: &ConnectionEvent) {
        Self::notify_observers_and_clear(&self.observers, event);
    }
}

#[async_trait]
impl Connection for QUICTransport {
    fn add_observer(&mut self, observer: ArcObserver) {
        // 已经关闭的流不能再报 Connected：否则上层会在一条死流上发 CONNECT 并干等 CONNECT_ACK。
        if self.is_closed.load(Ordering::SeqCst) {
            observer.on_event(&ConnectionEvent::Disconnected(
                STREAM_CLOSED_REASON.to_string(),
            ));
            return;
        }
        observer.on_event(&ConnectionEvent::Connected);
        // 与读任务的「先标记关闭、再取走观察者」配对：持锁时仍未关闭，读任务之后一定会通知到它；
        // 已经关闭，就由这里补一次 Disconnected。
        let registered = match self.observers.lock() {
            Ok(mut observers) if !self.is_closed.load(Ordering::SeqCst) => {
                observers.push(Arc::clone(&observer));
                true
            }
            _ => false,
        };
        if registered {
            self.reader_gate.notify_one();
        } else {
            observer.on_event(&ConnectionEvent::Disconnected(
                STREAM_CLOSED_REASON.to_string(),
            ));
        }
    }

    fn remove_observer(&mut self, observer: ArcObserver) {
        if let Ok(mut observers) = self.observers.lock() {
            observers.retain(|o| !Arc::ptr_eq(o, &observer));
        }
    }

    async fn send(&mut self, data: &[u8]) -> Result<()> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Err(Self::closed_error());
        }

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
        self.close_with_reason("Closed by client").await
    }

    async fn close_with_reason(&mut self, reason: &str) -> Result<()> {
        // 标记为已关闭：读任务此后读到的帧不再上报，读到下一帧或 EOF 即退出
        let was_closed = self.is_closed.swap(true, Ordering::SeqCst);
        // 从没挂过观察者的流（如协议竞速落选的连接），读任务还在等：放它退出
        self.reader_gate.notify_one();

        // 关闭发送流。对端先结束时读任务已经结束过它，那时再 finish 报的错不算关闭失败。
        let close_result = {
            let mut send = self.send_stream.lock().await;
            match send.finish() {
                Ok(()) => Ok(()),
                Err(_) if was_closed => Ok(()),
                Err(e) => Err(FlareError::io(e.to_string())),
            }
        };

        self.notify_self_observers_and_clear(&ConnectionEvent::Disconnected(reason.to_string()));
        close_result
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

impl Drop for QUICTransport {
    fn drop(&mut self) {
        // 没关就被释放、且从没挂过观察者的流，读任务还在等：放它退出，别连同接收流一起泄漏。
        self.is_closed.store(true, Ordering::SeqCst);
        self.reader_gate.notify_one();
    }
}
