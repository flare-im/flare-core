use crate::common::error::{FlareError, Result};
use crate::transport::connection::Connection;
use crate::transport::events::{
    ArcObserver, ConnectionEvent, attach_observer, notify_observers as notify_connection_observers,
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
    /// 流所在的 QUIC 连接（可选）。有它时 [`Connection::close_going_away`] 以应用错误码 0 和原因
    /// 关闭整条连接，对端读到的是 `ApplicationClosed` 与原因，而不只是流结束。
    connection: Option<quinn::Connection>,
}

/// 读流失败的两类结局。
enum ReadFailure {
    /// 对端在连接层面结束了连接（应用关闭、不带协议错误的传输层关闭、空闲超时、无状态重置）、
    /// 本端已关闭连接，或对端重置了这条流：这是断开，带上可读的原因。
    Closed(String),
    /// 真正的错误：本端或对端检测到的协议违例、版本不符、帧长度非法、读到一半流就结束等。
    Error(FlareError),
}

/// 对端以传输层 CONNECTION_CLOSE 结束连接时，这个错误码是否只是断开。
///
/// 正常关闭（`NO_ERROR`）、对端拒绝或自身故障（`CONNECTION_REFUSED`、`INTERNAL_ERROR`）、握手期间的
/// 应用关闭（`APPLICATION_ERROR`）、密钥用量到顶（`AEAD_LIMIT_REACHED`）与没有可用路径
/// （`NO_VIABLE_PATH`）都是断开，重连即可。其余错误码是对端判定我方违反协议（协议违例、帧编码、
/// 流量控制、流状态、传输参数、令牌、密钥更新）或 TLS 告警（`0x100`–`0x1ff`），按错误上报：
/// 重连多半还会撞上同一个问题，应当被看到。
fn peer_transport_close_is_disconnect(code: quinn::TransportErrorCode) -> bool {
    use quinn::TransportErrorCode as Code;
    [
        Code::NO_ERROR,
        Code::INTERNAL_ERROR,
        Code::CONNECTION_REFUSED,
        Code::APPLICATION_ERROR,
        Code::AEAD_LIMIT_REACHED,
        Code::NO_VIABLE_PATH,
    ]
    .contains(&code)
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
            connection: None,
        }
    }

    /// 记下流所在的 QUIC 连接，让 [`Connection::close_going_away`] 能在连接层面关闭并带上原因。
    pub fn with_connection(mut self, connection: quinn::Connection) -> Self {
        self.connection = Some(connection);
        self
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
                Err(ReadFailure::Closed(reason)) => {
                    // 连接级关闭（对端关连接、空闲超时、无状态重置）或流被重置：这是断开，不是错误。
                    // 以前一律报 Error，客户端把服务端停机、对端重启都当成故障处理。
                    debug!("[QUIC Transport] Connection closed: {}", reason);
                    Self::mark_closed_by_peer(&is_closed, &send_stream);
                    Self::notify_observers_and_clear(
                        &observers_arc,
                        &ConnectionEvent::Disconnected(reason),
                    );
                    break;
                }
                Err(ReadFailure::Error(e)) => {
                    debug!("[QUIC Transport] Read error: {}", e);
                    Self::mark_closed_by_peer(&is_closed, &send_stream);
                    Self::notify_observers_and_clear(&observers_arc, &ConnectionEvent::Error(e));
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
    async fn read_stream(
        recv: &mut quinn::RecvStream,
    ) -> std::result::Result<Vec<u8>, ReadFailure> {
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
                        return Err(ReadFailure::Error(FlareError::io(
                            "Stream closed while reading length prefix".to_string(),
                        )));
                    }
                }
                Ok(Some(n)) => {
                    length_bytes_read += n;
                }
                Err(e) => {
                    return Err(Self::classify_read_error(e));
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
            return Err(ReadFailure::Error(FlareError::io(format!(
                "Message too large: {} bytes",
                length
            ))));
        }

        // 读取完整的消息数据
        let mut buf = vec![0u8; length];
        let mut bytes_read = 0;

        while bytes_read < length {
            match recv.read(&mut buf[bytes_read..]).await {
                Ok(Some(0)) | Ok(None) => {
                    // EOF，但还没读完所有数据
                    return Err(ReadFailure::Error(FlareError::io(format!(
                        "Stream closed while reading message: expected {} bytes, got {}",
                        length, bytes_read
                    ))));
                }
                Ok(Some(n)) => {
                    bytes_read += n;
                }
                Err(e) => {
                    return Err(Self::classify_read_error(e));
                }
            }
        }

        Ok(buf)
    }

    /// 区分连接级关闭与真正的读错误。
    ///
    /// 对端以应用错误关闭连接时，原因是非空 UTF-8 就原样作为断开原因（例如服务端停机时的
    /// `SERVER_SHUTDOWN`），上层据此识别类型化原因。对端以传输层错误关闭连接时按错误码区分，
    /// 见 [`peer_transport_close_is_disconnect`]。
    fn classify_read_error(error: quinn::ReadError) -> ReadFailure {
        use quinn::{ConnectionError, ReadError};
        match &error {
            ReadError::ConnectionLost(ConnectionError::ApplicationClosed(close)) => {
                let reason = std::str::from_utf8(&close.reason)
                    .ok()
                    .map(str::trim)
                    .filter(|reason| !reason.is_empty());
                ReadFailure::Closed(match reason {
                    Some(reason) => reason.to_string(),
                    None => format!(
                        "QUIC connection closed by peer (application code {})",
                        close.error_code
                    ),
                })
            }
            ReadError::ConnectionLost(ConnectionError::ConnectionClosed(close)) => {
                if peer_transport_close_is_disconnect(close.error_code) {
                    ReadFailure::Closed(format!("QUIC connection closed by peer: {close}"))
                } else {
                    ReadFailure::Error(FlareError::protocol_error(format!(
                        "QUIC connection closed by peer with a protocol error: {close}"
                    )))
                }
            }
            ReadError::ConnectionLost(
                lost @ (ConnectionError::TimedOut
                | ConnectionError::Reset
                | ConnectionError::LocallyClosed),
            ) => ReadFailure::Closed(format!("QUIC connection closed: {lost}")),
            ReadError::Reset(code) => {
                ReadFailure::Closed(format!("QUIC stream reset by peer (code {code})"))
            }
            ReadError::ConnectionLost(lost) => {
                ReadFailure::Error(FlareError::io(format!("connection lost: {lost}")))
            }
            _ => ReadFailure::Error(FlareError::io(error.to_string())),
        }
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
        // 已经关闭的流只报 Disconnected，否则先报 Connected 再挂上；
        // 两种回调都隔离观察者 panic（见 `attach_observer`）。
        if attach_observer(
            &self.observers,
            &self.is_closed,
            observer,
            STREAM_CLOSED_REASON,
            "quic add_observer",
        ) {
            self.reader_gate.notify_one();
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

    /// 结束发送流后，有连接句柄时以应用错误码 0 和 `reason` 关闭整条连接：对端读到
    /// `ApplicationClosed` 与原因，而不是只看到流结束。
    async fn close_going_away(&mut self, reason: &str) -> Result<()> {
        let result = self.close_with_reason(reason).await;
        if let Some(connection) = &self.connection {
            connection.close(0u32.into(), reason.as_bytes());
        }
        result
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

#[cfg(test)]
mod tests {
    use super::*;
    use quinn::{
        ApplicationClose, ConnectionClose, ConnectionError, ReadError, TransportErrorCode, VarInt,
    };

    fn peer_transport_close(code: TransportErrorCode) -> ReadError {
        ReadError::ConnectionLost(ConnectionError::ConnectionClosed(ConnectionClose {
            error_code: code,
            frame_type: None,
            reason: bytes::Bytes::from_static(b"peer detail"),
        }))
    }

    fn closed(failure: ReadFailure) -> Option<String> {
        match failure {
            ReadFailure::Closed(reason) => Some(reason),
            ReadFailure::Error(_) => None,
        }
    }

    /// 对端以应用错误关闭连接是断开：原因原样带上（服务端停机时是 SERVER_SHUTDOWN）。
    #[test]
    fn application_close_is_a_disconnect_with_the_peer_reason() {
        let failure = QUICTransport::classify_read_error(ReadError::ConnectionLost(
            ConnectionError::ApplicationClosed(ApplicationClose {
                error_code: VarInt::from_u32(0),
                reason: bytes::Bytes::from_static(b"SERVER_SHUTDOWN"),
            }),
        ));
        let reason = closed(failure).expect("application close is a disconnect");
        assert_eq!(reason, "SERVER_SHUTDOWN");
        assert!(crate::common::is_server_shutdown_reason(&reason));

        let failure = QUICTransport::classify_read_error(ReadError::ConnectionLost(
            ConnectionError::ApplicationClosed(ApplicationClose {
                error_code: VarInt::from_u32(7),
                reason: bytes::Bytes::new(),
            }),
        ));
        assert_eq!(
            closed(failure).as_deref(),
            Some("QUIC connection closed by peer (application code 7)")
        );
    }

    /// 空闲超时、无状态重置、本端已关闭、对端重置流，以及对端不带协议错误的传输层关闭都是断开，
    /// 不是错误。
    #[test]
    fn connection_level_closes_are_disconnects() {
        for error in [
            ReadError::ConnectionLost(ConnectionError::TimedOut),
            ReadError::ConnectionLost(ConnectionError::Reset),
            ReadError::ConnectionLost(ConnectionError::LocallyClosed),
            ReadError::Reset(VarInt::from_u32(3)),
            peer_transport_close(TransportErrorCode::NO_ERROR),
            peer_transport_close(TransportErrorCode::INTERNAL_ERROR),
            peer_transport_close(TransportErrorCode::CONNECTION_REFUSED),
            peer_transport_close(TransportErrorCode::APPLICATION_ERROR),
            peer_transport_close(TransportErrorCode::AEAD_LIMIT_REACHED),
            peer_transport_close(TransportErrorCode::NO_VIABLE_PATH),
        ] {
            let description = error.to_string();
            assert!(
                closed(QUICTransport::classify_read_error(error)).is_some(),
                "{description} must be reported as a disconnect"
            );
        }
    }

    /// 协议违例、版本不符、流状态错误仍是错误；对端以协议类错误码或 TLS 告警关闭连接也是错误。
    #[test]
    fn protocol_failures_stay_errors() {
        for error in [
            ReadError::ConnectionLost(ConnectionError::VersionMismatch),
            ReadError::ConnectionLost(ConnectionError::CidsExhausted),
            ReadError::ClosedStream,
            ReadError::IllegalOrderedRead,
            peer_transport_close(TransportErrorCode::PROTOCOL_VIOLATION),
            peer_transport_close(TransportErrorCode::FRAME_ENCODING_ERROR),
            peer_transport_close(TransportErrorCode::FLOW_CONTROL_ERROR),
            peer_transport_close(TransportErrorCode::STREAM_STATE_ERROR),
            peer_transport_close(TransportErrorCode::TRANSPORT_PARAMETER_ERROR),
            peer_transport_close(TransportErrorCode::KEY_UPDATE_ERROR),
            peer_transport_close(TransportErrorCode::crypto(0x2a)),
        ] {
            let description = error.to_string();
            assert!(
                closed(QUICTransport::classify_read_error(error)).is_none(),
                "{description} must stay an error"
            );
        }
    }
}
