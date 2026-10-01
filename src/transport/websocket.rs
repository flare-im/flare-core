use crate::common::error::{FlareError, Result};
use crate::common::protocol::{Reliability, frame_with_system_command, pong};
use crate::transport::connection::Connection;
use crate::transport::events::{
    ArcObserver, ConnectionEvent, notify_observers as notify_connection_observers,
    notify_observers_and_clear as notify_connection_observers_and_clear,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::SinkExt;
use futures_util::stream::{SplitSink, SplitStream, StreamExt};
use prost::Message as ProstMessage;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Error as WsError, Message, error::ProtocolError};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

// 使用枚举来支持两种类型的 WebSocketStream
enum WebSocketSink {
    Tls(Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>),
    Plain(Arc<Mutex<SplitSink<WebSocketStream<TcpStream>, Message>>>),
}

/// 连接已关闭（对端关闭、读出错，或本端已关闭）之后，发送与挂观察者都报这个原因。
const CONNECTION_CLOSED_REASON: &str = "WebSocket connection is closed";

pub struct WebSocketTransport {
    sink: WebSocketSink,
    observers: Arc<std::sync::Mutex<Vec<ArcObserver>>>,
    last_active: Arc<std::sync::Mutex<std::time::Instant>>,
    /// 连接是否已经不可用。对端关闭、读出错、本端关闭都会置位：此后 `send` 立即失败，
    /// 新挂的观察者只会收到 Disconnected，不会被告知 Connected。
    is_closed: Arc<AtomicBool>,
    /// 读任务等第一个观察者挂上才开始读（本端关闭或传输对象释放时也放行，让它退出）。
    reader_gate: Arc<Notify>,
}

/// 读任务与传输对象共享的状态。
struct ReaderShared {
    observers: Arc<std::sync::Mutex<Vec<ArcObserver>>>,
    last_active: Arc<std::sync::Mutex<std::time::Instant>>,
    is_closed: Arc<AtomicBool>,
    reader_gate: Arc<Notify>,
}

impl ReaderShared {
    fn new() -> Self {
        Self {
            observers: Arc::new(std::sync::Mutex::new(Vec::new())),
            last_active: Arc::new(std::sync::Mutex::new(std::time::Instant::now())),
            is_closed: Arc::new(AtomicBool::new(false)),
            reader_gate: Arc::new(Notify::new()),
        }
    }

    fn for_task(&self) -> Self {
        Self {
            observers: Arc::clone(&self.observers),
            last_active: Arc::clone(&self.last_active),
            is_closed: Arc::clone(&self.is_closed),
            reader_gate: Arc::clone(&self.reader_gate),
        }
    }

    /// 等第一个观察者挂上再读。此前读出的帧没人接收，会被直接丢掉：服务端握手完成时客户端的
    /// CONNECT 往往已经到了，而观察者要等连接登记完才挂上——CONNECT 丢了，客户端只能干等
    /// CONNECT_ACK。返回 false 表示等待期间连接已关闭（本端关闭或传输对象被释放），读任务应退出。
    async fn wait_for_first_observer(&self) -> bool {
        self.reader_gate.notified().await;
        !self.is_closed.load(Ordering::SeqCst)
    }

    fn touch(&self) {
        if let Ok(mut active) = self.last_active.lock() {
            *active = std::time::Instant::now();
        }
    }

    /// 上报一个事件；终止事件（断开、出错）先标记关闭再取走观察者，返回 true 表示读任务应结束。
    fn dispatch(&self, event: ConnectionEvent) -> bool {
        let is_terminal = matches!(
            event,
            ConnectionEvent::Disconnected(_) | ConnectionEvent::Error(_)
        );
        if is_terminal {
            self.is_closed.store(true, Ordering::SeqCst);
            notify_connection_observers_and_clear(&self.observers, &event, "websocket observers");
        } else {
            notify_connection_observers(&self.observers, &event, "websocket observers");
        }
        is_terminal
    }
}

impl WebSocketTransport {
    pub fn new(stream: WebSocketStream<MaybeTlsStream<TcpStream>>) -> Self {
        Self::from_stream(stream)
    }

    fn event_from_websocket_error(error: WsError) -> ConnectionEvent {
        match error {
            WsError::ConnectionClosed | WsError::AlreadyClosed => {
                ConnectionEvent::Disconnected("WebSocket connection closed by peer".to_string())
            }
            WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => {
                ConnectionEvent::Disconnected(
                    "WebSocket peer disconnected without close handshake".to_string(),
                )
            }
            WsError::Io(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::UnexpectedEof
                ) =>
            {
                ConnectionEvent::Disconnected(format!("WebSocket peer disconnected: {}", err))
            }
            other => ConnectionEvent::Error(FlareError::connection_failed(other.to_string())),
        }
    }

    /// 从 `WebSocketStream<TcpStream>` 创建（在没有 TLS 时使用）
    ///
    /// 使用单独的 Plain 类型，避免 unsafe transmute
    pub fn from_tcp_stream(stream: WebSocketStream<TcpStream>) -> Self {
        let (sink_plain, receiver_plain) = stream.split();

        let shared = ReaderShared::new();
        let sink_arc = Arc::new(Mutex::new(sink_plain));

        tokio::spawn(Self::receiver_task_plain(
            receiver_plain,
            shared.for_task(),
            Arc::clone(&sink_arc),
        ));

        Self::with_shared(WebSocketSink::Plain(sink_arc), shared)
    }

    fn with_shared(sink: WebSocketSink, shared: ReaderShared) -> Self {
        Self {
            sink,
            observers: shared.observers,
            last_active: shared.last_active,
            is_closed: shared.is_closed,
            reader_gate: shared.reader_gate,
        }
    }

    fn from_stream(stream: WebSocketStream<MaybeTlsStream<TcpStream>>) -> Self {
        let (sink, receiver) = stream.split();
        let shared = ReaderShared::new();
        let sink_arc = Arc::new(Mutex::new(sink));

        tokio::spawn(Self::receiver_task(
            receiver,
            shared.for_task(),
            Arc::clone(&sink_arc),
        ));

        Self::with_shared(WebSocketSink::Tls(sink_arc), shared)
    }

    async fn receiver_task(
        mut receiver: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        shared: ReaderShared,
        sink_arc: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
    ) {
        if !shared.wait_for_first_observer().await {
            return;
        }
        while let Some(message) = receiver.next().await {
            // 本端已关闭：此后读到的帧不再上报
            if shared.is_closed.load(Ordering::SeqCst) {
                break;
            }
            // 收到消息时更新活跃时间
            shared.touch();

            let event = match message {
                Ok(msg) => match msg {
                    Message::Text(text) => Some(ConnectionEvent::Message(text.as_bytes().to_vec())),
                    Message::Binary(data) => Some(ConnectionEvent::Message(data.to_vec())),
                    Message::Close(frame) => {
                        let reason = frame
                            .map(|f| f.reason.to_string())
                            .unwrap_or_else(|| "Connection closed by peer".to_string());
                        Some(ConnectionEvent::Disconnected(reason))
                    }
                    Message::Ping(data) => {
                        // 收到 WebSocket 协议层的 PING
                        // 1. 先回复 WebSocket 协议层的 PONG（保持连接）
                        // 2. 然后使用 builder 构建应用层的 PONG Frame 并发送
                        if let Err(e) = Self::send_pong_response_tls(&sink_arc, &data).await {
                            Some(ConnectionEvent::Error(e))
                        } else if let Err(e) = Self::send_pong_frame_tls(&sink_arc).await {
                            Some(ConnectionEvent::Error(e))
                        } else {
                            None // PING/PONG 已处理，不需要触发事件
                        }
                    }
                    Message::Pong(_) => {
                        // 收到 WebSocket 协议层的 PONG，这是对我们之前发送的 PING 的响应
                        // 使用 builder 构建应用层的 PONG Frame，通过事件通知上层处理
                        match Self::build_pong_frame() {
                            Ok(pong_data) => Some(ConnectionEvent::Message(pong_data)),
                            Err(e) => Some(ConnectionEvent::Error(e)),
                        }
                    }
                    _ => None,
                },
                Err(e) => Some(Self::event_from_websocket_error(e)),
            };

            if let Some(event) = event
                && shared.dispatch(event)
            {
                break;
            }
        }
        // 流结束（对端直接断开 TCP、没有 Close 帧）也是终止：标记关闭并告知还挂着的观察者。
        if !shared.is_closed.load(Ordering::SeqCst) {
            shared.dispatch(ConnectionEvent::Disconnected(
                "WebSocket connection closed by peer".to_string(),
            ));
        }
    }

    async fn receiver_task_plain(
        mut receiver: SplitStream<WebSocketStream<TcpStream>>,
        shared: ReaderShared,
        sink_arc: Arc<Mutex<SplitSink<WebSocketStream<TcpStream>, Message>>>,
    ) {
        if !shared.wait_for_first_observer().await {
            return;
        }
        while let Some(message) = receiver.next().await {
            // 本端已关闭：此后读到的帧不再上报
            if shared.is_closed.load(Ordering::SeqCst) {
                break;
            }
            // 收到消息时更新活跃时间
            shared.touch();

            let event = match message {
                Ok(msg) => match msg {
                    Message::Text(text) => Some(ConnectionEvent::Message(text.as_bytes().to_vec())),
                    Message::Binary(data) => Some(ConnectionEvent::Message(data.to_vec())),
                    Message::Close(frame) => {
                        let reason = frame
                            .map(|f| f.reason.to_string())
                            .unwrap_or_else(|| "Connection closed by peer".to_string());
                        Some(ConnectionEvent::Disconnected(reason))
                    }
                    Message::Ping(data) => {
                        // 收到 WebSocket 协议层的 PING
                        // 1. 先回复 WebSocket 协议层的 PONG（保持连接）
                        // 2. 然后使用 builder 构建应用层的 PONG Frame 并发送
                        if let Err(e) = Self::send_pong_response_plain(&sink_arc, &data).await {
                            Some(ConnectionEvent::Error(e))
                        } else if let Err(e) = Self::send_pong_frame_plain(&sink_arc).await {
                            Some(ConnectionEvent::Error(e))
                        } else {
                            None // PING/PONG 已处理，不需要触发事件
                        }
                    }
                    Message::Pong(_) => {
                        // 收到 WebSocket 协议层的 PONG，这是对我们之前发送的 PING 的响应
                        // 使用 builder 构建应用层的 PONG Frame，通过事件通知上层处理
                        match Self::build_pong_frame() {
                            Ok(pong_data) => Some(ConnectionEvent::Message(pong_data)),
                            Err(e) => Some(ConnectionEvent::Error(e)),
                        }
                    }
                    _ => None,
                },
                Err(e) => Some(Self::event_from_websocket_error(e)),
            };

            if let Some(event) = event
                && shared.dispatch(event)
            {
                break;
            }
        }
        // 流结束（对端直接断开 TCP、没有 Close 帧）也是终止：标记关闭并告知还挂着的观察者。
        if !shared.is_closed.load(Ordering::SeqCst) {
            shared.dispatch(ConnectionEvent::Disconnected(
                "WebSocket connection closed by peer".to_string(),
            ));
        }
    }

    fn notify_observers_and_clear(&self, event: &ConnectionEvent) {
        notify_connection_observers_and_clear(&self.observers, event, "websocket observers");
    }

    /// 关闭连接并以 `reason` 通知观察者；`frame` 为关闭帧要带的关闭码与原因（`None` 不带）。
    async fn close_inner(&mut self, reason: &str, frame: Option<CloseFrame>) -> Result<()> {
        // 标记为已关闭：读任务此后读到的帧不再上报
        let was_closed = self.is_closed.swap(true, Ordering::SeqCst);
        // 从没挂过观察者的连接（如协议竞速落选的连接），读任务还在等：放它退出
        self.reader_gate.notify_one();

        let close_result = match &mut self.sink {
            WebSocketSink::Tls(sink) => {
                let mut s = sink.lock().await;
                Self::close_sink(&mut *s, frame).await
            }
            WebSocketSink::Plain(sink) => {
                let mut s = sink.lock().await;
                Self::close_sink(&mut *s, frame).await
            }
        };
        self.notify_observers_and_clear(&ConnectionEvent::Disconnected(reason.to_string()));
        // 对端先关闭时读任务已经结束了连接，那时再关报的错不算关闭失败。
        if was_closed { Ok(()) } else { close_result }
    }

    /// 先发带关闭码的关闭帧（如有），再结束发送端。
    async fn close_sink<S>(sink: &mut S, frame: Option<CloseFrame>) -> Result<()>
    where
        S: futures_util::Sink<Message, Error = WsError> + Unpin,
    {
        if let Some(frame) = frame {
            sink.send(Message::Close(Some(frame)))
                .await
                .map_err(|e| FlareError::connection_failed(e.to_string()))?;
        }
        sink.close()
            .await
            .map_err(|e| FlareError::connection_failed(e.to_string()))
    }

    /// 发送 WebSocket 协议层的 PONG 响应 (TLS)
    #[allow(clippy::type_complexity)]
    async fn send_pong_response_tls(
        sink: &Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
        data: &[u8],
    ) -> Result<()> {
        let mut sink = sink.lock().await;
        sink.send(Message::Pong(Bytes::from(data.to_vec())))
            .await
            .map_err(|e| FlareError::connection_failed(e.to_string()))?;
        Ok(())
    }

    /// 发送 WebSocket 协议层的 PONG 响应 (Plain)
    async fn send_pong_response_plain(
        sink: &Arc<Mutex<SplitSink<WebSocketStream<TcpStream>, Message>>>,
        data: &[u8],
    ) -> Result<()> {
        let mut sink = sink.lock().await;
        sink.send(Message::Pong(Bytes::from(data.to_vec())))
            .await
            .map_err(|e| FlareError::connection_failed(e.to_string()))?;
        Ok(())
    }

    /// 构建 PONG Frame 并返回序列化后的数据（用于事件通知）
    fn build_pong_frame() -> Result<Vec<u8>> {
        // 使用 builder 构建 PONG Frame
        let pong_frame = frame_with_system_command(pong(), Reliability::BestEffort);

        // 序列化为 protobuf
        let mut buf = Vec::new();
        pong_frame
            .encode(&mut buf)
            .map_err(|e| FlareError::encoding_error(e.to_string()))?;

        Ok(buf)
    }

    /// 发送应用层的 PONG Frame 消息 (TLS)
    #[allow(clippy::type_complexity)]
    async fn send_pong_frame_tls(
        sink: &Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
    ) -> Result<()> {
        // 构建 PONG Frame
        let pong_data = Self::build_pong_frame()?;

        // 通过 WebSocket 发送
        let mut sink = sink.lock().await;
        sink.send(Message::Binary(Bytes::from(pong_data)))
            .await
            .map_err(|e| FlareError::connection_failed(e.to_string()))?;

        Ok(())
    }

    /// 发送应用层的 PONG Frame 消息 (Plain)
    async fn send_pong_frame_plain(
        sink: &Arc<Mutex<SplitSink<WebSocketStream<TcpStream>, Message>>>,
    ) -> Result<()> {
        // 构建 PONG Frame
        let pong_data = Self::build_pong_frame()?;

        // 通过 WebSocket 发送
        let mut sink = sink.lock().await;
        sink.send(Message::Binary(Bytes::from(pong_data)))
            .await
            .map_err(|e| FlareError::connection_failed(e.to_string()))?;

        Ok(())
    }
}

#[async_trait]
impl Connection for WebSocketTransport {
    fn add_observer(&mut self, observer: ArcObserver) {
        // 已经关闭的连接不能再报 Connected：否则上层会在一条死连接上发 CONNECT 并干等 CONNECT_ACK。
        if self.is_closed.load(Ordering::SeqCst) {
            observer.on_event(&ConnectionEvent::Disconnected(
                CONNECTION_CLOSED_REASON.to_string(),
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
                CONNECTION_CLOSED_REASON.to_string(),
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
            return Err(FlareError::connection_closed(
                CONNECTION_CLOSED_REASON.to_string(),
            ));
        }
        if let Ok(mut active) = self.last_active.lock() {
            *active = std::time::Instant::now();
        }

        let message = Message::Binary(Bytes::from(data.to_vec()));

        match &mut self.sink {
            WebSocketSink::Tls(sink) => {
                let mut s = sink.lock().await;
                s.send(message)
                    .await
                    .map_err(|e| FlareError::connection_failed(e.to_string()))?;
            }
            WebSocketSink::Plain(sink) => {
                let mut s = sink.lock().await;
                s.send(message)
                    .await
                    .map_err(|e| FlareError::connection_failed(e.to_string()))?;
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.close_with_reason("Closed by client").await
    }

    async fn close_with_reason(&mut self, reason: &str) -> Result<()> {
        self.close_inner(reason, None).await
    }

    /// 关闭帧带 1001（Going Away）与 `reason`。不带关闭码时对端只能看到 1005（没有状态码），
    /// 分不清是服务端停机还是别的原因。
    async fn close_going_away(&mut self, reason: &str) -> Result<()> {
        let frame = CloseFrame {
            code: CloseCode::Away,
            reason: reason.to_string().into(),
        };
        self.close_inner(reason, Some(frame)).await
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

impl Drop for WebSocketTransport {
    fn drop(&mut self) {
        // 没关就被释放、且从没挂过观察者的连接，读任务还在等：放它退出，别连同连接一起泄漏。
        self.is_closed.store(true, Ordering::SeqCst);
        self.reader_gate.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_without_close_handshake_is_disconnected_event() {
        let event = WebSocketTransport::event_from_websocket_error(WsError::Protocol(
            ProtocolError::ResetWithoutClosingHandshake,
        ));

        assert!(matches!(event, ConnectionEvent::Disconnected(_)));
        assert!(!event.is_error());
    }

    #[test]
    fn transport_peer_disconnect_io_errors_are_disconnected_events() {
        for kind in [
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::ConnectionAborted,
            std::io::ErrorKind::BrokenPipe,
            std::io::ErrorKind::UnexpectedEof,
        ] {
            let event = WebSocketTransport::event_from_websocket_error(WsError::Io(
                std::io::Error::new(kind, "peer closed"),
            ));

            assert!(
                matches!(event, ConnectionEvent::Disconnected(_)),
                "expected {kind:?} to be classified as disconnected, got {event:?}"
            );
        }
    }

    #[test]
    fn malformed_websocket_protocol_errors_remain_error_events() {
        let event = WebSocketTransport::event_from_websocket_error(WsError::Protocol(
            ProtocolError::InvalidOpcode(0x0f),
        ));

        assert!(matches!(event, ConnectionEvent::Error(_)));
    }

    use crate::transport::events::ConnectionObserver;
    use tokio::net::TcpListener;

    /// 记下观察者收到的每个事件。
    #[derive(Default)]
    struct RecordingObserver {
        events: std::sync::Mutex<Vec<ConnectionEvent>>,
    }

    impl RecordingObserver {
        fn messages(&self) -> Vec<Vec<u8>> {
            self.events
                .lock()
                .unwrap()
                .iter()
                .filter_map(|event| event.message_data().map(<[u8]>::to_vec))
                .collect()
        }

        fn disconnected(&self) -> bool {
            self.events
                .lock()
                .unwrap()
                .iter()
                .any(|event| event.is_disconnected() || event.is_error())
        }
    }

    impl ConnectionObserver for RecordingObserver {
        fn on_event(&self, event: &ConnectionEvent) {
            self.events.lock().unwrap().push(event.clone());
        }
    }

    async fn wait_for(what: &str, check: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while !check() {
            assert!(std::time::Instant::now() < deadline, "{what}");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// 本机回环上建一对 WebSocket：服务端与 flare-core 服务端一样走 `from_tcp_stream`，
    /// 客户端与 flare-core 客户端一样走 `new`。
    async fn websocket_pair() -> (WebSocketTransport, WebSocketTransport) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let accept = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            tokio_tungstenite::accept_async(stream)
                .await
                .expect("server handshake")
        });
        let (client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .expect("client handshake");
        let server = accept.await.expect("accept task");
        (
            WebSocketTransport::from_tcp_stream(server),
            WebSocketTransport::new(client),
        )
    }

    /// 服务端握手完成之后、挂上观察者之前，客户端的第一帧（CONNECT）往往已经到了。
    /// 读任务必须等观察者挂上再读，否则这一帧被读出来却没人接收，客户端只能干等 CONNECT_ACK。
    #[tokio::test]
    async fn frame_sent_before_the_first_observer_is_delivered() {
        let (mut server, mut client) = websocket_pair().await;
        let client_observer = Arc::new(RecordingObserver::default());
        client.add_observer(client_observer);
        client.send(b"connect-frame").await.expect("client send");
        // 让这一帧先到服务端，再挂观察者
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let observer = Arc::new(RecordingObserver::default());
        server.add_observer(observer.clone());
        wait_for("the early frame must reach the first observer", || {
            observer.messages() == vec![b"connect-frame".to_vec()]
        })
        .await;
    }

    /// 观察者挂上之前对端就关闭了：观察者必须收到断开，否则连接永远停在「已连接」，
    /// 服务端的连接表只能等心跳超时才清掉它。
    #[tokio::test]
    async fn peer_close_before_the_first_observer_is_reported() {
        let (mut server, mut client) = websocket_pair().await;
        client.close().await.expect("client close");
        drop(client);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let observer = Arc::new(RecordingObserver::default());
        server.add_observer(observer.clone());
        wait_for("the observer must learn about the peer close", || {
            observer.disconnected()
        })
        .await;
        assert!(
            server.send(b"after-close").await.is_err(),
            "send on a closed connection must fail at once"
        );
    }

    /// 本端关闭之后再挂的观察者只收到 Disconnected，不会被告知 Connected。
    #[tokio::test]
    async fn observer_added_after_local_close_sees_only_disconnected() {
        let (mut server, _client) = websocket_pair().await;
        server.close().await.expect("close");

        let observer = Arc::new(RecordingObserver::default());
        server.add_observer(observer.clone());
        let events = observer.events.lock().unwrap();
        assert_eq!(events.len(), 1, "{events:?}");
        assert!(events[0].is_disconnected(), "{events:?}");
    }
}
