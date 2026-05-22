use crate::common::error::{FlareError, Result};
use crate::common::protocol::{Reliability, frame_with_system_command, pong};
use crate::transport::connection::Connection;
use crate::transport::events::{ArcObserver, ConnectionEvent};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::SinkExt;
use futures_util::stream::{SplitSink, SplitStream, StreamExt};
use prost::Message as ProstMessage;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing::warn;

// 使用枚举来支持两种类型的 WebSocketStream
enum WebSocketSink {
    Tls(Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>),
    Plain(Arc<Mutex<SplitSink<WebSocketStream<TcpStream>, Message>>>),
}

pub struct WebSocketTransport {
    sink: WebSocketSink,
    observers: Arc<std::sync::Mutex<Vec<ArcObserver>>>,
    last_active: Arc<std::sync::Mutex<std::time::Instant>>,
}

impl WebSocketTransport {
    pub fn new(stream: WebSocketStream<MaybeTlsStream<TcpStream>>) -> Self {
        Self::from_stream(stream)
    }

    /// 从 `WebSocketStream<TcpStream>` 创建（在没有 TLS 时使用）
    ///
    /// 使用单独的 Plain 类型，避免 unsafe transmute
    pub fn from_tcp_stream(stream: WebSocketStream<TcpStream>) -> Self {
        let (sink_plain, receiver_plain) = stream.split();

        let observers = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink_arc = Arc::new(Mutex::new(sink_plain));
        let last_active = Arc::new(std::sync::Mutex::new(std::time::Instant::now()));

        let task_observers = Arc::clone(&observers);
        let task_sink = Arc::clone(&sink_arc);
        let task_last_active = Arc::clone(&last_active);
        tokio::spawn(async move {
            Self::receiver_task_plain(receiver_plain, task_observers, task_sink, task_last_active)
                .await;
        });

        Self {
            sink: WebSocketSink::Plain(sink_arc),
            observers,
            last_active,
        }
    }

    fn from_stream(stream: WebSocketStream<MaybeTlsStream<TcpStream>>) -> Self {
        let (sink, receiver) = stream.split();
        let observers = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink_arc = Arc::new(Mutex::new(sink));
        let last_active = Arc::new(std::sync::Mutex::new(std::time::Instant::now()));

        let task_observers = Arc::clone(&observers);
        let task_sink = Arc::clone(&sink_arc);
        let task_last_active = Arc::clone(&last_active);
        tokio::spawn(Self::receiver_task(
            receiver,
            task_observers,
            task_sink,
            task_last_active,
        ));

        Self {
            sink: WebSocketSink::Tls(sink_arc),
            observers,
            last_active,
        }
    }

    async fn receiver_task(
        mut receiver: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        observers_arc: Arc<std::sync::Mutex<Vec<ArcObserver>>>,
        sink_arc: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
        last_active: Arc<std::sync::Mutex<std::time::Instant>>,
    ) {
        while let Some(message) = receiver.next().await {
            // 收到消息时更新活跃时间
            if let Ok(mut active) = last_active.lock() {
                *active = std::time::Instant::now();
            }

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
                Err(e) => Some(ConnectionEvent::Error(FlareError::connection_failed(
                    e.to_string(),
                ))),
            };

            if let Some(event) = event {
                let is_terminal = matches!(
                    event,
                    ConnectionEvent::Disconnected(_) | ConnectionEvent::Error(_)
                );

                Self::_notify_observers(&observers_arc, &event);

                if is_terminal {
                    break;
                }
            }
        }
    }

    async fn receiver_task_plain(
        mut receiver: SplitStream<WebSocketStream<TcpStream>>,
        observers_arc: Arc<std::sync::Mutex<Vec<ArcObserver>>>,
        sink_arc: Arc<Mutex<SplitSink<WebSocketStream<TcpStream>, Message>>>,
        last_active: Arc<std::sync::Mutex<std::time::Instant>>,
    ) {
        while let Some(message) = receiver.next().await {
            // 收到消息时更新活跃时间
            if let Ok(mut active) = last_active.lock() {
                *active = std::time::Instant::now();
            }

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
                Err(e) => Some(ConnectionEvent::Error(FlareError::connection_failed(
                    e.to_string(),
                ))),
            };

            if let Some(event) = event {
                let is_terminal = matches!(
                    event,
                    ConnectionEvent::Disconnected(_) | ConnectionEvent::Error(_)
                );

                Self::_notify_observers(&observers_arc, &event);

                if is_terminal {
                    break;
                }
            }
        }
    }

    // 私有辅助方法，用于通知所有观察者
    fn _notify_observers(
        observers_arc: &Arc<std::sync::Mutex<Vec<ArcObserver>>>,
        event: &ConnectionEvent,
    ) {
        let observers = match observers_arc.lock() {
            Ok(obs) => obs,
            Err(e) => {
                warn!("websocket observers lock poisoned: {e}");
                return;
            }
        };

        for observer in observers.iter() {
            observer.on_event(event);
        }
    }

    fn notify_observers(&self, event: &ConnectionEvent) {
        Self::_notify_observers(&self.observers, event);
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
        match &mut self.sink {
            WebSocketSink::Tls(sink) => {
                let mut s = sink.lock().await;
                s.close()
                    .await
                    .map_err(|e| FlareError::connection_failed(e.to_string()))?;
            }
            WebSocketSink::Plain(sink) => {
                let mut s = sink.lock().await;
                s.close()
                    .await
                    .map_err(|e| FlareError::connection_failed(e.to_string()))?;
            }
        }
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
