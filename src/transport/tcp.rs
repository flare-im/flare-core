//! Native TCP transport with length-prefixed Flare frames.
//!
//! # Wire format
//!
//! Same as QUIC bi-stream: `u32 BE length` + protobuf/JSON Frame bytes.
//!
//! # Role in the stack
//!
//! - **Transport only**: no CONNECT/negotiation semantics here.
//! - **Native only**: browser clients use WebSocket (`websocket_wasm`).
//! - Use cases: embedded gateways, internal mesh, custom load balancers, protocol tests.

use crate::common::error::{FlareError, Result};
use crate::transport::connection::Connection;
use crate::transport::events::{
    ArcObserver, ConnectionEvent, notify_observers as notify_connection_observers,
    notify_observers_and_clear as notify_connection_observers_and_clear,
};
use crate::transport::framing::{read_length_prefixed, write_length_prefixed};
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncWriteExt, split};
use tokio::net::TcpStream;
use tokio::sync::{Mutex as TokioMutex, Notify};
use tracing::debug;

/// 连接已关闭（对端关闭、读出错，或本端已关闭）之后，发送与挂观察者都报这个原因。
const CONNECTION_CLOSED_REASON: &str = "TCP connection is closed";

pub struct TCPTransport {
    write_half: Arc<TokioMutex<tokio::io::WriteHalf<TcpStream>>>,
    observers: Arc<Mutex<Vec<ArcObserver>>>,
    last_active: Arc<Mutex<std::time::Instant>>,
    /// 连接是否已经不可用。对端 EOF、读出错、本端关闭都会置位：此后 `send` 立即失败，
    /// 新挂的观察者只会收到 Disconnected，不会被告知 Connected。
    is_closed: Arc<AtomicBool>,
    /// 读任务等第一个观察者挂上才开始读（本端关闭或传输对象释放时也放行，让它退出）。
    reader_gate: Arc<Notify>,
}

impl TCPTransport {
    pub fn new(stream: TcpStream) -> Self {
        let (read_half, write_half) = split(stream);
        let observers = Arc::new(Mutex::new(Vec::new()));
        let last_active = Arc::new(Mutex::new(std::time::Instant::now()));
        let is_closed = Arc::new(AtomicBool::new(false));
        let reader_gate = Arc::new(Notify::new());
        let write_half = Arc::new(TokioMutex::new(write_half));

        tokio::spawn(Self::receiver_task(
            read_half,
            Arc::clone(&observers),
            Arc::clone(&last_active),
            Arc::clone(&is_closed),
            Arc::clone(&reader_gate),
        ));

        Self {
            write_half,
            observers,
            last_active,
            is_closed,
            reader_gate,
        }
    }

    async fn receiver_task(
        mut read_half: tokio::io::ReadHalf<TcpStream>,
        observers_arc: Arc<Mutex<Vec<ArcObserver>>>,
        last_active: Arc<Mutex<std::time::Instant>>,
        is_closed: Arc<AtomicBool>,
        reader_gate: Arc<Notify>,
    ) {
        // 第一个观察者挂上之前不读：此前读出的帧没人接收，会被直接丢掉。服务端 accept 返回时
        // 客户端的 CONNECT 往往已经到了，而观察者要等连接登记完才挂上——CONNECT 丢了，
        // 客户端只能干等 CONNECT_ACK。
        reader_gate.notified().await;

        loop {
            if is_closed.load(Ordering::SeqCst) {
                debug!("[TCP Transport] Receiver task: connection closed");
                break;
            }

            let read = read_length_prefixed(&mut read_half).await;
            if is_closed.load(Ordering::SeqCst) {
                debug!("[TCP Transport] Receiver task: closed locally");
                break;
            }

            match read {
                Ok(data) => {
                    if data.is_empty() {
                        debug!("[TCP Transport] EOF, peer closed connection");
                        is_closed.store(true, Ordering::SeqCst);
                        Self::notify_observers_and_clear(
                            &observers_arc,
                            &ConnectionEvent::Disconnected(
                                "TCP connection closed by peer".to_string(),
                            ),
                        );
                        break;
                    }

                    if let Ok(mut active) = last_active.lock() {
                        *active = std::time::Instant::now();
                    }
                    debug!("[TCP Transport] Received frame: {} bytes", data.len());
                    Self::notify_observers(&observers_arc, &ConnectionEvent::Message(data));
                }
                Err(e) => {
                    let err_str = e.to_string();
                    let event = if err_str.contains("peer disconnected")
                        || err_str.contains("Connection reset")
                        || err_str.contains("Broken pipe")
                    {
                        ConnectionEvent::Disconnected(err_str)
                    } else {
                        ConnectionEvent::Error(e)
                    };
                    is_closed.store(true, Ordering::SeqCst);
                    Self::notify_observers_and_clear(&observers_arc, &event);
                    break;
                }
            }
        }

        debug!("[TCP Transport] Receiver task ended");
    }

    fn notify_observers(observers_arc: &Arc<Mutex<Vec<ArcObserver>>>, event: &ConnectionEvent) {
        notify_connection_observers(observers_arc, event, "tcp observers");
    }

    fn notify_observers_and_clear(
        observers_arc: &Arc<Mutex<Vec<ArcObserver>>>,
        event: &ConnectionEvent,
    ) {
        notify_connection_observers_and_clear(observers_arc, event, "tcp observers");
    }
}

#[async_trait]
impl Connection for TCPTransport {
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

        let mut writer = self.write_half.lock().await;
        write_length_prefixed(&mut *writer, data).await?;
        writer
            .flush()
            .await
            .map_err(|e| FlareError::io(e.to_string()))?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.close_with_reason("Closed by local endpoint").await
    }

    async fn close_with_reason(&mut self, reason: &str) -> Result<()> {
        // 标记为已关闭：读任务此后读到的帧不再上报
        let was_closed = self.is_closed.swap(true, Ordering::SeqCst);
        // 从没挂过观察者的连接（如协议竞速落选的连接），读任务还在等：放它退出
        self.reader_gate.notify_one();

        let close_result = self
            .write_half
            .lock()
            .await
            .shutdown()
            .await
            .map_err(|e| FlareError::connection_closed(e.to_string()));

        Self::notify_observers_and_clear(
            &self.observers,
            &ConnectionEvent::Disconnected(reason.to_string()),
        );
        // 对端先关闭时读任务已经结束了连接，那时再关报的错不算关闭失败。
        if was_closed { Ok(()) } else { close_result }
    }

    fn last_active_time(&self) -> std::time::Instant {
        self.last_active
            .lock()
            .map(|guard| *guard)
            .unwrap_or_else(|_| std::time::Instant::now() - std::time::Duration::from_secs(3600))
    }

    fn update_active_time(&mut self) {
        if let Ok(mut active) = self.last_active.lock() {
            *active = std::time::Instant::now();
        }
    }
}

impl Drop for TCPTransport {
    fn drop(&mut self) {
        // 没关就被释放、且从没挂过观察者的连接，读任务还在等：放它退出，别连同连接一起泄漏。
        self.is_closed.store(true, Ordering::SeqCst);
        self.reader_gate.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::events::ConnectionObserver;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;

    struct CountingObserver {
        messages: Arc<AtomicUsize>,
    }

    impl ConnectionObserver for CountingObserver {
        fn on_event(&self, event: &ConnectionEvent) {
            if matches!(event, ConnectionEvent::Message(_)) {
                self.messages.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    #[tokio::test]
    async fn tcp_transport_delivers_inbound_frames() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");

        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut transport = TCPTransport::new(stream);
            let observer = Arc::new(CountingObserver {
                messages: Arc::new(AtomicUsize::new(0)),
            });
            let messages = Arc::clone(&observer.messages);
            transport.add_observer(observer);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            messages
        });

        let stream = TcpStream::connect(addr).await.expect("connect");
        let mut client = TCPTransport::new(stream);
        client.send(b"hello-tcp-frame").await.expect("client send");

        let messages = server_task.await.expect("server task");
        assert_eq!(messages.load(Ordering::SeqCst), 1);
    }

    /// 记下观察者收到的每个事件。
    #[derive(Default)]
    struct RecordingObserver {
        events: Mutex<Vec<ConnectionEvent>>,
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

    /// 服务端 accept 之后、挂上观察者之前，客户端的第一帧（CONNECT）往往已经到了。
    /// 读任务必须等观察者挂上再读，否则这一帧被读出来却没人接收，客户端只能干等 CONNECT_ACK。
    #[tokio::test]
    async fn frame_sent_before_the_first_observer_is_delivered() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");

        let mut client = TCPTransport::new(TcpStream::connect(addr).await.expect("connect"));
        let (stream, _) = listener.accept().await.expect("accept");
        let mut server = TCPTransport::new(stream);
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

    /// 观察者挂上之前对端就断开了：观察者必须收到断开，否则连接永远停在「已连接」，
    /// 服务端的连接表只能等心跳超时才清掉它。
    #[tokio::test]
    async fn peer_close_before_the_first_observer_is_reported() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");

        let client = TcpStream::connect(addr).await.expect("connect");
        let (stream, _) = listener.accept().await.expect("accept");
        let mut server = TCPTransport::new(stream);
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
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");

        let _client = TcpStream::connect(addr).await.expect("connect");
        let (stream, _) = listener.accept().await.expect("accept");
        let mut server = TCPTransport::new(stream);
        server.close().await.expect("close");

        let observer = Arc::new(RecordingObserver::default());
        server.add_observer(observer.clone());
        let events = observer.events.lock().unwrap();
        assert_eq!(events.len(), 1, "{events:?}");
        assert!(events[0].is_disconnected(), "{events:?}");
    }
}
