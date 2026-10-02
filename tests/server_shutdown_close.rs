//! 服务端停机时先告诉在线连接原因，客户端据此断开并在短暂的随机等待后重连。
//!
//! 本机回环（只绑 127.0.0.1）起一个真实的 flare-core WebSocket 服务端。复现的是 W6-S2 的 F5：
//! core 停机时 WebSocket 以 1005（没有状态码）关闭，关闭前没有带原因的系统帧，客户端分不清停机
//! 与故障，只能按错误立即重试。

#![cfg(all(
    feature = "client",
    feature = "server",
    feature = "websocket",
    not(target_arch = "wasm32")
))]

use async_trait::async_trait;
use flare_core::client::builder::flare::{FlareClient, FlareClientBuilder, MessageListener};
use flare_core::common::config_types::TransportProtocol;
use flare_core::common::device::{DeviceInfo, DevicePlatform};
use flare_core::common::error::Result;
use flare_core::common::protocol::flare::core::commands::command::Type as CommandType;
use flare_core::common::protocol::flare::core::commands::system_command::Type as SystemType;
use flare_core::common::protocol::{Frame, Reliability, frame_with_system_command, ping};
use flare_core::common::shutdown::{
    SERVER_SHUTDOWN_CLOSE_GRACE, SERVER_SHUTDOWN_REASON, SERVER_SHUTDOWN_RECONNECT_MAX,
    SERVER_SHUTDOWN_RECONNECT_MIN, is_server_shutdown_close, is_server_shutdown_reason,
};
use flare_core::server::{FlareServer, FlareServerBuilder, ServerEventHandler};
use futures_util::StreamExt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

struct NoopHandler;

#[async_trait]
impl ServerEventHandler for NoopHandler {}

/// `RUST_LOG=flare_core=debug` 时打印客户端与服务端日志，便于定位。
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

fn free_tcp_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind a probe TCP socket")
        .local_addr()
        .expect("probe address")
        .port()
}

async fn start_server(port: u16) -> FlareServer {
    start_server_with_grace(port, None).await
}

/// `grace` 为 `None` 时用默认停机宽限。
async fn start_server_with_grace(port: u16, grace: Option<Duration>) -> FlareServer {
    init_tracing();
    let address = format!("127.0.0.1:{port}");
    let mut builder = FlareServerBuilder::new(address.clone(), Arc::new(NoopHandler))
        .with_protocols(vec![TransportProtocol::WebSocket])
        .with_protocol_address(TransportProtocol::WebSocket, address);
    if let Some(grace) = grace {
        builder = builder.with_shutdown_close_grace(grace);
    }
    let server = builder.build().expect("build WebSocket server");
    server.start().await.expect("start WebSocket server");
    server
}

/// 记下客户端看到的 CONNECT_ACK、停机 CLOSE、断开原因与错误。
#[derive(Default)]
struct Recorder {
    acks: AtomicUsize,
    shutdown_close: AtomicBool,
    disconnects: Mutex<Vec<String>>,
    errors: Mutex<Vec<String>>,
}

impl Recorder {
    fn acks(&self) -> usize {
        self.acks.load(Ordering::SeqCst)
    }

    fn disconnects(&self) -> Vec<String> {
        self.disconnects.lock().unwrap().clone()
    }
}

#[async_trait]
impl MessageListener for Recorder {
    async fn on_message(&self, frame: &Frame) -> Result<Option<Frame>> {
        if let Some(CommandType::System(system)) =
            frame.command.as_ref().and_then(|c| c.r#type.as_ref())
        {
            if system.r#type == SystemType::ConnectAck as i32 {
                self.acks.fetch_add(1, Ordering::SeqCst);
            }
            if is_server_shutdown_close(system) {
                self.shutdown_close.store(true, Ordering::SeqCst);
            }
        }
        Ok(None)
    }

    async fn on_disconnect(&self, reason: Option<&str>) -> Result<()> {
        self.disconnects
            .lock()
            .unwrap()
            .push(reason.unwrap_or_default().to_string());
        Ok(())
    }

    async fn on_error(&self, error: &str) -> Result<()> {
        self.errors.lock().unwrap().push(error.to_string());
        Ok(())
    }
}

/// 配置的重连间隔远小于停机后的最短等待：停机后若仍按它重连，就是「按错误快速重试」。
const RECONNECT_INTERVAL: Duration = Duration::from_millis(100);

async fn connect(port: u16, recorder: Arc<Recorder>) -> FlareClient {
    let url = format!("ws://127.0.0.1:{port}");
    let client = FlareClientBuilder::new(url.clone())
        .with_protocol_url(TransportProtocol::WebSocket, url)
        .with_protocol_race(vec![TransportProtocol::WebSocket])
        .with_user_id("bob".to_string())
        .with_listener(recorder.clone())
        .with_device_info(DeviceInfo::new("bob-pc".to_string(), DevicePlatform::PC))
        .with_connect_timeout(Duration::from_secs(5))
        .with_reconnect_interval(RECONNECT_INTERVAL)
        .build_with_race()
        .await
        .expect("client connects");
    wait_until("CONNECT_ACK", Duration::from_secs(3), || async {
        recorder.acks() == 1
    })
    .await;
    client
}

async fn wait_until<F, Fut>(what: &str, deadline: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = Instant::now();
    while !check().await {
        assert!(
            started.elapsed() < deadline,
            "{what} not reached within {deadline:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// 停机：客户端先收到 CLOSE(SERVER_SHUTDOWN)，以这个原因自行断开（服务端不必等满宽限期）；
/// 监听端口随即释放，同一端口上的新实例能起来；客户端下一次发送先在抖动窗口里随机等一段再重连，
/// 而不是按 100 ms 的重连间隔立即重试。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_tells_clients_why_and_they_reconnect_after_a_jittered_pause() {
    let port = free_tcp_port();
    let server = start_server(port).await;
    let recorder = Arc::new(Recorder::default());
    let client = connect(port, recorder.clone()).await;
    wait_until("server sees the client", Duration::from_secs(3), || async {
        server.connection_count() == 1
    })
    .await;

    let started = Instant::now();
    server.stop().await.expect("stop server");
    let stop_took = started.elapsed();

    wait_until("client sees the close", Duration::from_secs(3), || async {
        !recorder.disconnects().is_empty()
    })
    .await;
    assert!(
        recorder.shutdown_close.load(Ordering::SeqCst),
        "the client must receive CLOSE(SERVER_SHUTDOWN) before the transport closes"
    );
    let reasons = recorder.disconnects();
    assert!(
        reasons
            .iter()
            .all(|reason| is_server_shutdown_reason(reason)),
        "disconnect reasons must carry SERVER_SHUTDOWN: {reasons:?}"
    );
    assert!(
        recorder.errors.lock().unwrap().is_empty(),
        "a planned shutdown is not a connection error"
    );
    assert!(
        stop_took < SERVER_SHUTDOWN_CLOSE_GRACE,
        "the client closed by itself, the server must not wait out the grace: {stop_took:?}"
    );
    assert!(!client.is_connected_async().await);

    // 停止接受新连接即释放监听端口：同一端口上的新实例能起来
    let restarted = start_server(port).await;

    let reconnect_started = Instant::now();
    tokio::time::timeout(
        SERVER_SHUTDOWN_RECONNECT_MAX + Duration::from_secs(5),
        client.send_frame(&frame_with_system_command(ping(), Reliability::AtLeastOnce)),
    )
    .await
    .expect("send after the shutdown must not hang")
    .expect("send after the shutdown reconnects");
    let reconnect_took = reconnect_started.elapsed();
    wait_until("second CONNECT_ACK", Duration::from_secs(3), || async {
        recorder.acks() == 2
    })
    .await;
    assert!(
        reconnect_took >= SERVER_SHUTDOWN_RECONNECT_MIN,
        "after a shutdown the client must pause before reconnecting, took {reconnect_took:?}"
    );
    assert!(
        reconnect_took < SERVER_SHUTDOWN_RECONNECT_MAX + Duration::from_secs(3),
        "took {reconnect_took:?}"
    );

    client.disconnect().await.expect("client disconnects");
    restarted.stop().await.expect("stop restarted server");
}

/// 不认得 CLOSE 的旧客户端（这里是一条只完成 WebSocket 握手的裸连接）：照样先收到那一帧，
/// 宽限期满后由服务端关闭，关闭帧是 1001（Going Away）、原因 SERVER_SHUTDOWN，而不是 1005。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clients_that_ignore_close_get_going_away_after_the_grace() {
    let port = free_tcp_port();
    let server = start_server(port).await;
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}"))
        .await
        .expect("raw WebSocket handshake");
    wait_until(
        "server sees the raw client",
        Duration::from_secs(3),
        || async { server.connection_count() == 1 },
    )
    .await;

    let stop = tokio::spawn(async move {
        let started = Instant::now();
        server.stop().await.expect("stop server");
        started.elapsed()
    });

    let mut frames_before_close = 0usize;
    let close = loop {
        match tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("server must close the raw client")
        {
            Some(Ok(Message::Close(frame))) => break frame,
            Some(Ok(Message::Binary(_) | Message::Text(_))) => frames_before_close += 1,
            Some(Ok(_)) => {}
            other => panic!("expected a close frame, got {other:?}"),
        }
    };
    let close = close.expect("the close frame must carry a status code");
    assert_eq!(close.code, CloseCode::Away);
    assert_eq!(close.reason.as_str(), SERVER_SHUTDOWN_REASON);
    assert!(
        frames_before_close >= 1,
        "the CLOSE system frame is sent before the transport close"
    );

    let stop_took = stop.await.expect("stop task");
    assert!(
        stop_took >= SERVER_SHUTDOWN_CLOSE_GRACE.saturating_sub(Duration::from_millis(50)),
        "the server waits out the grace for clients that ignore CLOSE: {stop_took:?}"
    );
    assert!(stop_took < SERVER_SHUTDOWN_CLOSE_GRACE + Duration::from_secs(3));
}

/// 停机宽限可按部署配置：配成 200 ms 时，不理会 CLOSE 的客户端在 200 ms 后就被关闭（1001），
/// 停机不再固定等满 1 s。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_shutdown_grace_is_configurable() {
    let grace = Duration::from_millis(200);
    let port = free_tcp_port();
    let server = start_server_with_grace(port, Some(grace)).await;
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}"))
        .await
        .expect("raw WebSocket handshake");
    wait_until(
        "server sees the raw client",
        Duration::from_secs(3),
        || async { server.connection_count() == 1 },
    )
    .await;

    let stop = tokio::spawn(async move {
        let started = Instant::now();
        server.stop().await.expect("stop server");
        started.elapsed()
    });
    let close = loop {
        match tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("server must close the raw client")
        {
            Some(Ok(Message::Close(frame))) => break frame,
            Some(Ok(_)) => {}
            other => panic!("expected a close frame, got {other:?}"),
        }
    };
    let close = close.expect("the close frame must carry a status code");
    assert_eq!(close.code, CloseCode::Away);
    assert_eq!(close.reason.as_str(), SERVER_SHUTDOWN_REASON);

    let stop_took = stop.await.expect("stop task");
    assert!(
        stop_took >= grace.saturating_sub(Duration::from_millis(50)),
        "the server still waits out the configured grace: {stop_took:?}"
    );
    assert!(
        stop_took < SERVER_SHUTDOWN_CLOSE_GRACE.saturating_sub(Duration::from_millis(200)),
        "a 200 ms grace must not wait for the 1 s default: {stop_took:?}"
    );
}
