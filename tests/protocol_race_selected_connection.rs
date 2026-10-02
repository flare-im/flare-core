//! 协议竞速（QUIC + WebSocket）选出连接之后，这条连接上的事件原样交给上层，未选中的连接不留下任何影响。
//!
//! 本机回环（只绑 127.0.0.1）起真实的 flare-core 服务端，客户端走 `FlareClientBuilder::build_with_race`，
//! 与 IM SDK 在原生端、Tauri 上的默认建连路径相同（同时配了 ws_url 与 quic_url）。
//!
//! - QUIC 不可达、WebSocket 胜出：选中的 WebSocket 连接以协议错误终止（经 TCP 中继注入非法帧），
//!   监听器必须收到这个错误。此前 `FlareObserver` 在竞速模式下按文本把它当作关闭未选中连接的正常错误
//!   吞掉，关掉了发送前重连的上层（IM SDK）收不到断开也收不到错误，停在一条死连接上。
//! - 两个协议都可达：竞速收尾时关掉的那条连接把「我方已主动断开」置在了共享标记上，选中的连接此后
//!   一直带着它，被服务端断开后发送前不再重连（"client was disconnected on request"）。
//! - UDP 被静默丢弃（QUIC 端口是只收不回的黑洞）、WebSocket 正常：WebSocket 一建好就胜出，竞速不等
//!   还在握手的 QUIC。此前竞速要等 QUIC 握手到连接超时（默认 30 s）才收尾，在竞速时限（默认 5 s）处
//!   整体失败，已经连上的 WebSocket 也一起丢掉；胜出的 WebSocket 还会在发 CONNECT 前重新拨号一次。
//! - 两个协议都连不上（QUIC 黑洞，WebSocket 端口只接受不应答）：竞速在竞速时限处失败，不等握手到
//!   连接超时。

#![cfg(all(
    feature = "client",
    feature = "server",
    feature = "websocket",
    feature = "quic",
    not(target_arch = "wasm32")
))]

use async_trait::async_trait;
use flare_core::client::builder::flare::{FlareClient, FlareClientBuilder, MessageListener};
use flare_core::common::cert::{SERVER_CERT_ENV, SERVER_KEY_ENV};
use flare_core::common::config_types::{TlsConfig, TransportProtocol};
use flare_core::common::device::{DeviceInfo, DevicePlatform};
use flare_core::common::error::Result;
use flare_core::common::protocol::flare::core::commands::command::Type as CommandType;
use flare_core::common::protocol::flare::core::commands::system_command::Type as SystemType;
use flare_core::common::protocol::{Frame, Reliability, frame_with_system_command, ping};
use flare_core::server::{FlareServer, FlareServerBuilder, ServerEventHandler};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

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

/// 服务端身份只能在进程里注入一次（证书按进程缓存），所有用例共用这一张自签证书。
fn server_identity() -> &'static Vec<u8> {
    static CERT_DER: OnceLock<Vec<u8>> = OnceLock::new();
    CERT_DER.get_or_init(|| {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("self-signed certificate");
        // SAFETY: 在任何服务端线程读取这两个变量之前、由 OnceLock 保证只写一次。
        unsafe {
            std::env::set_var(SERVER_CERT_ENV, certified.cert.pem());
            std::env::set_var(SERVER_KEY_ENV, certified.signing_key.serialize_pem());
        }
        certified.cert.der().to_vec()
    })
}

fn free_tcp_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind a probe TCP socket")
        .local_addr()
        .expect("probe address")
        .port()
}

fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .expect("bind a probe UDP socket")
        .local_addr()
        .expect("probe address")
        .port()
}

/// `quic_port` 为 `None` 时只起 WebSocket。
async fn start_server(ws_port: u16, quic_port: Option<u16>) -> FlareServer {
    init_tracing();
    let ws_address = format!("127.0.0.1:{ws_port}");
    let mut protocols = vec![TransportProtocol::WebSocket];
    let mut builder = FlareServerBuilder::new(ws_address.clone(), Arc::new(NoopHandler))
        .with_protocol_address(TransportProtocol::WebSocket, ws_address);
    if let Some(quic_port) = quic_port {
        server_identity();
        protocols.push(TransportProtocol::QUIC);
        builder = builder
            .with_protocol_address(TransportProtocol::QUIC, format!("127.0.0.1:{quic_port}"));
    }
    let server = builder
        .with_protocols(protocols)
        .build()
        .expect("build server");
    server.start().await.expect("start server");
    server
}

/// 记下客户端看到的 CONNECT_ACK、断开原因与错误。
#[derive(Default)]
struct Recorder {
    acks: AtomicUsize,
    disconnects: Mutex<Vec<String>>,
    errors: Mutex<Vec<String>>,
}

impl Recorder {
    fn acks(&self) -> usize {
        self.acks.load(Ordering::SeqCst)
    }

    fn errors(&self) -> Vec<String> {
        self.errors.lock().unwrap().clone()
    }
}

#[async_trait]
impl MessageListener for Recorder {
    async fn on_message(&self, frame: &Frame) -> Result<Option<Frame>> {
        if let Some(CommandType::System(system)) =
            frame.command.as_ref().and_then(|c| c.r#type.as_ref())
            && system.r#type == SystemType::ConnectAck as i32
        {
            self.acks.fetch_add(1, Ordering::SeqCst);
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

/// QUIC 排第一（与 SDK 默认的竞速顺序一致），WebSocket 第二。
fn race_builder(ws_url: String, quic_url: String, recorder: Arc<Recorder>) -> FlareClientBuilder {
    FlareClientBuilder::new(ws_url.clone())
        .with_protocol_url(TransportProtocol::WebSocket, ws_url)
        .with_protocol_url(TransportProtocol::QUIC, quic_url)
        .with_protocol_race(vec![TransportProtocol::QUIC, TransportProtocol::WebSocket])
        .with_user_id("bob".to_string())
        .with_listener(recorder)
        .with_device_info(DeviceInfo::new("bob-pc".to_string(), DevicePlatform::PC))
        .with_tls(TlsConfig {
            ca_cert_data: Some(server_identity().clone()),
            ..TlsConfig::none()
        })
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

/// 只绑 127.0.0.1 的 TCP 中继，一次只转发一条连接；客户端方向的写端留着，用来注入字节。
struct Relay {
    address: String,
    /// 客户端拨进来的连接数
    accepted: Arc<AtomicUsize>,
    to_client: Arc<tokio::sync::Mutex<Option<OwnedWriteHalf>>>,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    accept_loop: JoinHandle<()>,
}

impl Relay {
    async fn start(target: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind relay listener");
        let address = listener.local_addr().expect("relay address").to_string();
        let accepted = Arc::new(AtomicUsize::new(0));
        let to_client: Arc<tokio::sync::Mutex<Option<OwnedWriteHalf>>> =
            Arc::new(tokio::sync::Mutex::new(None));
        let tasks: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));
        let accept_loop = tokio::spawn({
            let accepted = accepted.clone();
            let to_client = to_client.clone();
            let tasks = tasks.clone();
            async move {
                while let Ok((client, _)) = listener.accept().await {
                    accepted.fetch_add(1, Ordering::SeqCst);
                    let Ok(server) = TcpStream::connect(&target).await else {
                        continue;
                    };
                    let (mut client_read, client_write) = client.into_split();
                    let (mut server_read, mut server_write) = server.into_split();
                    *to_client.lock().await = Some(client_write);
                    let upstream = tokio::spawn(async move {
                        let _ = tokio::io::copy(&mut client_read, &mut server_write).await;
                    });
                    let downstream = tokio::spawn({
                        let to_client = to_client.clone();
                        async move {
                            let mut buffer = vec![0u8; 16 * 1024];
                            loop {
                                let read = match server_read.read(&mut buffer).await {
                                    Ok(0) | Err(_) => break,
                                    Ok(read) => read,
                                };
                                let mut guard = to_client.lock().await;
                                let Some(writer) = guard.as_mut() else { break };
                                if writer.write_all(&buffer[..read]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    });
                    tasks.lock().unwrap().extend([upstream, downstream]);
                }
            }
        });
        Self {
            address,
            accepted,
            to_client,
            tasks,
            accept_loop,
        }
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    /// 选中的 WebSocket 连接以协议错误终止：停止转发，向客户端注入一个保留操作码的帧
    /// （FIN + 操作码 3，服务端到客户端不加掩码，载荷长度 0）。客户端一侧的套接字不关，
    /// 让客户端先读到这一帧。
    async fn corrupt(&self) {
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
        let mut guard = self.to_client.lock().await;
        let writer = guard.as_mut().expect("a relayed connection");
        writer
            .write_all(&[0x83, 0x00])
            .await
            .expect("inject a malformed frame");
        writer.flush().await.expect("flush the malformed frame");
    }

    fn stop(self) {
        self.accept_loop.abort();
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}

/// QUIC 不可达（UDP 端口上没有服务），WebSocket 胜出。选中的 WebSocket 连接
/// 以协议错误终止时，监听器收到这个错误；此前竞速模式下它被当作未选中连接的关闭错误吞掉。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_protocol_error_on_the_selected_websocket_connection_reaches_the_listener() {
    let ws_port = free_tcp_port();
    let server = start_server(ws_port, None).await;
    let relay = Relay::start(format!("127.0.0.1:{ws_port}")).await;
    let recorder = Arc::new(Recorder::default());
    let client: FlareClient = race_builder(
        format!("ws://{}", relay.address),
        format!("quic://127.0.0.1:{}", free_udp_port()),
        recorder.clone(),
    )
    .with_reconnect_on_send(false)
    .build_with_race()
    .await
    .expect("the race selects WebSocket");
    wait_until("CONNECT_ACK", Duration::from_secs(3), || async {
        recorder.acks() == 1
    })
    .await;
    assert_eq!(client.active_protocol(), TransportProtocol::WebSocket);

    relay.corrupt().await;
    wait_until(
        "the listener sees the protocol error",
        Duration::from_secs(5),
        || async {
            recorder
                .errors()
                .iter()
                .any(|error| error.contains("WebSocket protocol error"))
        },
    )
    .await;
    assert!(!client.is_connected_async().await);

    let _ = client.disconnect().await;
    relay.stop();
    server.stop().await.expect("stop server");
}

/// 两个协议都可达：竞速收尾时关掉的那条连接不能把「我方已主动断开」留给选中的连接。服务端断开
/// 选中的连接之后，下一次发送照常重连（发送前重连默认开着）。此前它报
/// "client was disconnected on request; not reconnecting"。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_the_race_loser_does_not_mark_the_selected_connection_as_closed_on_request() {
    let ws_port = free_tcp_port();
    let quic_port = free_udp_port();
    let server = start_server(ws_port, Some(quic_port)).await;
    let recorder = Arc::new(Recorder::default());
    let client: FlareClient = race_builder(
        format!("ws://127.0.0.1:{ws_port}"),
        format!("quic://127.0.0.1:{quic_port}"),
        recorder.clone(),
    )
    .with_connect_timeout(Duration::from_secs(3))
    .build_with_race()
    .await
    .expect("the race selects a protocol");
    wait_until("CONNECT_ACK", Duration::from_secs(3), || async {
        recorder.acks() == 1
    })
    .await;

    let connections = server
        .get_server_handle_components()
        .expect("connection manager")
        .list_connections()
        .await;
    assert!(!connections.is_empty());
    // 未选中的连接若还在表里，一并断开也无妨：它在客户端一侧已经关了
    for connection_id in &connections {
        let _ = server.disconnect(connection_id).await;
    }
    wait_until(
        "the client notices the close",
        Duration::from_secs(3),
        || async { !client.is_connected_async().await },
    )
    .await;

    tokio::time::timeout(
        Duration::from_secs(5),
        client.send_frame(&frame_with_system_command(ping(), Reliability::AtLeastOnce)),
    )
    .await
    .expect("send after the server close must not hang")
    .expect("send after the server close reconnects");
    wait_until("second CONNECT_ACK", Duration::from_secs(3), || async {
        recorder.acks() == 2
    })
    .await;

    let _ = client.disconnect().await;
    server.stop().await.expect("stop server");
}

/// 只绑 127.0.0.1 的 UDP 黑洞：收下所有数据报、一个也不回，模拟 UDP 被防火墙静默丢弃。
/// 端口上有套接字，所以也不会回 ICMP 端口不可达，QUIC 握手只能等到连接超时。
struct UdpBlackhole {
    address: String,
    received: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl UdpBlackhole {
    async fn start() -> Self {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind the UDP blackhole");
        let address = socket.local_addr().expect("blackhole address").to_string();
        let received = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn({
            let received = received.clone();
            async move {
                let mut buffer = vec![0u8; 2048];
                while socket.recv_from(&mut buffer).await.is_ok() {
                    received.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        Self {
            address,
            received,
            task,
        }
    }

    fn stop(self) {
        self.task.abort();
    }
}

/// 只绑 127.0.0.1 的 TCP 端口：接受连接、一个字节也不回（WebSocket 升级握手停在这里）。
struct SilentTcp {
    address: String,
    task: JoinHandle<()>,
}

impl SilentTcp {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the silent TCP listener");
        let address = listener.local_addr().expect("listener address").to_string();
        let task = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });
        Self { address, task }
    }

    fn stop(self) {
        self.task.abort();
    }
}

/// UDP 被静默丢弃（QUIC 端口是黑洞）、WebSocket 正常，连接超时与竞速时限都用默认值（30 s、5 s）：
/// WebSocket 一建好就胜出，竞速在有界时间内经 WebSocket 连上，不等还在握手的 QUIC；竞速建好的那条
/// WebSocket 连接直接用来发 CONNECT，不重新拨号。
///
/// 此前竞速等 QUIC 握手到连接超时才收尾，在竞速时限处以「Protocol race timed out after 5s」整体失败，
/// 已经连上的 WebSocket 也一起丢掉；原生端与 Tauri 在屏蔽 UDP 的网络里一直连不上。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_race_connects_over_websocket_at_once_when_udp_is_silently_dropped() {
    let ws_port = free_tcp_port();
    let server = start_server(ws_port, None).await;
    let relay = Relay::start(format!("127.0.0.1:{ws_port}")).await;
    let blackhole = UdpBlackhole::start().await;
    let recorder = Arc::new(Recorder::default());

    let started = Instant::now();
    let client: FlareClient = race_builder(
        format!("ws://{}", relay.address),
        format!("quic://{}", blackhole.address),
        recorder.clone(),
    )
    .build_with_race()
    .await
    .expect("the race selects WebSocket while QUIC is still handshaking");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "the race must not wait for the unselected QUIC handshake: took {elapsed:?}"
    );
    assert_eq!(client.active_protocol(), TransportProtocol::WebSocket);
    wait_until("CONNECT_ACK", Duration::from_secs(3), || async {
        recorder.acks() == 1
    })
    .await;
    assert_eq!(
        relay.accepted(),
        1,
        "CONNECT goes over the WebSocket connection the race established, without dialling again"
    );
    tokio::time::timeout(
        Duration::from_secs(3),
        client.send_frame(&frame_with_system_command(ping(), Reliability::AtLeastOnce)),
    )
    .await
    .expect("send must not hang")
    .expect("the selected WebSocket connection is usable");
    assert_eq!(relay.accepted(), 1);
    eprintln!(
        "race over a UDP blackhole: selected WebSocket after {elapsed:?}; the blackhole saw {} datagram(s)",
        blackhole.received.load(Ordering::SeqCst)
    );

    let _ = client.disconnect().await;
    blackhole.stop();
    relay.stop();
    server.stop().await.expect("stop server");
}

/// 两个协议都连不上（QUIC 端口是黑洞，WebSocket 端口只接受不应答），连接超时 10 s、竞速时限 1 s：
/// 竞速在竞速时限处失败，不等还在握手的连接收尾到连接超时。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_race_without_a_reachable_protocol_fails_at_the_race_timeout() {
    let blackhole = UdpBlackhole::start().await;
    let silent = SilentTcp::start().await;
    let recorder = Arc::new(Recorder::default());

    let started = Instant::now();
    let result = race_builder(
        format!("ws://{}", silent.address),
        format!("quic://{}", blackhole.address),
        recorder,
    )
    .with_connect_timeout(Duration::from_secs(10))
    .with_race_timeout(Duration::from_secs(1))
    .build_with_race()
    .await;
    let elapsed = started.elapsed();
    let error = match result {
        Ok(_) => panic!("neither protocol is reachable"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("Protocol race timed out"),
        "unexpected error: {error}"
    );
    assert!(
        elapsed < Duration::from_millis(2_500),
        "the race must fail at its own timeout, not after the handshakes time out: took {elapsed:?}"
    );

    blackhole.stop();
    silent.stop();
}
