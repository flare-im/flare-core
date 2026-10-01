//! 客户端断开后，服务端连接表要立即清掉这条连接。
//!
//! 本机回环起一个真实的 flare-core WebSocket 服务端（平台互斥的设备管理器），客户端走
//! `FlareClientBuilder` + 单协议 WebSocket。复现的是 W4-D 观察到的现象：同平台第二设备登录把
//! 主客户端踢掉、随后登出，服务端的连接数与用户数仍为 1。被踢的旧连接由设备冲突处理同步移除，
//! 而客户端自己断开的连接只通知了业务层，没有从连接表移除，要等心跳超时才被清掉；在此之前它
//! 仍算在线、仍在用户的投递路由里。

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
use flare_core::common::protocol::Frame;
use flare_core::common::protocol::flare::core::commands::command::Type as CommandType;
use flare_core::common::protocol::flare::core::commands::system_command::Type as SystemType;
use flare_core::server::{
    DeviceConflictStrategyBuilder, DeviceManager, FlareServer, FlareServerBuilder,
    ServerEventHandler,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

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

async fn start_server() -> (FlareServer, u16) {
    init_tracing();
    let port = free_tcp_port();
    let address = format!("127.0.0.1:{port}");
    let device_manager = Arc::new(DeviceManager::new(
        DeviceConflictStrategyBuilder::new()
            .platform_exclusive()
            .build(),
    ));
    let server = FlareServerBuilder::new(address.clone(), Arc::new(NoopHandler))
        .with_protocols(vec![TransportProtocol::WebSocket])
        .with_protocol_address(TransportProtocol::WebSocket, address)
        .with_device_manager(device_manager)
        .build()
        .expect("build WebSocket server");
    server.start().await.expect("start WebSocket server");
    (server, port)
}

/// 数 CONNECT_ACK：SDK 也是靠这一帧判定「已就绪」。
#[derive(Default)]
struct AckCounter {
    acks: AtomicUsize,
}

impl AckCounter {
    fn acks(&self) -> usize {
        self.acks.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl MessageListener for AckCounter {
    async fn on_message(&self, frame: &Frame) -> Result<Option<Frame>> {
        if let Some(CommandType::System(system)) =
            frame.command.as_ref().and_then(|c| c.r#type.as_ref())
            && system.r#type == SystemType::ConnectAck as i32
        {
            self.acks.fetch_add(1, Ordering::SeqCst);
        }
        Ok(None)
    }
}

async fn connect(port: u16, device_id: &str) -> FlareClient {
    let url = format!("ws://127.0.0.1:{port}");
    let acks = Arc::new(AckCounter::default());
    let client = FlareClientBuilder::new(url.clone())
        .with_protocol_url(TransportProtocol::WebSocket, url)
        .with_protocol_race(vec![TransportProtocol::WebSocket])
        .with_user_id("bob".to_string())
        .with_listener(acks.clone())
        .with_device_info(DeviceInfo::new(device_id.to_string(), DevicePlatform::PC))
        .with_connect_timeout(Duration::from_secs(5))
        .build_with_race()
        .await
        .expect("client connects");
    wait_until("CONNECT_ACK", || async { acks.acks() == 1 }).await;
    client
}

async fn wait_until<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    // 远小于心跳超时：清理不能靠心跳兜底
    let deadline = Duration::from_secs(3);
    let started = Instant::now();
    while !check().await {
        assert!(
            started.elapsed() < deadline,
            "{what} not reached within {deadline:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_counts(server: &FlareServer, connections: usize, users: usize) {
    wait_until(
        &format!("server counts back to {connections} connection(s) / {users} user(s)"),
        || async { server.connection_count() == connections && server.user_count() == users },
    )
    .await;
}

/// 同平台第二设备把主客户端踢掉、随后登出：服务端不应再留着任何连接或用户。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_device_logout_after_a_device_conflict_leaves_no_connection() {
    let (server, port) = start_server().await;

    let owner = connect(port, "owner-device").await;
    wait_for_counts(&server, 1, 1).await;

    let second = connect(port, "second-device").await;
    wait_until("owner kicked", || async {
        !owner.is_connected_async().await
    })
    .await;
    // 被踢的旧连接由设备冲突处理同步移除
    wait_for_counts(&server, 1, 1).await;

    second.disconnect().await.expect("second device logs out");
    wait_for_counts(&server, 0, 0).await;

    server.stop().await.expect("stop server");
}

/// 普通断开：客户端登出后服务端立即清掉连接，而不是等心跳超时。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_logout_removes_its_connection_at_once() {
    let (server, port) = start_server().await;

    let client = connect(port, "only-device").await;
    wait_for_counts(&server, 1, 1).await;

    client.disconnect().await.expect("client logs out");
    wait_for_counts(&server, 0, 0).await;

    server.stop().await.expect("stop server");
}
