//! 传输层以终止性错误结束一条连接之后，服务端要立即清掉这条连接。
//!
//! 本机回环起一个真实的 flare-core QUIC 服务端（平台互斥的设备管理器），客户端用裸 quinn 连接
//! 发 CONNECT，等连接登记完，然后不结束流、直接关掉整条 QUIC 连接——客户端进程被杀、网络切换时
//! 服务端看到的就是这样。服务端读流拿到的是读错误，传输层上报终止性 `Error` 而不是
//! `Disconnected`。修复前服务端只把它转给业务层的 `on_error`：连接留在连接表里、设备也不清，
//! 要等心跳超时（默认 90 s）才被清掉，期间仍计入连接数与用户数、仍在投递路由里。

#![cfg(all(
    feature = "client",
    feature = "server",
    feature = "quic",
    not(target_arch = "wasm32")
))]

use async_trait::async_trait;
use flare_core::client::QUICClient;
use flare_core::common::cert::{SERVER_CERT_ENV, SERVER_KEY_ENV};
use flare_core::common::config_types::{TlsConfig, TransportProtocol};
use flare_core::common::message::parser::PRE_NEGOTIATION_PARSER;
use flare_core::common::protocol::{
    Reliability, SerializationFormat, connect, frame_with_system_command,
};
use flare_core::server::{
    DeviceConflictStrategyBuilder, DeviceManager, DeviceOwner, FlareServer, FlareServerBuilder,
    ServerEventHandler,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// 自签证书：服务端身份经环境变量注入，每个测试进程只写一次。
fn server_identity() -> Vec<u8> {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("self-signed certificate");
    // SAFETY: 本测试二进制只有这一个用例，在服务端线程读取这两个变量之前写入。
    unsafe {
        std::env::set_var(SERVER_CERT_ENV, certified.cert.pem());
        std::env::set_var(SERVER_KEY_ENV, certified.signing_key.serialize_pem());
    }
    certified.cert.der().to_vec()
}

fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .expect("bind a probe UDP socket")
        .local_addr()
        .expect("probe address")
        .port()
}

struct NoopHandler;

#[async_trait]
impl ServerEventHandler for NoopHandler {}

/// `RUST_LOG=flare_core=debug` 时打印服务端日志，便于定位。
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

/// 远小于心跳超时：清理不能靠心跳兜底。
const CLEANUP_BOUND: Duration = Duration::from_secs(3);

async fn wait_until<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = Instant::now();
    while !check().await {
        assert!(
            started.elapsed() < CLEANUP_BOUND,
            "{what} not reached within {CLEANUP_BOUND:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// 一帧 CONNECT：协商前 JSON，带 4 字节大端长度前缀（与 QUIC 传输的分帧一致）。
fn connect_frame() -> Vec<u8> {
    let frame = frame_with_system_command(
        connect(
            SerializationFormat::Json,
            HashMap::from([
                ("user_id".to_string(), b"bob".to_vec()),
                ("device_id".to_string(), b"d1".to_vec()),
                ("platform".to_string(), b"web".to_vec()),
            ]),
        ),
        Reliability::AtLeastOnce,
    );
    let body = PRE_NEGOTIATION_PARSER
        .serialize(&frame)
        .expect("serialize CONNECT");
    let mut framed = (body.len() as u32).to_be_bytes().to_vec();
    framed.extend_from_slice(&body);
    framed
}

/// 客户端不结束流、直接关掉 QUIC 连接：服务端连接表、用户索引与设备都应立即清空。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_connection_lost_mid_stream_is_cleaned_up_at_once() {
    init_tracing();
    let cert_der = server_identity();
    let port = free_udp_port();
    let address = format!("127.0.0.1:{port}");
    let devices = Arc::new(DeviceManager::new(
        DeviceConflictStrategyBuilder::new()
            .platform_exclusive()
            .build(),
    ));
    let server: FlareServer = FlareServerBuilder::new(address.clone(), Arc::new(NoopHandler))
        .with_protocols(vec![TransportProtocol::QUIC])
        .with_protocol_address(TransportProtocol::QUIC, address)
        .with_device_manager(Arc::clone(&devices))
        .build()
        .expect("build QUIC server");
    server.start().await.expect("start QUIC server");

    let tls = TlsConfig {
        ca_cert_data: Some(cert_der),
        ..TlsConfig::none()
    };
    let (endpoint, _) = QUICClient::create_quic_endpoint_with_tls(&tls).expect("QUIC endpoint");
    let connection = endpoint
        .connect(
            format!("127.0.0.1:{port}").parse().expect("server address"),
            "localhost",
        )
        .expect("start QUIC handshake")
        .await
        .expect("QUIC handshake");
    let (mut send, _recv) = connection.open_bi().await.expect("open stream");
    send.write_all(&connect_frame())
        .await
        .expect("send CONNECT");

    let owner = DeviceOwner::new(None, "bob");
    wait_until("connection registered for bob", || async {
        server.connection_count() == 1
            && server.user_count() == 1
            && !devices.get_user_connections(&owner).await.is_empty()
    })
    .await;

    // 不 finish 流，直接关连接：服务端读流拿到的是读错误，而不是 EOF
    connection.close(0u32.into(), b"gone");

    wait_until(
        "server back to no connection, no user, no device",
        || async {
            server.connection_count() == 0
                && server.user_count() == 0
                && devices.get_user_connections(&owner).await.is_empty()
        },
    )
    .await;

    server.stop().await.expect("stop server");
}
