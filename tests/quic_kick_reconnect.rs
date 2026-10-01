//! QUIC 客户端被服务端踢下线之后的恢复，以及 CONNECT 被拒时客户端拿到的错误。
//!
//! 本机回环起一个真实的 flare-core QUIC 服务端（平台互斥的设备管理器），客户端走
//! `FlareClientBuilder` + 单协议 QUIC，与 SDK 在 `race_order=[QUIC]` 下的建连路径相同。
//!
//! 被踢那条用例复现的是 p0e2e 全量 E2E 里 `relation.block_prevents_im_single_chat` 的前置失败：
//! 同平台第二设备登录把主客户端踢掉、随后登出；主客户端下一次发送走惰性重连，传输层报了
//! Connected，却把 CONNECT 写进了已被服务端关掉的旧流，拿不到 CONNECT_ACK，
//! 要等到心跳失败（约 24 s）才真正重连，还被报成 token 被拒。

#![cfg(all(
    feature = "client",
    feature = "server",
    feature = "quic",
    not(target_arch = "wasm32")
))]

use async_trait::async_trait;
use flare_core::client::builder::flare::{FlareClient, FlareClientBuilder, MessageListener};
use flare_core::common::cert::{SERVER_CERT_ENV, SERVER_KEY_ENV};
use flare_core::common::compression::CompressionAlgorithm;
use flare_core::common::config_types::{TlsConfig, TransportProtocol};
use flare_core::common::device::{DeviceInfo, DevicePlatform};
use flare_core::common::error::{ErrorCode, Result};
use flare_core::common::protocol::flare::core::commands::command::Type as CommandType;
use flare_core::common::protocol::flare::core::commands::system_command::Type as SystemType;
use flare_core::common::protocol::{
    Frame, Reliability, SerializationFormat, frame_with_system_command, kicked, ping,
};
use flare_core::server::{
    AuthResult, Authenticator, DeviceConflictStrategyBuilder, DeviceManager, FlareServer,
    FlareServerBuilder, ServerEventHandler,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

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

/// 认证器按同一个结果拒绝全部 CONNECT。
struct RejectAll(AuthResult);

#[async_trait]
impl Authenticator for RejectAll {
    async fn authenticate(
        &self,
        _token: &str,
        _connection_id: &str,
        _device_info: Option<&DeviceInfo>,
        _metadata: Option<&HashMap<String, Vec<u8>>>,
    ) -> Result<AuthResult> {
        Ok(self.0.clone())
    }
}

/// `RUST_LOG=flare_core=debug` 时打印客户端与服务端日志，便于定位。
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

async fn start_server(authenticator: Option<Arc<dyn Authenticator>>) -> (FlareServer, u16) {
    init_tracing();
    server_identity();
    let port = free_udp_port();
    let address = format!("127.0.0.1:{port}");
    let device_manager = Arc::new(DeviceManager::new(
        DeviceConflictStrategyBuilder::new()
            .platform_exclusive()
            .build(),
    ));
    let mut builder = FlareServerBuilder::new(address.clone(), Arc::new(NoopHandler))
        .with_protocols(vec![TransportProtocol::QUIC])
        .with_protocol_address(TransportProtocol::QUIC, address)
        .with_device_manager(device_manager)
        .with_default_format(SerializationFormat::Protobuf)
        .with_default_compression(CompressionAlgorithm::Gzip);
    if let Some(authenticator) = authenticator {
        builder = builder.enable_auth().with_authenticator(authenticator);
    }
    let server = builder.build().expect("build QUIC server");
    server.start().await.expect("start QUIC server");
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

fn client_builder(port: u16, device_id: &str, listener: Arc<AckCounter>) -> FlareClientBuilder {
    let tls = TlsConfig {
        ca_cert_data: Some(server_identity().clone()),
        ..TlsConfig::none()
    };
    let url = format!("quic://127.0.0.1:{port}");
    FlareClientBuilder::new(url.clone())
        .with_protocol_url(TransportProtocol::QUIC, url)
        .with_protocol_race(vec![TransportProtocol::QUIC])
        .with_user_id("bob".to_string())
        .with_token("test-token".to_string())
        .with_listener(listener)
        .with_device_info(DeviceInfo::new(device_id.to_string(), DevicePlatform::PC))
        .with_format(SerializationFormat::Protobuf)
        .with_tls(tls)
        .with_connect_timeout(Duration::from_secs(5))
        // 比下面的恢复时限长：恢复不能靠「CONNECT 写失败 → 等一个重连间隔 → 再连」兜底
        .with_reconnect_interval(RECONNECT_INTERVAL)
}

/// 客户端的重连间隔。
const RECONNECT_INTERVAL: Duration = Duration::from_secs(3);
/// 被踢之后下一次发送重新拿到 CONNECT_ACK 的时限：必须直接新建连接完成握手，
/// 不能先在旧连接上失败一次、再等一个重连间隔。
const RECOVERY_BOUND: Duration = Duration::from_secs(2);

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

fn ping_frame() -> Frame {
    frame_with_system_command(ping(), Reliability::AtLeastOnce)
}

/// 被同平台第二设备踢掉、第二设备随后登出之后，主客户端的下一次发送必须在有界时间内
/// 走完一次新的 CONNECT → CONNECT_ACK，而不是把 CONNECT 写进已被服务端关掉的旧流。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kicked_quic_client_reconnects_with_a_fresh_connect() {
    let (server, port) = start_server(None).await;

    let owner_acks = Arc::new(AckCounter::default());
    let owner: FlareClient = client_builder(port, "owner-device", owner_acks.clone())
        .build_with_race()
        .await
        .expect("owner connects");
    wait_until("owner CONNECT_ACK", Duration::from_secs(5), || async {
        owner_acks.acks() == 1
    })
    .await;

    // 同一用户、同一平台的第二设备登录：服务端按平台互斥把主客户端踢掉。
    let second = client_builder(port, "second-device", Arc::new(AckCounter::default()))
        .build_with_race()
        .await
        .expect("second device connects");
    wait_until("owner kicked", Duration::from_secs(5), || async {
        !owner.is_connected_async().await
    })
    .await;
    second.disconnect().await.expect("second device logs out");

    // 主客户端下一次发送走惰性重连：必须在有界时间内重新 CONNECT 并拿到 CONNECT_ACK。
    let started = Instant::now();
    tokio::time::timeout(Duration::from_secs(5), owner.send_frame(&ping_frame()))
        .await
        .expect("send after the kick must not wait for a heartbeat failure")
        .expect("send after the kick re-CONNECTs");
    let recovered_in = started.elapsed();
    wait_until(
        "owner second CONNECT_ACK",
        Duration::from_secs(2),
        || async { owner_acks.acks() == 2 },
    )
    .await;
    assert!(
        recovered_in < RECOVERY_BOUND,
        "re-CONNECT took {recovered_in:?}"
    );

    owner.disconnect().await.expect("owner disconnects");
    server.stop().await.expect("stop server");
}

/// 服务端踢线（与网关一致：先发 KICKED 再关连接）之后，客户端立即感知断开，
/// 下一次发送新建连接并重新拿到 CONNECT_ACK。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_disconnect_is_seen_at_once_and_the_next_send_reconnects() {
    let (server, port) = start_server(None).await;

    let owner_acks = Arc::new(AckCounter::default());
    let owner = client_builder(port, "owner-device", owner_acks.clone())
        .build_with_race()
        .await
        .expect("owner connects");
    wait_until("owner CONNECT_ACK", Duration::from_secs(5), || async {
        owner_acks.acks() == 1
    })
    .await;

    let connection_id = server
        .get_server_handle_components()
        .expect("connection manager")
        .list_connections()
        .await
        .into_iter()
        .next()
        .expect("owner connection id");
    // 与网关踢线一致：先发带原因的 KICKED，再关连接。
    let kicked = frame_with_system_command(
        kicked("DEVICE_KICKED".to_string(), None),
        Reliability::AtLeastOnce,
    );
    server
        .send_to(&connection_id, &kicked)
        .await
        .expect("server sends KICKED");
    let kicked_at = Instant::now();
    server
        .disconnect(&connection_id)
        .await
        .expect("server kicks");
    wait_until(
        "owner notices the close",
        Duration::from_secs(2),
        || async { !owner.is_connected_async().await },
    )
    .await;
    assert!(kicked_at.elapsed() < Duration::from_secs(2));

    let started = Instant::now();
    tokio::time::timeout(Duration::from_secs(5), owner.send_frame(&ping_frame()))
        .await
        .expect("send after the server close must not hang")
        .expect("send after the server close re-CONNECTs");
    let recovered_in = started.elapsed();
    wait_until(
        "owner second CONNECT_ACK",
        Duration::from_secs(2),
        || async { owner_acks.acks() == 2 },
    )
    .await;
    assert!(
        recovered_in < RECOVERY_BOUND,
        "re-CONNECT took {recovered_in:?}"
    );

    owner.disconnect().await.expect("owner disconnects");
    server.stop().await.expect("stop server");
}

/// CONNECT 被拒时，客户端拿到的错误按服务端下发的类型化原因分类：
/// 租户不可用不是鉴权失败，鉴权失败才是鉴权失败；认证依赖暂时读不到（如租户状态库抖动）
/// 是可重试的 SERVICE_UNAVAILABLE，既不是鉴权失败也不是租户不可用。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejected_connect_surfaces_the_typed_reason() {
    for (auth_result, expected_code, expected_text) in [
        (
            AuthResult::failure("TENANT_UNAVAILABLE".to_string()),
            ErrorCode::PermissionDenied,
            "TENANT_UNAVAILABLE",
        ),
        (
            AuthResult::failure("Token 无效或已过期".to_string()),
            ErrorCode::AuthenticationFailed,
            "AUTHENTICATION_FAILED",
        ),
        (
            AuthResult::retryable_failure("tenant runtime projection unavailable".to_string()),
            ErrorCode::ServiceUnavailable,
            "SERVICE_UNAVAILABLE",
        ),
    ] {
        let auth_reason = auth_result.error_message.clone().unwrap_or_default();
        let retryable = auth_result.retryable;
        let (server, port) = start_server(Some(Arc::new(RejectAll(auth_result)))).await;
        let started = Instant::now();
        let error = match client_builder(port, "owner-device", Arc::new(AckCounter::default()))
            .build_with_race()
            .await
        {
            Ok(_) => panic!("CONNECT must be rejected ({auth_reason})"),
            Err(error) => error,
        };
        assert_eq!(error.code(), Some(expected_code), "{error}");
        assert!(error.to_string().contains(expected_text), "{error}");
        assert_eq!(expected_code.is_retryable(), retryable, "{error}");
        if retryable {
            // 服务端内部文本不下发；也不能带上会让客户端终止会话的租户原因
            assert!(!error.to_string().contains(&auth_reason), "{error}");
            assert!(!error.to_string().contains("TENANT_UNAVAILABLE"), "{error}");
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "rejection must not wait for the negotiation timeout"
        );
        server.stop().await.expect("stop server");
    }
}
