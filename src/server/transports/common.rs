//! 服务端传输协议公共模块
//!
//! 提供 WebSocket 和 QUIC 服务端共享的逻辑和辅助函数

use crate::common::generate_id;
use crate::common::shutdown::{SERVER_SHUTDOWN_REASON, server_shutdown_close_frame};
use crate::server::config::ServerConfig;
use crate::server::connection::ConnectionManager;
use crate::server::handle::ServerHandle;
use crate::server::transports::server_core::ServerCore;
use crate::transport::connection::Connection;
use futures_util::StreamExt;
use futures_util::stream;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// 服务端连接辅助函数
pub struct ServerConnectionHelper;

impl ServerConnectionHelper {
    /// 设置新连接（统一处理逻辑）
    ///
    /// 统一处理连接添加、观察者创建和注册等逻辑
    pub async fn setup_new_connection(
        connection: Box<dyn Connection>,
        manager: Arc<ConnectionManager>,
        config: &ServerConfig,
        core: Arc<ServerCore>,
    ) -> Result<String, crate::common::error::FlareError> {
        // 生成连接 ID
        let connection_id = generate_id();

        // 从 ServerCore 获取是否需要认证
        let requires_auth = core.auth_enabled();

        // 添加连接。容量检查必须和插入在同一个临界区内完成，避免并发握手超额注册。
        manager
            .add_connection_with_limit(
                connection_id.clone(),
                connection,
                None,
                requires_auth,
                config.max_connections,
            )
            .map_err(|e| {
                crate::common::error::FlareError::connection_failed(format!(
                    "Failed to add connection: {}",
                    e
                ))
            })?;

        // 创建观察者
        let observer = core.create_observer_with_core(connection_id.clone(), Arc::clone(&core))?;

        // 添加观察者到连接
        if let Some((conn, _)) = manager.get_connection(&connection_id) {
            let mut c = conn.lock().await;
            c.add_observer(observer);
        } else {
            return Err(crate::common::error::FlareError::connection_failed(
                "Failed to get connection after adding".to_string(),
            ));
        }

        Ok(connection_id)
    }

    /// 停止服务器（统一处理逻辑）
    ///
    /// 停止心跳检测，再按 [`Self::shutdown_connections`] 关闭所有在线连接。调用方应先停止接受新连接。
    pub async fn stop_server(
        core: &ServerCore,
        is_running: &Arc<Mutex<bool>>,
    ) -> Result<(), crate::common::error::FlareError> {
        *is_running.lock().await = false;

        // 停止心跳检测
        core.stop_heartbeat();

        Self::shutdown_connections(core, core.shutdown_close_grace()).await;

        Ok(())
    }

    /// 停机时关闭所有在线连接。
    ///
    /// 1. 给每条连接下发 CLOSE(`SERVER_SHUTDOWN`)（按连接各自协商的格式编码，走写队列），最多等
    ///    [`SHUTDOWN_BROADCAST_TIMEOUT`]；
    /// 2. 最多等 `grace`，让认得它的客户端自行断开；
    /// 3. 还在的连接由服务端关闭：WebSocket 关闭帧为 1001（Going Away）、原因 `SERVER_SHUTDOWN`，
    ///    QUIC 以应用错误码 0 与同一原因关闭连接，随后移出连接表。这一步合计不超过
    ///    [`SHUTDOWN_FORCE_CLOSE_BUDGET`]，到时还没关完的连接不再等关闭握手，直接移出连接表
    ///    （底层连接在后台关闭）。
    ///
    /// 以前直接关连接、不带原因：WebSocket 客户端看到的是 1005，分不清停机与故障，只能按错误立即重连。
    pub(crate) async fn shutdown_connections(core: &ServerCore, grace: Duration) {
        Self::shutdown_connections_within(core, grace, SHUTDOWN_FORCE_CLOSE_BUDGET).await;
    }

    /// 同 [`Self::shutdown_connections`]，第 3 步的总时限由调用方给定。
    pub(crate) async fn shutdown_connections_within(
        core: &ServerCore,
        grace: Duration,
        force_close_budget: Duration,
    ) {
        let online = core.connection_manager.connection_count();
        if online == 0 {
            return;
        }

        let manager_trait = core.connection_manager_trait();
        let frame = server_shutdown_close_frame();
        match tokio::time::timeout(
            SHUTDOWN_BROADCAST_TIMEOUT,
            manager_trait.broadcast_frame(&frame, None),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!(%error, "server shutdown: broadcasting CLOSE failed"),
            Err(_) => warn!(
                timeout_ms = SHUTDOWN_BROADCAST_TIMEOUT.as_millis() as u64,
                "server shutdown: broadcasting CLOSE timed out"
            ),
        }

        let deadline = tokio::time::Instant::now() + grace;
        while core.connection_manager.connection_count() > 0
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(SHUTDOWN_POLL_INTERVAL).await;
        }

        let remaining = core.list_connections().await;
        info!(
            online,
            closed_by_clients = online.saturating_sub(remaining.len()),
            closed_by_server = remaining.len(),
            "server shutdown: connections closed"
        );
        let manager_trait = &manager_trait;
        let force_close = stream::iter(remaining).for_each_concurrent(
            SHUTDOWN_CLOSE_CONCURRENCY,
            |connection_id| async move {
                if let Some((connection, _)) = manager_trait.get_connection(&connection_id).await {
                    let closed = tokio::time::timeout(SHUTDOWN_CLOSE_TIMEOUT, async {
                        connection
                            .lock()
                            .await
                            .close_going_away(SERVER_SHUTDOWN_REASON)
                            .await
                    })
                    .await;
                    if !matches!(closed, Ok(Ok(()))) {
                        debug!(connection_id = %connection_id, "server shutdown: close did not complete");
                    }
                }
                let _ = ServerHandle::disconnect(core, &connection_id).await;
            },
        );
        if tokio::time::timeout(force_close_budget, force_close)
            .await
            .is_ok()
        {
            return;
        }

        // 时限到：余下的连接不再等关闭握手，直接移出连接表（底层连接在后台关闭）。
        let left = core.list_connections().await;
        warn!(
            left = left.len(),
            budget_ms = force_close_budget.as_millis() as u64,
            "server shutdown: force-close budget exhausted, dropping the remaining connections"
        );
        let drop_left = async {
            for connection_id in &left {
                let _ = ServerHandle::disconnect(core, connection_id).await;
            }
        };
        if tokio::time::timeout(SHUTDOWN_DROP_TIMEOUT, drop_left)
            .await
            .is_err()
        {
            warn!("server shutdown: dropping the remaining connections timed out");
        }
    }
}

/// 停机时等客户端自行断开的宽限期间，多久查一次连接数。
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// 宽限期满后由服务端关闭连接时的并发度。
const SHUTDOWN_CLOSE_CONCURRENCY: usize = 64;

/// 单条连接的关闭最多等这么久：对端不读也不能拖住停机。
const SHUTDOWN_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

/// 广播停机 CLOSE 最多等这么久（内置连接管理器只入写队列，不等写出）。
pub(crate) const SHUTDOWN_BROADCAST_TIMEOUT: Duration = Duration::from_secs(1);

/// 宽限期满后由服务端关闭余下连接（第 3 步）的总时限。
///
/// 单条关闭有 [`SHUTDOWN_CLOSE_TIMEOUT`] 上限、[`SHUTDOWN_CLOSE_CONCURRENCY`] 并发，但条数没有
/// 上限：大量不读也不关的连接按批关闭要 N/64 秒，会让 `stop()` 超过运行时给的停机上限（接入
/// 网关为 10 s），进程被强杀，后面的收尾都做不了。停机总用时因此不超过广播 1 s、宽限（默认
/// 1 s）、本时限 5 s 与移出余下连接 [`SHUTDOWN_DROP_TIMEOUT`] 之和，约 7.5 s。
pub(crate) const SHUTDOWN_FORCE_CLOSE_BUDGET: Duration = Duration::from_secs(5);

/// 强制关闭时限到后，把余下连接移出连接表最多等这么久（内置连接管理器是同步的，几乎不耗时）。
const SHUTDOWN_DROP_TIMEOUT: Duration = Duration::from_millis(500);

/// 停止接受新连接的信号：accept 循环与它一起等，停机时先关入口，再处理在线连接。
#[derive(Clone)]
pub(crate) struct AcceptGate(Arc<tokio::sync::watch::Sender<bool>>);

impl AcceptGate {
    pub(crate) fn new() -> Self {
        Self(Arc::new(tokio::sync::watch::Sender::new(false)))
    }

    /// 停止接受新连接（幂等）。
    pub(crate) fn close(&self) {
        self.0.send_replace(true);
    }

    /// 给 accept 循环用的接收端：`closed().await` 在入口关闭时返回。
    pub(crate) fn watch(&self) -> AcceptGateWatch {
        AcceptGateWatch(self.0.subscribe())
    }
}

pub(crate) struct AcceptGateWatch(tokio::sync::watch::Receiver<bool>);

impl AcceptGateWatch {
    /// 入口已关闭时返回（已经关闭则立即返回）。
    pub(crate) async fn closed(&mut self) {
        let _ = self.0.wait_for(|closed| *closed).await;
    }
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;
    use crate::common::platform::{MonotonicInstant, monotonic_now};
    use crate::transport::events::ArcObserver;
    use async_trait::async_trait;

    /// 写得出去、却永远关不掉的连接：对端不读也不关，关闭握手一直挂着。
    struct NeverClosing {
        last_active: MonotonicInstant,
    }

    #[async_trait]
    impl Connection for NeverClosing {
        fn add_observer(&mut self, _observer: ArcObserver) {}

        fn remove_observer(&mut self, _observer: ArcObserver) {}

        async fn send(&mut self, _data: &[u8]) -> crate::common::error::Result<()> {
            Ok(())
        }

        async fn close(&mut self) -> crate::common::error::Result<()> {
            std::future::pending().await
        }

        fn last_active_time(&self) -> MonotonicInstant {
            self.last_active
        }

        fn update_active_time(&mut self) {
            self.last_active = monotonic_now();
        }
    }

    /// 大量关不掉的连接不能让停机超过强制关闭的总时限：200 条、单条 1 s 上限、64 并发时，
    /// 逐批关闭要约 4 s；有总时限时到点即把余下连接移出连接表。
    #[tokio::test]
    async fn force_close_phase_is_bounded_by_its_budget() {
        let manager = Arc::new(ConnectionManager::new());
        let core = ServerCore::new(&ServerConfig::default(), Some(Arc::clone(&manager)));
        for idx in 0..200 {
            manager
                .add_connection(
                    format!("never-closing-{idx}"),
                    Box::new(NeverClosing {
                        last_active: monotonic_now(),
                    }),
                    None,
                    false,
                )
                .unwrap();
        }

        let budget = Duration::from_millis(300);
        let started = std::time::Instant::now();
        ServerConnectionHelper::shutdown_connections_within(&core, Duration::ZERO, budget).await;
        let elapsed = started.elapsed();

        println!("200 never-closing connections shut down in {elapsed:?} (budget {budget:?})");
        assert!(
            elapsed < budget + SHUTDOWN_DROP_TIMEOUT + Duration::from_millis(500),
            "force-close phase took {elapsed:?}, budget {budget:?}"
        );
        assert_eq!(
            manager.connection_count(),
            0,
            "every connection must be removed"
        );
    }

    /// 默认时限下停机总用时留在接入网关 10 s 的运行时停机上限之内（宽限取默认 1 s）。
    #[test]
    fn default_shutdown_phases_fit_inside_the_gateway_stop_limit() {
        let total = SHUTDOWN_BROADCAST_TIMEOUT
            + crate::common::shutdown::SERVER_SHUTDOWN_CLOSE_GRACE
            + SHUTDOWN_FORCE_CLOSE_BUDGET
            + SHUTDOWN_DROP_TIMEOUT;
        assert!(total <= Duration::from_secs(8), "{total:?}");
    }
}
