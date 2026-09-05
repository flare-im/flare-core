//! 心跳管理模块
//!
//! 提供心跳机制的实现，保持连接活跃

use crate::common::platform::{MonotonicInstant, monotonic_now, sleep};
use crate::common::{HeartbeatAppState, HeartbeatConfig, MessageParser};
use crate::transport::connection::Connection;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{Mutex, Notify, mpsc};

/// 前台/网络恢复时一次性验活的等待窗口上界。
///
/// 正常 ping 往返 < 100ms；心跳默认 `timeout` 高达 90s（稳态容忍抖动），
/// 但回到前台后要立刻判定连接死活，不能等 90s。取 `min(timeout, PROBE_TIMEOUT)`
/// 作为探测窗口，让半开死连在几秒内被戳穿并触发重连。
const PROBE_TIMEOUT: Duration = Duration::from_secs(8);

/// 心跳管理器
pub struct HeartbeatManager {
    config: Arc<RwLock<HeartbeatConfig>>,
    last_ping: Arc<std::sync::Mutex<Option<MonotonicInstant>>>,
    // 使用 std::sync::Mutex，因为 record_pong 可能从同步上下文调用
    last_pong: Arc<std::sync::Mutex<Option<MonotonicInstant>>>,
    stop_tx: Option<mpsc::Sender<()>>,
}

fn read_config(config: &Arc<RwLock<HeartbeatConfig>>) -> HeartbeatConfig {
    config
        .read()
        .map(|guard| guard.clone())
        .unwrap_or_else(|_| HeartbeatConfig::default())
}

impl HeartbeatManager {
    /// 创建新的心跳管理器
    ///
    /// # 参数
    /// - `interval`: 心跳发送间隔
    /// - `timeout`: 等待 PONG 的超时时间
    pub fn new(interval: Duration, timeout: Duration) -> Self {
        Self::with_config(
            HeartbeatConfig::new()
                .with_interval(interval)
                .with_timeout(timeout),
        )
    }

    /// 使用完整心跳策略创建管理器。
    pub fn with_config(config: HeartbeatConfig) -> Self {
        Self::with_shared_config(Arc::new(RwLock::new(config)))
    }

    /// 使用共享心跳策略创建管理器；运行中更新该策略会影响后续心跳。
    pub fn with_shared_config(config: Arc<RwLock<HeartbeatConfig>>) -> Self {
        Self {
            config,
            last_ping: Arc::new(std::sync::Mutex::new(None)),
            last_pong: Arc::new(std::sync::Mutex::new(None)),
            stop_tx: None,
        }
    }

    /// 返回当前心跳策略快照。
    pub fn current_config(&self) -> HeartbeatConfig {
        read_config(&self.config)
    }

    /// 当前实际心跳间隔。
    pub fn effective_interval(&self) -> Duration {
        self.current_config().effective_interval()
    }

    /// 原子更新心跳策略。
    pub fn update_config(&self, update: impl FnOnce(&mut HeartbeatConfig)) {
        if let Ok(mut config) = self.config.write() {
            update(&mut config);
        }
    }

    /// 更新应用前后台状态。
    pub fn set_app_state(&self, state: HeartbeatAppState) {
        self.update_config(|config| {
            config.app_state = state;
        });
    }

    /// 更新 NAT 空闲超时探测结果。
    pub fn set_nat_timeout(&self, timeout: Option<Duration>) {
        self.update_config(|config| {
            config.nat_timeout = timeout;
        });
    }

    /// 启动心跳
    ///
    /// # 参数
    /// - `connection`: 连接实例
    /// - `parser`: 消息解析器的引用（用于序列化 ping 消息，始终使用最新的 parser）
    ///
    /// # 返回
    /// 停止心跳的发送端
    pub fn start(
        &mut self,
        connection: Arc<Mutex<Box<dyn Connection>>>,
        parser: Arc<tokio::sync::Mutex<MessageParser>>,
        probe_wake: Arc<Notify>,
    ) {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_tx = Some(tx);

        let config = Arc::clone(&self.config);
        let last_ping = Arc::clone(&self.last_ping);
        let last_pong = Arc::clone(&self.last_pong);

        let heartbeat_loop = async move {
            loop {
                let sleep_duration = read_config(&config).effective_interval();
                tokio::select! {
                    _ = sleep(sleep_duration) => {
                        if unanswered_ping_timed_out(
                            &last_ping,
                            &last_pong,
                            read_config(&config).timeout,
                        ) {
                            let mut conn = connection.lock().await;
                            let _ = conn.close().await;
                            break;
                        }

                        // 发送心跳
                        let ping_frame = crate::common::protocol::frame_with_system_command(
                            crate::common::protocol::ping(),
                            crate::common::protocol::Reliability::AtLeastOnce,
                        );

                        let data = {
                            let parser_guard = parser.lock().await;
                            match parser_guard.serialize(&ping_frame) {
                                Ok(d) => d,
                                Err(e) => {
                                    tracing::error!("[HeartbeatManager] 序列化心跳消息失败: {}", e);
                                    continue;
                                }
                            }
                        };

                        record_ping_start_if_idle(&last_ping, &last_pong);

                        // 使用 tokio::sync::Mutex，支持跨 await
                        let send_result = {
                            let mut conn = connection.lock().await;
                            conn.send(&data).await
                        };

                        if let Err(error) = send_result {
                            tracing::warn!("[HeartbeatManager] 发送心跳失败: {}", error);
                            let mut conn = connection.lock().await;
                            let _ = conn.close().await;
                            break;
                        }
                    }
                    _ = probe_wake.notified() => {
                        // 应用回到前台 / 网络恢复：立即验活，不等一个完整心跳周期（后台最长 120s）。
                        // 半开连接（服务端已回收、浏览器仍报 readyState=OPEN、onclose 从未触发）在此
                        // 被一枚即时 ping + 有界等待戳穿：窗口内无 PONG 即主动 close，
                        // 触发上层 Disconnected → 重连（带 token 刷新）自愈。
                        // 已有一枚超时未答的 ping：直接判死。
                        if unanswered_ping_timed_out(
                            &last_ping,
                            &last_pong,
                            read_config(&config).timeout,
                        ) {
                            let mut conn = connection.lock().await;
                            let _ = conn.close().await;
                            break;
                        }

                        let ping_frame = crate::common::protocol::frame_with_system_command(
                            crate::common::protocol::ping(),
                            crate::common::protocol::Reliability::AtLeastOnce,
                        );
                        let data = {
                            let parser_guard = parser.lock().await;
                            match parser_guard.serialize(&ping_frame) {
                                Ok(d) => Some(d),
                                Err(e) => {
                                    tracing::error!("[HeartbeatManager] 序列化探测心跳失败: {}", e);
                                    None
                                }
                            }
                        };
                        if let Some(data) = data {
                            // 强制一枚新 ping 时间戳：回前台必须重新验活，不看是否 idle。
                            let ping_at = monotonic_now();
                            if let Ok(mut lp) = last_ping.lock() {
                                *lp = Some(ping_at);
                            }
                            let send_result = {
                                let mut conn = connection.lock().await;
                                conn.send(&data).await
                            };
                            if let Err(error) = send_result {
                                tracing::warn!("[HeartbeatManager] 探测心跳发送失败: {}", error);
                                let mut conn = connection.lock().await;
                                let _ = conn.close().await;
                                break;
                            }
                            // 有界等待窗口（远短于 90s 心跳 timeout）：ping 往返正常 < 100ms。
                            let probe_window = read_config(&config).timeout.min(PROBE_TIMEOUT);
                            sleep(probe_window).await;
                            if !pong_covers_ping(&last_pong, ping_at) {
                                tracing::warn!(
                                    "[HeartbeatManager] 前台验活未收到 PONG，判定半开死连，主动断开以触发重连"
                                );
                                let mut conn = connection.lock().await;
                                let _ = conn.close().await;
                                break;
                            }
                        }
                    }
                    _ = rx.recv() => {
                        break;
                    }
                }
            }
        };

        #[cfg(target_arch = "wasm32")]
        crate::client::wasm_tokio::spawn_detached(heartbeat_loop);

        #[cfg(not(target_arch = "wasm32"))]
        crate::client::runtime::spawn_client_task(heartbeat_loop);
    }

    /// 停止心跳
    pub fn stop(&mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.try_send(());
        }
    }

    /// 记录收到 PONG
    pub fn record_pong(&self) {
        if let Ok(mut last) = self.last_pong.lock() {
            *last = Some(monotonic_now());
        }
    }

    /// 仅测试用：制造一次"已发 ping、尚未收到 pong"的状态。
    #[cfg(test)]
    pub(crate) fn mark_ping_for_test(&self) {
        if let Ok(mut last) = self.last_ping.lock() {
            *last = Some(monotonic_now());
        }
        if let Ok(mut last) = self.last_pong.lock() {
            *last = None;
        }
    }

    /// 仅测试用：按给定超时判定，免得测试去睡真实时间。
    #[cfg(test)]
    pub(crate) fn is_timeout_after_for_test(&self, timeout: Duration) -> bool {
        unanswered_ping_timed_out(&self.last_ping, &self.last_pong, timeout)
    }

    /// 仅测试用：stop 通道是否还在（`stop()` 会取走它）。
    #[cfg(test)]
    pub(crate) fn has_stop_channel_for_test(&self) -> bool {
        self.stop_tx.is_some()
    }

    /// 检查心跳是否超时
    pub fn is_timeout(&self) -> bool {
        unanswered_ping_timed_out(
            &self.last_ping,
            &self.last_pong,
            self.current_config().timeout,
        )
    }
}

fn unanswered_ping_timed_out(
    last_ping: &Arc<std::sync::Mutex<Option<MonotonicInstant>>>,
    last_pong: &Arc<std::sync::Mutex<Option<MonotonicInstant>>>,
    timeout: Duration,
) -> bool {
    let Ok(last_ping) = last_ping.lock() else {
        return false;
    };
    let Some(ping_time) = *last_ping else {
        return false;
    };
    if pong_covers_ping(last_pong, ping_time) {
        return false;
    }
    ping_time.elapsed() > timeout
}

fn record_ping_start_if_idle(
    last_ping: &Arc<std::sync::Mutex<Option<MonotonicInstant>>>,
    last_pong: &Arc<std::sync::Mutex<Option<MonotonicInstant>>>,
) {
    let Ok(mut last_ping) = last_ping.lock() else {
        return;
    };
    if let Some(ping_time) = *last_ping
        && !pong_covers_ping(last_pong, ping_time)
    {
        return;
    }
    *last_ping = Some(monotonic_now());
}

fn pong_covers_ping(
    last_pong: &Arc<std::sync::Mutex<Option<MonotonicInstant>>>,
    ping_time: MonotonicInstant,
) -> bool {
    last_pong
        .lock()
        .ok()
        .and_then(|last_pong| *last_pong)
        .is_some_and(|pong_time| pong_time >= ping_time)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::MessageParser;
    use crate::common::error::Result;
    use crate::common::platform::monotonic_now;
    use crate::transport::events::ArcObserver;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingConnection {
        sends: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
        last_active: MonotonicInstant,
    }

    #[async_trait]
    impl Connection for CountingConnection {
        fn add_observer(&mut self, _observer: ArcObserver) {}

        fn remove_observer(&mut self, _observer: ArcObserver) {}

        async fn send(&mut self, _data: &[u8]) -> Result<()> {
            self.sends.fetch_add(1, Ordering::SeqCst);
            self.last_active = monotonic_now();
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn last_active_time(&self) -> MonotonicInstant {
            self.last_active
        }

        fn update_active_time(&mut self) {
            self.last_active = monotonic_now();
        }
    }

    #[tokio::test]
    async fn stop_eventually_stabilizes_native_heartbeat_sends() {
        let sends = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let connection: Arc<Mutex<Box<dyn Connection>>> =
            Arc::new(Mutex::new(Box::new(CountingConnection {
                sends: Arc::clone(&sends),
                closes,
                last_active: monotonic_now(),
            })));
        let parser = Arc::new(tokio::sync::Mutex::new(MessageParser::json()));

        let mut heartbeat =
            HeartbeatManager::new(Duration::from_millis(10), Duration::from_secs(5));
        heartbeat.start(connection, parser, Arc::new(Notify::new()));

        let deadline = monotonic_now() + Duration::from_millis(100);
        while sends.load(Ordering::SeqCst) == 0 && monotonic_now() < deadline {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert!(
            sends.load(Ordering::SeqCst) > 0,
            "heartbeat should send at least one ping before stop"
        );

        heartbeat.stop();
        tokio::time::sleep(Duration::from_millis(30)).await;
        let stopped_count = sends.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(
            sends.load(Ordering::SeqCst),
            stopped_count,
            "heartbeat should stop sending after stop signal is processed"
        );
    }

    #[tokio::test]
    async fn unanswered_ping_closes_connection_after_timeout() {
        let sends = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let connection: Arc<Mutex<Box<dyn Connection>>> =
            Arc::new(Mutex::new(Box::new(CountingConnection {
                sends: Arc::clone(&sends),
                closes: Arc::clone(&closes),
                last_active: monotonic_now(),
            })));
        let parser = Arc::new(tokio::sync::Mutex::new(MessageParser::json()));

        let mut heartbeat =
            HeartbeatManager::new(Duration::from_millis(5), Duration::from_millis(15));
        heartbeat.start(connection, parser, Arc::new(Notify::new()));

        let deadline = monotonic_now() + Duration::from_millis(200);
        while closes.load(Ordering::SeqCst) == 0 && monotonic_now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        heartbeat.stop();

        assert!(
            sends.load(Ordering::SeqCst) > 0,
            "heartbeat should send ping before timeout"
        );
        assert!(
            closes.load(Ordering::SeqCst) > 0,
            "unanswered ping should close the connection"
        );
    }

    #[test]
    fn heartbeat_manager_reads_shared_runtime_policy_updates() {
        let heartbeat = HeartbeatManager::with_config(
            HeartbeatConfig::default().with_foreground_interval(Duration::from_secs(30)),
        );

        assert_eq!(heartbeat.effective_interval(), Duration::from_secs(30));

        heartbeat.set_app_state(HeartbeatAppState::Background);
        assert_eq!(heartbeat.effective_interval(), Duration::from_secs(120));

        heartbeat.set_nat_timeout(Some(Duration::from_secs(40)));
        assert_eq!(heartbeat.effective_interval(), Duration::from_secs(28));
    }

    /// 回到前台的即时验活：连接已死（永不回 PONG）时，一次 probe_wake 必须在
    /// 远短于心跳间隔的时间内主动 close，从而触发上层重连——而不是干等一个完整周期。
    #[tokio::test]
    async fn probe_wake_closes_dead_connection_without_waiting_full_interval() {
        let sends = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let connection: Arc<Mutex<Box<dyn Connection>>> =
            Arc::new(Mutex::new(Box::new(CountingConnection {
                sends: Arc::clone(&sends),
                closes: Arc::clone(&closes),
                last_active: monotonic_now(),
            })));
        let parser = Arc::new(tokio::sync::Mutex::new(MessageParser::json()));

        // 心跳间隔很大（不靠周期心跳），验活窗口很小（timeout=30ms → probe_window=30ms）。
        let mut heartbeat =
            HeartbeatManager::new(Duration::from_secs(3600), Duration::from_millis(30));
        let probe_wake = Arc::new(Notify::new());
        heartbeat.start(connection, parser, Arc::clone(&probe_wake));

        // 模拟应用回到前台：请求一次即时验活。连接从不回 PONG → 应被判死并 close。
        probe_wake.notify_one();

        let deadline = monotonic_now() + Duration::from_millis(800);
        while closes.load(Ordering::SeqCst) == 0 && monotonic_now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        heartbeat.stop();

        assert!(
            sends.load(Ordering::SeqCst) > 0,
            "probe 必须实际发出一枚验活 ping"
        );
        assert!(
            closes.load(Ordering::SeqCst) > 0,
            "未收到 PONG 的验活必须主动断开连接（远早于 3600s 心跳间隔）"
        );
    }

    /// 回到前台的即时验活：连接仍健康（窗口内收到 PONG）时，probe_wake 不得断开连接。
    #[tokio::test]
    async fn probe_wake_keeps_live_connection() {
        let sends = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let connection: Arc<Mutex<Box<dyn Connection>>> =
            Arc::new(Mutex::new(Box::new(CountingConnection {
                sends: Arc::clone(&sends),
                closes: Arc::clone(&closes),
                last_active: monotonic_now(),
            })));
        let parser = Arc::new(tokio::sync::Mutex::new(MessageParser::json()));

        // 验活窗口 500ms，留足时间在窗口内补记一枚 PONG。
        let mut heartbeat =
            HeartbeatManager::new(Duration::from_secs(3600), Duration::from_millis(500));
        let probe_wake = Arc::new(Notify::new());
        heartbeat.start(connection, parser, Arc::clone(&probe_wake));

        probe_wake.notify_one();
        // 等 probe 先打上 ping 时间戳，再在窗口内记一枚 PONG（模拟服务端回包）。
        tokio::time::sleep(Duration::from_millis(80)).await;
        heartbeat.record_pong();

        tokio::time::sleep(Duration::from_millis(600)).await;
        heartbeat.stop();

        assert!(
            sends.load(Ordering::SeqCst) > 0,
            "probe 仍会发出验活 ping"
        );
        assert_eq!(
            closes.load(Ordering::SeqCst),
            0,
            "窗口内收到 PONG 的健康连接不得被断开"
        );
    }
}
