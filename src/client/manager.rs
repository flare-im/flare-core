//! 客户端连接管理器
//!
//! 统一管理客户端连接、心跳、重连等生命周期
//! 提供自动重连、心跳管理、连接状态监控等功能

use crate::client::config::ClientConfig;
use crate::client::connection::ConnectionStateManager;
use crate::client::heartbeat::HeartbeatManager;
use crate::client::transports::Client;
use crate::common::MessageParser;
use crate::common::error::Result;
use crate::common::platform::sleep;
use crate::common::protocol::Frame;
use crate::transport::events::{ArcObserver, ConnectionEvent};

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// 自动重连退避的上限（与 IM SDK 的重连循环一致）。
pub const AUTO_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);

/// 自动重连退避名义值的下限：`reconnect_interval` 配得再小，两次尝试之间也至少隔这么久。
const AUTO_RECONNECT_MIN_NOMINAL: Duration = Duration::from_secs(1);

/// 第 `attempt` 次（从 1 起）自动重连前等待的区间（毫秒级，含两端）。
///
/// 名义值从 `base` 起每次翻倍（最多翻 4 次），夹在 1 s 与 [`AUTO_RECONNECT_MAX_DELAY`] 之间；
/// 区间是名义值的 80%–120%，上界不超过 [`AUTO_RECONNECT_MAX_DELAY`]。
pub fn auto_reconnect_delay_bounds(base: Duration, attempt: u32) -> (Duration, Duration) {
    let shift = attempt.saturating_sub(1).min(4);
    let cap_ms = AUTO_RECONNECT_MAX_DELAY.as_millis() as u64;
    let nominal_ms = (base.as_millis() as u64)
        .saturating_mul(1_u64 << shift)
        .clamp(AUTO_RECONNECT_MIN_NOMINAL.as_millis() as u64, cap_ms);
    let min_ms = nominal_ms.saturating_mul(80).div_ceil(100);
    let max_ms = nominal_ms
        .saturating_mul(120)
        .div_ceil(100)
        .clamp(min_ms, cap_ms);
    (Duration::from_millis(min_ms), Duration::from_millis(max_ms))
}

/// 第 `attempt` 次自动重连前的等待：在 [`auto_reconnect_delay_bounds`] 的区间里按毫秒均匀取值。
/// 抖动让同一刻断开的一批客户端错开重连，不在服务端恢复的瞬间一起涌上来。
pub fn auto_reconnect_delay(base: Duration, attempt: u32) -> Duration {
    use rand::Rng;
    let (min, max) = auto_reconnect_delay_bounds(base, attempt);
    Duration::from_millis(
        rand::thread_rng().gen_range(min.as_millis() as u64..=max.as_millis() as u64),
    )
}

/// 客户端连接管理器
///
/// 管理客户端连接的生命周期，包括：
/// - 自动连接和重连
/// - 心跳管理
/// - 连接状态监控
/// - 消息观察者管理
pub struct ClientConnectionManager {
    /// 客户端配置
    config: ClientConfig,
    /// 客户端实例
    client: Arc<Mutex<Box<dyn Client>>>,
    /// 连接状态管理器
    state_manager: Arc<ConnectionStateManager>,
    /// 心跳管理器
    heartbeat_manager: Arc<Mutex<Option<HeartbeatManager>>>,
    /// 消息解析器
    #[allow(dead_code)] // 保留用于未来扩展
    parser: MessageParser,
    /// 观察者列表
    observers: Arc<StdMutex<Vec<ArcObserver>>>,
    /// 重连任务句柄
    reconnect_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// 是否正在重连
    is_reconnecting: Arc<Mutex<bool>>,
}

impl ClientConnectionManager {
    /// 创建新的客户端连接管理器
    ///
    /// # 参数
    /// - `client`: 客户端实例（可以是 WebSocketClient、QUICClient 等）
    /// - `config`: 客户端配置
    pub fn new(client: Box<dyn Client>, config: ClientConfig) -> Self {
        let parser = MessageParser::new(
            config.serialization_format,
            config.compression.clone(),
            crate::common::encryption::EncryptionAlgorithm::None,
        );

        Self {
            config,
            client: Arc::new(Mutex::new(client)),
            state_manager: Arc::new(ConnectionStateManager::new()),
            heartbeat_manager: Arc::new(Mutex::new(None)),
            parser,
            observers: Arc::new(StdMutex::new(Vec::new())),
            reconnect_handle: Arc::new(Mutex::new(None)),
            is_reconnecting: Arc::new(Mutex::new(false)),
        }
    }

    /// 连接到服务器
    ///
    /// 自动处理连接、心跳启动等
    pub async fn connect(&self) -> Result<()> {
        info!("Connecting to server: {}", self.config.server_url);

        let mut client = self.client.lock().await;

        // 检查是否已连接
        if client.is_connected() {
            debug!("Already connected, skipping connect");
            return Ok(());
        }

        // 设置状态为连接中
        self.state_manager
            .set_state(crate::client::connection::ConnectionState::Connecting);

        // 执行连接
        match client.connect().await {
            Ok(()) => {
                info!("Successfully connected to server");
                self.state_manager
                    .set_state(crate::client::connection::ConnectionState::Connected);

                // 启动心跳（如果启用）
                if self.config.heartbeat.enabled {
                    self.start_heartbeat().await?;
                }

                // 通知观察者
                self.notify_observers(&ConnectionEvent::Connected);

                Ok(())
            }
            Err(e) => {
                error!("Failed to connect: {}", e);
                self.state_manager
                    .set_state(crate::client::connection::ConnectionState::Disconnected);
                Err(e)
            }
        }
    }

    /// 断开连接
    pub async fn disconnect(&self) -> Result<()> {
        info!("Disconnecting from server");

        // 停止重连
        self.stop_reconnect().await;

        // 停止心跳
        self.stop_heartbeat().await;

        // 断开连接
        let mut client = self.client.lock().await;
        let result = client.disconnect().await;

        self.state_manager
            .set_state(crate::client::connection::ConnectionState::Disconnected);
        self.notify_observers(&ConnectionEvent::Disconnected(String::new()));

        result
    }

    /// 发送消息
    pub async fn send_frame(&self, frame: &Frame) -> Result<()> {
        if !self.state_manager.get_state().can_send() {
            return Err(crate::common::error::FlareError::connection_failed(
                "Not connected".to_string(),
            ));
        }

        let mut client = self.client.lock().await;
        client.send_frame(frame).await
    }

    /// 启动心跳
    async fn start_heartbeat(&self) -> Result<()> {
        if !self.config.heartbeat.enabled {
            return Ok(());
        }

        debug!(
            "Starting heartbeat: interval={:?}, timeout={:?}",
            self.config.heartbeat.interval, self.config.heartbeat.timeout
        );

        let heartbeat = HeartbeatManager::new(
            self.config.heartbeat.interval,
            self.config.heartbeat.timeout,
        );

        // 需要从 Client 获取 Connection，但 Client trait 没有提供这个方法
        // 这里我们需要一个不同的设计
        // 暂时跳过，因为心跳应该在 Client 实现内部管理
        // 或者我们需要扩展 Client trait

        let mut hb_mgr = self.heartbeat_manager.lock().await;
        *hb_mgr = Some(heartbeat);

        Ok(())
    }

    /// 停止心跳
    async fn stop_heartbeat(&self) {
        let mut hb_mgr = self.heartbeat_manager.lock().await;
        if let Some(mut hb) = hb_mgr.take() {
            hb.stop();
        }
    }

    /// 启动自动重连
    ///
    /// 断开后按 [`auto_reconnect_delay`] 退避重连：从 `reconnect_interval` 起每次翻倍、封顶 30 s、
    /// 上下浮动 20%（按毫秒取值），与 IM SDK 的重连循环一致；每次尝试前先等，不立即重试。
    /// `max_reconnect_attempts` 为 `Some(n)` 时连续失败 n 次即停止（`None` 不限次数）。
    /// 重连成功后循环结束，计数随之清零。
    pub async fn start_auto_reconnect(&self) {
        let mut is_reconnecting = self.is_reconnecting.lock().await;
        if *is_reconnecting {
            return; // 已经启动了
        }
        *is_reconnecting = true;
        drop(is_reconnecting);

        let client = Arc::clone(&self.client);
        let state_mgr = Arc::clone(&self.state_manager);
        let config = self.config.clone();
        let heartbeat_cfg = self.config.heartbeat.clone();
        let heartbeat_mgr = Arc::clone(&self.heartbeat_manager);
        let observers = Arc::clone(&self.observers);
        let reconnect_handle = Arc::clone(&self.reconnect_handle);
        let is_reconnecting_flag = Arc::clone(&self.is_reconnecting);

        let handle = tokio::spawn(async move {
            // 本轮断开后已经尝试的次数
            let mut attempt: u32 = 0;
            loop {
                if !Self::needs_reconnect(&client, &state_mgr).await {
                    // 还连着（或别处已经连上）：等它断开
                    attempt = 0;
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }

                if let Some(max) = config.max_reconnect_attempts
                    && attempt >= max
                {
                    warn!(
                        attempts = attempt,
                        "auto reconnect attempts exhausted, giving up"
                    );
                    break;
                }
                attempt += 1;

                let wait = auto_reconnect_delay(config.reconnect_interval, attempt);
                info!(attempt, ?wait, "reconnecting after backoff");
                sleep(wait).await;
                // 等待期间可能已被别处连上
                if !Self::needs_reconnect(&client, &state_mgr).await {
                    continue;
                }

                state_mgr.set_state(crate::client::connection::ConnectionState::Connecting);
                let reconnect_result = {
                    let mut client_guard = client.lock().await;
                    client_guard.connect().await
                };

                match reconnect_result {
                    Ok(()) => {
                        info!(attempt, "reconnected");
                        state_mgr.set_state(crate::client::connection::ConnectionState::Connected);

                        // 重新启动心跳
                        if heartbeat_cfg.enabled {
                            let _hb_mgr = heartbeat_mgr.lock().await;
                            // 重新创建心跳管理器
                            // 注意：这里简化了，实际应该从 Client 获取 Connection
                        }

                        // 通知观察者
                        {
                            let observers_guard = observers.lock().unwrap();
                            for observer in observers_guard.iter() {
                                observer.on_event(&ConnectionEvent::Connected);
                            }
                        }

                        // 重连成功，退出重连循环
                        break;
                    }
                    Err(e) => {
                        warn!(attempt, error = %e, "reconnect failed");
                        state_mgr
                            .set_state(crate::client::connection::ConnectionState::Disconnected);
                    }
                }
            }

            *is_reconnecting_flag.lock().await = false;
            // 清理句柄
            let mut handle_guard = reconnect_handle.lock().await;
            *handle_guard = None;
        });

        let mut handle_guard = self.reconnect_handle.lock().await;
        *handle_guard = Some(handle);
    }

    /// 客户端未连接且状态为断开：需要重连。
    async fn needs_reconnect(
        client: &Arc<Mutex<Box<dyn Client>>>,
        state_mgr: &Arc<ConnectionStateManager>,
    ) -> bool {
        let client_guard = client.lock().await;
        !client_guard.is_connected()
            && matches!(
                state_mgr.get_state(),
                crate::client::connection::ConnectionState::Disconnected
            )
    }

    /// 停止自动重连
    async fn stop_reconnect(&self) {
        let mut handle_guard = self.reconnect_handle.lock().await;
        if let Some(handle) = handle_guard.take() {
            handle.abort();
        }

        *self.is_reconnecting.lock().await = false;
    }

    /// 添加观察者
    pub fn add_observer(&self, observer: ArcObserver) {
        let mut observers = self.observers.lock().unwrap();
        observers.push(observer);
    }

    /// 移除观察者
    pub fn remove_observer(&self, observer: ArcObserver) {
        let mut observers = self.observers.lock().unwrap();
        observers.retain(|o| !Arc::ptr_eq(o, &observer));
    }

    /// 通知所有观察者
    fn notify_observers(&self, event: &ConnectionEvent) {
        let observers = self.observers.lock().unwrap();
        for observer in observers.iter() {
            observer.on_event(event);
        }
    }

    /// 检查是否已连接
    pub async fn is_connected(&self) -> bool {
        let client = self.client.lock().await;
        client.is_connected()
    }

    /// 获取连接 ID
    pub async fn connection_id(&self) -> Option<String> {
        let client = self.client.lock().await;
        client.connection_id()
    }

    /// 获取连接状态
    pub fn state(&self) -> crate::client::connection::ConnectionState {
        self.state_manager.get_state()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// 前 `failures` 次建连失败、之后成功的客户端，记下每次建连的（虚拟）时刻。
    struct FlakyClient {
        failures: usize,
        attempts: Arc<StdMutex<Vec<tokio::time::Instant>>>,
        connected: AtomicBool,
    }

    #[async_trait]
    impl Client for FlakyClient {
        async fn connect(&mut self) -> Result<()> {
            let attempt = {
                let mut attempts = self.attempts.lock().unwrap();
                attempts.push(tokio::time::Instant::now());
                attempts.len()
            };
            if attempt <= self.failures {
                return Err(crate::common::error::FlareError::connection_failed(
                    "server unavailable".to_string(),
                ));
            }
            self.connected.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn disconnect(&mut self) -> Result<()> {
            self.connected.store(false, Ordering::SeqCst);
            Ok(())
        }

        async fn send_frame(&mut self, _frame: &Frame) -> Result<()> {
            Ok(())
        }

        fn is_connected(&self) -> bool {
            self.connected.load(Ordering::SeqCst)
        }

        fn add_observer(&mut self, _observer: ArcObserver) {}

        fn remove_observer(&mut self, _observer: ArcObserver) {}
    }

    fn manager(
        failures: usize,
        max_attempts: Option<u32>,
    ) -> (
        ClientConnectionManager,
        Arc<StdMutex<Vec<tokio::time::Instant>>>,
    ) {
        let attempts = Arc::new(StdMutex::new(Vec::new()));
        let client = FlakyClient {
            failures,
            attempts: attempts.clone(),
            connected: AtomicBool::new(false),
        };
        let config = ClientConfig::default()
            .with_reconnect_interval(Duration::from_secs(5))
            .with_max_reconnect_attempts(max_attempts);
        (
            ClientConnectionManager::new(Box::new(client), config),
            attempts,
        )
    }

    /// 退避区间：从配置的间隔起翻倍、封顶 30 s、±20%，名义值至少 1 s。
    #[test]
    fn backoff_doubles_with_jitter_and_is_capped() {
        let secs = |s: u64| Duration::from_secs(s);
        let base = secs(5);
        assert_eq!(auto_reconnect_delay_bounds(base, 1), (secs(4), secs(6)));
        assert_eq!(auto_reconnect_delay_bounds(base, 2), (secs(8), secs(12)));
        assert_eq!(auto_reconnect_delay_bounds(base, 3), (secs(16), secs(24)));
        for attempt in [4, 5, 10, u32::MAX] {
            assert_eq!(
                auto_reconnect_delay_bounds(base, attempt),
                (secs(24), secs(30)),
                "attempt {attempt}"
            );
        }
        assert_eq!(
            auto_reconnect_delay_bounds(Duration::ZERO, 1),
            (Duration::from_millis(800), Duration::from_millis(1_200)),
            "a zero interval still waits about a second"
        );

        let samples: std::collections::HashSet<Duration> =
            (0..200).map(|_| auto_reconnect_delay(base, 1)).collect();
        assert!(samples.iter().all(|d| (secs(4)..=secs(6)).contains(d)));
        assert!(
            samples.len() > 20,
            "jitter is drawn per millisecond, got {} distinct values",
            samples.len()
        );
    }

    /// 断开后每次重连前先按退避等：第一次不立即重试，间隔逐次翻倍、到 30 s 封顶；成功后停止。
    #[tokio::test(start_paused = true)]
    async fn auto_reconnect_backs_off_exponentially_up_to_the_cap() {
        let (manager, attempts) = manager(6, None);
        let started = tokio::time::Instant::now();
        manager.start_auto_reconnect().await;

        let deadline = started + Duration::from_secs(600);
        while attempts.lock().unwrap().len() < 7 {
            assert!(tokio::time::Instant::now() < deadline, "seven attempts");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tokio::time::sleep(Duration::from_secs(120)).await;

        let attempts = attempts.lock().unwrap().clone();
        assert_eq!(attempts.len(), 7, "the loop stops once reconnected");
        let mut previous = started;
        for (index, at) in attempts.iter().enumerate() {
            let attempt = index as u32 + 1;
            let gap = at.duration_since(previous);
            let (min, max) = auto_reconnect_delay_bounds(Duration::from_secs(5), attempt);
            assert!(
                gap >= min && gap <= max,
                "attempt {attempt} waited {gap:?}, expected {min:?}..={max:?}"
            );
            previous = *at;
        }
        assert_eq!(
            manager.state(),
            crate::client::connection::ConnectionState::Connected
        );
    }

    /// `max_reconnect_attempts` 有值时连续失败到这个次数就停止，不再无限重试。
    #[tokio::test(start_paused = true)]
    async fn auto_reconnect_stops_after_max_attempts() {
        let (manager, attempts) = manager(usize::MAX, Some(3));
        manager.start_auto_reconnect().await;
        tokio::time::sleep(Duration::from_secs(600)).await;

        assert_eq!(attempts.lock().unwrap().len(), 3);
        assert_eq!(
            manager.state(),
            crate::client::connection::ConnectionState::Disconnected
        );
        assert!(
            !*manager.is_reconnecting.lock().await,
            "an exhausted loop can be started again"
        );
    }
}
