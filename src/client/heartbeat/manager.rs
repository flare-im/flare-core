//! 心跳管理模块
//!
//! 提供心跳机制的实现，保持连接活跃

use crate::common::platform::{MonotonicInstant, monotonic_now, sleep};
use crate::common::{HeartbeatAppState, HeartbeatConfig, MessageParser};
use crate::transport::connection::Connection;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};

/// 心跳管理器
pub struct HeartbeatManager {
    config: Arc<RwLock<HeartbeatConfig>>,
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
    ) {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_tx = Some(tx);

        let config = Arc::clone(&self.config);
        let last_pong = Arc::clone(&self.last_pong);

        let heartbeat_loop = async move {
            loop {
                let sleep_duration = read_config(&config).effective_interval();
                tokio::select! {
                    _ = sleep(sleep_duration) => {
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

                        // 使用 tokio::sync::Mutex，支持跨 await
                        let send_result = {
                            let mut conn = connection.lock().await;
                            conn.send(&data).await
                        };

                        if send_result.is_ok() {
                            // 检查是否超时（在锁外检查）
                            let should_close = {
                                let timeout_duration = read_config(&config).timeout;
                                let last = last_pong.lock().unwrap();
                                if let Some(last_pong_time) = *last {
                                    last_pong_time.elapsed() > timeout_duration
                                } else {
                                    false
                                }
                            };

                            if should_close {
                                // 心跳超时，触发断开
                                {
                                    let mut conn = connection.lock().await;
                                    let _ = conn.close().await;
                                }
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

    /// 检查心跳是否超时
    pub fn is_timeout(&self) -> bool {
        if let Ok(last) = self.last_pong.lock()
            && let Some(last_pong_time) = *last
        {
            return last_pong_time.elapsed() > self.current_config().timeout;
        }
        // 如果从未收到 PONG，且启动后已超过超时时间，则认为超时
        false
    }
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
        heartbeat.start(connection, parser);

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
}
