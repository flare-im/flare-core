//! 心跳管理模块
//!
//! 提供心跳机制的实现，保持连接活跃

use crate::common::MessageParser;
use crate::common::platform::{MonotonicInstant, interval, monotonic_now};
use crate::transport::connection::Connection;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};

/// 心跳管理器
pub struct HeartbeatManager {
    interval: Duration,
    timeout: Duration,
    // 使用 std::sync::Mutex，因为 record_pong 可能从同步上下文调用
    last_pong: Arc<std::sync::Mutex<Option<MonotonicInstant>>>,
    stop_tx: Option<mpsc::Sender<()>>,
}

impl HeartbeatManager {
    /// 创建新的心跳管理器
    ///
    /// # 参数
    /// - `interval`: 心跳发送间隔
    /// - `timeout`: 等待 PONG 的超时时间
    pub fn new(interval: Duration, timeout: Duration) -> Self {
        Self {
            interval,
            timeout,
            last_pong: Arc::new(std::sync::Mutex::new(None)),
            stop_tx: None,
        }
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

        let interval_duration = self.interval;
        let timeout_duration = self.timeout;
        let last_pong = Arc::clone(&self.last_pong);

        let heartbeat_loop = async move {
            let mut interval_timer = interval(interval_duration);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
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
            return last_pong_time.elapsed() > self.timeout;
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
}
