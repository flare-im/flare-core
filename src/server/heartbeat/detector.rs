//! 服务端心跳检测器
//!
//! 定期检查连接的最后活跃时间，清理超时连接
//! 服务端不需要主动发送心跳，只需要检测客户端的心跳和消息

use crate::server::connection::ConnectionManagerTrait;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::info;

/// 心跳检测器
///
/// 定期检查连接的最后活跃时间，清理超时连接
pub struct HeartbeatDetector {
    connection_manager: Arc<dyn ConnectionManagerTrait>,
    timeout: Duration,
    check_interval: Duration,
    stop_tx: Option<tokio::sync::mpsc::Sender<()>>,
}

impl HeartbeatDetector {
    /// 创建新的心跳检测器
    ///
    /// # 参数
    /// - `connection_manager`: 连接管理器
    /// - `timeout`: 连接超时时间（没有心跳或消息的时间）
    /// - `check_interval`: 检查间隔（建议为 timeout 的 1/3 到 1/2）
    pub fn new(
        connection_manager: Arc<dyn ConnectionManagerTrait>,
        timeout: Duration,
        check_interval: Duration,
    ) -> Self {
        Self {
            connection_manager,
            timeout,
            check_interval,
            stop_tx: None,
        }
    }

    /// 启动心跳检测
    ///
    /// 定期检查所有连接的最后活跃时间，清理超时连接
    pub fn start(&mut self) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        self.stop_tx = Some(tx);

        let connection_manager = Arc::clone(&self.connection_manager);
        let timeout = self.timeout;
        let check_interval = self.check_interval;

        tokio::spawn(async move {
            let mut interval_timer = interval(check_interval);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        // 清理超时连接
                        let timeout_connections = connection_manager.cleanup_timeout_connections(timeout).await;
                        if !timeout_connections.is_empty() {
                            let (sample, omitted_count) =
                                Self::timeout_cleanup_log_sample(&timeout_connections, 8);
                            info!(
                                cleaned_count = timeout_connections.len(),
                                omitted_count,
                                sample = ?sample,
                                "Cleaned timeout connections"
                            );
                        }
                    }
                    _ = rx.recv() => {
                        break;
                    }
                }
            }
        });
    }

    /// 停止心跳检测
    pub fn stop(&mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.try_send(());
        }
    }

    fn timeout_cleanup_log_sample(connection_ids: &[String], limit: usize) -> (Vec<String>, usize) {
        let sample_len = connection_ids.len().min(limit);
        (
            connection_ids[..sample_len].to_vec(),
            connection_ids.len().saturating_sub(sample_len),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::error::Result;
    use crate::common::platform::{MonotonicInstant, monotonic_now};
    use crate::server::connection::ConnectionManager;
    use crate::transport::connection::Connection;
    use crate::transport::events::ArcObserver;
    use async_trait::async_trait;

    struct DetectorTestConnection {
        last_active: MonotonicInstant,
    }

    #[async_trait]
    impl Connection for DetectorTestConnection {
        fn add_observer(&mut self, _observer: ArcObserver) {}

        fn remove_observer(&mut self, _observer: ArcObserver) {}

        async fn send(&mut self, _data: &[u8]) -> Result<()> {
            self.last_active = monotonic_now();
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
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
    async fn stop_prevents_future_timeout_cleanup_checks() {
        let manager = Arc::new(ConnectionManager::new());
        manager
            .add_connection(
                "stays-connected".to_string(),
                Box::new(DetectorTestConnection {
                    last_active: monotonic_now(),
                }),
                None,
                false,
            )
            .expect("test connection should be added");

        let manager_trait: Arc<dyn ConnectionManagerTrait> = manager.clone();
        let mut detector = HeartbeatDetector::new(
            manager_trait,
            Duration::from_millis(20),
            Duration::from_millis(10),
        );
        detector.start();
        detector.stop();

        tokio::time::sleep(Duration::from_millis(70)).await;

        assert_eq!(
            manager.connection_count(),
            1,
            "stopped detector should not continue cleaning timeout connections"
        );
    }

    #[test]
    fn timeout_cleanup_log_sample_limits_connection_ids() {
        let connection_ids = (0..12).map(|idx| format!("conn-{idx}")).collect::<Vec<_>>();

        let (sample, omitted) = HeartbeatDetector::timeout_cleanup_log_sample(&connection_ids, 5);

        assert_eq!(
            sample,
            vec!["conn-0", "conn-1", "conn-2", "conn-3", "conn-4"]
        );
        assert_eq!(omitted, 7);
    }
}
