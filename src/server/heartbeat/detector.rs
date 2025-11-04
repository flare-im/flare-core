//! 服务端心跳检测器
//! 
//! 定期检查连接的最后活跃时间，清理超时连接
//! 服务端不需要主动发送心跳，只需要检测客户端的心跳和消息

use crate::server::connection::ConnectionManagerTrait;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{info, warn};

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
                            info!(
                                "Cleaned up {} timeout connections: {:?}",
                                timeout_connections.len(),
                                timeout_connections
                            );
                            
                            // 断开所有超时连接
                            for connection_id in timeout_connections {
                                if let Err(e) = connection_manager.remove_connection(&connection_id).await {
                                    warn!("Failed to remove timeout connection {}: {:?}", connection_id, e);
                                }
                            }
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
            let _ = tx.send(());
        }
    }
}
