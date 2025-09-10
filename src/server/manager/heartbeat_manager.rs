//! 心跳管理器
//!
//! 负责管理连接的心跳检测和过期处理

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{warn, debug};
use std::collections::HashMap;

use crate::common::{
    error::Result,
    connections::traits::ServerConnection,
};

/// 心跳管理器配置
#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    /// 心跳检测间隔
    pub check_interval: Duration,
    /// 连接超时时间
    pub connection_timeout: Duration,
    /// 是否启用自动清理
    pub enable_auto_cleanup: bool,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(10),
            connection_timeout: Duration::from_secs(30),
            enable_auto_cleanup: true,
        }
    }
}

/// 心跳管理器
pub struct HeartbeatManager<T: ServerConnection + ?Sized + 'static> {
    /// 配置
    config: HeartbeatConfig,
    /// 连接列表
    connections: Arc<RwLock<HashMap<String, Arc<T>>>>,
    /// 最后检查时间
    last_check: Arc<RwLock<Instant>>,
}

impl<T: ServerConnection + ?Sized + 'static> HeartbeatManager<T> {
    /// 创建新的心跳管理器
    pub fn new(config: HeartbeatConfig) -> Self {
        Self {
            config,
            connections: Arc::new(RwLock::new(HashMap::new())),
            last_check: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// 添加连接
    pub async fn add_connection(&self, connection: Arc<T>) {
        let connection_id = connection.get_id().to_string();
        let mut connections = self.connections.write().await;
        connections.insert(connection_id.clone(), connection.clone());
        debug!("连接已添加到心跳管理器: {}", connection.get_id());
    }

    /// 移除连接
    pub async fn remove_connection(&self, connection_id: &str) -> bool {
        let mut connections = self.connections.write().await;
        let removed = connections.remove(connection_id).is_some();
        if removed {
            debug!("连接已从心跳管理器移除: {}", connection_id);
        }
        removed
    }

    /// 获取连接
    pub async fn get_connection(&self, connection_id: &str) -> Option<Arc<T>> {
        let connections = self.connections.read().await;
        connections.get(connection_id).cloned()
    }

    /// 获取所有连接
    pub async fn get_all_connections(&self) -> Vec<Arc<T>> {
        let connections = self.connections.read().await;
        connections.values().cloned().collect()
    }

    /// 启动心跳检测任务
    pub fn start_heartbeat_task(&self) -> tokio::task::JoinHandle<()> {
        let manager = self.clone();
        tokio::spawn(async move {
            let mut interval = interval(manager.config.check_interval);
            loop {
                interval.tick().await;
                if let Err(e) = manager.check_heartbeat().await {
                    warn!("心跳检测失败: {}", e);
                }
            }
        })
    }

    /// 检查心跳
    pub async fn check_heartbeat(&self) -> Result<()> {
        if !self.config.enable_auto_cleanup {
            return Ok(());
        }

        let now = Instant::now();
        let mut expired_connections = Vec::new();

        // 检查所有连接的心跳
        {
            let connections = self.connections.read().await;
            for (connection_id, connection) in connections.iter() {
                let last_activity = connection.get_last_activity().await;
                if now.duration_since(last_activity) > self.config.connection_timeout {
                    expired_connections.push(connection_id.clone());
                }
            }
        }

        // 处理过期连接
        if !expired_connections.is_empty() {
            let mut connections = self.connections.write().await;
            for connection_id in expired_connections {
                if let Some(connection) = connections.remove(&connection_id) {
                    warn!("连接心跳超时，已断开: {}", connection_id);
                    // 尝试优雅地关闭连接
                    if let Err(e) = connection.close().await {
                        warn!("关闭过期连接失败: {} - {}", connection_id, e);
                    }
                }
            }
        }

        // 更新最后检查时间
        {
            let mut last_check = self.last_check.write().await;
            *last_check = now;
        }

        Ok(())
    }

    /// 获取统计信息
    pub async fn get_stats(&self) -> HeartbeatStats {
        let connections = self.connections.read().await;
        let last_check = self.last_check.read().await;
        
        HeartbeatStats {
            total_connections: connections.len(),
            last_check_time: *last_check,
            next_check_time: *last_check + self.config.check_interval,
        }
    }
}

impl<T: ServerConnection + ?Sized + 'static> Clone for HeartbeatManager<T> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            connections: Arc::clone(&self.connections),
            last_check: Arc::clone(&self.last_check),
        }
    }
}

/// 心跳统计信息
#[derive(Debug, Clone)]
pub struct HeartbeatStats {
    /// 总连接数
    pub total_connections: usize,
    /// 最后检查时间
    pub last_check_time: Instant,
    /// 下次检查时间
    pub next_check_time: Instant,
}