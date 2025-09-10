//! 基于连接的管理器实现
//!
//! 按连接ID管理连接，每个连接独立处理

use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{info, debug, warn};

use crate::common::{
    error::Result,
    connections::traits::ServerConnection,
    protocol::Frame,
};

use super::traits::{ConnectionManager, ManagerStats};
use super::heartbeat_manager::HeartbeatManager;

/// 基于连接的管理器统计信息
#[derive(Debug, Clone)]
pub struct ConnectionBasedStats {
    /// 总连接数
    pub total_connections: usize,
    /// 活跃连接数
    pub active_connections: usize,
    /// 总消息数
    pub total_messages: u64,
    /// 服务器启动时间
    pub started_at: Instant,
}

impl ConnectionBasedStats {
    /// 获取运行时间
    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }
}

/// 基于连接的管理器
#[derive(Clone)]
pub struct ConnectionBasedManager {
    /// 所有连接 (连接ID -> 连接实例)
    connections: Arc<RwLock<HashMap<String, Arc<dyn ServerConnection>>>>,
    /// 统计信息
    stats: Arc<RwLock<ConnectionBasedStats>>,
    /// 最后清理时间
    last_cleanup: Arc<RwLock<Instant>>,
    /// 清理间隔
    cleanup_interval: Duration,
    /// 心跳管理器
    heartbeat_manager: Arc<RwLock<Option<Arc<HeartbeatManager<dyn ServerConnection + 'static>>>>>,
}

#[async_trait::async_trait]
impl ConnectionManager for ConnectionBasedManager {
    /// 添加连接
    async fn add_connection(&self, connection: Arc<dyn ServerConnection>) -> Result<()> {
        self.add_connection(connection).await
    }
    
    /// 移除连接
    async fn remove_connection(&self, connection_id: &str) -> Result<()> {
        self.remove_connection(connection_id).await
    }
    
    /// 获取连接
    async fn get_connection(&self, connection_id: &str) -> Option<Arc<dyn ServerConnection>> {
        self.get_connection(connection_id).await
    }
    
    /// 获取所有连接
    async fn get_all_connections(&self) -> Vec<Arc<dyn ServerConnection>> {
        self.get_all_connections().await
    }
    
    /// 获取连接数量
    async fn get_connection_count(&self) -> usize {
        self.get_connection_count().await
    }
    
    /// 向指定连接发送消息
    async fn send_message_to_connection(&self, connection_id: &str, message: Frame) -> Result<()> {
        self.send_message_to_connection(connection_id, message).await
    }
    
    /// 广播消息到所有连接
    async fn broadcast_message(&self, message: Frame) -> Result<usize> {
        self.broadcast_message(message).await
    }
    
    /// 清理不活跃的连接
    async fn cleanup_inactive_connections(&self, timeout: Duration) -> usize {
        self.cleanup_inactive_connections(timeout).await
    }
    
    /// 获取统计信息
    async fn get_stats(&self) -> ManagerStats {
        let stats = self.get_stats().await;
        ManagerStats {
            total_connections: stats.total_connections,
            active_connections: stats.active_connections,
            total_messages: stats.total_messages,
            average_quality: 100, // 简化实现
            uptime: stats.uptime(),
        }
    }
    
    /// 清空所有连接
    async fn clear_all(&self) {
        self.clear_all().await
    }
    
    /// 检查是否需要清理
    async fn should_cleanup(&self) -> bool {
        self.should_cleanup().await
    }
    
    /// 注册到心跳管理器
    async fn register_heartbeat_manager(&self, heartbeat_manager: Arc<HeartbeatManager<dyn ServerConnection>>) {
        let mut hm = self.heartbeat_manager.write().await;
        *hm = Some(heartbeat_manager);
    }
}

impl ConnectionBasedManager {
    /// 创建新的连接管理器
    pub fn new() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(ConnectionBasedStats {
                total_connections: 0,
                active_connections: 0,
                total_messages: 0,
                started_at: Instant::now(),
            })),
            last_cleanup: Arc::new(RwLock::new(Instant::now())),
            cleanup_interval: Duration::from_secs(30), // 30秒清理一次
            heartbeat_manager: Arc::new(RwLock::new(None)),
        }
    }
    
    /// 创建带配置的连接管理器
    pub fn with_cleanup_interval(mut self, interval: Duration) -> Self {
        self.cleanup_interval = interval;
        self
    }
    
    /// 添加新连接
    pub async fn add_connection(&self, connection: Arc<dyn ServerConnection>) -> Result<()> {
        let connection_id = connection.get_id().to_string();
        
        // 检查是否已存在相同ID的连接
        {
            let connections = self.connections.read().await;
            if connections.contains_key(&connection_id) {
                warn!("连接已存在，将被替换: {}", connection_id);
            }
        }
        
        // 添加连接
        {
            let mut connections = self.connections.write().await;
            connections.insert(connection_id.clone(), connection.clone());
        }
        
        // 注册到心跳管理器
        {
            let heartbeat_manager = self.heartbeat_manager.read().await;
            if let Some(_hm) = heartbeat_manager.as_ref() {
                _hm.add_connection(connection).await;
            }
        }
        
        // 更新统计信息
        {
            let mut stats = self.stats.write().await;
            stats.total_connections += 1;
            stats.active_connections += 1;
        }
        
        info!("连接已添加: {}", connection_id);
        Ok(())
    }
    
    /// 移除连接
    pub async fn remove_connection(&self, connection_id: &str) -> Result<()> {
        let removed = {
            let mut connections = self.connections.write().await;
            connections.remove(connection_id).is_some()
        };
        
        if removed {
            // 从心跳管理器中移除
            {
                let heartbeat_manager = self.heartbeat_manager.read().await;
                if let Some(_hm) = heartbeat_manager.as_ref() {
                    _hm.remove_connection(connection_id).await;
                }
            }
            
            let mut stats = self.stats.write().await;
            stats.active_connections -= 1;
            info!("连接已移除: {}", connection_id);
        } else {
            warn!("尝试移除不存在的连接: {}", connection_id);
        }
        
        Ok(())
    }
    
    /// 获取连接
    pub async fn get_connection(&self, connection_id: &str) -> Option<Arc<dyn ServerConnection>> {
        let connections = self.connections.read().await;
        connections.get(connection_id).cloned()
    }
    
    /// 获取所有连接
    pub async fn get_all_connections(&self) -> Vec<Arc<dyn ServerConnection>> {
        let connections = self.connections.read().await;
        connections.values().cloned().collect()
    }
    
    /// 获取连接数量
    pub async fn get_connection_count(&self) -> usize {
        let connections = self.connections.read().await;
        connections.len()
    }
    
    /// 向指定连接发送消息
    pub async fn send_message_to_connection(&self, connection_id: &str, message: Frame) -> Result<()> {
        if let Some(connection) = self.get_connection(connection_id).await {
            // 克隆消息以避免借用问题
            let msg = message.clone();
            connection.send_message(msg).await?;
            // 更新统计信息
            let mut stats = self.stats.write().await;
            stats.total_messages += 1;
            Ok(())
        } else {
            Err(crate::common::error::FlareError::connection_failed(
                format!("连接不存在: {}", connection_id)
            ))
        }
    }
    
    /// 广播消息到所有连接
    pub async fn broadcast_message(&self, message: Frame) -> Result<usize> {
        // 收集所有连接ID，避免持有读锁太久
        let connection_ids: Vec<String> = {
            let connections = self.connections.read().await;
            connections.keys().cloned().collect()
        };
        
        let mut sent_count = 0;
        let mut failed_connections = Vec::new();
        
        // 向所有连接发送消息
        for connection_id in connection_ids {
            if let Some(connection) = self.get_connection(&connection_id).await {
                match connection.is_healthy().await {
                    true => {
                        // 连接健康，尝试发送消息
                        // 克隆消息以避免借用问题
                        let msg = message.clone();
                        match connection.send_message(msg).await {
                            Ok(()) => {
                                sent_count += 1;
                                debug!("消息已发送到连接: {}", connection_id);
                            }
                            Err(e) => {
                                warn!("向连接发送消息失败: {} - 错误: {}", connection_id, e);
                                failed_connections.push(connection_id);
                            }
                        }
                    }
                    false => {
                        warn!("连接不健康，无法发送消息: {}", connection_id);
                        failed_connections.push(connection_id);
                    }
                }
            }
        }
        
        // 更新统计信息
        {
            let mut stats = self.stats.write().await;
            stats.total_messages += sent_count as u64;
        }
        
        Ok(sent_count)
    }
    
    /// 清理不活跃的连接
    pub async fn cleanup_inactive_connections(&self, timeout: Duration) -> usize {
        // 收集所有连接ID，避免持有读锁太久
        let connection_ids: Vec<String> = {
            let connections = self.connections.read().await;
            connections.keys().cloned().collect()
        };
        
        let mut inactive_connections = Vec::new();
        
        // 找出不活跃的连接
        for connection_id in connection_ids {
            if let Some(connection) = self.get_connection(&connection_id).await {
                let last_activity = connection.get_last_activity().await;
                if last_activity.elapsed() > timeout {
                    inactive_connections.push(connection_id);
                }
            }
        }
        
        // 移除不活跃的连接
        let removed_count = {
            let mut connections = self.connections.write().await;
            let mut count = 0;
            for connection_id in &inactive_connections {
                if connections.remove(connection_id).is_some() {
                    count += 1;
                }
            }
            count
        };
        
        // 从心跳管理器中移除
        {
            let heartbeat_manager = self.heartbeat_manager.read().await;
            if let Some(_hm) = heartbeat_manager.as_ref() {
                for connection_id in &inactive_connections {
                    _hm.remove_connection(connection_id).await;
                }
            }
        }
        
        // 更新统计信息
        if removed_count > 0 {
            let mut stats = self.stats.write().await;
            stats.active_connections -= removed_count;
            
            let mut last_cleanup = self.last_cleanup.write().await;
            *last_cleanup = Instant::now();
            
            info!("清理不活跃连接: {} 个", removed_count);
        }
        
        removed_count
    }
    
    /// 获取统计信息
    pub async fn get_stats(&self) -> ConnectionBasedStats {
        let stats = self.stats.read().await;
        stats.clone()
    }
    
    /// 清空所有连接
    pub async fn clear_all(&self) {
        let mut connections = self.connections.write().await;
        let count = connections.len();
        connections.clear();
        
        // 清空心跳管理器
        {
            let heartbeat_manager = self.heartbeat_manager.read().await;
            if let Some(_hm) = heartbeat_manager.as_ref() {
                // 注意：这里我们不直接清空心跳管理器，因为可能有其他组件也在使用它
                // 只是清空连接列表
            }
        }
        
        let mut stats = self.stats.write().await;
        stats.active_connections = 0;
        info!("已清空所有连接，共移除 {} 个连接", count);
    }
    
    /// 检查是否需要清理
    pub async fn should_cleanup(&self) -> bool {
        let last_cleanup = self.last_cleanup.read().await;
        last_cleanup.elapsed() > self.cleanup_interval
    }
}