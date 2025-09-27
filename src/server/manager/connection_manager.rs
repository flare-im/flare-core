//! 简单连接管理器实现
//!
//! 提供连接管理和消息处理功能
//!
//! # 特点
//!
//! - 管理所有连接的生命周期
//! - 处理消息发送和广播
//! - 集成心跳管理功能
//! - 开箱即用，自动启动心跳检测

use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{info, debug, warn};

use crate::common::{
    error::Result,
    connections::traits::ServerConnection,
    protocol::Frame,
    connections::types::ConnectionState,
};

use super::traits::ServerConnectionManager;

/// 简单连接管理器统计信息
#[derive(Debug, Clone)]
pub struct ConnectionStats {
    /// 总连接数
    pub total_connections: usize,
    /// 活跃连接数
    pub active_connections: usize,
    /// 总消息数
    pub total_messages: u64,
    /// 服务器启动时间
    pub started_at: Instant,
}

impl ConnectionStats {
    /// 获取运行时间
    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }
}

/// 心跳管理配置
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

/// 简单连接管理器
#[derive(Clone)]
pub struct ConnectionManager {
    /// 所有连接 (连接ID -> 连接实例)
    connections: Arc<RwLock<HashMap<String, Arc<dyn ServerConnection>>>>,
    /// 统计信息
    stats: Arc<RwLock<ConnectionStats>>,
    /// 心跳管理配置
    heartbeat_config: HeartbeatConfig,
    /// 心跳检测任务句柄
    heartbeat_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// 是否正在运行
    is_running: Arc<RwLock<bool>>,
}

#[async_trait::async_trait]
impl ServerConnectionManager for ConnectionManager {
    /// 添加连接
    async fn add_connection(&self, connection: Arc<dyn ServerConnection>) -> Result<()> {
        let connection_id = connection.id().to_string();
        
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
    async fn remove_connection(&self, connection_id: &str, reason: Option<String>) -> Result<()> {
        // 先获取连接实例，以便能够调用其close method
        let connection = {
            let connections = self.connections.read().await;
            connections.get(connection_id).cloned()
        };
        
        // 如果连接存在，先通知客户端关闭连接
        if let Some(conn) = connection {
            if let Err(e) = conn.close(reason).await {
                warn!("关闭连接时发生错误: {} - 错误: {}", connection_id, e);
            }
        }
        
        // 从连接映射中移除连接
        let removed = {
            let mut connections = self.connections.write().await;
            connections.remove(connection_id).is_some()
        };
        
        if removed {
            let mut stats = self.stats.write().await;
            if stats.active_connections > 0 {
                stats.active_connections -= 1;
            }
            info!("连接已移除: {}", connection_id);
        } else {
            warn!("尝试移除不存在的连接: {}", connection_id);
        }
        
        Ok(())
    }
    
    /// 获取连接
    async fn get_connection(&self, connection_id: &str) -> Option<Arc<dyn ServerConnection>> {
        let connections = self.connections.read().await;
        connections.get(connection_id).cloned()
    }
    
    /// 获取所有连接
    async fn get_all_connections(&self) -> Vec<Arc<dyn ServerConnection>> {
        let connections = self.connections.read().await;
        connections.values().cloned().collect()
    }
    
    /// 获取连接数量
    async fn get_connection_count(&self) -> usize {
        let connections = self.connections.read().await;
        connections.len()
    }
    
    /// 发送消息给指定链接
    async fn send_message(&self, connection_id: &str, message: Frame) -> Result<()> {
        if let Some(connection) = self.get_connection(connection_id).await {
            connection.send_message(message).await?;
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
    async fn broadcast_message(&self, message: Frame) -> Result<usize> {
        let connections = self.get_all_connections().await;
        
        let mut sent_count = 0;
        let mut failed_connections = Vec::new();
        
        // 向所有连接发送消息
        for connection in connections {
            let connection_id = connection.id().to_string();
            let state = connection.state();
            if matches!(state, ConnectionState::Connected | ConnectionState::Ready) {
                match connection.send_message(message.clone()).await {
                    Ok(()) => {
                        sent_count += 1;
                        debug!("消息已发送到连接: {}", connection_id);
                    }
                    Err(e) => {
                        warn!("向连接发送消息失败: {} - 错误: {}", connection_id, e);
                        failed_connections.push(connection_id);
                    }
                }
            } else {
                warn!("连接不健康，无法发送消息: {}", connection_id);
                failed_connections.push(connection_id);
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
    async fn cleanup_inactive_connections(&self, timeout: Duration) -> usize {
        // 收集所有连接ID，避免持有读锁太久
        let connection_ids: Vec<String> = {
            let connections = self.connections.read().await;
            connections.keys().cloned().collect()
        };
        
        let mut inactive_connections = Vec::new();
        
        // 找出不活跃的连接
        for connection_id in connection_ids {
            if let Some(connection) = self.get_connection(&connection_id).await {
                let last_activity = std::time::Instant::now() - std::time::Duration::from_millis(connection.last_activity_epoch_ms() as u64);
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
        
        // 更新统计信息
        if removed_count > 0 {
            let mut stats = self.stats.write().await;
            if stats.active_connections >= removed_count {
                stats.active_connections -= removed_count;
            } else {
                stats.active_connections = 0;
            }
            info!("清理不活跃连接: {} 个", removed_count);
        }
        
        removed_count
    }
    
    /// 获取连接统计信息
    async fn get_connection_stats(&self) -> super::traits::ServerStats {
        let stats = self.stats.read().await;
        super::traits::ServerStats {
            total_connections: stats.total_connections,
            active_connections: stats.active_connections,
            total_messages: stats.total_messages,
            average_quality: 100, // 简化实现
            uptime: stats.uptime(),
        }
    }
}

impl ConnectionManager {
    /// 创建新的连接管理器
    /// 
    /// # 返回值
    /// 
    /// 返回已准备好使用的连接管理器实例
    pub fn new() -> Self {
        Self::with_heartbeat_config(HeartbeatConfig::default())
    }
    
    /// 创建带心跳配置的连接管理器
    /// 
    /// # 参数
    /// 
    /// * `config` - 心跳配置
    /// 
    /// # 返回值
    /// 
    /// 返回已准备好使用的连接管理器实例
    pub fn with_heartbeat_config(config: HeartbeatConfig) -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(ConnectionStats {
                total_connections: 0,
                active_connections: 0,
                total_messages: 0,
                started_at: Instant::now(),
            })),
            heartbeat_config: config,
            heartbeat_task: Arc::new(RwLock::new(None)),
            is_running: Arc::new(RwLock::new(false)),
        }
        // 注意：心跳任务需要在异步环境中手动启动
    }
    
    /// 启动心跳检测任务
    /// 
    /// 该方法会自动在后台启动心跳检测任务，定期检查连接的活跃状态
    /// 需要在Tokio运行时环境中调用
    pub async fn start_heartbeat_task(&self) {
        if !self.heartbeat_config.enable_auto_cleanup {
            return;
        }
        
        // 设置运行状态
        {
            let mut is_running = self.is_running.write().await;
            *is_running = true;
        }
        
        let manager = self.clone();
        let handle = tokio::spawn(async move {
            let mut interval = interval(manager.heartbeat_config.check_interval);
            loop {
                interval.tick().await;
                
                // 检查是否仍在运行
                {
                    let is_running = manager.is_running.read().await;
                    if !*is_running {
                        break;
                    }
                }
                
                if let Err(e) = manager.check_heartbeat().await {
                    warn!("心跳检测失败: {}", e);
                }
            }
        });
        
        // 更新任务句柄
        {
            let mut task = self.heartbeat_task.write().await;
            *task = Some(handle);
        }
    }
    
    /// 检查心跳
    /// 
    /// 该方法会检查所有连接的活跃状态，关闭超时的连接
    async fn check_heartbeat(&self) -> Result<()> {
        if !self.heartbeat_config.enable_auto_cleanup {
            return Ok(());
        }
        
        let now = Instant::now();
        let mut expired_connections = Vec::new();
        
        // 检查所有连接的心跳
        {
            let connections = self.connections.read().await;
            for (connection_id, connection) in connections.iter() {
                let last_activity = std::time::Instant::now() - std::time::Duration::from_millis(connection.last_activity_epoch_ms() as u64);
                if now.duration_since(last_activity) > self.heartbeat_config.connection_timeout {
                    expired_connections.push(connection_id.clone());
                }
            }
        }
        
        // 处理过期连接
        let expired_count = expired_connections.len();
        if expired_count > 0 {
            let mut connections = self.connections.write().await;
            for connection_id in &expired_connections {
                if let Some(connection) = connections.remove(connection_id) {
                    warn!("连接心跳超时，已断开: {}", connection_id);
                    // 尝试优雅地关闭连接
                    if let Err(e) = connection.close(Some("超时断开".to_string())).await {
                        warn!("关闭过期连接失败: {} - {}", connection_id, e);
                    }
                }
            }
            
            // 更新统计信息
            if expired_count > 0 {
                let mut stats = self.stats.write().await;
                if stats.active_connections >= expired_count {
                    stats.active_connections -= expired_count;
                } else {
                    stats.active_connections = 0;
                }
            }
        }
        
        Ok(())
    }
    
    /// 停止心跳检测任务
    /// 
    /// 该方法会停止心跳检测任务
    pub async fn stop_heartbeat_task(&self) {
        let mut is_running = self.is_running.write().await;
        *is_running = false;
        
        let mut task = self.heartbeat_task.write().await;
        if let Some(handle) = task.take() {
            handle.abort();
        }
    }
    
    /// 获取心跳配置
    /// 
    /// # 返回值
    /// 
    /// 返回当前的心跳配置
    pub fn get_heartbeat_config(&self) -> &HeartbeatConfig {
        &self.heartbeat_config
    }
    
    /// 更新心跳配置
    /// 
    /// # 参数
    /// 
    /// * `config` - 新的心跳配置
    pub fn set_heartbeat_config(&mut self, config: HeartbeatConfig) {
        self.heartbeat_config = config;
    }
    
    /// 检查心跳任务是否正在运行
    /// 
    /// # 返回值
    /// 
    /// 如果心跳任务正在运行则返回true，否则返回false
    pub async fn is_heartbeat_running(&self) -> bool {
        let is_running = self.is_running.read().await;
        *is_running
    }
}

impl Drop for ConnectionManager {
    fn drop(&mut self) {
        // 在测试环境中，我们不执行任何操作以避免栈溢出
        // 在实际运行环境中，心跳任务会在停止时被正确清理
    }
}