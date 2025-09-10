//! 连接管理器trait定义
//!
//! 定义连接管理器的统一接口

use std::sync::Arc;
use std::time::Duration;

use crate::common::{
    error::Result,
    connections::traits::ServerConnection,
    protocol::Frame,
};

/// 连接管理器trait
#[async_trait::async_trait]
pub trait ConnectionManager: Send + Sync {
    /// 添加连接
    async fn add_connection(&self, connection: Arc<dyn ServerConnection>) -> Result<()>;
    
    /// 移除连接
    async fn remove_connection(&self, connection_id: &str) -> Result<()>;
    
    /// 获取连接
    async fn get_connection(&self, connection_id: &str) -> Option<Arc<dyn ServerConnection>>;
    
    /// 获取所有连接
    async fn get_all_connections(&self) -> Vec<Arc<dyn ServerConnection>>;
    
    /// 获取连接数量
    async fn get_connection_count(&self) -> usize;
    
    /// 向指定连接发送消息
    async fn send_message_to_connection(&self, connection_id: &str, message: Frame) -> Result<()>;
    
    /// 广播消息到所有连接
    async fn broadcast_message(&self, message: Frame) -> Result<usize>;
    
    /// 清理不活跃的连接
    async fn cleanup_inactive_connections(&self, timeout: Duration) -> usize;
    
    /// 获取统计信息
    async fn get_stats(&self) -> ManagerStats;
    
    /// 清空所有连接
    async fn clear_all(&self);
    
    /// 检查是否需要清理
    async fn should_cleanup(&self) -> bool;
    
    /// 注册到心跳管理器
    async fn register_heartbeat_manager(&self, heartbeat_manager: Arc<super::heartbeat_manager::HeartbeatManager<dyn ServerConnection>>);
}

/// 管理器统计信息
#[derive(Debug, Clone)]
pub struct ManagerStats {
    /// 总连接数
    pub total_connections: usize,
    /// 活跃连接数
    pub active_connections: usize,
    /// 总消息数
    pub total_messages: u64,
    /// 平均连接质量
    pub average_quality: u8,
    /// 服务器启动时间
    pub uptime: Duration,
}