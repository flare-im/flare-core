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
pub trait ServerConnectionManager: Send + Sync {
    /// 添加连接
    async fn add_connection(&self, connection: Arc<dyn ServerConnection>) -> Result<()>;
    
    /// 移除连接
    async fn remove_connection(&self, connection_id: &str, reason: Option< String>) -> Result<()>;
    
    /// 获取连接
    async fn get_connection(&self, connection_id: &str) -> Option<Arc<dyn ServerConnection>>;
    
    /// 获取所有连接
    async fn get_all_connections(&self) -> Vec<Arc<dyn ServerConnection>>;
    
    /// 获取连接数量
    async fn get_connection_count(&self) -> usize;
    
    /// 发送消息给指定链接
    async fn send_message(&self, connection_id: &str, message: Frame) -> Result<()>;
    
    /// 广播消息到所有连接
    async fn broadcast_message(&self, message: Frame) -> Result<usize>;
    
    /// 清理不活跃的连接
    async fn cleanup_inactive_connections(&self, timeout: Duration) -> usize;
    
    /// 获取连接统计信息
    async fn get_connection_stats(&self) -> ServerStats;
    
    /// 启动所有管理任务
    async fn start_tasks(&self);
    
    /// 停止所有管理任务
    async fn stop_tasks(&self);
    
    /// 检查任务是否正在运行
    async fn are_tasks_running(&self) -> bool;
}

/// 服务端统计信息
#[derive(Debug, Clone)]
pub struct ServerStats {
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