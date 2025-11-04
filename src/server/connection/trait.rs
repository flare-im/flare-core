//! 服务端连接管理器抽象
//! 
//! 定义服务端连接管理的标准接口，支持用户自定义实现
//! 默认实现使用 ConnectionManager

use crate::common::error::Result;
use crate::transport::connection::Connection;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;

/// 连接信息
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    /// 连接 ID（唯一标识符）
    pub connection_id: String,
    /// 用户 ID（如果已认证）
    pub user_id: Option<String>,
    /// 创建时间戳（Unix 时间戳，秒）
    pub created_at: u64,
    /// 最后活跃时间戳（Unix 时间戳，秒）
    pub last_active: u64,
    /// 连接元数据
    pub metadata: std::collections::HashMap<String, String>,
}

/// 连接管理器抽象 trait
/// 
/// 实现此 trait 以提供自定义的连接管理逻辑
/// 例如：基于 Redis 的分布式连接管理、基于数据库的持久化等
#[async_trait]
pub trait ConnectionManagerTrait: Send + Sync {
    /// 添加连接
    async fn add_connection(
        &self,
        connection_id: String,
        connection: Arc<Mutex<Box<dyn Connection>>>,
        user_id: Option<String>,
    ) -> Result<()>;

    /// 移除连接
    async fn remove_connection(&self, connection_id: &str) -> Result<()>;

    /// 获取连接
    async fn get_connection(
        &self,
        connection_id: &str,
    ) -> Option<(Arc<Mutex<Box<dyn Connection>>>, ConnectionInfo)>;

    /// 获取用户的所有连接 ID
    async fn get_user_connections(&self, user_id: &str) -> Vec<String>;

    /// 绑定用户到连接
    async fn bind_user(&self, connection_id: &str, user_id: String) -> Result<()>;

    /// 更新连接的最后活跃时间
    async fn update_connection_active(&self, connection_id: &str) -> Result<()>;

    /// 获取所有连接 ID
    async fn list_connections(&self) -> Vec<String>;

    /// 获取连接总数
    async fn connection_count(&self) -> usize;

    /// 清理超时连接
    async fn cleanup_timeout_connections(&self, timeout: Duration) -> Vec<String>;

    /// 向指定连接发送数据
    async fn send_to_connection(&self, connection_id: &str, data: &[u8]) -> Result<()>;

    /// 向指定用户的所有连接发送数据
    async fn send_to_user(&self, user_id: &str, data: &[u8]) -> Result<()>;

    /// 广播消息到所有连接
    async fn broadcast(&self, data: &[u8]) -> Result<()>;

    /// 广播消息到所有连接，排除指定连接
    async fn broadcast_except(&self, data: &[u8], exclude_connection_id: &str) -> Result<()>;
}

/// 连接统计信息
#[derive(Debug, Clone)]
pub struct ConnectionStats {
    /// 总连接数
    pub total_connections: usize,
    /// 总用户数
    pub total_users: usize,
} 