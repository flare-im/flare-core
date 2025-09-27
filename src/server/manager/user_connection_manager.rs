//! 用户连接管理器实现
//!
//! 管理用户与连接的关系，处理认证超时
//!
//! # 特点
//!
//! - 管理用户和连接ID的对照关系
//! - 处理连接的验证超时
//! - 认证完成后才算可用链接
//!
//! # 使用示例
//!
//! ```rust
//! use flare_core::server::{UserConnectionManager, ConnectionManager};
//! use std::sync::Arc;
//!
//! let base_manager = Arc::new(ConnectionManager::new());
//! let manager = UserConnectionManager::new(base_manager);
//! ```

use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{info, warn};
use tracing::log::debug;
use crate::common::{
    error::Result,
    connections::traits::ServerConnection,
    protocol::Frame,
    connections::types::{Platform, ConnectionState},
};

use super::{
    connection_manager::ConnectionManager,
    traits::ServerConnectionManager,
};

/// 用户连接信息
#[derive(Debug, Clone)]
pub struct UserConnectionInfo {
    /// 用户ID
    pub user_id: String,
    /// 平台信息
    pub platform: Platform,
    /// 设备ID
    pub device_id: Option<String>,
    /// 连接建立时间
    pub connected_at: Instant,
    /// 最后活跃时间
    pub last_activity: Instant,
    /// 是否已完成验证
    pub is_authenticated: bool,
}

/// 待验证连接信息
#[derive(Debug, Clone)]
pub struct PendingAuthInfo {
    /// 连接建立时间
    pub connected_at: Instant,
    /// 最后活跃时间
    pub last_activity: Instant,
    /// 平台信息
    pub platform: Option<Platform>,
    /// 设备ID
    pub device_id: Option<String>,
}

/// 用户连接管理器统计信息
#[derive(Debug, Clone)]
pub struct UserConnectionStats {
    /// 总用户数
    pub total_users: usize,
    /// 活跃用户数
    pub active_users: usize,
    /// 总连接数
    pub total_connections: usize,
    /// 活跃连接数
    pub active_connections: usize,
    /// 待验证连接数
    pub pending_auth_connections: usize,
    /// 总消息数
    pub total_messages: u64,
    /// 服务器启动时间
    pub started_at: Instant,
}

impl UserConnectionStats {
    /// 获取运行时间
    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }
}

/// 用户连接管理器
#[derive(Clone)]
pub struct UserConnectionManager {
    /// 基础连接管理器
    connection_manager: Arc<ConnectionManager>,
    /// 用户信息 (用户ID -> (平台 -> 连接信息))
    users: Arc<RwLock<HashMap<String, HashMap<Platform, UserConnectionInfo>>>>,
    /// 连接与用户映射 (连接ID -> (用户ID, 平台))
    connection_to_user: Arc<RwLock<HashMap<String, (String, Platform)>>>,
    /// 待验证连接 (连接ID -> 待验证信息)
    pending_auth_connections: Arc<RwLock<HashMap<String, PendingAuthInfo>>>,
    /// 统计信息
    stats: Arc<RwLock<UserConnectionStats>>,
    /// 认证超时时间
    auth_timeout: Duration,
}

impl UserConnectionManager {
    /// 创建新的用户连接管理器
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self::with_config(connection_manager, Duration::from_secs(30))
    }
    
    /// 创建带配置的用户连接管理器
    pub fn with_config(
        connection_manager: Arc<ConnectionManager>, 
        auth_timeout: Duration
    ) -> Self {
        Self {
            connection_manager,
            users: Arc::new(RwLock::new(HashMap::new())),
            connection_to_user: Arc::new(RwLock::new(HashMap::new())),
            pending_auth_connections: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(UserConnectionStats {
                total_users: 0,
                active_users: 0,
                total_connections: 0,
                active_connections: 0,
                pending_auth_connections: 0,
                total_messages: 0,
                started_at: Instant::now(),
            })),
            auth_timeout,
        }
    }
    
    /// 添加待验证连接
    pub async fn add_pending_connection(&self, connection_id: String) -> Result<()> {
        let now = Instant::now();
        
        // 添加到待验证连接列表
        {
            let mut pending = self.pending_auth_connections.write().await;
            pending.insert(connection_id.clone(), PendingAuthInfo {
                connected_at: now,
                last_activity: now,
                platform: None,
                device_id: None,
            });
        }
        
        // 更新统计信息
        {
            let mut stats = self.stats.write().await;
            stats.pending_auth_connections += 1;
        }
        
        info!("待验证连接已添加: {}", connection_id);
        Ok(())
    }

    /// 启动认证超时清理任务
    /// 
    /// 定期清理超时的待验证连接
    pub async fn start_auth_timeout_cleanup_task(&self) -> tokio::task::JoinHandle<()> {
        let manager = self.clone();
        tokio::spawn(async move {
            loop {
                // 每隔认证超时时间的一半执行一次清理
                tokio::time::sleep(manager.auth_timeout / 2).await;
                
                let removed_count = manager.cleanup_timeout_pending_connections().await;
                if removed_count > 0 {
                    info!("清理了 {} 个超时的待验证连接", removed_count);
                }
            }
        })
    }
    
    
    /// 完成连接认证并绑定用户
    pub async fn complete_authentication(
        &self, 
        connection_id: String,
        user_id: String,
        platform: Platform,
        device_id: Option<String>
    ) -> Result<()> {
        let now = Instant::now();
        
        // 从待验证列表中移除
        let pending_removed = {
            let mut pending = self.pending_auth_connections.write().await;
            pending.remove(&connection_id).is_some()
        };
        
        if !pending_removed {
            // 打印当前待认证连接列表用于调试
            let pending_connections: Vec<String> = {
                let pending = self.pending_auth_connections.read().await;
                pending.keys().cloned().collect()
            };
            debug!("待验证连接不存在: {}，当前待认证连接: {:?}", connection_id, pending_connections);
        }
        
        // 更新统计信息
        {
            let mut stats = self.stats.write().await;
            stats.pending_auth_connections -= 1;
        }
        
        // 获取连接实例
        let connection = if let Some(conn) = self.connection_manager.get_connection(&connection_id).await {
            conn
        } else {
            return Err(crate::common::error::FlareError::connection_failed(
                format!("连接不存在: {}", connection_id)
            ));
        };
        
        // 设置用户ID到连接
        connection.set_user_id(user_id.clone()).await;
        
        // 添加用户信息
        {
            let mut users = self.users.write().await;
            let user_platforms = users.entry(user_id.clone()).or_insert_with(HashMap::new);
            
            // 如果这是该用户的新平台，更新用户统计
            if !user_platforms.contains_key(&platform) {
                let mut stats = self.stats.write().await;
                stats.active_users += 1;
            }
            
            user_platforms.insert(platform.clone(), UserConnectionInfo {
                user_id: user_id.clone(),
                platform: platform.clone(),
                device_id: device_id.clone(),
                connected_at: now,
                last_activity: now,
                is_authenticated: true,
            });
        }
        
        // 建立连接与用户的映射
        {
            let mut connection_to_user = self.connection_to_user.write().await;
            connection_to_user.insert(connection_id.clone(), (user_id.clone(), platform.clone()));
        }
        
        // 更新统计信息
        {
            let mut stats = self.stats.write().await;
            stats.total_connections += 1;
            stats.active_connections += 1;
            stats.total_users += 1;
        }
        
        info!("连接认证完成并绑定用户: 连接={} 用户={} 平台={:?}", connection_id, user_id, platform);
        Ok(())
    }
    
    /// 移除连接
    pub async fn remove_connection(&self, connection_id: &str,reason:Option<String>) -> Result<()> {
        // 从基础连接管理器中移除连接
        self.connection_manager.remove_connection(connection_id,reason).await?;
        
        // 从用户映射中移除
        let user_info_opt = {
            let mut connection_to_user = self.connection_to_user.write().await;
            connection_to_user.remove(connection_id)
        };
        
        // 从用户信息中移除
        if let Some((user_id, platform)) = user_info_opt {
            let mut should_remove_user = false;
            
            {
                let mut users = self.users.write().await;
                if let Some(user_platforms) = users.get_mut(&user_id) {
                    user_platforms.remove(&platform);
                    
                    // 如果用户没有其他平台的连接了，移除用户
                    if user_platforms.is_empty() {
                        should_remove_user = true;
                    }
                }
            }
            
            if should_remove_user {
                let mut users = self.users.write().await;
                users.remove(&user_id);
                
                // 更新统计信息
                let mut stats = self.stats.write().await;
                stats.active_users -= 1;
            }
            
            // 更新统计信息 - 确保不会下溢
            let mut stats = self.stats.write().await;
            if stats.active_connections > 0 {
                stats.active_connections -= 1;
            }
            
            info!("用户连接已移除: 连接={} 用户={} 平台={:?}", connection_id, user_id, platform);
        } else {
            // 更新统计信息 - 确保不会下溢
            let mut stats = self.stats.write().await;
            if stats.active_connections > 0 {
                stats.active_connections -= 1;
            }
            
            info!("连接已移除: {}", connection_id);
        }
        
        Ok(())
    }
    
    /// 获取用户信息
    pub async fn get_user_info(&self, user_id: &str) -> Option<HashMap<Platform, UserConnectionInfo>> {
        let users = self.users.read().await;
        users.get(user_id).cloned()
    }
    
    /// 获取连接对应的用户ID和平台
    pub async fn get_user_by_connection(&self, connection_id: &str) -> Option<(String, Platform)> {
        let connection_to_user = self.connection_to_user.read().await;
        connection_to_user.get(connection_id).cloned()
    }
    
    /// 获取用户的所有连接
    pub async fn get_user_connections(&self, user_id: &str) -> Vec<Arc<dyn ServerConnection>> {
        // 通过连接到用户的映射来查找属于该用户的所有连接
        let user_connection_ids: Vec<String> = {
            let connection_to_user = self.connection_to_user.read().await;
            connection_to_user
                .iter()
                .filter(|(_, (uid, _))| uid == user_id)
                .map(|(connection_id, _)| connection_id.clone())
                .collect()
        };
        
        // 获取这些连接的实际连接对象
        let mut connections = Vec::new();
        for connection_id in user_connection_ids {
            if let Some(connection) = self.connection_manager.get_connection(&connection_id).await {
                connections.push(connection);
            }
        }
        
        connections
    }
    
    /// 移除用户的所有连接
    pub async fn remove_user_connections(&self, user_id: &str, reason:Option<String>) -> Result<usize> {
        let connection_ids: Vec<String> = {
            let connection_to_user = self.connection_to_user.read().await;
            connection_to_user
                .iter()
                .filter(|(_, (uid, _))| uid == user_id)
                .map(|(cid, _)| cid.clone())
                .collect()
        };
        
        let mut removed_count = 0;
        
        // 移除每个连接
        for connection_id in &connection_ids {
            if self.remove_connection(connection_id,reason.clone()).await.is_ok() {
                removed_count += 1;
            }
        }
        
        Ok(removed_count)
    }
    
    /// 清理超时的待验证连接
    pub async fn cleanup_timeout_pending_connections(&self) -> usize {
        let now = Instant::now();
        let mut timeout_pending_connections = Vec::new();
        
        // 检查待验证连接
        {
            let pending_connections: HashMap<String, PendingAuthInfo> = {
                let pending = self.pending_auth_connections.read().await;
                pending.clone()
            };
            
            for (connection_id, info) in pending_connections {
                if now.duration_since(info.connected_at) > self.auth_timeout {
                    timeout_pending_connections.push(connection_id);
                }
            }
        }
        
        let mut removed_count = 0;
        
        // 移除超时的待验证连接
        for connection_id in &timeout_pending_connections {
            // 从基础连接管理器中移除连接
            if self.connection_manager.remove_connection(connection_id,Some("Authentication timeout".to_string())).await.is_ok() {
                let pending_removed = {
                    let mut pending = self.pending_auth_connections.write().await;
                    pending.remove(connection_id).is_some()
                };
                
                if pending_removed {
                    removed_count += 1;
                    
                    // 更新统计信息
                    let mut stats = self.stats.write().await;
                    stats.pending_auth_connections -= 1;
                    
                    info!("超时待验证连接已移除: {}", connection_id);
                }
            }
        }
        
        removed_count
    }
    
 
    /// 主动断开未认证的连接（认证错误时调用）
    pub async fn disconnect_unauthenticated_connection(&self, connection_id: &str, reason: Option< String>) -> Result<()> {
        // 检查连接是否在待验证列表中
        let is_pending = {
            let pending = self.pending_auth_connections.read().await;
            pending.contains_key(connection_id)
        };
        
        if !is_pending {
            return Err(crate::common::error::FlareError::connection_failed(
                format!("连接不在待验证列表中: {}", connection_id)
            ));
        }

        
        // 从基础连接管理器中移除连接
        self.connection_manager.remove_connection(connection_id,reason).await?;
        
        // 从待验证连接列表中移除
        let pending_removed = {
            let mut pending = self.pending_auth_connections.write().await;
            pending.remove(connection_id).is_some()
        };
        
        if pending_removed {
            // 更新统计信息
            let mut stats = self.stats.write().await;
            stats.pending_auth_connections -= 1;
        }
        
        Ok(())
    }
    
    /// 获取统计信息
    pub async fn get_stats(&self) -> UserConnectionStats {
        let stats = self.stats.read().await;
        stats.clone()
    }
    
    /// 清空所有连接
    pub async fn clear_all(&self) {
        // 清空基础连接管理器
        let connections = self.connection_manager.get_all_connections().await;
        for connection in connections {
            let connection_id = connection.id().to_string();

            if let Err(e) = self.connection_manager.remove_connection(&connection_id, Some("Server shutdown".to_string())).await {
                warn!("移除连接失败: {} - 错误: {}", connection_id, e);
            }
        }


        let mut users = self.users.write().await;
        let mut connection_to_user = self.connection_to_user.write().await;
        let mut pending = self.pending_auth_connections.write().await;
        
        let user_count = users.len();
        let connection_count = connection_to_user.len();
        let pending_count = pending.len();
        
        users.clear();
        connection_to_user.clear();
        pending.clear();
        
        let mut stats = self.stats.write().await;
        stats.active_users = 0;
        stats.active_connections = 0;
        stats.pending_auth_connections = 0;
        
        info!("已清空所有连接，共移除 {} 个用户、{} 个连接和 {} 个待验证连接", 
              user_count, connection_count, pending_count);
    }
    
    /// 获取待验证连接数量
    pub async fn get_pending_auth_count(&self) -> usize {
        let pending = self.pending_auth_connections.read().await;
        pending.len()
    }
    
    /// 获取已认证用户数量
    pub async fn get_authenticated_user_count(&self) -> usize {
        let users = self.users.read().await;
        users.len()
    }
    
    /// 获取基础连接管理器
    pub fn get_connection_manager(&self) -> &Arc<ConnectionManager> {
        &self.connection_manager
    }
    
    /// 向指定用户发送消息
    pub async fn send_message_to_user(&self, user_id: &str, message: Frame) -> Result<usize> {
        let connections = self.get_user_connections(user_id).await;
        let mut sent_count = 0;
        
        for connection in connections {
            match connection.state() {
                ConnectionState::Connected | ConnectionState::Ready => {
                    match connection.send_message(message.clone()).await {
                        Ok(()) => {
                            sent_count += 1;
                            info!("消息已发送到用户连接: 用户={} 连接={}", user_id, connection.id());
                        }
                        Err(e) => {
                            warn!("向用户连接发送消息失败: 用户={} 连接={} - 错误: {}", 
                                  user_id, connection.id(), e);
                        }
                    }
                }
                _ => {
                    warn!("用户连接不健康，无法发送消息: 用户={} 连接={}", user_id, connection.id());
                }
            }
        }
        
        Ok(sent_count)
    }
    
    /// 广播消息到所有用户
    pub async fn broadcast_message_to_users(&self, message: Frame) -> Result<usize> {
        let connections = self.connection_manager.get_all_connections().await;
        let mut total_sent = 0;
        
        for connection in connections {
            match connection.state() {
                ConnectionState::Connected | ConnectionState::Ready => {
                    match connection.send_message(message.clone()).await {
                        Ok(()) => {
                            total_sent += 1;
                            info!("消息已广播到连接: {}", connection.id());
                        }
                        Err(e) => {
                            warn!("向连接广播消息失败: {} - 错误: {}", connection.id(), e);
                        }
                    }
                }
                _ => {
                    warn!("连接不健康，无法广播消息: {}", connection.id());
                }
            }
        }
        
        Ok(total_sent)
    }
    
    /// 处理认证结果
    /// 
    /// 标记用户验证通过还是验证失败，支持验证等待期间多次验证
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `user_id` - 用户ID
    /// * `platform` - 平台信息
    /// * `success` - 认证是否成功
    /// * `error_message` - 错误消息（认证失败时提供）
    /// * `user_info` - 用户信息（认证成功时提供）
    pub async fn process_authentication_result(
        &self,
        connection_id: String,
        user_id: String,
        platform: String,
        success: bool,
        error_message: Option<String>,
        user_info: Option<Vec<u8>>,
    ) -> Result<()> {
        // 解析平台信息
        let platform_enum = crate::common::connections::enums::Platform::from_str(&platform);
        debug!("处理认证结果: 连接={} 用户={} 平台={} 认证结果={}",connection_id, user_id, platform, success);
        if success {
            // 认证成功，完成连接认证并绑定用户
            self.complete_authentication(
                connection_id.clone(),
                user_id.clone(),
                platform_enum.clone(),
                None, // device_id
            ).await?;
            
            // 获取连接并发送认证成功响应
            if let Some(connection) = self.connection_manager.get_connection(&connection_id).await {
                // 调用连接的authenticate方法来设置连接状态为Ready
                connection.authenticate(
                    true, // success
                    platform_enum,
                    user_id,
                    user_info.clone(), // info
                    None, // reason
                ).await?;
                
                // 发送认证成功响应消息
                let message_id = crate::common::protocol::factory::FrameFactory::generate_message_id();
                let auth_response = crate::common::protocol::factory::FrameFactory::create_auth_response_frame(
                    message_id, // message_id
                    true,  // success
                    200,  // status
                    user_info,  // user_info
                    None   // error_message
                ).unwrap();
                
                if let Err(e) = connection.send_message(auth_response).await {
                    warn!("发送认证成功响应失败: {} - 错误: {}", connection_id, e);
                }
            }
        } else {
            // 认证失败，断开连接
            if let Some(connection) = self.connection_manager.get_connection(&connection_id).await {
                // 调用连接的authenticate方法来设置连接状态为认证失败
                connection.authenticate(
                    false, // success
                    platform_enum,
                    user_id,
                    None, // info
                    error_message.clone(), // reason
                ).await?;
                
                // 发送认证失败响应消息
                let message_id = crate::common::protocol::factory::FrameFactory::generate_message_id();
                let auth_response = crate::common::protocol::factory::FrameFactory::create_auth_response_frame(
                    message_id, // message_id
                    false,  // success
                    401,  // status
                    None,  // user_info
                    error_message.clone()   // error_message
                ).unwrap();
                
                if let Err(e) = connection.send_message(auth_response).await {
                    warn!("发送认证失败响应失败: {} - 错误: {}", connection_id, e);
                }
                
                // 断开连接
                self.disconnect_unauthenticated_connection(
                    &connection_id, 
                    Some(error_message.unwrap_or_else(|| "Authentication failed".to_string()))
                ).await?;
            }
        }
        
        Ok(())
    }
}

#[async_trait::async_trait]
impl ServerConnectionManager for UserConnectionManager {
    /// 添加连接
    async fn add_connection(&self, connection: Arc<dyn ServerConnection>) -> Result<()> {
        // 先添加到基础连接管理器
        self.connection_manager.add_connection(connection.clone()).await?;
        debug!("添加连接: {}", connection.id());
        
        // 添加到待验证连接列表
        self.add_pending_connection(connection.id().to_string()).await?;
        
        Ok(())
    }
    
    /// 移除连接
    async fn remove_connection(&self, connection_id: &str, reason: Option<String>) -> Result<()> {
        // 使用我们自己的移除连接方法，它会处理用户关系
        self.remove_connection(connection_id, reason).await
    }
    
    /// 获取连接
    async fn get_connection(&self, connection_id: &str) -> Option<Arc<dyn ServerConnection>> {
        self.connection_manager.get_connection(connection_id).await
    }
    
    /// 获取所有连接
    async fn get_all_connections(&self) -> Vec<Arc<dyn ServerConnection>> {
        self.connection_manager.get_all_connections().await
    }
    
    /// 获取连接数量
    async fn get_connection_count(&self) -> usize {
        self.connection_manager.get_connection_count().await
    }
    
    /// 发送消息给指定链接
    async fn send_message(&self, connection_id: &str, message: Frame) -> Result<()> {
        self.connection_manager.send_message(connection_id, message).await
    }
    
    /// 广播消息到所有连接
    async fn broadcast_message(&self, message: Frame) -> Result<usize> {
        self.connection_manager.broadcast_message(message).await
    }
    
    /// 清理不活跃的连接
    async fn cleanup_inactive_connections(&self, timeout: Duration) -> usize {
        self.connection_manager.cleanup_inactive_connections(timeout).await
    }
    
    /// 获取连接统计信息
    async fn get_connection_stats(&self) -> super::traits::ServerStats {
        self.connection_manager.get_connection_stats().await
    }
}
