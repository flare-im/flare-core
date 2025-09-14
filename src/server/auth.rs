//! 认证管理器
//!
//! 负责处理连接的身份认证

use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{info, warn, debug};

use crate::common::{
    error::Result,
    connections::traits::ServerConnection,
    connections::enums::Platform, // 使用统一的Platform类型
};

/// 认证状态
#[derive(Debug, Clone, PartialEq)]
pub enum AuthStatus {
    /// 等待认证
    Pending,
    /// 认证成功
    Authenticated(String), // 用户ID
    /// 认证失败
    Failed,
    /// 认证超时
    Timeout,
}

// 移除重复的Platform定义，使用crate::common::connections::enums::Platform

/// 认证信息
#[derive(Debug, Clone)]
pub struct AuthInfo {
    /// 连接ID
    pub connection_id: String,
    /// 认证状态
    pub status: AuthStatus,
    /// 连接时间
    pub connected_at: Instant,
    /// 最后活动时间
    pub last_activity: Instant,
    /// 用户ID（认证成功后）
    pub user_id: Option<String>,
    /// 平台信息
    pub platform: Option<Platform>,
    /// 设备ID
    pub device_id: Option<String>,
    /// 应用版本
    pub app_version: Option<String>,
}

/// 认证处理器 trait
#[async_trait::async_trait]
pub trait AuthHandler: Send + Sync {
    /// 验证认证信息
    ///
    /// # 参数
    ///
    /// * `auth_data` - 认证数据
    ///
    /// # 返回值
    ///
    /// 返回用户ID或错误
    async fn authenticate(&self, auth_data: Vec<u8>) -> Result<String>;
    
    /// 验证认证信息（带平台信息）
    ///
    /// # 参数
    ///
    /// * `auth_data` - 认证数据
    /// * `platform` - 平台信息
    /// * `device_id` - 设备ID
    /// * `app_version` - 应用版本
    ///
    /// # 返回值
    ///
    /// 返回用户ID或错误
    async fn authenticate_with_platform(
        &self, 
        auth_data: Vec<u8>, 
        _platform: Option<Platform>,
        _device_id: Option<String>,
        _app_version: Option<String>,
    ) -> Result<String> {
        // 简单认证处理器忽略平台信息
        self.authenticate(auth_data).await
    }
}

/// 简单的认证处理器实现
pub struct SimpleAuthHandler {
    /// 用户凭证映射 (token -> user_id)
    users: Arc<RwLock<HashMap<String, String>>>,
}

impl SimpleAuthHandler {
    /// 创建新的简单认证处理器
    pub fn new() -> Self {
        let users = Arc::new(RwLock::new(HashMap::new()));
        Self { users }
    }

    /// 添加用户凭证
    pub async fn add_user(&self, token: String, user_id: String) {
        let mut users = self.users.write().await;
        users.insert(token, user_id);
    }
}

#[async_trait::async_trait]
impl AuthHandler for SimpleAuthHandler {
    async fn authenticate(&self, auth_data: Vec<u8>) -> Result<String> {
        let token = String::from_utf8(auth_data)
            .map_err(|e| crate::common::error::FlareError::general_error(format!("无效的认证令牌: {}", e)))?;
        
        let users = self.users.read().await;
        if let Some(user_id) = users.get(&token) {
            Ok(user_id.clone())
        } else {
            Err(crate::common::error::FlareError::authentication_expired("无效的认证令牌".to_string()))
        }
    }
    
    async fn authenticate_with_platform(
        &self, 
        auth_data: Vec<u8>, 
        _platform: Option<Platform>,
        _device_id: Option<String>,
        _app_version: Option<String>,
    ) -> Result<String> {
        // 简单认证处理器忽略平台信息
        self.authenticate(auth_data).await
    }
}

/// 认证管理器
pub struct AuthManager {
    /// 认证处理器
    auth_handler: Arc<dyn AuthHandler>,
    /// 待认证连接 (connection_id -> AuthInfo)
    pending_connections: Arc<RwLock<HashMap<String, AuthInfo>>>,
    /// 已认证用户 (user_id -> platform -> connection_id)
    authenticated_users: Arc<RwLock<HashMap<String, HashMap<Platform, String>>>>,
    /// 认证超时时间
    auth_timeout: Duration,
}

impl AuthManager {
    /// 创建新的认证管理器
    pub fn new(auth_handler: Arc<dyn AuthHandler>, auth_timeout: Duration) -> Self {
        Self {
            auth_handler,
            pending_connections: Arc::new(RwLock::new(HashMap::new())),
            authenticated_users: Arc::new(RwLock::new(HashMap::new())),
            auth_timeout,
        }
    }

    /// 添加待认证连接
    pub async fn add_pending_connection(&self, connection: Arc<dyn ServerConnection>) {
        let connection_id = connection.id().to_string(); // 修复：使用id()而不是get_id()
        let auth_info = AuthInfo {
            connection_id: connection_id.clone(),
            status: AuthStatus::Pending,
            connected_at: Instant::now(),
            last_activity: Instant::now(),
            user_id: None,
            platform: None,
            device_id: None,
            app_version: None,
        };

        let mut pending = self.pending_connections.write().await;
        pending.insert(connection_id, auth_info);
        debug!("连接已添加到待认证列表: {}", connection.id()); // 修复：使用id()而不是get_id()
    }
    
    /// 设置连接的平台信息
    pub async fn set_connection_platform(
        &self, 
        connection_id: &str, 
        platform: Platform, 
        device_id: Option<String>, 
        app_version: Option<String>
    ) {
        let mut pending = self.pending_connections.write().await;
        if let Some(auth_info) = pending.get_mut(connection_id) {
            auth_info.platform = Some(platform);
            auth_info.device_id = device_id;
            auth_info.app_version = app_version;
            auth_info.last_activity = Instant::now();
        }
    }

    /// 处理认证消息
    pub async fn handle_auth_message(&self, connection_id: &str, auth_data: Vec<u8>) -> Result<AuthStatus> {
        // 更新活动时间
        {
            let mut pending = self.pending_connections.write().await;
            if let Some(auth_info) = pending.get_mut(connection_id) {
                auth_info.last_activity = Instant::now();
            } else {
                // 连接不存在，返回错误
                return Err(crate::common::error::FlareError::authentication_expired(
                    "连接不存在或已超时".to_string()
                ));
            }
        }

        // 获取平台信息
        let (platform, device_id, app_version) = {
            let pending = self.pending_connections.read().await;
            if let Some(auth_info) = pending.get(connection_id) {
                (auth_info.platform.clone(), auth_info.device_id.clone(), auth_info.app_version.clone())
            } else {
                (None, None, None)
            }
        };

        // 尝试认证
        let auth_result = self.auth_handler.authenticate_with_platform(
            auth_data, 
            platform.clone(), 
            device_id.clone(), 
            app_version.clone()
        ).await;

        match auth_result {
            Ok(user_id) => {
                // 认证成功
                let mut pending = self.pending_connections.write().await;
                if let Some(auth_info) = pending.get_mut(connection_id) {
                    auth_info.status = AuthStatus::Authenticated(user_id.clone());
                    auth_info.user_id = Some(user_id.clone());
                    info!("连接认证成功: {} -> 用户: {} 平台: {:?}", connection_id, user_id, platform);
                }
                
                // 记录已认证用户
                if let Some(platform) = platform {
                    let mut authenticated = self.authenticated_users.write().await;
                    authenticated.entry(user_id.clone())
                        .or_insert_with(HashMap::new)
                        .insert(platform, connection_id.to_string());
                }
                
                Ok(AuthStatus::Authenticated(user_id))
            }
            Err(e) => {
                // 认证失败
                let mut pending = self.pending_connections.write().await;
                if let Some(auth_info) = pending.get_mut(connection_id) {
                    auth_info.status = AuthStatus::Failed;
                    warn!("连接认证失败: {} - 错误: {}", connection_id, e);
                }
                
                // 记录认证失败事件
                warn!("认证失败: 连接 {} 认证失败，错误: {}", connection_id, e);
                Ok(AuthStatus::Failed)
            }
        }
    }

    /// 检查连接是否已认证
    pub async fn is_authenticated(&self, connection_id: &str) -> Option<AuthInfo> {
        let pending = self.pending_connections.read().await;
        pending.get(connection_id).cloned()
    }

    /// 移除已认证的连接
    pub async fn remove_authenticated(&self, connection_id: &str) -> Option<AuthInfo> {
        let mut pending = self.pending_connections.write().await;
        let auth_info = pending.remove(connection_id);
        
        // 从已认证用户中移除
        if let Some(ref auth_info) = auth_info {
            if let (Some(user_id), Some(platform)) = (&auth_info.user_id, &auth_info.platform) {
                let mut authenticated = self.authenticated_users.write().await;
                if let Some(platform_map) = authenticated.get_mut(user_id) {
                    platform_map.remove(platform);
                    if platform_map.is_empty() {
                        authenticated.remove(user_id);
                    }
                }
            }
        }
        
        auth_info
    }
    
    /// 获取用户在指定平台的连接
    pub async fn get_user_connection_on_platform(&self, user_id: &str, platform: &Platform) -> Option<String> {
        let authenticated = self.authenticated_users.read().await;
        authenticated.get(user_id)
            .and_then(|platform_map| platform_map.get(platform))
            .cloned()
    }
    
    /// 获取用户的所有在线平台
    pub async fn get_user_online_platforms(&self, user_id: &str) -> Vec<Platform> {
        let authenticated = self.authenticated_users.read().await;
        authenticated.get(user_id)
            .map(|platform_map| platform_map.keys().cloned().collect())
            .unwrap_or_else(Vec::new)
    }
    
    /// 强制用户在指定平台下线
    pub async fn force_logout_platform(&self, user_id: &str, platform: &Platform) -> Option<String> {
        let mut authenticated = self.authenticated_users.write().await;
        authenticated.get_mut(user_id)
            .and_then(|platform_map| platform_map.remove(platform))
    }

    /// 清理超时的连接
    pub async fn cleanup_timeout_connections(&self) -> usize {
        let now = Instant::now();
        let mut timeout_connections = Vec::new();

        {
            let pending = self.pending_connections.read().await;
            for (connection_id, auth_info) in pending.iter() {
                if now.duration_since(auth_info.last_activity) > self.auth_timeout {
                    timeout_connections.push(connection_id.clone());
                }
            }
        }

        let removed_count = {
            let mut pending = self.pending_connections.write().await;
            let mut count = 0;
            for connection_id in &timeout_connections {
                if pending.remove(connection_id).is_some() {
                    count += 1;
                }
            }
            count
        };

        if removed_count > 0 {
            info!("清理超时认证连接: {} 个", removed_count);
        }

        removed_count
    }

    /// 获取待认证连接数量
    pub async fn get_pending_count(&self) -> usize {
        let pending = self.pending_connections.read().await;
        pending.len()
    }
    
    /// 获取已认证用户数量
    pub async fn get_authenticated_user_count(&self) -> usize {
        let authenticated = self.authenticated_users.read().await;
        authenticated.len()
    }
}