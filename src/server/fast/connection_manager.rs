//! 带认证功能的连接管理器

use crate::common::connections::traits::ServerConnection;
use crate::common::connections::enums::ConnectionState;
use crate::common::error::FlareError;
use crate::server::manager::traits::{ConnectionManager as BaseConnectionManager, AggregatedStats, BroadcastStats, ConnectionInfo};
use crate::server::fast::auth::AuthProvider;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use tracing::{info, warn, debug};

/// 连接认证状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthState {
    /// 等待认证
    Pending,
    /// 认证中
    Authenticating,
    /// 认证成功
    Authenticated,
    /// 认证失败
    Failed,
}

/// 带认证信息的连接条目
pub struct AuthenticatedConnection {
    /// 连接实例
    pub connection: Arc<dyn ServerConnection>,
    /// 认证状态
    pub auth_state: AuthState,
    /// 用户ID（认证成功后设置）
    pub user_id: Option<String>,
    /// 连接创建时间
    pub created_at: u64,
    /// 等待认证的超时时间（毫秒）
    pub auth_timeout_ms: u64,
}

impl AuthenticatedConnection {
    /// 创建新的认证连接条目
    pub fn new(connection: Arc<dyn ServerConnection>, auth_timeout_ms: u64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
            
        Self {
            connection,
            auth_state: AuthState::Pending,
            user_id: None,
            created_at: now,
            auth_timeout_ms,
        }
    }
    
    /// 检查是否认证超时
    pub fn is_auth_timeout(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
            
        now > self.created_at + self.auth_timeout_ms
    }
    
    /// 设置认证状态
    pub fn set_auth_state(&mut self, state: AuthState) {
        self.auth_state = state;
    }
    
    /// 设置用户ID
    pub fn set_user_id(&mut self, user_id: String) {
        self.user_id = Some(user_id);
        self.auth_state = AuthState::Authenticated;
    }
}

/// 带认证功能的连接管理器
pub struct AuthenticatedConnectionManager {
    /// 按连接ID索引的连接映射
    connections: DashMap<String, AuthenticatedConnection>,
    /// 认证提供者
    auth_provider: Arc<dyn AuthProvider>,
    /// 默认认证超时时间（毫秒）
    default_auth_timeout_ms: u64,
}

impl AuthenticatedConnectionManager {
    /// 创建新的带认证功能的连接管理器
    pub fn new(auth_provider: Arc<dyn AuthProvider>, default_auth_timeout_ms: u64) -> Self {
        Self {
            connections: DashMap::new(),
            auth_provider,
            default_auth_timeout_ms,
        }
    }
    
    /// 获取认证提供者
    pub fn auth_provider(&self) -> &Arc<dyn AuthProvider> {
        &self.auth_provider
    }
    
    /// 验证连接是否已认证
    pub fn is_connection_authenticated(&self, connection_id: &str) -> bool {
        if let Some(conn) = self.connections.get(connection_id) {
            conn.auth_state == AuthState::Authenticated
        } else {
            false
        }
    }
    
    /// 获取连接的用户ID
    pub fn get_connection_user_id(&self, connection_id: &str) -> Option<String> {
        if let Some(conn) = self.connections.get(connection_id) {
            conn.user_id.clone()
        } else {
            None
        }
    }
    
    /// 认证连接
    pub fn authenticate_connection(&self, connection_id: &str, token: &str) -> Result<String, FlareError> {
        // 验证令牌
        let user_id = self.auth_provider.validate_token(token)?;
        
        // 更新连接的认证状态
        if let Some(mut conn) = self.connections.get_mut(connection_id) {
            conn.set_user_id(user_id.clone());
            info!("连接 {} 认证成功，用户ID: {}", connection_id, user_id);
        } else {
            return Err(FlareError::authentication_failed("连接不存在".to_string()));
        }
        
        Ok(user_id)
    }
    
    /// 清理未认证的超时连接
    pub fn cleanup_unauthenticated_connections(&self) -> Result<(), FlareError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
            
        let mut to_remove: Vec<String> = Vec::new();
        
        // 查找未认证且超时的连接
        for item in self.connections.iter() {
            let id = item.key();
            let conn = item.value();
            
            // 只处理未认证的连接
            if conn.auth_state != AuthState::Authenticated && 
               now > conn.created_at + conn.auth_timeout_ms {
                to_remove.push(id.clone());
            }
        }
        
        // 移除超时的未认证连接
        let removed_count = to_remove.len();
        for id in to_remove {
            if let Some(conn) = self.remove_connection(&id) {
                warn!("清理未认证超时连接: ID={}, 超时时间={}ms", id, self.default_auth_timeout_ms);
                
                // 关闭连接
                if let Err(e) = conn.close(Some("authentication timeout".into())) {
                    warn!("关闭未认证超时连接时出错: ID={}, 错误={:?}", id, e);
                }
            }
        }
        
        if removed_count > 0 {
            info!("共清理 {} 个未认证超时连接", removed_count);
        }
        
        Ok(())
    }
}

#[async_trait::async_trait]
impl BaseConnectionManager for AuthenticatedConnectionManager {
    /// 添加连接
    fn add_connection(&self, conn: Arc<dyn ServerConnection>) -> bool {
        let id = conn.id();
        
        // 记录日志
        info!("添加新连接: ID={}", id);
        debug!("连接状态: {:?}", conn.state());
        
        // 创建认证连接条目
        let auth_conn = AuthenticatedConnection::new(conn, self.default_auth_timeout_ms);
        
        // 添加到连接映射
        let is_new = !self.connections.contains_key(&id);
        self.connections.insert(id, auth_conn);
        
        is_new
    }

    /// 移除连接
    fn remove_connection(&self, id: &str) -> Option<Arc<dyn ServerConnection>> {
        // 从连接映射中移除
        let conn = self.connections.remove(id).map(|(_, v)| v.connection);
        
        if conn.is_some() {
            info!("移除连接: ID={}", id);
        }
        
        conn
    }

    /// 获取连接
    fn get_connection(&self, id: &str) -> Option<Arc<dyn ServerConnection>> {
        self.connections.get(id).map(|r| Arc::clone(&r.connection))
    }

    /// 获取聚合统计信息
    fn stats_snapshot(&self) -> AggregatedStats {
        let total = self.connections.len();
        
        let mut active = 0;
        let mut authenticated = 0;
        let mut pending_auth = 0;
        let mut failed_auth = 0;
        let mut total_quality = 0u64;
        let mut quality_count = 0u64;
        let mut by_state: HashMap<ConnectionState, usize> = HashMap::with_capacity(16);
        
        // 遍历所有连接收集统计信息
        for item in self.connections.iter() {
            let conn = item.value();
            let stats = conn.connection.stats();
            let state = conn.connection.state().clone();
            
            // 统计各状态连接数
            *by_state.entry(state.clone()).or_insert(0) += 1;
            
            // 检查连接是否活跃
            if matches!(state, ConnectionState::Connected) {
                active += 1;
            }
            
            // 统计认证状态
            match conn.auth_state {
                AuthState::Authenticated => authenticated += 1,
                AuthState::Pending => pending_auth += 1,
                AuthState::Failed => failed_auth += 1,
                AuthState::Authenticating => pending_auth += 1, // 认证中也算作等待认证
            }
            
            // 统计连接质量
            if let Some(quality) = stats.quality {
                total_quality += quality as u64;
                quality_count += 1;
            }
        }
        
        let avg_quality = if quality_count > 0 {
            Some((total_quality / quality_count) as u8)
        } else {
            None
        };
        
        // 添加认证状态统计到by_state
        by_state.insert(ConnectionState::Custom("Authenticated".to_string()), authenticated);
        by_state.insert(ConnectionState::Custom("PendingAuth".to_string()), pending_auth);
        by_state.insert(ConnectionState::Custom("FailedAuth".to_string()), failed_auth);
        
        AggregatedStats { 
            total, 
            active, 
            failed: failed_auth, 
            reconnecting: 0, 
            msg_send_rate: None, 
            msg_recv_rate: None, 
            avg_quality,
            by_state,
        }
    }

    /// 清理过期连接
    fn cleanup(&self, heartbeat_monitor_timeout_ms: u64) -> Result<(), FlareError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
            
        let mut to_remove: Vec<String> = Vec::new();
        
        // 查找过期连接
        for item in self.connections.iter() {
            let id = item.key();
            let conn = item.value();
            let last_activity = conn.connection.last_activity_epoch_ms();
            
            if now.saturating_sub(last_activity) > heartbeat_monitor_timeout_ms {
                to_remove.push(id.clone());
            }
        }
        
        // 移除过期连接
        let removed_count = to_remove.len();
        for id in to_remove {
            if let Some(conn) = self.remove_connection(&id) {
                warn!("清理过期连接: ID={}, 超时时间={}ms", id, heartbeat_monitor_timeout_ms);
                
                // 关闭连接
                if let Err(e) = conn.close(Some("heartbeat monitor timeout".into())) {
                    warn!("关闭过期连接时出错: ID={}, 错误={:?}", id, e);
                }
            }
        }
        
        if removed_count > 0 {
            info!("共清理 {} 个过期连接", removed_count);
        }
        
        // 清理未认证的超时连接
        self.cleanup_unauthenticated_connections()?;
        
        Ok(())
    }
    
    /// 获取连接总数
    fn connection_count(&self) -> usize {
        self.connections.len()
    }
    
    /// 获取所有连接ID列表
    fn all_connection_ids(&self) -> Vec<String> {
        self.connections.iter().map(|item| item.key().clone()).collect()
    }
    
    /// 广播消息给所有连接
    fn broadcast_message(&self, frame: crate::common::protocol::frame::Frame) -> Result<BroadcastStats, FlareError> {
        let mut stats = BroadcastStats::default();
        
        for item in self.connections.iter() {
            let conn = item.value();
            // 只向已认证的连接广播消息
            if conn.auth_state == AuthState::Authenticated {
                match conn.connection.send_message(frame.clone()) {
                    Ok(()) => {
                        stats.success += 1;
                    }
                    Err(e) => {
                        stats.failed += 1;
                        warn!("广播消息失败: 连接ID={}, 错误={:?}", item.key(), e);
                    }
                }
            }
        }
        
        Ok(stats)
    }
    
    /// 按连接状态过滤连接
    fn get_connections_by_state(&self, state: ConnectionState) -> Vec<String> {
        self.connections
            .iter()
            .filter(|item| {
                let conn = item.value();
                // 对于自定义认证状态的特殊处理
                match &state {
                    ConnectionState::Custom(custom_state) => {
                        match custom_state.as_str() {
                            "Authenticated" => conn.auth_state == AuthState::Authenticated,
                            "PendingAuth" => conn.auth_state == AuthState::Pending || conn.auth_state == AuthState::Authenticating,
                            "FailedAuth" => conn.auth_state == AuthState::Failed,
                            _ => conn.connection.state().clone() == state,
                        }
                    }
                    _ => conn.connection.state().clone() == state,
                }
            })
            .map(|item| item.key().clone())
            .collect()
    }
    
    /// 获取连接的详细信息
    fn get_connection_info(&self, id: &str) -> Option<ConnectionInfo> {
        self.connections.get(id).map(|item| {
            let conn = item.value();
            let stats = conn.connection.stats();
            
            ConnectionInfo {
                id: id.to_string(),
                state: conn.connection.state().clone(),
                user_id: conn.user_id.clone(),
                established_at: stats.established_epoch_ms,
                last_activity_at: stats.last_activity_epoch_ms,
                quality: stats.quality,
                messages_sent: stats.messages_sent,
                messages_received: stats.messages_received,
            }
        })
    }
    
    /// 获取所有连接的详细信息
    fn all_connection_info(&self) -> Vec<ConnectionInfo> {
        self.connections
            .iter()
            .map(|item| {
                let conn = item.value();
                let stats = conn.connection.stats();
                
                ConnectionInfo {
                    id: item.key().clone(),
                    state: conn.connection.state().clone(),
                    user_id: conn.user_id.clone(),
                    established_at: stats.established_epoch_ms,
                    last_activity_at: stats.last_activity_epoch_ms,
                    quality: stats.quality,
                    messages_sent: stats.messages_sent,
                    messages_received: stats.messages_received,
                }
            })
            .collect()
    }
    
    /// 获取事件处理器适配器
    async fn get_event_handler_adapter(&self) -> crate::server::events::handler::EventHandlerAdapter {
        // AuthenticatedConnectionManager不直接管理事件处理器
        // 返回一个空的事件处理器适配器
        crate::server::events::handler::EventHandlerAdapter::new()
    }
    
    /// 设置事件处理器
    async fn set_event_handler(&self, _handler: Arc<dyn crate::server::events::handler::EnhancedEventHandler>) {
        // AuthenticatedConnectionManager不直接管理事件处理器
        // 这个方法是空实现
    }
}
