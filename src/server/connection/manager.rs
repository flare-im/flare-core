//! 连接管理器模块
//! 
//! 提供连接的统一管理、存储和查询功能
//! 支持按连接 ID、用户 ID 等方式管理连接

use crate::common::error::{FlareError, Result};
use crate::server::connection::r#trait::{ConnectionManagerTrait, ConnectionStats as TraitConnectionStats};
use crate::transport::connection::Connection;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use std::time::{Duration, Instant};

/// 连接信息
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    /// 连接 ID（唯一标识符）
    pub connection_id: String,
    /// 用户 ID（如果已认证）
    pub user_id: Option<String>,
    /// 创建时间
    pub created_at: Instant,
    /// 最后活跃时间
    pub last_active: Instant,
    /// 连接元数据
    pub metadata: HashMap<String, String>,
    /// 设备信息（如果已提供）
    pub device_info: Option<crate::common::device::DeviceInfo>,
    /// 序列化格式（由客户端协商决定，默认 JSON）
    pub serialization_format: crate::common::protocol::SerializationFormat,
    /// 压缩算法（由客户端协商决定，默认不压缩）
    pub compression: crate::common::compression::CompressionAlgorithm,
}

impl ConnectionInfo {
    /// 创建新的连接信息
    pub fn new(connection_id: String) -> Self {
        let now = Instant::now();
        Self {
            connection_id,
            user_id: None,
            created_at: now,
            last_active: now,
            metadata: HashMap::new(),
            device_info: None,
            // 默认使用 JSON 且不压缩（客户端可以协商）
            serialization_format: crate::common::protocol::SerializationFormat::Json,
            compression: crate::common::compression::CompressionAlgorithm::None,
        }
    }
    
    /// 设置设备信息
    pub fn with_device_info(mut self, device_info: crate::common::device::DeviceInfo) -> Self {
        self.device_info = Some(device_info);
        self
    }
    
    /// 设置序列化格式
    pub fn with_serialization_format(
        mut self,
        format: crate::common::protocol::SerializationFormat,
    ) -> Self {
        self.serialization_format = format;
        self
    }
    
    /// 设置压缩算法
    pub fn with_compression(
        mut self,
        compression: crate::common::compression::CompressionAlgorithm,
    ) -> Self {
        self.compression = compression;
        self
    }

    /// 检查连接是否超时
    pub fn is_timeout(&self, timeout: Duration) -> bool {
        self.last_active.elapsed() > timeout
    }

    /// 更新最后活跃时间
    pub fn update_active(&mut self) {
        self.last_active = Instant::now();
    }
}

/// 连接管理器
/// 
/// 管理所有活跃连接，支持按 ID 查询、按用户 ID 查询等功能
pub struct ConnectionManager {
    /// 连接存储：connection_id -> (Connection, ConnectionInfo)
    connections: Arc<RwLock<HashMap<String, (Arc<Mutex<Box<dyn Connection>>>, ConnectionInfo)>>>,
    /// 用户 ID 到连接 ID 的映射（一个用户可能有多个连接）
    user_connections: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl ConnectionManager {
    /// 创建新的连接管理器
    pub fn new() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            user_connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 添加连接
    /// 
    /// # 参数
    /// - `connection_id`: 连接唯一标识符
    /// - `connection`: 连接实例
    /// - `user_id`: 可选的用户 ID（如果已认证）
    /// 
    /// # 返回
    /// 如果连接 ID 已存在，返回错误
    pub fn add_connection(
        &self,
        connection_id: String,
        connection: Box<dyn Connection>,
        user_id: Option<String>,
    ) -> Result<()> {
        let mut connections = self.connections.write()
            .map_err(|_| FlareError::general_error("Failed to lock connections"))?;
        
        if connections.contains_key(&connection_id) {
            return Err(FlareError::protocol_error(format!(
                "Connection {} already exists",
                connection_id
            )));
        }

        let mut info = ConnectionInfo::new(connection_id.clone());
        info.user_id = user_id.clone();
        
        connections.insert(connection_id.clone(), (Arc::new(Mutex::new(connection)), info));

        // 如果提供了用户 ID，添加到用户连接映射
        if let Some(user_id) = user_id {
            let mut user_connections = self.user_connections.write()
                .map_err(|_| FlareError::general_error("Failed to lock user_connections"))?;
            user_connections
                .entry(user_id)
                .or_insert_with(Vec::new)
                .push(connection_id);
        }

        Ok(())
    }

    /// 移除连接
    /// 
    /// # 参数
    /// - `connection_id`: 要移除的连接 ID
    /// 
    /// # 返回
    /// 如果连接不存在，返回错误
    pub fn remove_connection(&self, connection_id: &str) -> Result<()> {
        let mut connections = self.connections.write()
            .map_err(|_| FlareError::general_error("Failed to lock connections"))?;
        
        let (_, info) = connections.remove(connection_id)
            .ok_or_else(|| FlareError::protocol_error(format!("Connection {} not found", connection_id)))?;

        // 如果连接关联了用户，从用户连接映射中移除
        if let Some(user_id) = info.user_id {
            let mut user_connections = self.user_connections.write()
                .map_err(|_| FlareError::general_error("Failed to lock user_connections"))?;
            if let Some(conn_ids) = user_connections.get_mut(&user_id) {
                conn_ids.retain(|id| id != connection_id);
                if conn_ids.is_empty() {
                    user_connections.remove(&user_id);
                }
            }
        }

        Ok(())
    }

    /// 获取连接
    /// 
    /// # 参数
    /// - `connection_id`: 连接 ID
    /// 
    /// # 返回
    /// 连接实例和连接信息的元组，如果不存在则返回 None
    pub fn get_connection(
        &self,
        connection_id: &str,
    ) -> Option<(Arc<Mutex<Box<dyn Connection>>>, ConnectionInfo)> {
        self.connections.read()
            .ok()
            .and_then(|connections| {
                connections.get(connection_id).map(|(conn, info)| {
                    // 返回最新的连接信息（包括最新的 user_id）
                    (Arc::clone(conn), info.clone())
                })
            })
    }

    /// 获取用户的所有连接
    /// 
    /// # 参数
    /// - `user_id`: 用户 ID
    /// 
    /// # 返回
    /// 该用户的所有连接 ID 列表
    pub fn get_user_connections(&self, user_id: &str) -> Vec<String> {
        self.user_connections.read()
            .ok()
            .and_then(|user_connections| {
                user_connections.get(user_id).cloned()
            })
            .unwrap_or_default()
    }

    /// 更新连接的用户 ID（用于认证后绑定用户）
    /// 
    /// # 参数
    /// - `connection_id`: 连接 ID
    /// - `user_id`: 新的用户 ID
    pub fn bind_user(&self, connection_id: &str, user_id: String) -> Result<()> {
        let mut connections = self.connections.write()
            .map_err(|_| FlareError::general_error("Failed to lock connections"))?;
        
        let (_, info) = connections.get_mut(connection_id)
            .ok_or_else(|| FlareError::protocol_error(format!("Connection {} not found", connection_id)))?;

        // 如果之前有用户 ID，先移除旧映射
        if let Some(old_user_id) = &info.user_id {
            let mut user_connections = self.user_connections.write()
                .map_err(|_| FlareError::general_error("Failed to lock user_connections"))?;
            if let Some(conn_ids) = user_connections.get_mut(old_user_id) {
                conn_ids.retain(|id| id != connection_id);
                if conn_ids.is_empty() {
                    user_connections.remove(old_user_id);
                }
            }
        }

        // 更新用户 ID
        info.user_id = Some(user_id.clone());

        // 添加到新用户映射
        let mut user_connections = self.user_connections.write()
            .map_err(|_| FlareError::general_error("Failed to lock user_connections"))?;
        user_connections
            .entry(user_id)
            .or_insert_with(Vec::new)
            .push(connection_id.to_string());

        Ok(())
    }

    /// 更新连接的最后活跃时间
    pub fn update_connection_active(&self, connection_id: &str) -> Result<()> {
        let mut connections = self.connections.write()
            .map_err(|_| FlareError::general_error("Failed to lock connections"))?;
        
        let (_, info) = connections.get_mut(connection_id)
            .ok_or_else(|| FlareError::protocol_error(format!("Connection {} not found", connection_id)))?;
        
        info.update_active();
        Ok(())
    }
    
    /// 更新连接的协商信息（设备信息、序列化格式、压缩算法）
    pub fn update_connection_negotiation(
        &self,
        connection_id: &str,
        device_info: Option<crate::common::device::DeviceInfo>,
        serialization_format: crate::common::protocol::SerializationFormat,
        compression: crate::common::compression::CompressionAlgorithm,
        user_id: Option<String>,
    ) -> Result<()> {
        let mut connections = self.connections.write()
            .map_err(|_| FlareError::general_error("Failed to lock connections"))?;
        
        let (_, info) = connections.get_mut(connection_id)
            .ok_or_else(|| FlareError::protocol_error(format!("Connection {} not found", connection_id)))?;
        
        // 更新协商信息
        info.device_info = device_info;
        info.serialization_format = serialization_format;
        info.compression = compression;
        
        // 如果提供了用户 ID，更新用户 ID
        if let Some(user_id) = user_id {
            // 如果之前有用户 ID，先移除旧映射
            if let Some(old_user_id) = &info.user_id {
                let mut user_connections = self.user_connections.write()
                    .map_err(|_| FlareError::general_error("Failed to lock user_connections"))?;
                if let Some(conn_ids) = user_connections.get_mut(old_user_id) {
                    conn_ids.retain(|id| id != connection_id);
                    if conn_ids.is_empty() {
                        user_connections.remove(old_user_id);
                    }
                }
            }
            
            // 更新用户 ID
            info.user_id = Some(user_id.clone());
            
            // 添加到新用户映射
            let mut user_connections = self.user_connections.write()
                .map_err(|_| FlareError::general_error("Failed to lock user_connections"))?;
            user_connections
                .entry(user_id)
                .or_insert_with(Vec::new)
                .push(connection_id.to_string());
        }
        
        Ok(())
    }

    /// 获取所有连接 ID
    pub fn list_connections(&self) -> Vec<String> {
        self.connections.read()
            .ok()
            .map(|connections| connections.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// 获取连接总数
    pub fn connection_count(&self) -> usize {
        self.connections.read()
            .ok()
            .map(|connections| connections.len())
            .unwrap_or(0)
    }

    /// 清理超时连接
    /// 
    /// # 参数
    /// - `timeout`: 超时时间
    /// 
    /// # 返回
    /// 被清理的连接 ID 列表
    pub fn cleanup_timeout_connections(&self, timeout: Duration) -> Vec<String> {
        let timeout_connections: Vec<String> = {
            let connections = self.connections.read().ok();
            if let Some(connections) = connections {
                connections
                    .iter()
                    .filter(|(_, (_, info))| info.is_timeout(timeout))
                    .map(|(id, _)| id.clone())
                    .collect()
            } else {
                Vec::new()
            }
        };

        for connection_id in &timeout_connections {
            let _ = self.remove_connection(connection_id);
        }

        timeout_connections
    }

    /// 获取连接统计信息
    pub fn stats(&self) -> TraitConnectionStats {
        let connections = self.connections.read().ok();
        let user_connections = self.user_connections.read().ok();

        let total_connections = connections.as_ref().map(|c| c.len()).unwrap_or(0);
        let total_users = user_connections.as_ref().map(|u| u.len()).unwrap_or(0);

        TraitConnectionStats {
            total_connections,
            total_users,
        }
    }
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ConnectionManagerTrait for ConnectionManager {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    
    async fn add_connection(
        &self,
        connection_id: String,
        connection: Arc<Mutex<Box<dyn Connection>>>,
        user_id: Option<String>,
    ) -> Result<()> {
        // 将 Arc<Mutex<Box<dyn Connection>>> 转换为 Box<dyn Connection>
        // 注意：这需要从 Arc 中取出，但 Arc 可能被多个地方引用
        // 对于默认实现，我们需要一个不同的方式
        // 由于 ConnectionManager 内部使用 Arc<Mutex<Box<dyn Connection>>>，
        // 我们需要保持一致性
        let mut connections = self.connections.write()
            .map_err(|_| FlareError::general_error("Failed to lock connections"))?;
        
        if connections.contains_key(&connection_id) {
            return Err(FlareError::protocol_error(format!(
                "Connection {} already exists",
                connection_id
            )));
        }

        let mut info = ConnectionInfo::new(connection_id.clone());
        info.user_id = user_id.clone();
        
        connections.insert(connection_id.clone(), (Arc::clone(&connection), info));

        // 如果提供了用户 ID，添加到用户连接映射
        if let Some(user_id) = user_id {
            let mut user_connections = self.user_connections.write()
                .map_err(|_| FlareError::general_error("Failed to lock user_connections"))?;
            user_connections
                .entry(user_id)
                .or_insert_with(Vec::new)
                .push(connection_id);
        }

        Ok(())
    }

    async fn remove_connection(&self, connection_id: &str) -> Result<()> {
        ConnectionManager::remove_connection(self, connection_id)
    }

    async fn get_connection(
        &self,
        connection_id: &str,
    ) -> Option<(Arc<Mutex<Box<dyn Connection>>>, crate::server::connection::r#trait::ConnectionInfo)> {
        ConnectionManager::get_connection(self, connection_id).map(|(conn, info)| {
            // 转换 ConnectionInfo 格式（从 Instant 转换为 Unix 时间戳）
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
            let created_at_secs = now.saturating_sub(info.created_at.elapsed().as_secs());
            let last_active_secs = now.saturating_sub(info.last_active.elapsed().as_secs());
            
            let trait_info = crate::server::connection::r#trait::ConnectionInfo {
                connection_id: info.connection_id,
                user_id: info.user_id,
                created_at: created_at_secs,
                last_active: last_active_secs,
                metadata: info.metadata,
                device_info: info.device_info.clone(),
                serialization_format: info.serialization_format,
                compression: info.compression,
            };
            (conn, trait_info)
        })
    }

    async fn get_user_connections(&self, user_id: &str) -> Vec<String> {
        ConnectionManager::get_user_connections(self, user_id)
    }

    async fn bind_user(&self, connection_id: &str, user_id: String) -> Result<()> {
        ConnectionManager::bind_user(self, connection_id, user_id)
    }

    async fn update_connection_active(&self, connection_id: &str) -> Result<()> {
        ConnectionManager::update_connection_active(self, connection_id)
    }

    async fn list_connections(&self) -> Vec<String> {
        ConnectionManager::list_connections(self)
    }

    async fn connection_count(&self) -> usize {
        ConnectionManager::connection_count(self)
    }

    async fn cleanup_timeout_connections(&self, timeout: Duration) -> Vec<String> {
        ConnectionManager::cleanup_timeout_connections(self, timeout)
    }

    async fn send_to_connection(&self, connection_id: &str, data: &[u8]) -> Result<()> {
        let (connection, _) = ConnectionManager::get_connection(self, connection_id)
            .ok_or_else(|| FlareError::protocol_error(format!("Connection {} not found", connection_id)))?;
        
        let mut conn = connection.lock().await;
        conn.send(data).await
    }

    async fn send_to_user(&self, user_id: &str, data: &[u8]) -> Result<()> {
        let connection_ids = ConnectionManager::get_user_connections(self, user_id);
        
        for connection_id in connection_ids {
            if let Err(e) = self.send_to_connection(&connection_id, data).await {
                tracing::warn!("Failed to send to connection {}: {:?}", connection_id, e);
            }
        }
        
        Ok(())
    }

    async fn broadcast(&self, data: &[u8]) -> Result<()> {
        let connection_ids = ConnectionManager::list_connections(self);
        
        for connection_id in connection_ids {
            if let Err(e) = self.send_to_connection(&connection_id, data).await {
                tracing::warn!("Failed to broadcast to connection {}: {:?}", connection_id, e);
            }
        }
        
        Ok(())
    }

    async fn broadcast_except(&self, data: &[u8], exclude_connection_id: &str) -> Result<()> {
        let connection_ids: Vec<String> = ConnectionManager::list_connections(self)
            .into_iter()
            .filter(|id| id != exclude_connection_id)
            .collect();
        
        for connection_id in connection_ids {
            if let Err(e) = self.send_to_connection(&connection_id, data).await {
                tracing::warn!("Failed to broadcast to connection {}: {:?}", connection_id, e);
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::connection::Connection;
    use crate::transport::events::ArcObserver;
    use async_trait::async_trait;
    use std::sync::Mutex;

    struct MockConnection {
        last_active: Mutex<Instant>,
    }

    impl MockConnection {
        fn new() -> Self {
            Self {
                last_active: Mutex::new(Instant::now()),
            }
        }
    }

    #[async_trait]
    impl Connection for MockConnection {
        fn add_observer(&mut self, _observer: ArcObserver) {}
        fn remove_observer(&mut self, _observer: ArcObserver) {}
        async fn send(&mut self, _data: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
        fn last_active_time(&self) -> Instant {
            *self.last_active.lock().unwrap()
        }
        fn update_active_time(&mut self) {
            *self.last_active.lock().unwrap() = Instant::now();
        }
    }

    #[test]
    fn test_add_and_get_connection() {
        let manager = ConnectionManager::new();
        let connection = Box::new(MockConnection::new());
        
        manager.add_connection("conn1".to_string(), connection, None).unwrap();
        
        let (_, info) = manager.get_connection("conn1").unwrap();
        assert_eq!(info.connection_id, "conn1");
    }

    #[test]
    fn test_remove_connection() {
        let manager = ConnectionManager::new();
        let connection = Box::new(MockConnection::new());
        
        manager.add_connection("conn1".to_string(), connection, None).unwrap();
        assert_eq!(manager.connection_count(), 1);
        
        manager.remove_connection("conn1").unwrap();
        assert_eq!(manager.connection_count(), 0);
    }

    #[test]
    fn test_user_binding() {
        let manager = ConnectionManager::new();
        let connection = Box::new(MockConnection::new());
        
        manager.add_connection("conn1".to_string(), connection, None).unwrap();
        manager.bind_user("conn1", "user1".to_string()).unwrap();
        
        let connections = manager.get_user_connections("user1");
        assert_eq!(connections, vec!["conn1"]);
    }

    #[test]
    fn test_cleanup_timeout() {
        let manager = ConnectionManager::new();
        let connection = Box::new(MockConnection::new());
        
        manager.add_connection("conn1".to_string(), connection, None).unwrap();
        
        // 等待一段时间，让连接超时
        std::thread::sleep(Duration::from_millis(10));
        
        let cleaned = manager.cleanup_timeout_connections(Duration::from_millis(5));
        assert!(cleaned.contains(&"conn1".to_string()));
    }
}

