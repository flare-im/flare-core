//! 连接管理器模块
//!
//! 提供连接的统一管理、存储和查询功能
//! 支持按连接 ID、用户 ID 等方式管理连接

use crate::common::error::{FlareError, Result};
use crate::server::connection::r#trait::{
    ConnectionManagerTrait, ConnectionStats as TraitConnectionStats,
};
use crate::transport::connection::Connection;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// 连接信息
#[derive(Clone)]
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
    /// 加密凡事（协商决定）
    pub encryption: crate::common::encryption::EncryptionAlgorithm,
    /// 是否已验证（如果启用认证，只有已验证的连接才能收发消息）
    pub authenticated: bool,
    /// 认证时间戳（Unix 时间戳，秒，如果已验证）
    pub authenticated_at: Option<u64>,
    /// 协商是否已完成（CONNECT 和 CONNECT_ACK 完成）
    pub negotiation_completed: bool,
    /// 协商是否已确认（客户端收到 CONNECT_ACK 后发送确认，服务端收到后标记）
    /// 确认后，消息必须严格按照协商好的方式处理，不再容错
    pub negotiation_confirmed: bool,
    /// 缓存的 MessageParser（协商完成后创建，避免每次消息处理都创建新的 parser）
    pub cached_parser: Option<std::sync::Arc<crate::common::MessageParser>>,
    /// 缓存的 MessagePipeline（协商完成后创建，如果配置了中间件或处理器）
    pub cached_pipeline: Option<std::sync::Arc<crate::common::message::pipeline::MessagePipeline>>,
}

impl ConnectionInfo {
    /// 创建新的连接信息
    ///
    /// # 参数
    /// - `connection_id`: 连接 ID
    /// - `requires_auth`: 是否需要认证（如果为 false，连接直接标记为已验证）
    pub fn new(connection_id: String, requires_auth: bool) -> Self {
        let now = Instant::now();
        let authenticated = !requires_auth; // 如果不需要认证，直接标记为已验证
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
            encryption: crate::common::encryption::EncryptionAlgorithm::None,
            authenticated,
            authenticated_at: authenticated.then(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            }),
            negotiation_completed: false, // 初始状态：协商未完成
            negotiation_confirmed: false, // 初始状态：协商未确认
            cached_parser: None,          // 初始状态：未缓存 parser
            cached_pipeline: None,        // 初始状态：未缓存 pipeline
        }
    }

    /// 标记为已验证
    pub fn set_authenticated(&mut self, user_id: Option<String>) {
        self.authenticated = true;
        self.authenticated_at = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );
        if let Some(uid) = user_id {
            self.user_id = Some(uid);
        }
    }

    /// 检查连接是否已验证
    pub fn is_authenticated(&self) -> bool {
        self.authenticated
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
    #[allow(clippy::type_complexity)]
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
    /// - `requires_auth`: 是否需要认证（如果为 false，连接直接标记为已验证）
    ///
    /// # 返回
    /// 如果连接 ID 已存在，返回错误
    pub fn add_connection(
        &self,
        connection_id: String,
        connection: Box<dyn Connection>,
        user_id: Option<String>,
        requires_auth: bool,
    ) -> Result<()> {
        let mut connections = self
            .connections
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connections"))?;

        if connections.contains_key(&connection_id) {
            return Err(FlareError::protocol_error(format!(
                "Connection {} already exists",
                connection_id
            )));
        }

        let mut info = ConnectionInfo::new(connection_id.clone(), requires_auth);
        info.user_id = user_id.clone();

        connections.insert(
            connection_id.clone(),
            (Arc::new(Mutex::new(connection)), info),
        );

        // 如果提供了用户 ID，添加到用户连接映射
        if let Some(user_id) = user_id {
            let mut user_connections = self
                .user_connections
                .write()
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
        let mut connections = self
            .connections
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connections"))?;

        let (_, info) = connections.remove(connection_id).ok_or_else(|| {
            FlareError::protocol_error(format!("Connection {} not found", connection_id))
        })?;

        // 如果连接关联了用户，从用户连接映射中移除
        if let Some(user_id) = info.user_id {
            let mut user_connections = self
                .user_connections
                .write()
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
    #[allow(clippy::type_complexity)]
    pub fn get_connection(
        &self,
        connection_id: &str,
    ) -> Option<(Arc<Mutex<Box<dyn Connection>>>, ConnectionInfo)> {
        let connections = self.connections.read().ok()?;
        let (conn, info) = connections.get(connection_id)?;
        let conn_clone = Arc::clone(conn);
        let info_clone = info.clone();
        drop(connections);
        Some((conn_clone, info_clone))
    }

    /// 获取用户的所有连接
    ///
    /// # 参数
    /// - `user_id`: 用户 ID
    ///
    /// # 返回
    /// 该用户的所有连接 ID 列表
    pub fn get_user_connections(&self, user_id: &str) -> Vec<String> {
        self.user_connections
            .read()
            .ok()
            .and_then(|user_connections| user_connections.get(user_id).cloned())
            .unwrap_or_default()
    }

    /// 更新连接的用户 ID（用于认证后绑定用户）
    ///
    /// # 参数
    /// - `connection_id`: 连接 ID
    /// - `user_id`: 新的用户 ID
    pub fn bind_user(&self, connection_id: &str, user_id: String) -> Result<()> {
        let mut connections = self
            .connections
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connections"))?;

        let (_, info) = connections.get_mut(connection_id).ok_or_else(|| {
            FlareError::protocol_error(format!("Connection {} not found", connection_id))
        })?;

        // 如果之前有用户 ID，先移除旧映射
        if let Some(old_user_id) = &info.user_id {
            let mut user_connections = self
                .user_connections
                .write()
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
        let mut user_connections = self
            .user_connections
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock user_connections"))?;
        user_connections
            .entry(user_id)
            .or_insert_with(Vec::new)
            .push(connection_id.to_string());

        Ok(())
    }

    /// 更新连接的最后活跃时间
    pub fn update_connection_active(&self, connection_id: &str) -> Result<()> {
        let mut connections = self
            .connections
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connections"))?;

        if let Some((_, info)) = connections.get_mut(connection_id) {
            info.update_active();
            drop(connections);
            Ok(())
        } else {
            drop(connections);
            Err(FlareError::protocol_error(format!(
                "Connection {} not found",
                connection_id
            )))
        }
    }

    /// 设置连接为已验证状态
    pub fn set_connection_authenticated(
        &self,
        connection_id: &str,
        user_id: Option<String>,
    ) -> Result<()> {
        let mut connections = self
            .connections
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connections"))?;

        let (_, info) = connections.get_mut(connection_id).ok_or_else(|| {
            FlareError::protocol_error(format!("Connection {} not found", connection_id))
        })?;

        // 保存旧的 user_id（在调用 set_authenticated 之前）
        let old_user_id = info.user_id.clone();

        let final_user_id = user_id.or(old_user_id.clone());

        // 设置认证状态（如果 final_user_id 是 Some，会设置 user_id）
        info.set_authenticated(final_user_id.clone());

        // 如果有 user_id（传入的或已存在的），确保用户连接映射正确
        if let Some(user_id) = final_user_id {
            let mut user_connections = self
                .user_connections
                .write()
                .map_err(|_| FlareError::general_error("Failed to lock user_connections"))?;

            // 如果 user_id 发生变化，需要更新映射
            let user_id_changed = old_user_id
                .as_ref()
                .map(|old| old != &user_id)
                .unwrap_or(true);

            if user_id_changed {
                // 如果之前有旧用户 ID，先移除旧映射
                if let Some(old_user_id) = old_user_id {
                    if let Some(conn_ids) = user_connections.get_mut(&old_user_id) {
                        conn_ids.retain(|id| id != connection_id);
                        if conn_ids.is_empty() {
                            user_connections.remove(&old_user_id);
                        }
                    }
                }

                // 添加新映射（检查是否已存在，避免重复）
                let conn_ids = user_connections
                    .entry(user_id.clone())
                    .or_insert_with(Vec::new);
                if !conn_ids.contains(&connection_id.to_string()) {
                    conn_ids.push(connection_id.to_string());
                }
            } else {
                // user_id 没有变化，只需确保映射存在
                let conn_ids = user_connections.entry(user_id).or_insert_with(Vec::new);
                if !conn_ids.contains(&connection_id.to_string()) {
                    conn_ids.push(connection_id.to_string());
                }
            }
        }

        Ok(())
    }

    /// 更新连接的协商信息（设备信息、序列化格式、压缩算法）
    pub fn update_connection_negotiation(
        &self,
        connection_id: &str,
        device_info: Option<crate::common::device::DeviceInfo>,
        serialization_format: crate::common::protocol::SerializationFormat,
        compression: crate::common::compression::CompressionAlgorithm,
        encryption: crate::common::encryption::EncryptionAlgorithm,
        user_id: Option<String>,
        metadata: Option<HashMap<String,String>>,
    ) -> Result<()> {
        let mut connections = self
            .connections
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connections"))?;

        let (_, info) = connections.get_mut(connection_id).ok_or_else(|| {
            FlareError::protocol_error(format!("Connection {} not found", connection_id))
        })?;

        // 更新协商信息（但不标记协商完成）
        // 协商完成将在 CONNECT_ACK 发送完成后由 update_connection_negotiation_with_pipeline 标记
        info.device_info = device_info;
        info.serialization_format = serialization_format;
        info.compression = compression;
        info.encryption = encryption;
        // 若有传入 metadata，将其所有键值合并到 ConnectionInfo.metadata（同 key 覆盖）
        if let Some(meta) = metadata {
            for (k, v) in meta {
                info.metadata.insert(k, v);
            }
        }
        // 注意：这里不设置 negotiation_completed = true
        // 也不创建 cached_parser，这些将在 CONNECT_ACK 发送完成后设置

        // 保存旧的 user_id（在修改之前）
        let old_user_id = info.user_id.clone();

        let user_id_to_set = user_id.clone().or(old_user_id.clone());

        // 添加调试日志
        if user_id_to_set.is_none() {
            tracing::trace!(connection_id = %connection_id,incoming_user_id = ?user_id,old_user_id = ?old_user_id,"update_connection_negotiation: user_id_to_set is None, user_id will not be set");
        }

        if let Some(user_id_val) = user_id_to_set {
            // 如果之前有用户 ID 且与新 user_id 不同，先移除旧映射
            if let Some(old_user_id) = old_user_id {
                if old_user_id != user_id_val {
                    let mut user_connections = self.user_connections.write().map_err(|_| {
                        FlareError::general_error("Failed to lock user_connections")
                    })?;
                    if let Some(conn_ids) = user_connections.get_mut(&old_user_id) {
                        conn_ids.retain(|id| id != connection_id);
                        if conn_ids.is_empty() {
                            user_connections.remove(&old_user_id);
                        }
                    }
                }
            }

            // 更新用户 ID
            info.user_id = Some(user_id_val.clone());

            // 添加到新用户映射
            let mut user_connections = self
                .user_connections
                .write()
                .map_err(|_| FlareError::general_error("Failed to lock user_connections"))?;
            user_connections
                .entry(user_id_val)
                .or_insert_with(Vec::new)
                .push(connection_id.to_string());
        }

        Ok(())
    }

    /// 更新连接的协商信息（设备信息、序列化格式、压缩算法、加密方式）并设置 pipeline
    #[allow(clippy::too_many_arguments)]
    pub fn update_connection_negotiation_with_pipeline(
        &self,
        connection_id: &str,
        device_info: Option<crate::common::device::DeviceInfo>,
        serialization_format: crate::common::protocol::SerializationFormat,
        compression: crate::common::compression::CompressionAlgorithm,
        encryption: crate::common::encryption::EncryptionAlgorithm,
        user_id: Option<String>,
        parser: crate::common::MessageParser,
        pipeline: Option<std::sync::Arc<crate::common::message::pipeline::MessagePipeline>>,
    ) -> Result<()> {
        let mut connections = self
            .connections
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connections"))?;

        let (_, info) = connections.get_mut(connection_id).ok_or_else(|| {
            FlareError::protocol_error(format!("Connection {} not found", connection_id))
        })?;

        // 更新协商信息
        info.device_info = device_info;
        info.serialization_format = serialization_format;
        info.compression = compression;
        info.encryption = encryption;
        // 标记协商已完成
        info.negotiation_completed = true;
        // 缓存 parser 和 pipeline
        info.cached_parser = Some(std::sync::Arc::new(parser));
        info.cached_pipeline = pipeline;

        // 保存旧的 user_id（在修改之前）
        let old_user_id = info.user_id.clone();

        let user_id_to_set = user_id.clone().or(old_user_id.clone());

        // 添加调试日志
        if user_id_to_set.is_none() {
            tracing::trace!(
                connection_id = %connection_id,
                incoming_user_id = ?user_id,
                old_user_id = ?old_user_id,
                "update_connection_negotiation_with_pipeline: user_id_to_set is None, user_id will not be set"
            );
        }

        if let Some(user_id_val) = user_id_to_set {
            // 如果之前有用户 ID 且与新 user_id 不同，先移除旧映射
            if let Some(old_user_id) = old_user_id {
                if old_user_id != user_id_val {
                    let mut user_connections = self.user_connections.write().map_err(|_| {
                        FlareError::general_error("Failed to lock user_connections")
                    })?;
                    if let Some(conn_ids) = user_connections.get_mut(&old_user_id) {
                        conn_ids.retain(|id| id != connection_id);
                        if conn_ids.is_empty() {
                            user_connections.remove(&old_user_id);
                        }
                    }
                }
            }

            // 更新用户 ID
            info.user_id = Some(user_id_val.clone());

            // 添加到新用户映射
            let mut user_connections = self
                .user_connections
                .write()
                .map_err(|_| FlareError::general_error("Failed to lock user_connections"))?;
            user_connections
                .entry(user_id_val)
                .or_insert_with(Vec::new)
                .push(connection_id.to_string());
        }

        Ok(())
    }

    /// 标记协商已确认（客户端收到 CONNECT_ACK 后发送确认）
    pub fn mark_negotiation_confirmed(&self, connection_id: &str) -> Result<()> {
        let mut connections = self
            .connections
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connections"))?;

        let (_, info) = connections.get_mut(connection_id).ok_or_else(|| {
            FlareError::protocol_error(format!("Connection {} not found", connection_id))
        })?;

        if !info.negotiation_completed {
            return Err(FlareError::protocol_error(format!(
                "Cannot confirm negotiation for connection {}: negotiation not completed",
                connection_id
            )));
        }

        info.negotiation_confirmed = true;
        tracing::trace!(
            "[ConnectionManager] 协商已确认: connection_id={}",
            connection_id
        );

        Ok(())
    }

    /// 获取所有连接 ID
    pub fn list_connections(&self) -> Vec<String> {
        self.connections
            .read()
            .ok()
            .map(|connections| connections.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// 获取连接总数
    pub fn connection_count(&self) -> usize {
        self.connections
            .read()
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
        // 注意：trait 方法不能直接传递 requires_auth，我们需要从 ServerCore 获取
        // 但这里我们暂时使用 true（需要认证），实际值应该在调用时通过 ServerCore 的 auth_enabled() 获取
        // 由于 ConnectionManager 不知道 ServerCore，我们暂时使用 true
        // 实际应用中，连接会在 CONNECT 消息处理时被标记为已验证
        let requires_auth = true; // 默认需要认证，如果不需要认证，连接会在 CONNECT 消息处理时被标记为已验证

        // 将 Arc<Mutex<Box<dyn Connection>>> 转换为 Box<dyn Connection>
        // 注意：这需要从 Arc 中取出，但 Arc 可能被多个地方引用
        // 对于默认实现，我们需要一个不同的方式
        // 由于 ConnectionManager 内部使用 Arc<Mutex<Box<dyn Connection>>>，
        // 我们需要保持一致性
        let mut connections = self
            .connections
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connections"))?;

        if connections.contains_key(&connection_id) {
            return Err(FlareError::protocol_error(format!(
                "Connection {} already exists",
                connection_id
            )));
        }

        let mut info = ConnectionInfo::new(connection_id.clone(), requires_auth);
        info.user_id = user_id.clone();

        connections.insert(connection_id.clone(), (Arc::clone(&connection), info));

        // 如果提供了用户 ID，添加到用户连接映射
        if let Some(user_id) = user_id {
            let mut user_connections = self
                .user_connections
                .write()
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
    ) -> Option<(
        Arc<Mutex<Box<dyn Connection>>>,
        crate::server::connection::r#trait::ConnectionInfo,
    )> {
        ConnectionManager::get_connection(self, connection_id).map(|(conn, info)| {
            // 转换 ConnectionInfo 格式（从 Instant 转换为 Unix 时间戳）
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
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
                encryption: info.encryption,
                authenticated: info.authenticated,
                authenticated_at: info.authenticated_at,
                negotiation_completed: info.negotiation_completed,
                negotiation_confirmed: info.negotiation_confirmed,
                cached_parser: info.cached_parser.clone(),
                cached_pipeline: info.cached_pipeline.clone(),
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

    async fn set_connection_authenticated(
        &self,
        connection_id: &str,
        user_id: Option<String>,
    ) -> Result<()> {
        // ConnectionManager::set_connection_authenticated 是同步方法，直接调用
        ConnectionManager::set_connection_authenticated(self, connection_id, user_id)
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
        let (connection, _) =
            ConnectionManager::get_connection(self, connection_id).ok_or_else(|| {
                FlareError::protocol_error(format!("Connection {} not found", connection_id))
            })?;

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
                tracing::warn!(
                    "Failed to broadcast to connection {}: {:?}",
                    connection_id,
                    e
                );
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
                tracing::warn!(
                    "Failed to broadcast to connection {}: {:?}",
                    connection_id,
                    e
                );
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

        manager
            .add_connection("conn1".to_string(), connection, None, false)
            .unwrap();

        let (_, info) = manager.get_connection("conn1").unwrap();
        assert_eq!(info.connection_id, "conn1");
    }

    #[test]
    fn test_remove_connection() {
        let manager = ConnectionManager::new();
        let connection = Box::new(MockConnection::new());

        manager
            .add_connection("conn1".to_string(), connection, None, false)
            .unwrap();
        assert_eq!(manager.connection_count(), 1);

        manager.remove_connection("conn1").unwrap();
        assert_eq!(manager.connection_count(), 0);
    }

    #[test]
    fn test_user_binding() {
        let manager = ConnectionManager::new();
        let connection = Box::new(MockConnection::new());

        manager
            .add_connection("conn1".to_string(), connection, None, false)
            .unwrap();
        manager.bind_user("conn1", "user1".to_string()).unwrap();

        let connections = manager.get_user_connections("user1");
        assert_eq!(connections, vec!["conn1"]);
    }

    #[test]
    fn test_cleanup_timeout() {
        let manager = ConnectionManager::new();
        let connection = Box::new(MockConnection::new());

        manager
            .add_connection("conn1".to_string(), connection, None, false)
            .unwrap();

        // 等待一段时间，让连接超时
        std::thread::sleep(Duration::from_millis(10));

        let cleaned = manager.cleanup_timeout_connections(Duration::from_millis(5));
        assert!(cleaned.contains(&"conn1".to_string()));
    }
}
