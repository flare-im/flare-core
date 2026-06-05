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
use futures_util::stream::{self, StreamExt};
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::sync::{
    Arc, RwLock, Weak,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc};

const DEFAULT_FANOUT_CONCURRENCY: usize = 256;
const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_WRITE_QUEUE_CAPACITY: usize = 1024;
const CONNECTION_SHARD_COUNT: usize = 64;
const USER_CONNECTION_SHARD_COUNT: usize = 64;

type ConnectionHandle = Arc<Mutex<Box<dyn Connection>>>;
type ConnectionWriteHandle = Arc<ConnectionWriteQueue>;
type ConnectionEntry = (ConnectionHandle, ConnectionWriteHandle, ConnectionInfo);
type ConnectionHandleSnapshot = (String, ConnectionWriteHandle);
type ConnectionAuthSnapshot = (String, ConnectionWriteHandle, bool);
type ConnectionSnapshot = (String, ConnectionWriteHandle, ConnectionInfo);
type TimeoutConnectionSnapshot = (String, ConnectionHandle, Option<String>);
type ConnectionShard = RwLock<HashMap<String, ConnectionEntry>>;
type UserConnectionShard = RwLock<HashMap<String, Vec<String>>>;

struct QueuedWrite {
    data: Vec<u8>,
}

#[derive(Clone)]
struct ConnectionRemovalRegistry {
    connection_shards: Weak<Vec<ConnectionShard>>,
    user_connection_shards: Weak<Vec<UserConnectionShard>>,
    connection_count: Arc<AtomicUsize>,
    user_count: Arc<AtomicUsize>,
}

impl ConnectionRemovalRegistry {
    fn remove_connection(&self, connection_id: &str) -> bool {
        let Some(connection_shards) = self.connection_shards.upgrade() else {
            return false;
        };
        let shard_index = ConnectionManager::shard_index(connection_id, connection_shards.len());

        let Ok(mut shard) = connection_shards[shard_index].write() else {
            return false;
        };

        let Some((_, _, info)) = shard.remove(connection_id) else {
            return false;
        };
        drop(shard);

        self.connection_count.fetch_sub(1, Ordering::Relaxed);

        if let Some(user_id) = info.user_id {
            self.remove_user_connection(&user_id, connection_id);
        }

        true
    }

    fn remove_user_connection(&self, user_id: &str, connection_id: &str) {
        let Some(user_connection_shards) = self.user_connection_shards.upgrade() else {
            return;
        };
        let shard_index = ConnectionManager::shard_index(user_id, user_connection_shards.len());

        let Ok(mut shard) = user_connection_shards[shard_index].write() else {
            return;
        };

        if ConnectionManager::remove_user_connection_index(&mut shard, user_id, connection_id) {
            self.user_count.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

struct ConnectionWriteQueue {
    sender: mpsc::Sender<QueuedWrite>,
    receiver: std::sync::Mutex<Option<mpsc::Receiver<QueuedWrite>>>,
    connection: ConnectionHandle,
    closed: Arc<std::sync::atomic::AtomicBool>,
    writer_started: std::sync::atomic::AtomicBool,
    connection_id: String,
    send_timeout: Duration,
    removal_registry: ConnectionRemovalRegistry,
}

impl ConnectionWriteQueue {
    fn new(
        connection_id: String,
        connection: ConnectionHandle,
        send_timeout: Duration,
        queue_capacity: usize,
        removal_registry: ConnectionRemovalRegistry,
    ) -> ConnectionWriteHandle {
        let (sender, receiver) = mpsc::channel(queue_capacity.max(1));
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        Arc::new(Self {
            sender,
            receiver: std::sync::Mutex::new(Some(receiver)),
            connection: Arc::clone(&connection),
            closed: Arc::clone(&closed),
            connection_id,
            send_timeout,
            removal_registry,
            writer_started: std::sync::atomic::AtomicBool::new(false),
        })
    }

    fn ensure_writer_started(&self) -> Result<()> {
        if self.writer_started.load(Ordering::Acquire) {
            return Ok(());
        }

        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            FlareError::connection_failed("Connection write queue requires a Tokio runtime")
        })?;

        if self
            .writer_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }

        let Some(receiver) = self
            .receiver
            .lock()
            .ok()
            .and_then(|mut receiver| receiver.take())
        else {
            self.closed.store(true, Ordering::Release);
            return Err(FlareError::connection_closed(
                "Connection write queue receiver is unavailable",
            ));
        };

        handle.spawn(Self::writer_task(
            self.connection_id.clone(),
            Arc::clone(&self.connection),
            receiver,
            self.send_timeout,
            Arc::clone(&self.closed),
            self.removal_registry.clone(),
        ));

        Ok(())
    }

    fn try_enqueue(&self, data: &[u8]) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(FlareError::connection_closed(
                "Connection write queue is closed",
            ));
        }
        self.ensure_writer_started()?;

        self.sender
            .try_send(QueuedWrite {
                data: data.to_vec(),
            })
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => {
                    FlareError::connection_timeout("Connection write queue is full")
                }
                mpsc::error::TrySendError::Closed(_) => {
                    FlareError::connection_closed("Connection write queue is closed")
                }
            })
    }

    fn mark_closed(&self) -> bool {
        !self.closed.swap(true, Ordering::AcqRel)
    }

    fn close_underlying_in_background(&self) {
        if !self.mark_closed() {
            return;
        }

        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }

        let connection = Arc::clone(&self.connection);
        tokio::spawn(async move {
            let mut conn = connection.lock().await;
            let _ = conn.close().await;
        });
    }

    async fn writer_task(
        connection_id: String,
        connection: ConnectionHandle,
        mut receiver: mpsc::Receiver<QueuedWrite>,
        send_timeout: Duration,
        closed: Arc<std::sync::atomic::AtomicBool>,
        removal_registry: ConnectionRemovalRegistry,
    ) {
        while let Some(write) = receiver.recv().await {
            if closed.load(Ordering::Acquire) {
                break;
            }

            let result = {
                let mut conn = connection.lock().await;
                tokio::time::timeout(send_timeout, conn.send(&write.data)).await
            };

            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    closed.store(true, Ordering::Release);
                    tracing::warn!(
                        connection_id = %connection_id,
                        error = ?err,
                        "Connection writer failed; isolating connection"
                    );
                    let mut conn = connection.lock().await;
                    let _ = conn.close().await;
                    removal_registry.remove_connection(&connection_id);
                    break;
                }
                Err(_) => {
                    closed.store(true, Ordering::Release);
                    tracing::warn!(
                        connection_id = %connection_id,
                        timeout = ?send_timeout,
                        "Connection writer timed out; isolating connection"
                    );
                    let mut conn = connection.lock().await;
                    let _ = conn.close().await;
                    removal_registry.remove_connection(&connection_id);
                    break;
                }
            }
        }
    }
}

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
    /// 分片连接存储：connection_id -> (Connection, ConnectionInfo)
    connection_shards: Arc<Vec<ConnectionShard>>,
    /// 分片用户连接索引：user_id -> connection_ids
    user_connection_shards: Arc<Vec<UserConnectionShard>>,
    /// 当前连接数。高频读路径不应抢全局连接表锁。
    connection_count: Arc<AtomicUsize>,
    /// 当前有连接绑定的用户数。指标采集不应抢用户索引锁。
    user_count: Arc<AtomicUsize>,
    /// 单次底层写入超时时间
    send_timeout: Duration,
    /// fanout 发送最大并发度
    fanout_concurrency: usize,
    /// 每连接写队列容量
    write_queue_capacity: usize,
}

impl ConnectionManager {
    /// 创建新的连接管理器
    pub fn new() -> Self {
        Self::with_send_timeout(DEFAULT_SEND_TIMEOUT)
    }

    /// 使用指定写超时创建连接管理器
    pub fn with_send_timeout(send_timeout: Duration) -> Self {
        Self::with_limits(send_timeout, DEFAULT_FANOUT_CONCURRENCY)
    }

    /// 使用指定写超时和 fanout 并发度创建连接管理器
    pub fn with_limits(send_timeout: Duration, fanout_concurrency: usize) -> Self {
        Self::with_write_queue_limits(
            send_timeout,
            fanout_concurrency,
            DEFAULT_WRITE_QUEUE_CAPACITY,
        )
    }

    /// 使用指定写超时、fanout 并发度和每连接写队列容量创建连接管理器。
    pub fn with_write_queue_limits(
        send_timeout: Duration,
        fanout_concurrency: usize,
        write_queue_capacity: usize,
    ) -> Self {
        Self {
            connection_shards: Arc::new(
                (0..CONNECTION_SHARD_COUNT)
                    .map(|_| RwLock::new(HashMap::new()))
                    .collect(),
            ),
            user_connection_shards: Arc::new(
                (0..USER_CONNECTION_SHARD_COUNT)
                    .map(|_| RwLock::new(HashMap::new()))
                    .collect(),
            ),
            connection_count: Arc::new(AtomicUsize::new(0)),
            user_count: Arc::new(AtomicUsize::new(0)),
            send_timeout,
            fanout_concurrency: fanout_concurrency.max(1),
            write_queue_capacity: write_queue_capacity.max(1),
        }
    }

    fn removal_registry(&self) -> ConnectionRemovalRegistry {
        ConnectionRemovalRegistry {
            connection_shards: Arc::downgrade(&self.connection_shards),
            user_connection_shards: Arc::downgrade(&self.user_connection_shards),
            connection_count: Arc::clone(&self.connection_count),
            user_count: Arc::clone(&self.user_count),
        }
    }

    fn new_connection_entry(
        &self,
        connection_id: &str,
        connection: ConnectionHandle,
        info: ConnectionInfo,
    ) -> ConnectionEntry {
        let writer = ConnectionWriteQueue::new(
            connection_id.to_string(),
            Arc::clone(&connection),
            self.send_timeout,
            self.write_queue_capacity,
            self.removal_registry(),
        );
        (connection, writer, info)
    }

    fn shard_index(key: &str, shard_count: usize) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % shard_count
    }

    fn connection_shard_index(&self, connection_id: &str) -> usize {
        Self::shard_index(connection_id, self.connection_shards.len())
    }

    fn connection_shard(&self, connection_id: &str) -> &ConnectionShard {
        &self.connection_shards[self.connection_shard_index(connection_id)]
    }

    fn user_connection_shard_index(&self, user_id: &str) -> usize {
        Self::shard_index(user_id, self.user_connection_shards.len())
    }

    fn user_connection_shard(&self, user_id: &str) -> &UserConnectionShard {
        &self.user_connection_shards[self.user_connection_shard_index(user_id)]
    }

    fn reserve_connection_slot(&self, max_connections: usize) -> Result<()> {
        loop {
            let current = self.connection_count.load(Ordering::Relaxed);
            if current >= max_connections {
                return Err(FlareError::connection_failed(format!(
                    "Connection limit exceeded: {}",
                    max_connections
                )));
            }

            if self
                .connection_count
                .compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    fn release_connection_slot(&self) {
        self.connection_count.fetch_sub(1, Ordering::Relaxed);
    }

    fn insert_user_connection(&self, user_id: String, connection_id: &str) -> Result<()> {
        let mut user_connections = self
            .user_connection_shard(&user_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock user_connection shard"))?;
        if Self::insert_user_connection_index(&mut user_connections, user_id, connection_id) {
            self.user_count.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn remove_user_connection(&self, user_id: &str, connection_id: &str) -> Result<()> {
        let mut user_connections = self
            .user_connection_shard(user_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock user_connection shard"))?;
        if Self::remove_user_connection_index(&mut user_connections, user_id, connection_id) {
            self.user_count.fetch_sub(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn insert_user_connection_index(
        user_connections: &mut HashMap<String, Vec<String>>,
        user_id: String,
        connection_id: &str,
    ) -> bool {
        let is_new_user = !user_connections.contains_key(&user_id);
        let conn_ids = user_connections.entry(user_id).or_default();
        if !conn_ids.iter().any(|id| id == connection_id) {
            conn_ids.push(connection_id.to_string());
        }
        is_new_user
    }

    fn remove_user_connection_index(
        user_connections: &mut HashMap<String, Vec<String>>,
        user_id: &str,
        connection_id: &str,
    ) -> bool {
        let Some(conn_ids) = user_connections.get_mut(user_id) else {
            return false;
        };

        conn_ids.retain(|id| id != connection_id);
        if conn_ids.is_empty() {
            user_connections.remove(user_id);
            true
        } else {
            false
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
        self.add_connection_with_limit(
            connection_id,
            connection,
            user_id,
            requires_auth,
            usize::MAX,
        )
    }

    /// 添加连接，并在同一个写锁临界区内检查容量。
    ///
    /// 这个入口用于传输层新连接注册，避免先 `connection_count` 再 `add_connection`
    /// 在高并发握手完成时产生超额注册。
    pub fn add_connection_with_limit(
        &self,
        connection_id: String,
        connection: Box<dyn Connection>,
        user_id: Option<String>,
        requires_auth: bool,
        max_connections: usize,
    ) -> Result<()> {
        self.reserve_connection_slot(max_connections)?;

        let mut shard = match self.connection_shard(&connection_id).write() {
            Ok(shard) => shard,
            Err(_) => {
                self.release_connection_slot();
                return Err(FlareError::general_error("Failed to lock connection shard"));
            }
        };

        if shard.contains_key(&connection_id) {
            self.release_connection_slot();
            return Err(FlareError::protocol_error(format!(
                "Connection {} already exists",
                connection_id
            )));
        }

        let mut info = ConnectionInfo::new(connection_id.clone(), requires_auth);
        info.user_id = user_id.clone();

        let connection = Arc::new(Mutex::new(connection));
        let entry = self.new_connection_entry(&connection_id, connection, info);
        shard.insert(connection_id.clone(), entry);

        // 如果提供了用户 ID，添加到用户连接映射
        if let Some(user_id) = user_id
            && let Err(err) = self.insert_user_connection(user_id, &connection_id)
        {
            shard.remove(&connection_id);
            self.release_connection_slot();
            return Err(err);
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
        let mut shard = self
            .connection_shard(connection_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connection shard"))?;

        let (_, writer, info) = shard.remove(connection_id).ok_or_else(|| {
            FlareError::protocol_error(format!("Connection {} not found", connection_id))
        })?;
        writer.close_underlying_in_background();
        self.release_connection_slot();

        // 如果连接关联了用户，从用户连接映射中移除
        if let Some(user_id) = info.user_id {
            self.remove_user_connection(&user_id, connection_id)?;
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
    ) -> Option<(ConnectionHandle, ConnectionInfo)> {
        let shard = self.connection_shard(connection_id).read().ok()?;
        let (conn, _, info) = shard.get(connection_id)?;
        let conn_clone = Arc::clone(conn);
        let info_clone = info.clone();
        drop(shard);
        Some((conn_clone, info_clone))
    }

    fn get_connection_snapshot(&self, connection_id: &str) -> Option<ConnectionSnapshot> {
        let shard = self.connection_shard(connection_id).read().ok()?;
        let (_, writer, info) = shard.get(connection_id)?;
        Some((connection_id.to_string(), Arc::clone(writer), info.clone()))
    }

    fn connection_handles(&self) -> Vec<ConnectionHandleSnapshot> {
        self.connection_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .ok()
                    .map(|connections| {
                        connections
                            .iter()
                            .map(|(id, (_, writer, _))| (id.clone(), Arc::clone(writer)))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    fn connection_handles_except(
        &self,
        exclude_connection_id: &str,
    ) -> Vec<ConnectionHandleSnapshot> {
        self.connection_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .ok()
                    .map(|connections| {
                        connections
                            .iter()
                            .filter(|(id, _)| id.as_str() != exclude_connection_id)
                            .map(|(id, (_, writer, _))| (id.clone(), Arc::clone(writer)))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    fn connection_handles_for_ids(
        &self,
        connection_ids: Vec<String>,
    ) -> Vec<ConnectionHandleSnapshot> {
        let mut ids_by_shard = vec![Vec::new(); self.connection_shards.len()];
        for connection_id in connection_ids {
            let shard_index = self.connection_shard_index(&connection_id);
            ids_by_shard[shard_index].push(connection_id);
        }

        ids_by_shard
            .into_iter()
            .enumerate()
            .flat_map(|(shard_index, ids)| {
                if ids.is_empty() {
                    return Vec::new();
                }

                self.connection_shards[shard_index]
                    .read()
                    .ok()
                    .map(|connections| {
                        ids.into_iter()
                            .filter_map(|id| {
                                connections
                                    .get(&id)
                                    .map(|(_, writer, _)| (id, Arc::clone(writer)))
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    fn connection_auth_snapshots(&self) -> Vec<ConnectionAuthSnapshot> {
        self.connection_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .ok()
                    .map(|connections| {
                        connections
                            .iter()
                            .map(|(id, (_, writer, info))| {
                                (id.clone(), Arc::clone(writer), info.authenticated)
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    fn connection_auth_snapshots_except(
        &self,
        exclude_connection_id: &str,
    ) -> Vec<ConnectionAuthSnapshot> {
        self.connection_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .ok()
                    .map(|connections| {
                        connections
                            .iter()
                            .filter(|(id, _)| id.as_str() != exclude_connection_id)
                            .map(|(id, (_, writer, info))| {
                                (id.clone(), Arc::clone(writer), info.authenticated)
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    fn connection_auth_snapshots_for_ids(
        &self,
        connection_ids: Vec<String>,
    ) -> Vec<ConnectionAuthSnapshot> {
        let mut ids_by_shard = vec![Vec::new(); self.connection_shards.len()];
        for connection_id in connection_ids {
            let shard_index = self.connection_shard_index(&connection_id);
            ids_by_shard[shard_index].push(connection_id);
        }

        ids_by_shard
            .into_iter()
            .enumerate()
            .flat_map(|(shard_index, ids)| {
                if ids.is_empty() {
                    return Vec::new();
                }

                self.connection_shards[shard_index]
                    .read()
                    .ok()
                    .map(|connections| {
                        ids.into_iter()
                            .filter_map(|id| {
                                connections.get(&id).map(|(_, writer, info)| {
                                    (id, Arc::clone(writer), info.authenticated)
                                })
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    fn connection_snapshots(&self) -> Vec<ConnectionSnapshot> {
        self.connection_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .ok()
                    .map(|connections| {
                        connections
                            .iter()
                            .map(|(id, (_, writer, info))| {
                                (id.clone(), Arc::clone(writer), info.clone())
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    fn connection_snapshots_except(&self, exclude_connection_id: &str) -> Vec<ConnectionSnapshot> {
        self.connection_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .ok()
                    .map(|connections| {
                        connections
                            .iter()
                            .filter(|(id, _)| id.as_str() != exclude_connection_id)
                            .map(|(id, (_, writer, info))| {
                                (id.clone(), Arc::clone(writer), info.clone())
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    fn connection_snapshots_for_ids(&self, connection_ids: Vec<String>) -> Vec<ConnectionSnapshot> {
        let mut ids_by_shard = vec![Vec::new(); self.connection_shards.len()];
        for connection_id in connection_ids {
            let shard_index = self.connection_shard_index(&connection_id);
            ids_by_shard[shard_index].push(connection_id);
        }

        ids_by_shard
            .into_iter()
            .enumerate()
            .flat_map(|(shard_index, ids)| {
                if ids.is_empty() {
                    return Vec::new();
                }

                self.connection_shards[shard_index]
                    .read()
                    .ok()
                    .map(|connections| {
                        ids.into_iter()
                            .filter_map(|id| {
                                connections
                                    .get(&id)
                                    .map(|(_, writer, info)| (id, Arc::clone(writer), info.clone()))
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    fn timeout_connection_snapshots(&self, timeout: Duration) -> Vec<TimeoutConnectionSnapshot> {
        self.connection_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .ok()
                    .map(|connections| {
                        connections
                            .iter()
                            .filter(|(_, (_, _, info))| info.is_timeout(timeout))
                            .map(|(id, (connection, _, info))| {
                                (id.clone(), Arc::clone(connection), info.user_id.clone())
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }

    fn remove_connection_snapshots<I>(&self, connection_ids: I) -> Vec<String>
    where
        I: IntoIterator<Item = String>,
    {
        let mut ids_by_shard = vec![Vec::new(); self.connection_shards.len()];
        for connection_id in connection_ids {
            let shard_index = self.connection_shard_index(&connection_id);
            ids_by_shard[shard_index].push(connection_id);
        }

        let mut removed_ids = Vec::new();
        let mut removed_user_connections = Vec::new();

        for (shard_index, ids) in ids_by_shard.into_iter().enumerate() {
            if ids.is_empty() {
                continue;
            }

            let Ok(mut shard) = self.connection_shards[shard_index].write() else {
                continue;
            };

            for connection_id in ids {
                if let Some((_, writer, info)) = shard.remove(&connection_id) {
                    writer.close_underlying_in_background();
                    if let Some(user_id) = info.user_id {
                        removed_user_connections.push((user_id, connection_id.clone()));
                    }
                    removed_ids.push(connection_id);
                }
            }
        }

        if !removed_ids.is_empty() {
            self.connection_count
                .fetch_sub(removed_ids.len(), Ordering::Relaxed);
        }

        self.remove_user_connections_batch(removed_user_connections);

        removed_ids
    }

    fn remove_user_connections_batch<I>(&self, user_connections: I)
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let mut entries_by_shard = vec![Vec::new(); self.user_connection_shards.len()];
        for (user_id, connection_id) in user_connections {
            let shard_index = self.user_connection_shard_index(&user_id);
            entries_by_shard[shard_index].push((user_id, connection_id));
        }

        let removed_users = entries_by_shard
            .into_iter()
            .enumerate()
            .map(|(shard_index, entries)| {
                if entries.is_empty() {
                    return 0;
                }

                let Ok(mut shard) = self.user_connection_shards[shard_index].write() else {
                    return 0;
                };

                entries
                    .into_iter()
                    .filter(|(user_id, connection_id)| {
                        Self::remove_user_connection_index(&mut shard, user_id, connection_id)
                    })
                    .count()
            })
            .sum::<usize>();

        if removed_users > 0 {
            self.user_count.fetch_sub(removed_users, Ordering::Relaxed);
        }
    }

    /// 获取用户的所有连接
    ///
    /// # 参数
    /// - `user_id`: 用户 ID
    ///
    /// # 返回
    /// 该用户的所有连接 ID 列表
    pub fn get_user_connections(&self, user_id: &str) -> Vec<String> {
        self.user_connection_shard(user_id)
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
        let mut shard = self
            .connection_shard(connection_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connection shard"))?;

        let (_, _, info) = shard.get_mut(connection_id).ok_or_else(|| {
            FlareError::protocol_error(format!("Connection {} not found", connection_id))
        })?;

        // 如果之前有用户 ID，先移除旧映射
        if let Some(old_user_id) = &info.user_id {
            self.remove_user_connection(old_user_id, connection_id)?;
        }

        // 更新用户 ID
        info.user_id = Some(user_id.clone());

        // 添加到新用户映射
        self.insert_user_connection(user_id, connection_id)?;

        Ok(())
    }

    /// 更新连接的最后活跃时间
    pub fn update_connection_active(&self, connection_id: &str) -> Result<()> {
        let mut shard = self
            .connection_shard(connection_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connection shard"))?;

        if let Some((_, _, info)) = shard.get_mut(connection_id) {
            info.update_active();
            drop(shard);
            Ok(())
        } else {
            drop(shard);
            Err(FlareError::protocol_error(format!(
                "Connection {} not found",
                connection_id
            )))
        }
    }

    fn update_connections_active<I>(&self, connection_ids: I) -> usize
    where
        I: IntoIterator<Item = String>,
    {
        let now = Instant::now();
        let mut ids_by_shard = vec![Vec::new(); self.connection_shards.len()];
        for connection_id in connection_ids {
            let shard_index = self.connection_shard_index(&connection_id);
            ids_by_shard[shard_index].push(connection_id);
        }

        ids_by_shard
            .into_iter()
            .enumerate()
            .map(|(shard_index, ids)| {
                if ids.is_empty() {
                    return 0;
                }

                let Ok(mut shard) = self.connection_shards[shard_index].write() else {
                    return 0;
                };

                let mut updated = 0;
                for connection_id in ids {
                    if let Some((_, _, info)) = shard.get_mut(&connection_id) {
                        info.last_active = now;
                        updated += 1;
                    }
                }
                updated
            })
            .sum()
    }

    fn record_successful_connection_id(
        successful_ids: &Arc<std::sync::Mutex<Vec<String>>>,
        connection_id: String,
    ) {
        if let Ok(mut ids) = successful_ids.lock() {
            ids.push(connection_id);
        }
    }

    fn take_successful_connection_ids(
        successful_ids: Arc<std::sync::Mutex<Vec<String>>>,
    ) -> Vec<String> {
        Arc::try_unwrap(successful_ids)
            .ok()
            .and_then(|ids| ids.into_inner().ok())
            .unwrap_or_default()
    }

    /// 设置连接为已验证状态
    pub fn set_connection_authenticated(
        &self,
        connection_id: &str,
        user_id: Option<String>,
    ) -> Result<()> {
        let mut shard = self
            .connection_shard(connection_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connection shard"))?;

        let (_, _, info) = shard.get_mut(connection_id).ok_or_else(|| {
            FlareError::protocol_error(format!("Connection {} not found", connection_id))
        })?;

        // 保存旧的 user_id（在调用 set_authenticated 之前）
        let old_user_id = info.user_id.clone();

        let final_user_id = user_id.or(old_user_id.clone());

        // 设置认证状态（如果 final_user_id 是 Some，会设置 user_id）
        info.set_authenticated(final_user_id.clone());

        // 如果有 user_id（传入的或已存在的），确保用户连接映射正确
        if let Some(user_id) = final_user_id {
            // 如果 user_id 发生变化，需要更新映射
            let user_id_changed = old_user_id
                .as_ref()
                .map(|old| old != &user_id)
                .unwrap_or(true);

            if user_id_changed {
                // 如果之前有旧用户 ID，先移除旧映射
                if let Some(old_user_id) = old_user_id {
                    self.remove_user_connection(&old_user_id, connection_id)?;
                }

                // 添加新映射（检查是否已存在，避免重复）
                self.insert_user_connection(user_id, connection_id)?;
            } else {
                // user_id 没有变化，只需确保映射存在
                self.insert_user_connection(user_id, connection_id)?;
            }
        }

        Ok(())
    }

    /// 更新连接的协商信息（设备信息、序列化格式、压缩算法）
    #[allow(clippy::too_many_arguments)]
    pub fn update_connection_negotiation(
        &self,
        connection_id: &str,
        device_info: Option<crate::common::device::DeviceInfo>,
        serialization_format: crate::common::protocol::SerializationFormat,
        compression: crate::common::compression::CompressionAlgorithm,
        encryption: crate::common::encryption::EncryptionAlgorithm,
        user_id: Option<String>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<()> {
        let mut shard = self
            .connection_shard(connection_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connection shard"))?;

        let (_, _, info) = shard.get_mut(connection_id).ok_or_else(|| {
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
            if let Some(old_user_id) = old_user_id
                && old_user_id != user_id_val
            {
                self.remove_user_connection(&old_user_id, connection_id)?;
            }

            // 更新用户 ID
            info.user_id = Some(user_id_val.clone());

            // 添加到新用户映射
            self.insert_user_connection(user_id_val, connection_id)?;
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
        let mut shard = self
            .connection_shard(connection_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connection shard"))?;

        let (_, _, info) = shard.get_mut(connection_id).ok_or_else(|| {
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
            if let Some(old_user_id) = old_user_id
                && old_user_id != user_id_val
            {
                self.remove_user_connection(&old_user_id, connection_id)?;
            }

            // 更新用户 ID
            info.user_id = Some(user_id_val.clone());

            // 添加到新用户映射
            self.insert_user_connection(user_id_val, connection_id)?;
        }

        Ok(())
    }

    /// 标记协商已确认（客户端收到 CONNECT_ACK 后发送确认）
    pub fn mark_negotiation_confirmed(&self, connection_id: &str) -> Result<()> {
        let mut shard = self
            .connection_shard(connection_id)
            .write()
            .map_err(|_| FlareError::general_error("Failed to lock connection shard"))?;

        let (_, _, info) = shard.get_mut(connection_id).ok_or_else(|| {
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
        self.connection_shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .ok()
                    .map(|connections| connections.keys().cloned().collect::<Vec<_>>())
                    .unwrap_or_default()
            })
            .collect()
    }

    /// 获取连接总数
    pub fn connection_count(&self) -> usize {
        self.connection_count.load(Ordering::Relaxed)
    }

    /// 获取当前绑定了连接的用户数
    pub fn user_count(&self) -> usize {
        self.user_count.load(Ordering::Relaxed)
    }

    /// 清理超时连接
    ///
    /// # 参数
    /// - `timeout`: 超时时间
    ///
    /// # 返回
    /// 被清理的连接 ID 列表
    pub fn cleanup_timeout_connections(&self, timeout: Duration) -> Vec<String> {
        let timeout_connections = self.timeout_connection_snapshots(timeout);
        self.remove_connection_snapshots(
            timeout_connections
                .iter()
                .map(|(connection_id, _, _)| connection_id.clone()),
        )
    }

    /// 获取连接统计信息
    pub fn stats(&self) -> TraitConnectionStats {
        let total_connections = self.connection_count();
        let total_users = self.user_count();

        TraitConnectionStats {
            total_connections,
            total_users,
        }
    }

    fn frame_allowed_before_auth(frame: &crate::common::protocol::Frame) -> bool {
        frame
            .command
            .as_ref()
            .and_then(|cmd| {
                if let Some(crate::common::protocol::flare::core::commands::command::Type::System(
                    sys_cmd,
                )) = &cmd.r#type
                {
                    Some(
                        sys_cmd.r#type
                            == crate::common::protocol::flare::core::commands::system_command::Type::ConnectAck
                                as i32
                            || sys_cmd.r#type
                                == crate::common::protocol::flare::core::commands::system_command::Type::Ping
                                    as i32
                            || sys_cmd.r#type
                                == crate::common::protocol::flare::core::commands::system_command::Type::Pong
                                    as i32
                            || sys_cmd.r#type
                                == crate::common::protocol::flare::core::commands::system_command::Type::Error
                                    as i32
                            || sys_cmd.r#type
                                == crate::common::protocol::flare::core::commands::system_command::Type::Close
                                    as i32,
                    )
                } else {
                    None
                }
            })
            .unwrap_or(false)
    }

    fn serialize_frame_for_connection(
        connection_id: &str,
        info: &ConnectionInfo,
        frame: &crate::common::protocol::Frame,
        parser: Option<&crate::common::MessageParser>,
    ) -> Result<Vec<u8>> {
        Self::ensure_frame_allowed_for_connection(connection_id, info, frame)?;

        if let Some(parser) = parser {
            return parser.serialize(frame);
        }

        if let Some(parser) = &info.cached_parser {
            return parser.serialize(frame);
        }

        crate::common::MessageParser::new(
            info.serialization_format,
            info.compression.clone(),
            info.encryption.clone(),
        )
        .serialize(frame)
    }

    fn ensure_frame_allowed_for_connection(
        connection_id: &str,
        info: &ConnectionInfo,
        frame: &crate::common::protocol::Frame,
    ) -> Result<()> {
        if info.authenticated || Self::frame_allowed_before_auth(frame) {
            return Ok(());
        }

        Err(FlareError::authentication_failed(format!(
            "连接 {} 未验证，无法发送消息",
            connection_id
        )))
    }

    async fn send_to_connection_handle(
        &self,
        connection_id: &str,
        connection: ConnectionWriteHandle,
        data: &[u8],
    ) -> Result<()> {
        match connection.try_enqueue(data) {
            Ok(()) => Ok(()),
            Err(err) => {
                connection.close_underlying_in_background();
                let _ = ConnectionManager::remove_connection(self, connection_id);
                Err(err)
            }
        }
    }

    async fn send_frame_to_snapshot(
        &self,
        snapshot: ConnectionSnapshot,
        frame: &crate::common::protocol::Frame,
        parser: Option<&crate::common::MessageParser>,
    ) -> Result<()> {
        let connection_id = self
            .send_frame_to_snapshot_without_active(snapshot, frame, parser)
            .await?;
        ConnectionManager::update_connection_active(self, &connection_id)?;
        Ok(())
    }

    async fn send_frame_to_snapshot_without_active(
        &self,
        snapshot: ConnectionSnapshot,
        frame: &crate::common::protocol::Frame,
        parser: Option<&crate::common::MessageParser>,
    ) -> Result<String> {
        let (connection_id, connection, info) = snapshot;
        let data = Self::serialize_frame_for_connection(&connection_id, &info, frame, parser)?;

        self.send_to_connection_handle(&connection_id, connection, &data)
            .await?;
        Ok(connection_id)
    }

    async fn send_serialized_frame_to_auth_snapshot_without_active(
        &self,
        snapshot: ConnectionAuthSnapshot,
        frame: &crate::common::protocol::Frame,
        data: &[u8],
    ) -> Result<String> {
        let (connection_id, connection, authenticated) = snapshot;
        if !authenticated && !Self::frame_allowed_before_auth(frame) {
            return Err(FlareError::authentication_failed(format!(
                "连接 {} 未验证，无法发送消息",
                connection_id
            )));
        }

        self.send_to_connection_handle(&connection_id, connection, data)
            .await?;
        Ok(connection_id)
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
        self.reserve_connection_slot(usize::MAX)?;

        let mut shard = match self.connection_shard(&connection_id).write() {
            Ok(shard) => shard,
            Err(_) => {
                self.release_connection_slot();
                return Err(FlareError::general_error("Failed to lock connection shard"));
            }
        };

        if shard.contains_key(&connection_id) {
            self.release_connection_slot();
            return Err(FlareError::protocol_error(format!(
                "Connection {} already exists",
                connection_id
            )));
        }

        let mut info = ConnectionInfo::new(connection_id.clone(), requires_auth);
        info.user_id = user_id.clone();

        let entry = self.new_connection_entry(&connection_id, Arc::clone(&connection), info);
        shard.insert(connection_id.clone(), entry);

        // 如果提供了用户 ID，添加到用户连接映射
        if let Some(user_id) = user_id
            && let Err(err) = self.insert_user_connection(user_id, &connection_id)
        {
            shard.remove(&connection_id);
            self.release_connection_slot();
            return Err(err);
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
        let timeout_connections = self.timeout_connection_snapshots(timeout);

        for (_, connection, _) in &timeout_connections {
            let mut conn = connection.lock().await;
            let _ = conn.close().await;
        }

        self.remove_connection_snapshots(
            timeout_connections
                .iter()
                .map(|(connection_id, _, _)| connection_id.clone()),
        )
    }

    async fn send_to_connection(&self, connection_id: &str, data: &[u8]) -> Result<()> {
        let (_, connection, _) = self.get_connection_snapshot(connection_id).ok_or_else(|| {
            FlareError::protocol_error(format!("Connection {} not found", connection_id))
        })?;

        self.send_to_connection_handle(connection_id, connection, data)
            .await
    }

    async fn send_to_user(&self, user_id: &str, data: &[u8]) -> Result<()> {
        let connections =
            self.connection_handles_for_ids(ConnectionManager::get_user_connections(self, user_id));

        stream::iter(connections)
            .for_each_concurrent(
                self.fanout_concurrency,
                |(connection_id, connection)| async move {
                    if let Err(e) = self
                        .send_to_connection_handle(&connection_id, connection, data)
                        .await
                    {
                        tracing::warn!("Failed to send to connection {}: {:?}", connection_id, e);
                    }
                },
            )
            .await;

        Ok(())
    }

    async fn broadcast(&self, data: &[u8]) -> Result<()> {
        let connections = self.connection_handles();

        stream::iter(connections)
            .for_each_concurrent(
                self.fanout_concurrency,
                |(connection_id, connection)| async move {
                    if let Err(e) = self
                        .send_to_connection_handle(&connection_id, connection, data)
                        .await
                    {
                        tracing::warn!(
                            "Failed to broadcast to connection {}: {:?}",
                            connection_id,
                            e
                        );
                    }
                },
            )
            .await;

        Ok(())
    }

    async fn broadcast_except(&self, data: &[u8], exclude_connection_id: &str) -> Result<()> {
        let connections = self.connection_handles_except(exclude_connection_id);

        stream::iter(connections)
            .for_each_concurrent(
                self.fanout_concurrency,
                |(connection_id, connection)| async move {
                    if let Err(e) = self
                        .send_to_connection_handle(&connection_id, connection, data)
                        .await
                    {
                        tracing::warn!(
                            "Failed to broadcast to connection {}: {:?}",
                            connection_id,
                            e
                        );
                    }
                },
            )
            .await;

        Ok(())
    }

    async fn send_frame_to(
        &self,
        connection_id: &str,
        frame: &crate::common::protocol::Frame,
        parser: Option<&crate::common::MessageParser>,
    ) -> Result<()> {
        let snapshot = self.get_connection_snapshot(connection_id).ok_or_else(|| {
            FlareError::connection_failed(format!("连接 {} 不存在", connection_id))
        })?;

        self.send_frame_to_snapshot(snapshot, frame, parser).await
    }

    async fn send_frame_to_user(
        &self,
        user_id: &str,
        frame: &crate::common::protocol::Frame,
        parser: Option<&crate::common::MessageParser>,
    ) -> Result<()> {
        let connection_ids = ConnectionManager::get_user_connections(self, user_id);

        if let Some(parser) = parser {
            let connections = self.connection_auth_snapshots_for_ids(connection_ids);
            let data = match parser.serialize(frame) {
                Ok(data) => data,
                Err(e) => {
                    tracing::warn!("Failed to serialize frame for user {}: {:?}", user_id, e);
                    return Ok(());
                }
            };

            let successful_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
            stream::iter(connections)
                .for_each_concurrent(self.fanout_concurrency, |snapshot| {
                    let successful_ids = Arc::clone(&successful_ids);
                    let data = data.as_slice();
                    async move {
                        let connection_id = snapshot.0.clone();
                        let result = self
                            .send_serialized_frame_to_auth_snapshot_without_active(
                                snapshot, frame, data,
                            )
                            .await;
                        match result {
                            Ok(connection_id) => {
                                Self::record_successful_connection_id(
                                    &successful_ids,
                                    connection_id,
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to send frame to connection {}: {:?}",
                                    connection_id,
                                    e
                                );
                            }
                        }
                    }
                })
                .await;
            self.update_connections_active(Self::take_successful_connection_ids(successful_ids));

            return Ok(());
        }

        let connections = self.connection_snapshots_for_ids(connection_ids);
        let successful_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
        stream::iter(connections)
            .for_each_concurrent(self.fanout_concurrency, |snapshot| {
                let successful_ids = Arc::clone(&successful_ids);
                async move {
                    let connection_id = snapshot.0.clone();
                    let result = self
                        .send_frame_to_snapshot_without_active(snapshot, frame, parser)
                        .await;
                    match result {
                        Ok(connection_id) => {
                            Self::record_successful_connection_id(&successful_ids, connection_id);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to send frame to connection {}: {:?}",
                                connection_id,
                                e
                            );
                        }
                    }
                }
            })
            .await;
        self.update_connections_active(Self::take_successful_connection_ids(successful_ids));

        Ok(())
    }

    async fn broadcast_frame(
        &self,
        frame: &crate::common::protocol::Frame,
        parser: Option<&crate::common::MessageParser>,
    ) -> Result<()> {
        if let Some(parser) = parser {
            let connections = self.connection_auth_snapshots();
            let data = match parser.serialize(frame) {
                Ok(data) => data,
                Err(e) => {
                    tracing::warn!("Failed to serialize broadcast frame: {:?}", e);
                    return Ok(());
                }
            };

            let successful_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
            stream::iter(connections)
                .for_each_concurrent(self.fanout_concurrency, |snapshot| {
                    let successful_ids = Arc::clone(&successful_ids);
                    let data = data.as_slice();
                    async move {
                        let connection_id = snapshot.0.clone();
                        let result = self
                            .send_serialized_frame_to_auth_snapshot_without_active(
                                snapshot, frame, data,
                            )
                            .await;
                        match result {
                            Ok(connection_id) => {
                                Self::record_successful_connection_id(
                                    &successful_ids,
                                    connection_id,
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to broadcast frame to connection {}: {:?}",
                                    connection_id,
                                    e
                                );
                            }
                        }
                    }
                })
                .await;
            self.update_connections_active(Self::take_successful_connection_ids(successful_ids));

            return Ok(());
        }

        let connections = self.connection_snapshots();
        let successful_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
        stream::iter(connections)
            .for_each_concurrent(self.fanout_concurrency, |snapshot| {
                let successful_ids = Arc::clone(&successful_ids);
                async move {
                    let connection_id = snapshot.0.clone();
                    let result = self
                        .send_frame_to_snapshot_without_active(snapshot, frame, parser)
                        .await;
                    match result {
                        Ok(connection_id) => {
                            Self::record_successful_connection_id(&successful_ids, connection_id);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to broadcast frame to connection {}: {:?}",
                                connection_id,
                                e
                            );
                        }
                    }
                }
            })
            .await;
        self.update_connections_active(Self::take_successful_connection_ids(successful_ids));

        Ok(())
    }

    async fn broadcast_frame_except(
        &self,
        frame: &crate::common::protocol::Frame,
        exclude_connection_id: &str,
        parser: Option<&crate::common::MessageParser>,
    ) -> Result<()> {
        if let Some(parser) = parser {
            let connections = self.connection_auth_snapshots_except(exclude_connection_id);
            let data = match parser.serialize(frame) {
                Ok(data) => data,
                Err(e) => {
                    tracing::warn!("Failed to serialize broadcast frame: {:?}", e);
                    return Ok(());
                }
            };

            let successful_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
            stream::iter(connections)
                .for_each_concurrent(self.fanout_concurrency, |snapshot| {
                    let successful_ids = Arc::clone(&successful_ids);
                    let data = data.as_slice();
                    async move {
                        let connection_id = snapshot.0.clone();
                        let result = self
                            .send_serialized_frame_to_auth_snapshot_without_active(
                                snapshot, frame, data,
                            )
                            .await;
                        match result {
                            Ok(connection_id) => {
                                Self::record_successful_connection_id(
                                    &successful_ids,
                                    connection_id,
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to broadcast frame to connection {}: {:?}",
                                    connection_id,
                                    e
                                );
                            }
                        }
                    }
                })
                .await;
            self.update_connections_active(Self::take_successful_connection_ids(successful_ids));

            return Ok(());
        }

        let connections = self.connection_snapshots_except(exclude_connection_id);
        let successful_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
        stream::iter(connections)
            .for_each_concurrent(self.fanout_concurrency, |snapshot| {
                let successful_ids = Arc::clone(&successful_ids);
                async move {
                    let connection_id = snapshot.0.clone();
                    let result = self
                        .send_frame_to_snapshot_without_active(snapshot, frame, parser)
                        .await;
                    match result {
                        Ok(connection_id) => {
                            Self::record_successful_connection_id(&successful_ids, connection_id);
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to broadcast frame to connection {}: {:?}",
                                connection_id,
                                e
                            );
                        }
                    }
                }
            })
            .await;
        self.update_connections_active(Self::take_successful_connection_ids(successful_ids));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::serializer::{SerializationUtil, Serializer};
    use crate::transport::connection::Connection;
    use crate::transport::events::ArcObserver;
    use async_trait::async_trait;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    struct MockConnection {
        last_active: Mutex<Instant>,
        closed: Arc<AtomicBool>,
        send_delay: Option<Duration>,
        send_count: Arc<AtomicUsize>,
    }

    impl MockConnection {
        fn new() -> Self {
            Self {
                last_active: Mutex::new(Instant::now()),
                closed: Arc::new(AtomicBool::new(false)),
                send_delay: None,
                send_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn with_closed_flag(closed: Arc<AtomicBool>) -> Self {
            Self {
                last_active: Mutex::new(Instant::now()),
                closed,
                send_delay: None,
                send_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn with_send_probe(send_delay: Duration, send_count: Arc<AtomicUsize>) -> Self {
            Self {
                last_active: Mutex::new(Instant::now()),
                closed: Arc::new(AtomicBool::new(false)),
                send_delay: Some(send_delay),
                send_count,
            }
        }
    }

    struct CountingSerializer {
        serialize_count: Arc<AtomicUsize>,
    }

    impl Serializer for CountingSerializer {
        fn serialize(&self, _frame: &crate::common::protocol::Frame) -> Result<Vec<u8>> {
            self.serialize_count.fetch_add(1, Ordering::SeqCst);
            Ok(b"counting-serializer-frame".to_vec())
        }

        fn deserialize(&self, _data: &[u8]) -> Result<crate::common::protocol::Frame> {
            Ok(crate::common::protocol::Frame::default())
        }

        fn format(&self) -> crate::common::protocol::SerializationFormat {
            crate::common::protocol::SerializationFormat::Protobuf
        }

        fn name(&self) -> &'static str {
            "connection_manager_broadcast_frame_counting_serializer"
        }
    }

    #[async_trait]
    impl Connection for MockConnection {
        fn add_observer(&mut self, _observer: ArcObserver) {}
        fn remove_observer(&mut self, _observer: ArcObserver) {}
        async fn send(&mut self, _data: &[u8]) -> Result<()> {
            self.send_count.fetch_add(1, Ordering::SeqCst);
            if let Some(delay) = self.send_delay {
                tokio::time::sleep(delay).await;
            }
            Ok(())
        }
        async fn close(&mut self) -> Result<()> {
            self.closed.store(true, Ordering::SeqCst);
            Ok(())
        }
        fn last_active_time(&self) -> Instant {
            *self.last_active.lock().unwrap()
        }
        fn update_active_time(&mut self) {
            *self.last_active.lock().unwrap() = Instant::now();
        }
    }

    async fn wait_for_send_count(send_count: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_millis(500), async {
            while send_count.load(Ordering::SeqCst) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "timed out waiting for send_count to reach {expected}; current={}",
                send_count.load(Ordering::SeqCst)
            )
        });
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
        assert_eq!(manager.user_count(), 1);

        manager.remove_connection("conn1").unwrap();
        assert_eq!(manager.user_count(), 0);
    }

    #[test]
    fn test_same_user_multiple_connections_count_as_one_user() {
        let manager = ConnectionManager::new();

        manager
            .add_connection(
                "conn1".to_string(),
                Box::new(MockConnection::new()),
                Some("user1".to_string()),
                false,
            )
            .unwrap();
        manager
            .add_connection(
                "conn2".to_string(),
                Box::new(MockConnection::new()),
                Some("user1".to_string()),
                false,
            )
            .unwrap();

        assert_eq!(manager.user_count(), 1);

        manager.remove_connection("conn1").unwrap();
        assert_eq!(manager.user_count(), 1);

        manager.remove_connection("conn2").unwrap();
        assert_eq!(manager.user_count(), 0);
    }

    #[test]
    fn test_add_connection_with_limit_rejects_when_capacity_full() {
        let manager = ConnectionManager::new();

        manager
            .add_connection_with_limit(
                "conn1".to_string(),
                Box::new(MockConnection::new()),
                None,
                false,
                1,
            )
            .unwrap();

        let result = manager.add_connection_with_limit(
            "conn2".to_string(),
            Box::new(MockConnection::new()),
            None,
            false,
            1,
        );

        assert!(result.is_err());
        assert_eq!(manager.connection_count(), 1);
        assert!(manager.get_connection("conn2").is_none());
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
        assert_eq!(manager.connection_count(), 0);
    }

    #[test]
    fn timeout_connection_snapshots_keep_handles_after_connection_shards_are_locked() {
        let manager = ConnectionManager::new();

        for idx in 0..2 {
            manager
                .add_connection(
                    format!("conn{idx}"),
                    Box::new(MockConnection::new()),
                    Some(format!("user{idx}")),
                    false,
                )
                .unwrap();
        }

        std::thread::sleep(Duration::from_millis(10));

        let snapshots = manager.timeout_connection_snapshots(Duration::from_millis(5));
        assert_eq!(snapshots.len(), 2);

        let _shard_guards: Vec<_> = manager
            .connection_shards
            .iter()
            .map(|shard| shard.write().unwrap())
            .collect();

        let mut snapshot_ids: Vec<_> = snapshots
            .into_iter()
            .map(|(connection_id, connection, user_id)| {
                assert!(connection.try_lock().is_ok());
                assert!(user_id.is_some());
                connection_id
            })
            .collect();
        snapshot_ids.sort();

        assert_eq!(snapshot_ids, vec!["conn0", "conn1"]);
    }

    #[tokio::test]
    async fn trait_cleanup_timeout_connections_closes_underlying_connection() {
        let manager = ConnectionManager::new();
        let closed = Arc::new(AtomicBool::new(false));

        manager
            .add_connection(
                "conn1".to_string(),
                Box::new(MockConnection::with_closed_flag(Arc::clone(&closed))),
                None,
                false,
            )
            .unwrap();

        std::thread::sleep(Duration::from_millis(10));

        let cleaned =
            ConnectionManagerTrait::cleanup_timeout_connections(&manager, Duration::from_millis(5))
                .await;

        assert_eq!(cleaned, vec!["conn1".to_string()]);
        assert!(closed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn trait_broadcast_sends_to_connections_concurrently() {
        let manager = ConnectionManager::new();
        let send_count = Arc::new(AtomicUsize::new(0));

        for idx in 0..3 {
            manager
                .add_connection(
                    format!("conn{idx}"),
                    Box::new(MockConnection::with_send_probe(
                        Duration::from_millis(100),
                        Arc::clone(&send_count),
                    )),
                    None,
                    false,
                )
                .unwrap();
        }

        let started = Instant::now();
        ConnectionManagerTrait::broadcast(&manager, b"payload")
            .await
            .unwrap();
        let elapsed = started.elapsed();

        wait_for_send_count(&send_count, 3).await;
        assert_eq!(send_count.load(Ordering::SeqCst), 3);
        assert!(
            elapsed < Duration::from_millis(220),
            "broadcast should fan out concurrently; elapsed={elapsed:?}"
        );
    }

    #[tokio::test]
    async fn trait_broadcast_frame_sends_to_connections_concurrently() {
        let manager = ConnectionManager::new();
        let send_count = Arc::new(AtomicUsize::new(0));

        for idx in 0..3 {
            manager
                .add_connection(
                    format!("conn{idx}"),
                    Box::new(MockConnection::with_send_probe(
                        Duration::from_millis(100),
                        Arc::clone(&send_count),
                    )),
                    None,
                    false,
                )
                .unwrap();
        }

        let frame = crate::common::protocol::frame_with_system_command(
            crate::common::protocol::ping(),
            crate::common::protocol::Reliability::AtLeastOnce,
        );

        let started = Instant::now();
        ConnectionManagerTrait::broadcast_frame(&manager, &frame, None)
            .await
            .unwrap();
        let elapsed = started.elapsed();

        wait_for_send_count(&send_count, 3).await;
        assert_eq!(send_count.load(Ordering::SeqCst), 3);
        assert!(
            elapsed < Duration::from_millis(220),
            "broadcast_frame should fan out concurrently; elapsed={elapsed:?}"
        );
    }

    #[tokio::test]
    async fn broadcast_frame_with_explicit_parser_serializes_once() {
        let manager = ConnectionManager::new();
        let send_count = Arc::new(AtomicUsize::new(0));
        let serialize_count = Arc::new(AtomicUsize::new(0));

        SerializationUtil::register_custom(Arc::new(CountingSerializer {
            serialize_count: Arc::clone(&serialize_count),
        }));

        for idx in 0..3 {
            manager
                .add_connection(
                    format!("conn{idx}"),
                    Box::new(MockConnection::with_send_probe(
                        Duration::ZERO,
                        Arc::clone(&send_count),
                    )),
                    None,
                    false,
                )
                .unwrap();
        }

        let parser = crate::common::MessageParser::with_custom_format(
            "connection_manager_broadcast_frame_counting_serializer",
            crate::common::compression::CompressionAlgorithm::None,
            crate::common::encryption::EncryptionAlgorithm::None,
        );
        let frame = crate::common::protocol::frame_with_system_command(
            crate::common::protocol::ping(),
            crate::common::protocol::Reliability::AtLeastOnce,
        );

        ConnectionManagerTrait::broadcast_frame(&manager, &frame, Some(&parser))
            .await
            .unwrap();

        wait_for_send_count(&send_count, 3).await;
        assert_eq!(send_count.load(Ordering::SeqCst), 3);
        assert_eq!(
            serialize_count.load(Ordering::SeqCst),
            1,
            "broadcast_frame should serialize once when every recipient uses the explicit parser"
        );
    }

    #[tokio::test]
    async fn write_worker_times_out_and_removes_slow_connection() {
        let manager = ConnectionManager::with_send_timeout(Duration::from_millis(50));
        let send_count = Arc::new(AtomicUsize::new(0));

        manager
            .add_connection(
                "slow".to_string(),
                Box::new(MockConnection::with_send_probe(
                    Duration::from_millis(250),
                    Arc::clone(&send_count),
                )),
                None,
                false,
            )
            .unwrap();

        let started = Instant::now();
        let result = ConnectionManagerTrait::send_to_connection(&manager, "slow", b"payload").await;
        let elapsed = started.elapsed();

        assert!(result.is_ok());
        assert!(
            elapsed < Duration::from_millis(50),
            "send should enqueue without waiting for the slow socket write; elapsed={elapsed:?}"
        );
        wait_for_send_count(&send_count, 1).await;
        tokio::time::timeout(Duration::from_millis(500), async {
            while manager.get_connection("slow").is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("slow connection should be removed after writer timeout");
        assert!(manager.get_connection("slow").is_none());
        assert_eq!(manager.connection_count(), 0);
    }

    #[tokio::test]
    async fn bounded_write_queue_isolates_slow_consumer_without_waiting_for_socket_write() {
        let manager = Arc::new(ConnectionManager::with_write_queue_limits(
            Duration::from_secs(5),
            256,
            1,
        ));
        let send_count = Arc::new(AtomicUsize::new(0));

        manager
            .add_connection(
                "slow".to_string(),
                Box::new(MockConnection::with_send_probe(
                    Duration::from_millis(250),
                    Arc::clone(&send_count),
                )),
                None,
                false,
            )
            .unwrap();

        let first_manager = Arc::clone(&manager);
        let first_send = tokio::spawn(async move {
            ConnectionManagerTrait::send_to_connection(&*first_manager, "slow", b"first").await
        });

        let wait_started = tokio::time::timeout(Duration::from_millis(100), async {
            while send_count.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            wait_started.is_ok(),
            "first write should reach the underlying connection"
        );

        let second_started = Instant::now();
        let second = ConnectionManagerTrait::send_to_connection(&*manager, "slow", b"second").await;
        let second_elapsed = second_started.elapsed();

        let third_started = Instant::now();
        let third = ConnectionManagerTrait::send_to_connection(&*manager, "slow", b"third").await;
        let third_elapsed = third_started.elapsed();

        assert!(
            second.is_ok(),
            "one queued write should be accepted while the socket write is in flight"
        );
        assert!(
            second_elapsed < Duration::from_millis(50),
            "enqueue should not wait for the slow socket write; elapsed={second_elapsed:?}"
        );
        assert!(
            third.is_err(),
            "full queue should isolate the slow consumer"
        );
        assert!(
            third_elapsed < Duration::from_millis(50),
            "full queue detection should be immediate; elapsed={third_elapsed:?}"
        );
        assert!(manager.get_connection("slow").is_none());
        assert_eq!(manager.connection_count(), 0);

        let _ = first_send.await;
    }

    #[test]
    fn connection_snapshots_can_be_used_after_connection_shards_are_locked() {
        let manager = ConnectionManager::new();

        for idx in 0..2 {
            manager
                .add_connection(
                    format!("conn{idx}"),
                    Box::new(MockConnection::new()),
                    None,
                    false,
                )
                .unwrap();
        }

        let snapshots = manager.connection_snapshots();
        assert_eq!(snapshots.len(), 2);

        let _shard_guards: Vec<_> = manager
            .connection_shards
            .iter()
            .map(|shard| shard.write().unwrap())
            .collect();

        let mut snapshot_ids: Vec<_> = snapshots
            .into_iter()
            .map(|(connection_id, _writer, info)| {
                assert_eq!(info.connection_id, connection_id);
                connection_id
            })
            .collect();
        snapshot_ids.sort();

        assert_eq!(snapshot_ids, vec!["conn0", "conn1"]);
    }

    #[test]
    fn update_connections_active_batches_successful_connection_ids() {
        let manager = ConnectionManager::new();
        let old_active = Instant::now() - Duration::from_secs(60);

        for idx in 0..3 {
            let connection_id = format!("conn{idx}");
            manager
                .add_connection(
                    connection_id.clone(),
                    Box::new(MockConnection::new()),
                    None,
                    false,
                )
                .unwrap();

            let mut shard = manager.connection_shard(&connection_id).write().unwrap();
            let (_, _, info) = shard.get_mut(&connection_id).unwrap();
            info.last_active = old_active;
        }

        let updated = manager.update_connections_active([
            "conn0".to_string(),
            "conn1".to_string(),
            "missing".to_string(),
        ]);

        assert_eq!(updated, 2);
        assert!(manager.get_connection("conn0").unwrap().1.last_active > old_active);
        assert!(manager.get_connection("conn1").unwrap().1.last_active > old_active);
        assert_eq!(
            manager.get_connection("conn2").unwrap().1.last_active,
            old_active
        );
    }

    #[tokio::test]
    async fn configured_fanout_concurrency_limits_broadcast_parallelism() {
        let manager = ConnectionManager::with_limits(Duration::from_secs(10), 1);
        let send_count = Arc::new(AtomicUsize::new(0));

        for idx in 0..3 {
            manager
                .add_connection(
                    format!("conn{idx}"),
                    Box::new(MockConnection::with_send_probe(
                        Duration::from_millis(80),
                        Arc::clone(&send_count),
                    )),
                    None,
                    false,
                )
                .unwrap();
        }

        let started = Instant::now();
        ConnectionManagerTrait::broadcast(&manager, b"payload")
            .await
            .unwrap();
        let elapsed = started.elapsed();

        wait_for_send_count(&send_count, 3).await;
        assert_eq!(send_count.load(Ordering::SeqCst), 3);
        assert!(
            elapsed < Duration::from_millis(50),
            "broadcast should enqueue without waiting for slow socket writes; elapsed={elapsed:?}"
        );
    }

    #[test]
    fn connection_count_does_not_depend_on_connections_map_lock() {
        let manager = ConnectionManager::new();

        manager
            .add_connection(
                "conn1".to_string(),
                Box::new(MockConnection::new()),
                None,
                false,
            )
            .unwrap();

        let poison_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let shard = manager.connection_shard_index("conn1");
            let _guard = manager.connection_shards[shard].write().unwrap();
            panic!("poison connection shard lock");
        }));
        assert!(poison_result.is_err());

        assert_eq!(manager.connection_count(), 1);
    }

    #[test]
    fn stats_total_users_does_not_depend_on_user_connections_map_lock() {
        let manager = ConnectionManager::new();

        manager
            .add_connection(
                "conn1".to_string(),
                Box::new(MockConnection::new()),
                Some("user1".to_string()),
                false,
            )
            .unwrap();

        let poison_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = manager.user_connection_shard("user1").write().unwrap();
            panic!("poison user_connection shard lock");
        }));
        assert!(poison_result.is_err());

        assert_eq!(manager.stats().total_users, 1);
    }

    #[test]
    fn add_connection_releases_reserved_slot_when_shard_lock_fails() {
        let manager = ConnectionManager::new();
        let connection_id = "conn1";

        let poison_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = manager.connection_shard(connection_id).write().unwrap();
            panic!("poison connection shard lock");
        }));
        assert!(poison_result.is_err());

        let result = manager.add_connection(
            connection_id.to_string(),
            Box::new(MockConnection::new()),
            None,
            false,
        );

        assert!(result.is_err());
        assert_eq!(manager.connection_count(), 0);
    }

    #[test]
    fn add_connection_rolls_back_insert_when_user_index_lock_fails() {
        let manager = ConnectionManager::new();

        let poison_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = manager.user_connection_shard("user1").write().unwrap();
            panic!("poison user_connection shard lock");
        }));
        assert!(poison_result.is_err());

        let result = manager.add_connection(
            "conn1".to_string(),
            Box::new(MockConnection::new()),
            Some("user1".to_string()),
            false,
        );

        assert!(result.is_err());
        assert_eq!(manager.connection_count(), 0);
        assert!(manager.get_connection("conn1").is_none());
        assert_eq!(manager.user_count(), 0);
    }

    #[test]
    fn locked_connection_shard_does_not_block_other_shard_registration() {
        let manager = Arc::new(ConnectionManager::new());
        let locked_id = "locked-shard-connection";
        let locked_shard = manager.connection_shard_index(locked_id);
        let other_id = (0..10_000)
            .map(|idx| format!("other-shard-{idx}"))
            .find(|id| manager.connection_shard_index(id) != locked_shard)
            .expect("should find an id in a different shard");

        let _held_read_lock = manager.connection_shards[locked_shard].read().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let manager_clone = Arc::clone(&manager);

        std::thread::spawn(move || {
            let result = manager_clone.add_connection(
                other_id,
                Box::new(MockConnection::new()),
                None,
                false,
            );
            tx.send(result.is_ok()).unwrap();
        });

        assert!(rx.recv_timeout(Duration::from_millis(150)).unwrap());
    }

    #[test]
    fn locked_user_connection_shard_does_not_block_other_user_binding() {
        let manager = Arc::new(ConnectionManager::new());
        let locked_user_id = "locked-user";
        let locked_shard = manager.user_connection_shard_index(locked_user_id);
        let other_user_id = (0..10_000)
            .map(|idx| format!("other-user-{idx}"))
            .find(|id| manager.user_connection_shard_index(id) != locked_shard)
            .expect("should find a user id in a different shard");

        manager
            .add_connection(
                "conn1".to_string(),
                Box::new(MockConnection::new()),
                None,
                false,
            )
            .unwrap();

        let _held_read_lock = manager.user_connection_shards[locked_shard].read().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let manager_clone = Arc::clone(&manager);

        std::thread::spawn(move || {
            let result = manager_clone.bind_user("conn1", other_user_id);
            tx.send(result.is_ok()).unwrap();
        });

        assert!(rx.recv_timeout(Duration::from_millis(150)).unwrap());
    }
}
