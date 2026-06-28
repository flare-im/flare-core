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

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

mod lifecycle;
mod ops;
#[cfg(test)]
mod tests;
mod trait_impl;
