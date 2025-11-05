//! ServerHandle trait 及其默认实现
//! 
//! 提供消息发送和连接管理的轻量级接口

use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::server::connection::ConnectionManagerTrait;
use crate::common::MessageParser;
use async_trait::async_trait;
use std::sync::Arc;

/// 服务器操作处理器
/// 
/// 提供消息发送和连接管理的轻量级接口
/// 可以在任何需要发送消息或管理连接的地方注入此 trait，而不需要注入整个 Server
/// 
/// # 示例
/// 
/// ```rust
/// use flare_core::server::ServerHandle;
/// use std::sync::Arc;
/// 
/// struct MyHandler {
///     server_handle: Arc<dyn ServerHandle>,
/// }
/// 
/// impl MyHandler {
///     async fn send_message(&self, connection_id: &str, frame: &Frame) -> Result<()> {
///         self.server_handle.send_to(connection_id, frame).await
///     }
/// }
/// ```
#[async_trait]
pub trait ServerHandle: Send + Sync {
    /// 向指定连接发送消息
    /// 
    /// # 参数
    /// - `connection_id`: 目标连接 ID
    /// - `frame`: 要发送的 Frame
    /// 
    /// # 返回
    /// 发送成功返回 `Ok(())`，失败返回错误
    async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()>;
    
    /// 向指定用户的所有连接发送消息
    /// 
    /// # 参数
    /// - `user_id`: 目标用户 ID
    /// - `frame`: 要发送的 Frame
    /// 
    /// # 返回
    /// 发送成功返回 `Ok(())`，失败返回错误
    async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()>;
    
    /// 广播消息到所有连接
    /// 
    /// # 参数
    /// - `frame`: 要广播的 Frame
    /// 
    /// # 返回
    /// 广播成功返回 `Ok(())`，失败返回错误
    async fn broadcast(&self, frame: &Frame) -> Result<()>;
    
    /// 广播消息到所有连接，排除指定的连接
    /// 
    /// # 参数
    /// - `frame`: 要广播的 Frame
    /// - `exclude_connection_id`: 要排除的连接 ID
    /// 
    /// # 返回
    /// 广播成功返回 `Ok(())`，失败返回错误
    async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()>;
    
    /// 断开指定连接
    /// 
    /// # 参数
    /// - `connection_id`: 要断开的连接 ID
    /// 
    /// # 返回
    /// 断开成功返回 `Ok(())`，失败返回错误
    async fn disconnect(&self, connection_id: &str) -> Result<()>;
    
    /// 获取连接数量
    /// 
    /// # 返回
    /// 当前连接数量
    fn connection_count(&self) -> usize;
    
    /// 获取用户数量
    /// 
    /// # 返回
    /// 当前用户数量
    fn user_count(&self) -> usize;
}

/// ServerHandle 的默认实现
/// 
/// 基于连接管理器和消息解析器实现，轻量级且易于使用
/// 
/// # 示例
/// 
/// ```rust
/// use flare_core::server::DefaultServerHandle;
/// use flare_core::server::connection::ConnectionManager;
/// use flare_core::common::MessageParser;
/// use std::sync::Arc;
/// 
/// let connection_manager = Arc::new(ConnectionManager::new());
/// let parser = MessageParser::new(/* ... */);
/// let handle = Arc::new(DefaultServerHandle::new(
///     connection_manager as Arc<dyn ConnectionManagerTrait>,
///     parser,
/// ));
/// ```
pub struct DefaultServerHandle {
    /// 连接管理器
    connection_manager: Arc<dyn ConnectionManagerTrait>,
    /// 消息解析器（用于序列化 Frame）
    parser: MessageParser,
}

impl DefaultServerHandle {
    /// 创建新的 ServerHandle 实例
    /// 
    /// # 参数
    /// - `connection_manager`: 连接管理器 trait 对象
    /// - `parser`: 消息解析器，用于序列化 Frame
    /// 
    /// # 返回
    /// 返回新的 `DefaultServerHandle` 实例
    pub fn new(
        connection_manager: Arc<dyn ConnectionManagerTrait>,
        parser: MessageParser,
    ) -> Self {
        Self {
            connection_manager,
            parser,
        }
    }
    
    /// 获取连接数量（同步方法）
    /// 
    /// 尝试从 ConnectionManager 获取同步统计（如果可能），否则使用异步方法
    fn get_connection_count(&self) -> usize {
        // 尝试将 trait object 转换为具体的 ConnectionManager 类型
        // 如果成功，使用同步方法；否则使用异步方法
        if let Some(manager) = self.connection_manager.as_any().downcast_ref::<crate::server::connection::ConnectionManager>() {
            // 使用同步方法（更快）
            manager.connection_count()
        } else {
            // 回退到异步方法
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::try_current()
                    .map(|handle| {
                        handle.block_on(async {
                            self.connection_manager.connection_count().await
                        })
                    })
                    .unwrap_or_else(|_| {
                        tokio::runtime::Runtime::new()
                            .unwrap()
                            .block_on(async {
                                self.connection_manager.connection_count().await
                            })
                    })
            })
        }
    }
    
    /// 获取用户数量（同步方法）
    /// 
    /// 尝试从 ConnectionManager 获取同步统计（如果可能），否则使用异步方法
    fn get_user_count(&self) -> usize {
        // 尝试将 trait object 转换为具体的 ConnectionManager 类型
        if let Some(manager) = self.connection_manager.as_any().downcast_ref::<crate::server::connection::ConnectionManager>() {
            // 使用同步方法（更快）
            manager.stats().total_users
        } else {
            // 回退到异步方法：遍历所有连接并统计不同的用户 ID
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::try_current()
                    .map(|handle| {
                        handle.block_on(async {
                            let connection_ids = self.connection_manager.list_connections().await;
                            let mut user_set = std::collections::HashSet::new();
                            for conn_id in connection_ids {
                                if let Some((_, info)) = self.connection_manager.get_connection(&conn_id).await {
                                    if let Some(user_id) = info.user_id {
                                        user_set.insert(user_id);
                                    }
                                }
                            }
                            user_set.len()
                        })
                    })
                    .unwrap_or_else(|_| {
                        tokio::runtime::Runtime::new()
                            .unwrap()
                            .block_on(async {
                                let connection_ids = self.connection_manager.list_connections().await;
                                let mut user_set = std::collections::HashSet::new();
                                for conn_id in connection_ids {
                                    if let Some((_, info)) = self.connection_manager.get_connection(&conn_id).await {
                                        if let Some(user_id) = info.user_id {
                                            user_set.insert(user_id);
                                        }
                                    }
                                }
                                user_set.len()
                            })
                    })
            })
        }
    }
}

#[async_trait]
impl ServerHandle for DefaultServerHandle {
    async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        self.connection_manager
            .send_frame_to(connection_id, frame, &self.parser)
            .await
    }
    
    async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        self.connection_manager
            .send_frame_to_user(user_id, frame, &self.parser)
            .await
    }
    
    async fn broadcast(&self, frame: &Frame) -> Result<()> {
        self.connection_manager
            .broadcast_frame(frame, &self.parser)
            .await
    }
    
    async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        self.connection_manager
            .broadcast_frame_except(frame, exclude_connection_id, &self.parser)
            .await
    }
    
    async fn disconnect(&self, connection_id: &str) -> Result<()> {
        self.connection_manager.remove_connection(connection_id).await
    }
    
    fn connection_count(&self) -> usize {
        self.get_connection_count()
    }
    
    fn user_count(&self) -> usize {
        self.get_user_count()
    }
}

