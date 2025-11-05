//! 服务端连接管理器抽象
//! 
//! 定义服务端连接管理的标准接口，支持用户自定义实现
//! 默认实现使用 ConnectionManager

use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::common::MessageParser;
use crate::transport::connection::Connection;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;

/// 连接信息（Trait 版本，用于跨异步边界传递）
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
    /// 设备信息（如果已提供）
    pub device_info: Option<crate::common::device::DeviceInfo>,
    /// 序列化格式（由客户端协商决定）
    pub serialization_format: crate::common::protocol::SerializationFormat,
    /// 压缩算法（由客户端协商决定）
    pub compression: crate::common::compression::CompressionAlgorithm,
}

/// 连接管理器抽象 trait
/// 
/// 实现此 trait 以提供自定义的连接管理逻辑
/// 例如：基于 Redis 的分布式连接管理、基于数据库的持久化等
#[async_trait]
pub trait ConnectionManagerTrait: Send + Sync + std::any::Any {
    /// 获取 Any 引用，用于类型向下转换
    fn as_any(&self) -> &dyn std::any::Any;
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

    // ========== 底层发送方法（字节数组）==========
    
    /// 向指定连接发送数据（字节数组）
    async fn send_to_connection(&self, connection_id: &str, data: &[u8]) -> Result<()>;

    /// 向指定用户的所有连接发送数据（字节数组）
    async fn send_to_user(&self, user_id: &str, data: &[u8]) -> Result<()>;

    /// 广播消息到所有连接（字节数组）
    async fn broadcast(&self, data: &[u8]) -> Result<()>;

    /// 广播消息到所有连接，排除指定连接（字节数组）
    async fn broadcast_except(&self, data: &[u8], exclude_connection_id: &str) -> Result<()>;

    // ========== Frame 级别发送方法（需要 MessageParser）==========
    
    /// 向指定连接发送 Frame（自动序列化）
    /// 
    /// # 参数
    /// - `connection_id`: 连接 ID
    /// - `frame`: 要发送的 Frame
    /// - `parser`: 消息解析器，用于序列化 Frame（如果为 None，则从连接的协商信息创建）
    /// 
    /// # 返回
    /// 发送成功返回 `Ok(())`，失败返回错误
    /// 
    /// # 注意
    /// 如果 parser 为 None，将从连接的 ConnectionInfo 中获取协商后的序列化格式和压缩算法创建 parser
    async fn send_frame_to(
        &self,
        connection_id: &str,
        frame: &Frame,
        parser: Option<&MessageParser>,
    ) -> Result<()> {
        // 如果提供了 parser，使用它；否则从连接的协商信息创建 parser
        let data = if let Some(p) = parser {
            p.serialize(frame)?
        } else {
            // 从连接的协商信息创建 parser
            if let Some((_, info)) = self.get_connection(connection_id).await {
                let connection_parser = MessageParser::new(
                    info.serialization_format,
                    info.compression,
                );
                connection_parser.serialize(frame)?
            } else {
                // 如果连接不存在，使用默认 JSON parser
                MessageParser::json().serialize(frame)?
            }
        };
        self.send_to_connection(connection_id, &data).await?;
        self.update_connection_active(connection_id).await?;
        Ok(())
    }

    /// 向指定用户的所有连接发送 Frame（自动序列化）
    /// 
    /// # 参数
    /// - `user_id`: 用户 ID
    /// - `frame`: 要发送的 Frame
    /// - `parser`: 消息解析器，用于序列化 Frame（如果为 None，则为每个连接使用其协商的格式）
    /// 
    /// # 返回
    /// 发送成功返回 `Ok(())`，失败返回错误
    async fn send_frame_to_user(
        &self,
        user_id: &str,
        frame: &Frame,
        parser: Option<&MessageParser>,
    ) -> Result<()> {
        let connection_ids = self.get_user_connections(user_id).await;
        for conn_id in connection_ids {
            // 为每个连接使用其协商的格式（如果 parser 为 None）
            let _ = self.send_frame_to(&conn_id, frame, parser).await;
        }
        Ok(())
    }

    /// 广播 Frame 到所有连接（自动序列化）
    /// 
    /// # 参数
    /// - `frame`: 要广播的 Frame
    /// - `parser`: 消息解析器，用于序列化 Frame（如果为 None，则为每个连接使用其协商的格式）
    /// 
    /// # 返回
    /// 广播成功返回 `Ok(())`，失败返回错误
    async fn broadcast_frame(
        &self,
        frame: &Frame,
        parser: Option<&MessageParser>,
    ) -> Result<()> {
        let connection_ids = self.list_connections().await;
        for conn_id in connection_ids {
            // 为每个连接使用其协商的格式（如果 parser 为 None）
            let _ = self.send_frame_to(&conn_id, frame, parser).await;
        }
        Ok(())
    }

    /// 广播 Frame 到所有连接，排除指定连接（自动序列化）
    /// 
    /// # 参数
    /// - `frame`: 要广播的 Frame
    /// - `exclude_connection_id`: 要排除的连接 ID
    /// - `parser`: 消息解析器，用于序列化 Frame（如果为 None，则为每个连接使用其协商的格式）
    /// 
    /// # 返回
    /// 广播成功返回 `Ok(())`，失败返回错误
    async fn broadcast_frame_except(
        &self,
        frame: &Frame,
        exclude_connection_id: &str,
        parser: Option<&MessageParser>,
    ) -> Result<()> {
        let connection_ids = self.list_connections().await;
        for conn_id in connection_ids {
            if conn_id != exclude_connection_id {
                // 为每个连接使用其协商的格式（如果 parser 为 None）
                let _ = self.send_frame_to(&conn_id, frame, parser).await;
            }
        }
        Ok(())
    }
}

/// 连接统计信息
#[derive(Debug, Clone)]
pub struct ConnectionStats {
    /// 总连接数
    pub total_connections: usize,
    /// 总用户数
    pub total_users: usize,
} 