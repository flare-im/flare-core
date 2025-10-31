use crate::common::error::Result;
use crate::transport::events::ArcObserver;
use async_trait::async_trait;
use std::time::Instant;

/// A unified transport layer connection interface.
///
/// This trait abstracts the underlying transport protocol (e.g., WebSocket, QUIC)
/// and provides a common set of methods for interacting with a connection.
/// It uses an observer pattern to notify interested parties of connection events.
#[async_trait]
pub trait Connection: Send + Sync {
    /// Adds an observer to the connection.
    ///
    /// The observer will be notified of connection events.
    fn add_observer(&mut self, observer: ArcObserver);

    /// Removes an observer from the connection.
    fn remove_observer(&mut self, observer: ArcObserver);

    /// Sends data over the connection.
    async fn send(&mut self, data: &[u8]) -> Result<()>;

    /// Closes the connection.
    async fn close(&mut self) -> Result<()>;

    /// 获取最后活跃时间
    /// 
    /// 返回连接的最后活跃时间戳，用于判断连接是否还在使用中。
    /// 活跃时间会在以下情况下更新：
    /// - 发送消息时
    /// - 收到消息时
    fn last_active_time(&self) -> Instant;

    /// 更新最后活跃时间
    /// 
    /// 通常在发送或接收消息时自动调用，但也可以手动调用。
    fn update_active_time(&mut self);
}