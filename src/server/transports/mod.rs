//! 服务端传输协议模块
//! 
//! 提供多种传输协议的服务端实现：
//! - QUIC：基于 QUIC 协议的服务端
//! - WebSocket：基于 WebSocket 协议的服务端
//! - Unified：统一服务端，支持多种协议
//! 
//! 同时定义服务端的标准 trait 接口

use crate::common::error::Result;
use crate::common::protocol::Frame;
use async_trait::async_trait;

/// 连接处理器
/// 
/// 处理单个客户端连接的逻辑
#[async_trait]
pub trait ConnectionHandler: Send + Sync {
    /// 处理接收到的 Frame 消息
    /// 
    /// # 参数
    /// - `frame`: 接收到的 Frame
    /// - `connection_id`: 连接 ID
    /// 
    /// # 返回
    /// 如果需要回复，返回 `Some(Frame)`，否则返回 `None`
    async fn handle_frame(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>>;
    
    /// 处理连接建立事件
    /// 
    /// # 参数
    /// - `connection_id`: 连接 ID
    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        let _ = connection_id;
        Ok(())
    }
    
    /// 处理连接断开事件
    /// 
    /// # 参数
    /// - `connection_id`: 连接 ID
    async fn on_disconnect(&self, connection_id: &str) -> Result<()> {
        let _ = connection_id;
        Ok(())
    }
}

/// 服务端标准接口
/// 
/// 实现此 trait 以创建自定义服务端实现
/// 
/// # 示例
/// 
/// ```rust
/// use flare_core::server::{Server, ConnectionHandler};
/// use flare_core::common::error::Result;
/// use flare_core::common::protocol::Frame;
/// 
/// struct MyCustomServer {
///     handler: Arc<dyn ConnectionHandler>,
/// }
/// 
/// #[async_trait]
/// impl Server for MyCustomServer {
///     async fn start(&mut self) -> Result<()> {
///         // 实现启动逻辑
///         Ok(())
///     }
///     
///     async fn stop(&mut self) -> Result<()> {
///         // 实现停止逻辑
///         Ok(())
///     }
///     
///     async fn broadcast(&self, frame: &Frame) -> Result<()> {
///         // 实现广播逻辑
///         Ok(())
///     }
///     
///     fn is_running(&self) -> bool {
///         true
///     }
/// }
/// ```
#[async_trait]
pub trait Server: Send + Sync {
    /// 启动服务器
    /// 
    /// # 返回
    /// 启动成功返回 `Ok(())`，失败返回错误
    async fn start(&mut self) -> Result<()>;
    
    /// 停止服务器
    /// 
    /// # 返回
    /// 停止成功返回 `Ok(())`，失败返回错误
    async fn stop(&mut self) -> Result<()>;
    
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
    async fn broadcast_except(&self, frame: &Frame, _exclude_connection_id: &str) -> Result<()> {
        // 默认实现：获取所有连接，排除指定连接，然后逐个发送
        // 子类可以覆盖此方法以提供更高效的实现
        // 注意：默认实现会广播给所有人，包括要排除的连接
        // 子类应该覆盖此方法以正确实现排除逻辑
        self.broadcast(frame).await
    }
    
    /// 检查服务器运行状态
    /// 
    /// # 返回
    /// 如果正在运行返回 `true`，否则返回 `false`
    fn is_running(&self) -> bool;
    
    /// 获取连接数量
    fn connection_count(&self) -> usize;
    
    /// 获取用户数量
    fn user_count(&self) -> usize;
    
    /// 断开指定连接
    /// 
    /// # 参数
    /// - `connection_id`: 要断开的连接 ID
    /// 
    /// # 返回
    /// 断开成功返回 `Ok(())`，失败返回错误
    async fn disconnect(&self, connection_id: &str) -> Result<()>;
}

pub mod quic;
pub mod unified;
pub mod websocket;

// 重新导出常用类型
pub use quic::QUICServer;
pub use unified::UnifiedServer;
pub use websocket::WebSocketServer;
