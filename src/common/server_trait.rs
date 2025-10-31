//! 服务端标准接口
//! 
//! 定义服务端的标准 trait，支持用户自定义实现

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
/// use flare_core::common::server_trait::{Server, ConnectionHandler};
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

