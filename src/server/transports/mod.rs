//! 服务端传输协议模块
//! 
//! 提供多种传输协议的服务端实现：
//! - QUIC：基于 QUIC 协议的服务端
//! - WebSocket：基于 WebSocket 协议的服务端
//! - Hybrid：混合服务端，支持多种协议
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
/// 注意：消息发送和连接管理功能请使用 `ServerHandle` trait
/// 
/// # 示例
/// 
/// ```rust
/// use flare_core::server::{Server, ConnectionHandler};
/// use flare_core::common::error::Result;
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
    
    /// 检查服务器运行状态
    /// 
    /// # 返回
    /// 如果正在运行返回 `true`，否则返回 `false`
    fn is_running(&self) -> bool;
}

pub mod quic;
pub mod hybrid;
pub mod websocket;
pub mod server_core;
mod common;

// 重新导出常用类型
pub use quic::QUICServer;
pub use hybrid::HybridServer;
pub use websocket::WebSocketServer;
