//! 客户端传输协议模块
//!
//! 提供多种传输协议的客户端实现：
//! - QUIC：基于 QUIC 协议的客户端
//! - WebSocket：基于 WebSocket 协议的客户端
//! - Hybrid：混合客户端，支持多种协议
//!
//! 同时定义客户端的标准 trait 接口

use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::transport::events::ArcObserver;
use async_trait::async_trait;

/// 客户端标准接口
///
/// 实现此 trait 以创建自定义客户端实现
///
/// # 示例
///
/// ```rust
/// use flare_core::client::Client;
/// use flare_core::common::error::Result;
/// use flare_core::common::protocol::Frame;
///
/// struct MyCustomClient;
///
/// #[async_trait]
/// impl Client for MyCustomClient {
///     async fn connect(&mut self) -> Result<()> {
///         // 实现连接逻辑
///         Ok(())
///     }
///     
///     async fn disconnect(&mut self) -> Result<()> {
///         // 实现断开逻辑
///         Ok(())
///     }
///     
///     async fn send_frame(&mut self, frame: &Frame) -> Result<()> {
///         // 实现发送逻辑
///         Ok(())
///     }
///     
///     fn is_connected(&self) -> bool {
///         // 返回连接状态
///         true
///     }
///     
///     fn add_observer(&mut self, observer: ArcObserver) {
///         // 添加观察者
///     }
/// }
/// ```
#[async_trait]
pub trait Client: Send + Sync {
    /// 连接到服务器
    ///
    /// # 返回
    /// 连接成功返回 `Ok(())`，失败返回错误
    async fn connect(&mut self) -> Result<()>;

    /// 断开连接
    ///
    /// # 返回
    /// 断开成功返回 `Ok(())`，失败返回错误
    async fn disconnect(&mut self) -> Result<()>;

    /// 发送 Frame 消息
    ///
    /// # 参数
    /// - `frame`: 要发送的 Frame
    ///
    /// # 返回
    /// 发送成功返回 `Ok(())`，失败返回错误
    async fn send_frame(&mut self, frame: &Frame) -> Result<()>;

    /// 检查连接状态
    ///
    /// # 返回
    /// 如果已连接返回 `true`，否则返回 `false`
    fn is_connected(&self) -> bool;

    /// 添加观察者
    ///
    /// 观察者会收到连接事件和消息事件
    ///
    /// # 参数
    /// - `observer`: 连接观察者
    fn add_observer(&mut self, observer: ArcObserver);

    /// 移除观察者
    ///
    /// # 参数
    /// - `observer`: 要移除的观察者
    fn remove_observer(&mut self, observer: ArcObserver);

    /// 获取连接 ID（如果已连接）
    ///
    /// # 返回
    /// 连接 ID，如果未连接则返回 `None`
    fn connection_id(&self) -> Option<String> {
        None
    }
}

pub mod client_core;
mod common;
pub mod hybrid;
pub mod quic;
pub mod websocket;

// 重新导出常用类型
pub use client_core::ClientCore;
pub use hybrid::HybridClient;
pub use quic::QUICClient;
pub use websocket::WebSocketClient;
