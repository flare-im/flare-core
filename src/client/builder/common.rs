//! 客户端构建器通用组件
//!
//! 提供所有构建器共享的辅助类型和函数，体现"公共逻辑统一处理"的设计原则。
//!
//! ## 设计原则
//!
//! - **统一包装接口**：`ClientWrapper` 为所有构建模式提供统一的客户端访问接口
//! - **共享底层实现**：所有模式都基于 `HybridClient`，共享核心能力
//! - **零成本抽象**：包装层不带来运行时开销

use crate::client::{Client, HybridClient};
use crate::common::config_types::TransportProtocol;
use crate::common::error::Result;
use crate::common::protocol::Frame;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// 客户端包装器
///
/// 提供统一的客户端访问接口，所有构建模式都使用此包装器。
///
/// ## 设计原则
///
/// - **统一接口**：所有模式（简单、观察者、Flare）都通过 `ClientWrapper` 访问客户端功能
/// - **共享底层**：基于 `HybridClient`，共享所有核心能力
/// - **类型安全**：通过 Rust 类型系统保证接口的正确性
pub struct ClientWrapper {
    client: Arc<Mutex<HybridClient>>,
}

impl ClientWrapper {
    /// 创建新的客户端包装器
    pub fn new(client: HybridClient) -> Self {
        Self {
            client: Arc::new(Mutex::new(client)),
        }
    }

    /// 获取内部的 HybridClient（用于实现 Client trait）
    pub fn client(&self) -> &Arc<Mutex<HybridClient>> {
        &self.client
    }

    /// 连接到服务器
    pub async fn connect(&self) -> Result<()> {
        let mut client = self.client.lock().await;
        client.connect().await
    }

    /// 断开连接
    pub async fn disconnect(&self) -> Result<()> {
        let mut client = self.client.lock().await;
        client.disconnect().await
    }

    /// 发送消息并等待响应（按 message_id 匹配）
    pub async fn send_frame_and_wait(&self, frame: &Frame, timeout: Duration) -> Result<Frame> {
        let mut client = self.client.lock().await;
        client.send_frame_and_wait(frame, timeout).await
    }

    /// 发送消息 Frame
    pub async fn send_frame(&self, frame: &Frame) -> Result<()> {
        let mut client = self.client.lock().await;
        client.send_frame(frame).await
    }

    /// 检查连接状态
    pub fn is_connected(&self) -> bool {
        tokio::task::block_in_place(|| {
            let client = self.client.blocking_lock();
            client.is_connected()
        })
    }

    /// 获取连接 ID
    pub fn connection_id(&self) -> Option<String> {
        tokio::task::block_in_place(|| {
            let client = self.client.blocking_lock();
            client.connection_id()
        })
    }

    /// 获取活动协议
    pub fn active_protocol(&self) -> TransportProtocol {
        tokio::task::block_in_place(|| {
            let client = self.client.blocking_lock();
            client.active_protocol()
        })
    }

    /// 使用 ClientCore 执行操作（用于访问路由等功能）
    ///
    /// 注意：由于生命周期限制，不能直接返回 ClientCore 的引用
    /// 使用此方法在闭包中访问 ClientCore
    pub fn with_core<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&crate::client::transports::ClientCore) -> R,
    {
        tokio::task::block_in_place(|| {
            let client = self.client.blocking_lock();
            f(client.core())
        })
    }
}
