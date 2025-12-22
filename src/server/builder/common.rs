//! 服务端构建器通用组件
//!
//! 提供所有构建器共享的辅助类型和函数，体现"公共逻辑统一处理"的设计原则。
//!
//! ## 设计原则
//!
//! - **统一包装接口**：`ServerWrapper` 为所有构建模式提供统一的 `ServerHandle` 访问接口
//! - **共享底层实现**：所有模式都基于 `HybridServer`，共享核心能力
//! - **零成本抽象**：包装层不带来运行时开销

use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::server::connection::ConnectionManagerTrait;
use crate::server::handle::{DefaultServerHandle, ServerHandle};
use crate::server::{HybridServer, Server};
use std::sync::Arc;
use tokio::sync::Mutex;

/// 服务端包装器
///
/// 提供统一的 `ServerHandle` 访问接口，所有构建模式都使用此包装器。
///
/// ## 设计原则
///
/// - **统一接口**：所有模式（简单、观察者、Flare）都通过 `ServerWrapper` 访问服务端功能
/// - **共享底层**：基于 `HybridServer`，共享所有核心能力
/// - **类型安全**：通过 Rust 类型系统保证接口的正确性
pub struct ServerWrapper {
    server: Arc<Mutex<HybridServer>>,
}

impl ServerWrapper {
    /// 创建新的服务端包装器
    pub fn new(server: HybridServer) -> Self {
        Self {
            server: Arc::new(Mutex::new(server)),
        }
    }

    /// 获取内部的 HybridServer（用于实现 Server trait）
    pub fn server(&self) -> &Arc<Mutex<HybridServer>> {
        &self.server
    }

    /// 获取 ServerHandle 组件（用于创建 DefaultServerHandle）
    ///
    /// # 返回
    /// 返回 ConnectionManagerTrait，如果 ServerCore 未初始化则返回 None
    pub fn get_server_handle_components(&self) -> Option<Arc<dyn ConnectionManagerTrait>> {
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
            s.core().map(|core| core.connection_manager_trait())
        })
    }

    /// 获取 ServerHandle（直接使用 HybridServer 作为 ServerHandle）
    ///
    /// # 返回
    /// 返回 ServerHandle trait object，如果 ServerCore 未初始化则返回 None
    pub fn get_server_handle(&self) -> Option<Arc<dyn ServerHandle>> {
        // HybridServer 实现了 ServerHandle，所以我们可以直接返回它
        // 但需要包装为 Arc，由于 HybridServer 在 Mutex 中，我们需要一个包装器
        self.get_server_handle_components().map(|manager_trait| {
            Arc::new(DefaultServerHandle::new(manager_trait)) as Arc<dyn ServerHandle>
        })
    }

    /// 启动服务器
    pub async fn start(&self) -> Result<()> {
        let mut s = self.server.lock().await;
        s.start().await
    }

    /// 停止服务器
    pub async fn stop(&self) -> Result<()> {
        let mut s = self.server.lock().await;
        s.stop().await
    }

    /// 检查服务器是否运行
    pub fn is_running(&self) -> bool {
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
            s.is_running()
        })
    }

    /// 获取连接数量
    pub fn connection_count(&self) -> usize {
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
            ServerHandle::connection_count(&*s)
        })
    }

    /// 获取用户数量
    pub fn user_count(&self) -> usize {
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
            ServerHandle::user_count(&*s)
        })
    }

    /// 向指定连接发送消息
    pub async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        let s = self.server.lock().await;
        ServerHandle::send_to(&*s, connection_id, frame).await
    }

    /// 向指定用户的所有连接发送消息
    pub async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        let s = self.server.lock().await;
        ServerHandle::send_to_user(&*s, user_id, frame).await
    }

    /// 广播消息到所有连接
    pub async fn broadcast(&self, frame: &Frame) -> Result<()> {
        let s = self.server.lock().await;
        ServerHandle::broadcast(&*s, frame).await
    }

    /// 广播消息到所有连接，排除指定连接
    pub async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        let s = self.server.lock().await;
        ServerHandle::broadcast_except(&*s, frame, exclude_connection_id).await
    }

    /// 断开指定连接
    pub async fn disconnect(&self, connection_id: &str) -> Result<()> {
        let s = self.server.lock().await;
        ServerHandle::disconnect(&*s, connection_id).await
    }

    /// 获取协议列表
    pub fn protocols(&self) -> Vec<crate::common::config_types::TransportProtocol> {
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
            s.protocols().to_vec()
        })
    }
}

/// 验证认证配置
///
/// 如果启用了认证但未提供认证器，返回配置错误
/// 这体现了"公共逻辑统一处理"的原则：所有需要认证的模式共享相同的验证逻辑
pub fn validate_auth_config(
    config: &crate::server::config::ServerConfig,
    authenticator: &Option<Arc<dyn crate::server::auth::Authenticator>>,
) -> Result<()> {
    if config.auth_enabled && authenticator.is_none() {
        return Err(crate::common::error::FlareError::localized(
            crate::common::error::ErrorCode::ConfigurationError,
            "认证已启用但未提供认证器，请使用 with_authenticator() 设置认证器",
        ));
    }
    Ok(())
}

/// 创建消息解析器
///
/// 根据配置中的默认序列化格式和压缩算法创建解析器
/// 这体现了"公共逻辑统一处理"的原则：所有需要解析器的模式共享相同的创建逻辑
pub fn create_message_parser(
    config: &crate::server::config::ServerConfig,
) -> crate::common::MessageParser {
    crate::common::MessageParser::new(
        config.default_serialization_format,
        config.default_compression.clone(),
        config.default_encryption.clone(),
    )
}
