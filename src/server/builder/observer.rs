//! 观察者模式服务端构建器（基本装修）
//!
//! 提供基本实现，用户可以自定义观察器和处理器
//!
//! ## 特点
//! - ✅ 自定义观察器：实现 `ConnectionHandler` trait 自定义消息处理
//! - ✅ 设备管理：支持设备冲突策略和多端管理
//! - ✅ 事件处理：支持自定义事件处理器
//! - ✅ 连接管理：支持共享连接管理器
//! - ✅ 灵活扩展：可以添加自定义的观察器和处理器
//!
//! ## 适用场景
//! - 需要自定义消息处理逻辑
//! - 需要设备管理和多端控制
//! - 需要事件驱动的架构
//! - 需要共享连接状态（多服务器实例）

use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::server::builder::{BaseServerBuilderConfig, ServerWrapper};
use crate::server::connection::ConnectionManager;
use crate::server::handle::ServerHandle;
use crate::server::{ConnectionHandler, HybridServer};
use std::sync::Arc;

/// 观察者模式服务端构建器
///
/// 使用实现了 ConnectionHandler trait 的处理器
pub struct ObserverServerBuilder {
    base: BaseServerBuilderConfig,
    handler: Option<Arc<dyn ConnectionHandler>>,
    connection_manager: Option<Arc<ConnectionManager>>,
    device_manager: Option<Arc<crate::server::device::DeviceManager>>,
    event_handler: Option<Arc<dyn crate::server::events::handler::ServerEventHandler>>,
}

impl ObserverServerBuilder {
    /// 创建新的观察者模式构建器
    pub fn new(bind_address: impl Into<String>) -> Self {
        Self {
            base: BaseServerBuilderConfig::new(bind_address),
            handler: None,
            connection_manager: None,
            device_manager: None,
            event_handler: None,
        }
    }

    /// 设置认证器（如果启用认证，必须提供）
    ///
    /// 如果设置了认证器，还需要在配置中启用认证：
    /// ```rust
    /// .enable_auth()
    /// .with_authenticator(authenticator)
    /// ```
    pub fn with_authenticator(
        mut self,
        authenticator: Arc<dyn crate::server::auth::Authenticator>,
    ) -> Self {
        self.base = self.base.with_authenticator(authenticator);
        self
    }

    /// 启用认证
    pub fn enable_auth(mut self) -> Self {
        self.base = self.base.enable_auth();
        self
    }

    /// 设置认证超时时间
    pub fn with_auth_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.base = self.base.with_auth_timeout(timeout);
        self
    }

    /// 设置设备管理器（用于设备冲突管理）
    pub fn with_device_manager(
        mut self,
        device_manager: Arc<crate::server::device::DeviceManager>,
    ) -> Self {
        self.device_manager = Some(device_manager);
        self
    }

    /// 设置事件处理器（可选，用于细化的命令处理）
    pub fn with_event_handler(
        mut self,
        event_handler: Arc<dyn crate::server::events::handler::ServerEventHandler>,
    ) -> Self {
        self.event_handler = Some(event_handler);
        self
    }

    /// 设置连接处理器（必须）
    pub fn with_handler(mut self, handler: Arc<dyn ConnectionHandler>) -> Self {
        self.handler = Some(handler);
        self
    }

    /// 设置连接管理器（可选，用于共享连接状态）
    pub fn with_connection_manager(mut self, manager: Arc<ConnectionManager>) -> Self {
        self.connection_manager = Some(manager);
        self
    }

    /// 设置传输协议
    pub fn with_protocol(
        mut self,
        protocol: crate::common::config_types::TransportProtocol,
    ) -> Self {
        self.base = self.base.with_protocol(protocol);
        self
    }

    /// 启用多协议监听
    pub fn with_protocols(
        mut self,
        protocols: Vec<crate::common::config_types::TransportProtocol>,
    ) -> Self {
        self.base = self.base.with_protocols(protocols);
        self
    }

    /// 为特定协议设置监听地址
    pub fn with_protocol_address(
        mut self,
        protocol: crate::common::config_types::TransportProtocol,
        address: String,
    ) -> Self {
        self.base = self.base.with_protocol_address(protocol, address);
        self
    }

    /// 设置最大连接数
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.base = self.base.with_max_connections(max);
        self
    }

    /// 设置心跳配置
    pub fn with_heartbeat(
        mut self,
        heartbeat: crate::common::config_types::HeartbeatConfig,
    ) -> Self {
        self.base = self.base.with_heartbeat(heartbeat);
        self
    }

    /// 设置 TLS 配置
    pub fn with_tls(mut self, tls: crate::common::config_types::TlsConfig) -> Self {
        self.base = self.base.with_tls(tls);
        self
    }

    /// 设置默认序列化格式（用于协商，默认 Protobuf）
    pub fn with_default_format(
        mut self,
        format: crate::common::protocol::SerializationFormat,
    ) -> Self {
        self.base = self.base.with_default_format(format);
        self
    }

    /// 设置默认压缩算法（用于协商，默认 None）
    pub fn with_default_compression(
        mut self,
        compression: crate::common::compression::CompressionAlgorithm,
    ) -> Self {
        self.base = self.base.with_default_compression(compression);
        self
    }

    /// 构建服务端
    pub fn build(self) -> Result<ObserverServer> {
        let handler = self.handler.ok_or_else(|| {
            crate::common::error::FlareError::general_error("Handler is required")
        })?;

        // 在创建 HybridServer 时就传入设备管理器和事件处理器
        // 这样确保 ServerCore 在创建时就有正确的配置，避免后续修改 Arc 的问题
        let server = HybridServer::with_connection_manager(
            self.base.config,
            handler,
            self.connection_manager,
            self.device_manager,
            self.event_handler,
            self.base.authenticator,
        )?;

        Ok(ObserverServer {
            wrapper: ServerWrapper::new(server),
        })
    }
}

/// 观察者模式服务器实例
pub struct ObserverServer {
    wrapper: ServerWrapper,
}

impl ObserverServer {
    /// 启动服务器
    pub async fn start(&mut self) -> Result<()> {
        self.wrapper.start().await
    }

    /// 停止服务器
    pub async fn stop(&mut self) -> Result<()> {
        self.wrapper.stop().await
    }

    /// 检查服务器是否运行
    pub fn is_running(&self) -> bool {
        self.wrapper.is_running()
    }

    /// 获取连接数量
    pub fn connection_count(&self) -> usize {
        self.wrapper.connection_count()
    }

    /// 获取用户数量
    pub fn user_count(&self) -> usize {
        self.wrapper.user_count()
    }

    /// 向指定连接发送消息
    pub async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        self.wrapper.send_to(connection_id, frame).await
    }

    /// 向指定用户的所有连接发送消息
    pub async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        self.wrapper.send_to_user(user_id, frame).await
    }

    /// 广播消息到所有连接
    pub async fn broadcast(&self, frame: &Frame) -> Result<()> {
        self.wrapper.broadcast(frame).await
    }

    /// 广播消息到所有连接，排除指定连接
    pub async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        self.wrapper
            .broadcast_except(frame, exclude_connection_id)
            .await
    }

    /// 断开指定连接
    pub async fn disconnect(&self, connection_id: &str) -> Result<()> {
        self.wrapper.disconnect(connection_id).await
    }

    /// 获取协议列表
    pub fn protocols(&self) -> Vec<crate::common::config_types::TransportProtocol> {
        self.wrapper.protocols()
    }

    /// 获取连接管理器（用于创建 DefaultServerHandle）
    ///
    /// # 返回
    /// 返回 ConnectionManagerTrait
    pub fn get_server_handle_components(
        &self,
    ) -> Option<Arc<dyn crate::server::connection::ConnectionManagerTrait>> {
        self.wrapper.get_server_handle_components()
    }

    /// 获取 ServerHandle（用于消息发送和连接管理）
    pub fn get_server_handle(&self) -> Option<Arc<dyn ServerHandle>> {
        self.wrapper.get_server_handle()
    }
}
