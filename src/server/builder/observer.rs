//! 观察者模式服务端构建器
//!
//! 提供基本功能实现，使用 `ServerEventHandler` trait 处理消息和事件。
//!
//! ## 特点
//! - ✅ **实现 `ServerEventHandler` trait（必需）**：提供细化的命令处理方法
//! - ✅ **自动消息路由**：`ServerMessageWrapper` 自动将消息路由到对应的处理方法
//! - ✅ **自动 ACK 处理**：框架自动处理 ACK 和错误响应
//! - ✅ **设备管理**：支持设备冲突策略和多端管理
//! - ✅ **认证机制**：支持 Token 认证
//! - ✅ **连接管理**：支持共享连接管理器（多服务器实例）
//!
//! ## 适用场景
//! - 需要自定义消息处理逻辑但不需要完整功能集
//! - 需要设备管理和多端控制
//! - 需要事件驱动的架构
//! - 需要共享连接状态（多服务器实例）
//!
//! ## 架构说明
//!
//! 观察者模式基于 `HybridServer`，使用 `ServerMessageWrapper` 作为消息处理器。
//! `ServerMessageWrapper` 自动将消息路由到 `ServerEventHandler` 的对应方法，
//! 并处理 ACK 和错误响应。

use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::server::HybridServer;
use crate::server::builder::{BaseServerBuilderConfig, ServerWrapper};
use crate::server::connection::ConnectionManager;
use crate::server::handle::ServerHandle;
use std::sync::Arc;
use tracing::{error, info};

/// 观察者模式服务端构建器
///
/// 提供基本功能实现，使用 `ServerEventHandler` trait 处理消息和事件。
///
/// ## 设计原则
///
/// - **公共逻辑统一处理**：基于 `HybridServer`，共享所有核心能力
/// - **自动消息路由**：`ServerMessageWrapper` 自动将消息路由到 `ServerEventHandler` 的对应方法
/// - **自动 ACK 处理**：如果 handler 返回 `None`，框架自动发送 ACK
/// - **错误处理**：处理失败时自动发送错误 ACK，确保客户端能收到响应
///
/// ## 使用方式
///
/// 用户只需要实现 `ServerEventHandler` trait，框架会自动处理消息路由和 ACK。
pub struct ObserverServerBuilder {
    base: BaseServerBuilderConfig,
    connection_manager: Option<Arc<ConnectionManager>>,
    device_manager: Option<Arc<crate::server::device::DeviceManager>>,
    event_handler: Arc<dyn crate::server::events::handler::ServerEventHandler>,
}

impl ObserverServerBuilder {
    /// 创建新的观察者模式构建器
    ///
    /// # 参数
    /// - `bind_address`: 绑定地址
    /// - `event_handler`: 事件处理器（必须），用户只需要实现 `ServerEventHandler` 的 `handle_message` 方法即可
    pub fn new(
        bind_address: impl Into<String>,
        event_handler: Arc<dyn crate::server::events::handler::ServerEventHandler>,
    ) -> Self {
        Self {
            base: BaseServerBuilderConfig::new(bind_address),
            connection_manager: None,
            device_manager: None,
            event_handler,
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
    ///
    /// # 错误处理
    /// - 如果配置无效（如启用了认证但未提供认证器），返回配置错误
    /// - 如果服务器初始化失败，返回相应的错误
    ///
    /// # 返回
    /// - `Ok(ObserverServer)` - 成功构建的服务端实例
    /// - `Err(FlareError)` - 构建失败的错误信息
    pub fn build(self) -> Result<ObserverServer> {
        // 验证配置（使用公共验证逻辑）
        crate::server::builder::common::validate_auth_config(
            &self.base.config,
            &self.base.authenticator,
        )?;

        // 创建消息解析器（使用公共创建逻辑）
        // let parser = crate::server::builder::common::create_message_parser(&self.base.config);

        info!(
            "[ObserverServerBuilder] 开始构建服务端: bind_address={}, protocols={:?}",
            self.base.config.bind_address,
            self.base.config.get_protocols()
        );

        let server = HybridServer::with_connection_manager(
            self.base.config,
            self.connection_manager,
            self.device_manager,
            Some(self.event_handler),
            self.base.authenticator,
        )
        .map_err(|e| {
            error!("[ObserverServerBuilder] 构建服务端失败: {}", e);
            e
        })?;

        info!("[ObserverServerBuilder] 服务端构建成功");
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
