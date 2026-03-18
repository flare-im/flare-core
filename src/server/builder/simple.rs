//! 简单模式服务端构建器
//!
//! 提供最小实现，使用闭包定义消息处理逻辑，适合快速原型开发和学习。
//!
//! ## 特点
//! - ✅ **使用闭包处理消息和连接事件**：无需实现 trait，直接使用闭包
//! - ✅ **最小依赖**：只提供基本的消息处理功能
//! - ✅ **零配置**：使用默认配置即可运行
//! - ✅ **轻量级**：不包含中间件、管道等高级功能
//! - ✅ **快速上手**：几行代码即可启动服务器
//!
//! ## 适用场景
//! - 快速原型开发
//! - 学习和测试
//! - 小型应用
//! - 需要完全控制消息处理流程的场景
//!
//! ## 架构说明
//!
//! 简单模式基于 `HybridServer`，共享所有核心能力（多协议支持、序列化协商、心跳检测等），
//! 但消息处理逻辑通过闭包定义，不依赖高级抽象。

use crate::common::error::Result;
use crate::common::protocol::{Frame, PayloadCommand};
use crate::server::HybridServer;
use crate::server::builder::{BaseServerBuilderConfig, ServerWrapper};
use crate::server::events::handler::ServerEventHandler;
use crate::server::handle::ServerHandle;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

/// 消息处理上下文
///
/// 提供给消息处理函数的上下文，包含连接信息和服务器操作处理器
pub struct MessageContext {
    /// 连接 ID
    pub connection_id: String,
    /// 服务器操作处理器（轻量级，用于发送消息和连接管理）
    handle: Arc<dyn ServerHandle>,
}

impl MessageContext {
    /// 创建新的消息上下文
    fn new(connection_id: String, handle: Arc<dyn ServerHandle>) -> Self {
        Self {
            connection_id,
            handle,
        }
    }

    /// 向指定连接发送消息
    pub async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        self.handle.send_to(connection_id, frame).await
    }

    /// 向指定用户的所有连接发送消息
    pub async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        self.handle.send_to_user(user_id, frame).await
    }

    /// 广播消息到所有连接
    pub async fn broadcast(&self, frame: &Frame) -> Result<()> {
        self.handle.broadcast(frame).await
    }

    /// 广播消息到所有连接，排除指定连接
    pub async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        self.handle
            .broadcast_except(frame, exclude_connection_id)
            .await
    }

    /// 断开指定连接
    pub async fn disconnect(&self, connection_id: &str) -> Result<()> {
        self.handle.disconnect(connection_id).await
    }

    /// 获取连接数量
    pub fn connection_count(&self) -> usize {
        self.handle.connection_count()
    }

    /// 获取用户数量
    pub fn user_count(&self) -> usize {
        self.handle.user_count()
    }
}

/// 消息处理函数类型
pub type MessageHandlerFn = Box<
    dyn for<'a> Fn(
            &'a Frame,
            &'a MessageContext,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Option<Frame>>> + Send + 'a>,
        > + Send
        + Sync,
>;

/// 连接事件处理函数类型
pub type OnConnectFn = Box<
    dyn for<'a> Fn(
            &'a str,
            &'a MessageContext,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>>
        + Send
        + Sync,
>;

/// 断开连接事件处理函数类型
pub type OnDisconnectFn = Box<
    dyn for<'a> Fn(
            &'a str,
            &'a MessageContext,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>>
        + Send
        + Sync,
>;

/// 简化的连接处理器（仅用于存储闭包和 handle）
struct SimpleConnectionHandler {
    message_handler: Option<MessageHandlerFn>,
    on_connect: Option<OnConnectFn>,
    on_disconnect: Option<OnDisconnectFn>,
    handle: Arc<Mutex<Option<Arc<dyn ServerHandle>>>>,
}

impl SimpleConnectionHandler {
    async fn set_handle(&self, handle: Arc<dyn ServerHandle>) {
        *self.handle.lock().await = Some(handle);
    }
}

/// 将 SimpleConnectionHandler 适配为 ServerEventHandler
struct SimpleEventHandlerAdapter {
    handler: Arc<SimpleConnectionHandler>,
}

#[async_trait]
impl ServerEventHandler for SimpleEventHandlerAdapter {
    async fn handle_message(
        &self,
        command: &PayloadCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        let handle = {
            let handle_guard = self.handler.handle.lock().await;
            handle_guard.clone()
        };

        let context = if let Some(ref handle) = handle {
            MessageContext::new(connection_id.to_string(), Arc::clone(handle))
        } else {
            return Err(crate::common::error::FlareError::general_error(
                "Server handle is not available",
            ));
        };

        let frame = Frame {
            message_id: command.message_id.clone(),
            command: Some(crate::common::protocol::flare::core::commands::Command {
                r#type: Some(
                    crate::common::protocol::flare::core::commands::command::Type::Payload(
                        command.clone(),
                    ),
                ),
            }),
            metadata: std::collections::HashMap::new(),
            reliability: crate::common::protocol::Reliability::AtLeastOnce as i32,
            timestamp: 0,
        };

        if let Some(ref message_handler) = self.handler.message_handler {
            message_handler(&frame, &context).await
        } else {
            Ok(None)
        }
    }

    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        let handle = {
            let handle_guard = self.handler.handle.lock().await;
            handle_guard.clone()
        };

        let context = if let Some(ref handle) = handle {
            MessageContext::new(connection_id.to_string(), Arc::clone(handle))
        } else {
            return Err(crate::common::error::FlareError::general_error(
                "Server handle is not available",
            ));
        };

        if let Some(ref on_connect) = self.handler.on_connect {
            on_connect(connection_id, &context).await
        } else {
            Ok(())
        }
    }

    async fn on_disconnect(&self, connection_id: &str, _reason: Option<&str>) -> Result<()> {
        let handle = {
            let handle_guard = self.handler.handle.lock().await;
            handle_guard.clone()
        };

        let context = if let Some(ref handle) = handle {
            MessageContext::new(connection_id.to_string(), Arc::clone(handle))
        } else {
            return Err(crate::common::error::FlareError::general_error(
                "Server handle is not available",
            ));
        };

        if let Some(ref on_disconnect) = self.handler.on_disconnect {
            on_disconnect(connection_id, &context).await
        } else {
            Ok(())
        }
    }
}

/// 简化的服务器实例
///
/// 提供简化的接口，自动处理服务器引用
pub struct SimpleServer {
    wrapper: ServerWrapper,
    handler: Arc<SimpleConnectionHandler>,
    handle: Arc<dyn ServerHandle>,
}

impl SimpleServer {
    /// 启动服务器
    pub async fn start(&mut self) -> Result<()> {
        // 设置 ServerHandle
        self.handler.set_handle(Arc::clone(&self.handle)).await;
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

    /// 获取 ServerHandle（用于消息发送和连接管理）
    pub fn handle(&self) -> Arc<dyn ServerHandle> {
        Arc::clone(&self.handle)
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
}

/// 简单模式服务端构建器
///
/// 提供最小实现，使用闭包定义消息处理逻辑，适合快速原型开发和学习。
///
/// ## 设计原则
///
/// - **公共逻辑统一处理**：基于 `HybridServer`，共享所有核心能力（多协议、序列化协商、心跳等）
/// - **最小抽象**：消息处理通过闭包定义，不依赖 trait 实现
/// - **零配置**：使用默认配置即可运行
///
/// ## 使用方式
///
/// 使用闭包定义消息处理和连接事件处理逻辑，无需实现 trait。
pub struct ServerBuilder {
    base: BaseServerBuilderConfig,
    message_handler: Option<MessageHandlerFn>,
    on_connect: Option<OnConnectFn>,
    on_disconnect: Option<OnDisconnectFn>,
}

impl ServerBuilder {
    /// 创建新的服务端构建器
    ///
    /// # 参数
    /// - `bind_address`: 监听地址，例如 "0.0.0.0:8080"
    pub fn new(bind_address: impl Into<String>) -> Self {
        Self {
            base: BaseServerBuilderConfig::new(bind_address),
            message_handler: None,
            on_connect: None,
            on_disconnect: None,
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

    /// 设置消息处理函数
    ///
    /// # 参数
    /// - `handler`: 消息处理函数，接收 `Frame` 和 `MessageContext`，返回 `Option<Frame>`（可选回复）
    pub fn on_message<F>(mut self, handler: F) -> Self
    where
        F: for<'a> Fn(
                &'a Frame,
                &'a MessageContext,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<Option<Frame>>> + Send + 'a>,
            > + Send
            + Sync
            + 'static,
    {
        self.message_handler = Some(Box::new(move |frame, ctx| handler(frame, ctx)));
        self
    }

    /// 设置连接建立事件处理函数
    ///
    /// # 参数
    /// - `handler`: 连接建立处理函数，接收 connection_id 和 MessageContext
    pub fn on_connect<F>(mut self, handler: F) -> Self
    where
        F: for<'a> Fn(
                &'a str,
                &'a MessageContext,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>,
            > + Send
            + Sync
            + 'static,
    {
        self.on_connect = Some(Box::new(move |conn_id, ctx| handler(conn_id, ctx)));
        self
    }

    /// 设置连接断开事件处理函数
    ///
    /// # 参数
    /// - `handler`: 连接断开处理函数，接收 connection_id 和 MessageContext
    pub fn on_disconnect<F>(mut self, handler: F) -> Self
    where
        F: for<'a> Fn(
                &'a str,
                &'a MessageContext,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>,
            > + Send
            + Sync
            + 'static,
    {
        self.on_disconnect = Some(Box::new(move |conn_id, ctx| handler(conn_id, ctx)));
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
    /// # 返回
    /// 返回配置好的 SimpleServer 实例
    pub fn build(self) -> Result<SimpleServer> {
        let handler = Arc::new(SimpleConnectionHandler {
            message_handler: self.message_handler,
            on_connect: self.on_connect,
            on_disconnect: self.on_disconnect,
            handle: Arc::new(Mutex::new(None)),
        });

        let event_handler: Arc<dyn crate::server::events::handler::ServerEventHandler> =
            Arc::new(SimpleEventHandlerAdapter {
                handler: handler.clone(),
            });

        let server = HybridServer::with_connection_manager(
            self.base.config,
            None,
            None,
            Some(event_handler),
            self.base.authenticator,
        )?;

        // 使用 ServerWrapper 统一管理
        let wrapper = ServerWrapper::new(server);

        // 创建 ServerHandle（通过 ServerWrapper）
        let handle = wrapper.get_server_handle().ok_or_else(|| {
            crate::common::error::FlareError::general_error("Failed to create ServerHandle")
        })?;

        Ok(SimpleServer {
            wrapper,
            handler,
            handle,
        })
    }
}
