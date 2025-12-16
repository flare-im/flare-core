//! Flare 服务端构建器（精装修）
//!
//! 提供完整功能，包含所有 `common` 和 `server` 模块的能力
//! 用户只需简单配置即可使用，也可以自定义中间件、处理器等扩展功能
//!
//! ## 特点
//! - ✅ **消息管道**：自动处理序列化、压缩、加密
//! - ✅ **中间件支持**：日志、性能监控、验证等
//! - ✅ **处理器链**：可组合多个处理器
//! - ✅ **序列化协商**：自动协商最佳序列化格式（JSON/Protobuf）
//! - ✅ **压缩协商**：自动协商压缩算法（Gzip/Zstd/None）
//! - ✅ **加密支持**：AES-256-GCM 加密
//! - ✅ **设备管理**：完整的设备冲突策略
//! - ✅ **认证机制**：JWT Token 认证
//! - ✅ **心跳检测**：自动心跳和超时管理
//! - ✅ **多协议支持**：WebSocket + QUIC 双协议
//! - ✅ **简单易用**：只需实现 `MessageListener` 即可
//! - ✅ **高度可扩展**：可以自定义中间件、处理器覆盖默认实现
//!
//! ## 适用场景
//! - 生产环境
//! - 需要完整功能的企业应用
//! - 需要高性能和可扩展性的场景
//! - 需要统一消息处理流程的场景

use crate::common::MessageParser;
use crate::common::error::Result;
use crate::common::message::{
    ArcMessageMiddleware, ArcMessageProcessor, MessageContext, MessagePipeline, MessageProcessor,
};
use crate::common::protocol::Frame;
use crate::server::builder::{BaseServerBuilderConfig, ServerWrapper};
use crate::server::connection::ConnectionManager;
use crate::server::events::handler::ServerEventHandler;
use crate::server::handle::ServerHandle;
use crate::server::{ConnectionHandler, HybridServer};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

/// 消息监听器
///
/// 用户只需要实现这个简单的接口就能处理消息
#[async_trait]
pub trait MessageListener: Send + Sync {
    /// 处理收到的消息
    ///
    /// # 参数
    /// - `frame`: 收到的消息 Frame
    /// - `connection_id`: 连接 ID
    ///
    /// # 返回
    /// - `Ok(Some(Frame))`: 需要发送的响应
    /// - `Ok(None)`: 不需要响应
    /// - `Err`: 处理失败
    async fn on_message(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        let _ = (frame, connection_id);
        Ok(None)
    }

    /// 连接建立时调用
    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        let _ = connection_id;
        Ok(())
    }

    /// 连接断开时调用
    async fn on_disconnect(&self, connection_id: &str, reason: Option<&str>) -> Result<()> {
        let _ = (connection_id, reason);
        Ok(())
    }
}

/// Flare 服务端构建器
///
/// 提供简单易用的 API，自动集成所有功能：
/// - 消息管道（中间件、处理器）
/// - 序列化协商
/// - 压缩/解压
/// - 加密/解密
/// - 设备管理
/// - 认证
///
/// 这是最高抽象级别的构建器，适合生产环境使用
pub struct FlareServerBuilder {
    base: BaseServerBuilderConfig,
    listener: Option<Arc<dyn MessageListener>>,
    middlewares: Vec<ArcMessageMiddleware>,
    processors: Vec<ArcMessageProcessor>,
    connection_manager: Option<Arc<ConnectionManager>>,
    device_manager: Option<Arc<crate::server::device::DeviceManager>>,
    event_handler: Option<Arc<dyn ServerEventHandler>>,
}

impl FlareServerBuilder {
    /// 创建新的 Flare 服务端构建器
    pub fn new(bind_address: impl Into<String>) -> Self {
        Self {
            base: BaseServerBuilderConfig::new(bind_address),
            listener: None,
            middlewares: Vec::new(),
            processors: Vec::new(),
            connection_manager: None,
            device_manager: None,
            event_handler: None,
        }
    }

    /// 设置消息监听器（必须）
    pub fn with_listener(mut self, listener: Arc<dyn MessageListener>) -> Self {
        self.listener = Some(listener);
        self
    }

    /// 添加中间件
    pub fn with_middleware(mut self, middleware: ArcMessageMiddleware) -> Self {
        self.middlewares.push(middleware);
        self
    }

    /// 添加处理器
    pub fn with_processor(mut self, processor: ArcMessageProcessor) -> Self {
        self.processors.push(processor);
        self
    }

    /// 设置连接管理器（可选）
    pub fn with_connection_manager(mut self, manager: Arc<ConnectionManager>) -> Self {
        self.connection_manager = Some(manager);
        self
    }

    /// 设置设备管理器（可选）
    pub fn with_device_manager(
        mut self,
        device_manager: Arc<crate::server::device::DeviceManager>,
    ) -> Self {
        self.device_manager = Some(device_manager);
        self
    }

    /// 设置事件处理器（可选）
    pub fn with_event_handler(mut self, event_handler: Arc<dyn ServerEventHandler>) -> Self {
        self.event_handler = Some(event_handler);
        self
    }

    /// 设置认证器（可选）
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

    // ============================================================
    // 配置方法（委托给 BaseServerBuilderConfig）
    // ============================================================

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

    /// 设置默认序列化格式
    pub fn with_default_format(
        mut self,
        format: crate::common::protocol::SerializationFormat,
    ) -> Self {
        self.base = self.base.with_default_format(format);
        self
    }

    /// 设置默认压缩算法
    pub fn with_default_compression(
        mut self,
        compression: crate::common::compression::CompressionAlgorithm,
    ) -> Self {
        self.base = self.base.with_default_compression(compression);
        self
    }

    /// 设置最大连接数
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.base = self.base.with_max_connections(max);
        self
    }

    /// 设置连接超时
    pub fn with_connection_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.base = self.base.with_connection_timeout(timeout);
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

    /// 设置设备冲突策略
    pub fn with_device_conflict_strategy(
        mut self,
        strategy: crate::common::device::DeviceConflictStrategy,
    ) -> Self {
        self.base = self.base.with_device_conflict_strategy(strategy);
        self
    }

    /// 构建服务端
    pub fn build(self) -> Result<FlareServer> {
        let listener = self.listener.ok_or_else(|| {
            crate::common::error::FlareError::protocol_error(
                "MessageListener is required".to_string(),
            )
        })?;

        // 创建消息管道（使用默认 JSON 解析器，协商后会更新）
        let pipeline = Arc::new(Mutex::new(MessagePipeline::new(MessageParser::json())));

        // 添加用户提供的中间件
        let middlewares = self.middlewares;
        let processors = self.processors;

        // 创建连接处理器（集成消息管道）
        let handler = Arc::new(FlareHandler {
            pipeline: pipeline.clone(),
            listener: listener.clone(),
        });

        // 直接使用 HybridServer，而不是通过 ObserverServer
        // 这样可以更好地控制消息管道的集成
        let server = HybridServer::with_connection_manager(
            self.base.config,
            handler.clone(),
            self.connection_manager,
            self.device_manager,
            self.event_handler,
            self.base.authenticator,
        )?;

        let wrapper = ServerWrapper::new(server);

        // 初始化消息管道（添加中间件和处理器）
        let pipeline_clone = pipeline.clone();
        let listener_clone = listener.clone();
        tokio::spawn(async move {
            let pipeline = pipeline_clone.lock().await;

            // 添加用户提供的中间件
            for middleware in middlewares {
                pipeline.add_middleware(middleware).await;
            }

            // 创建监听器处理器
            let listener_processor = Arc::new(ListenerProcessor {
                listener: listener_clone.clone(),
            });
            pipeline.add_processor(listener_processor).await;

            // 添加用户提供的处理器
            for processor in processors {
                pipeline.add_processor(processor).await;
            }
        });

        Ok(FlareServer {
            wrapper,
            pipeline,
            listener,
        })
    }
}

/// Flare 服务端
pub struct FlareServer {
    wrapper: ServerWrapper,
    #[allow(dead_code)] // 保留用于未来扩展（如动态更新管道配置）
    pipeline: Arc<Mutex<MessagePipeline>>,
    #[allow(dead_code)] // 保留用于未来扩展（如动态更新监听器）
    listener: Arc<dyn MessageListener>,
}

impl FlareServer {
    /// 启动服务端
    pub async fn start(&self) -> Result<()> {
        self.wrapper.start().await
    }

    /// 停止服务端
    pub async fn stop(&self) -> Result<()> {
        self.wrapper.stop().await
    }

    /// 检查服务端是否运行
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

    /// 获取 ServerHandle 组件（用于创建 DefaultServerHandle）
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

/// Flare 服务端处理器
struct FlareHandler {
    pipeline: Arc<Mutex<MessagePipeline>>,
    listener: Arc<dyn MessageListener>,
}

#[async_trait]
impl ConnectionHandler for FlareHandler {
    async fn handle_frame(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        let pipeline = self.pipeline.lock().await;
        pipeline.process_frame(frame, Some(connection_id)).await
    }

    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        info!("[FlareServer] ✅ 新连接: {}", connection_id);
        self.listener.on_connect(connection_id).await
    }

    async fn on_disconnect(&self, connection_id: &str) -> Result<()> {
        info!("[FlareServer] ❌ 连接断开: {}", connection_id);
        self.listener.on_disconnect(connection_id, None).await
    }
}

/// 监听器处理器
struct ListenerProcessor {
    listener: Arc<dyn MessageListener>,
}

#[async_trait]
impl MessageProcessor for ListenerProcessor {
    async fn process(&self, ctx: &MessageContext) -> Result<Option<Frame>> {
        if let Some(connection_id) = &ctx.connection_id {
            self.listener.on_message(&ctx.frame, connection_id).await
        } else {
            Ok(None)
        }
    }

    fn name(&self) -> &str {
        "ListenerProcessor"
    }
}
