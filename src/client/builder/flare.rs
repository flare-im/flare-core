//! Flare 客户端构建器（精装修）
//!
//! 提供完整功能，包含所有 `common` 和 `client` 模块的能力
//! 用户只需简单配置即可使用，也可以自定义中间件、处理器等扩展功能
//!
//! ## 特点
//! - ✅ **消息管道**：自动处理序列化、压缩、加密
//! - ✅ **中间件支持**：日志、性能监控、验证等
//! - ✅ **处理器链**：可组合多个处理器
//! - ✅ **序列化协商**：自动协商最佳序列化格式（JSON/Protobuf）
//! - ✅ **压缩协商**：自动协商压缩算法（Gzip/Zstd/None）
//! - ✅ **加密支持**：AES-256-GCM 加密
//! - ✅ **心跳管理**：自动心跳和超时管理
//! - ✅ **自动重连**：支持断线重连
//! - ✅ **多协议支持**：WebSocket + QUIC 双协议竞速
//! - ✅ **简单易用**：只需实现 `MessageListener` 即可
//! - ✅ **高度可扩展**：可以自定义中间件、处理器覆盖默认实现
//! - ✅ **统一事件处理**：使用 Observer 模式统一处理所有事件（连接事件、消息事件等）
//!
//! ## 适用场景
//! - 生产环境
//! - 需要完整功能的企业应用
//! - 需要高性能和可扩展性的场景
//! - 需要统一消息处理流程的场景

use crate::client::builder::{BaseClientBuilderConfig, ClientWrapper};
use crate::client::{Client, HybridClient};
use crate::common::MessageParser;
use crate::common::error::Result;
use crate::common::message::{
    ArcMessageMiddleware, ArcMessageProcessor, MessageContext, MessagePipeline, MessageProcessor,
};
use crate::common::protocol::Frame;
use crate::transport::events::{ConnectionEvent, ConnectionObserver};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// 消息监听器
///
/// 用户只需要实现这个简单的接口就能处理消息
#[async_trait]
pub trait MessageListener: Send + Sync {
    /// 处理收到的消息
    ///
    /// # 参数
    /// - `frame`: 收到的消息 Frame
    ///
    /// # 返回
    /// - `Ok(Some(Frame))`: 需要发送的响应
    /// - `Ok(None)`: 不需要响应
    /// - `Err`: 处理失败
    async fn on_message(&self, frame: &Frame) -> Result<Option<Frame>> {
        let _ = frame;
        Ok(None)
    }

    /// 连接建立时调用
    async fn on_connect(&self) -> Result<()> {
        Ok(())
    }

    /// 连接断开时调用
    async fn on_disconnect(&self, reason: Option<&str>) -> Result<()> {
        let _ = reason;
        Ok(())
    }

    /// 连接错误时调用
    async fn on_error(&self, error: &str) -> Result<()> {
        let _ = error;
        Ok(())
    }
}

/// Flare 客户端构建器
///
/// 提供简单易用的 API，自动集成所有功能：
/// - 消息管道（中间件、处理器）
/// - 序列化协商
/// - 压缩/解压
/// - 加密/解密
/// - 心跳管理
/// - 自动重连
pub struct FlareClientBuilder {
    base: BaseClientBuilderConfig,
    listener: Option<Arc<dyn MessageListener>>,
    middlewares: Vec<ArcMessageMiddleware>,
    processors: Vec<ArcMessageProcessor>,
    observers: Vec<Arc<dyn ConnectionObserver>>,
}

impl FlareClientBuilder {
    /// 创建新的 Flare 客户端构建器
    pub fn new(server_url: impl Into<String>) -> Self {
        Self {
            base: BaseClientBuilderConfig::new(server_url),
            listener: None,
            middlewares: Vec::new(),
            processors: Vec::new(),
            observers: Vec::new(),
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

    /// 添加连接观察者（可选）
    ///
    /// 观察者会收到连接事件（Connected、Disconnected、Error、Message）
    ///
    /// ## 设计说明
    ///
    /// 统一使用 Observer 模式处理所有事件，包括：
    /// - 连接事件（Connected、Disconnected、Error）
    /// - 消息事件（Message，包含已解析的 Frame）
    ///
    /// 这种设计符合 IM SDK 的最佳实践（参考微信、飞书、WhatsApp 等），具有以下优势：
    /// - **简化 API**：用户只需要实现一个接口
    /// - **更灵活**：可以注册多个观察者，每个处理不同的事情
    /// - **更符合观察者模式**：所有事件都通过观察者处理
    /// - **减少概念复杂度**：不需要理解多个不同的机制
    ///
    /// ## 示例
    ///
    /// ```rust,no_run
    /// use flare_core::client::builder::flare::FlareClientBuilder;
    /// use flare_core::transport::events::{ConnectionObserver, ConnectionEvent};
    /// use std::sync::Arc;
    ///
    /// struct MyObserver;
    ///
    /// impl ConnectionObserver for MyObserver {
    ///     fn on_event(&self, event: &ConnectionEvent) {
    ///         match event {
    ///             ConnectionEvent::Connected => {
    ///                 println!("已连接");
    ///             }
    ///             ConnectionEvent::Disconnected(reason) => {
    ///                 println!("连接断开: {}", reason);
    ///             }
    ///             ConnectionEvent::Message(data) => {
    ///                 // 处理原始消息数据
    ///                 println!("收到消息: {} bytes", data.len());
    ///             }
    ///             ConnectionEvent::Error(err) => {
    ///                 println!("连接错误: {:?}", err);
    ///             }
    ///         }
    ///     }
    /// }
    ///
    /// let builder = FlareClientBuilder::new("ws://127.0.0.1:8080")
    ///     .with_observer(Arc::new(MyObserver));
    /// ```
    pub fn with_observer(mut self, observer: Arc<dyn ConnectionObserver>) -> Self {
        self.observers.push(observer);
        self
    }

    // ============================================================
    // 配置方法（委托给 BaseClientBuilderConfig）
    // ============================================================

    /// 设置传输协议
    pub fn with_protocol(
        mut self,
        protocol: crate::common::config_types::TransportProtocol,
    ) -> Self {
        self.base = self.base.with_protocol(protocol);
        self
    }

    /// 启用多协议竞速
    pub fn with_protocol_race(
        mut self,
        protocols: Vec<crate::common::config_types::TransportProtocol>,
    ) -> Self {
        self.base = self.base.with_protocol_race(protocols);
        self
    }

    /// 为特定协议设置服务器地址
    pub fn with_protocol_url(
        mut self,
        protocol: crate::common::config_types::TransportProtocol,
        url: String,
    ) -> Self {
        self.base = self.base.with_protocol_url(protocol, url);
        self
    }

    /// 设置用户 ID
    pub fn with_user_id(mut self, user_id: String) -> Self {
        self.base = self.base.with_user_id(user_id);
        self
    }

    /// 设置序列化格式
    pub fn with_format(mut self, format: crate::common::protocol::SerializationFormat) -> Self {
        self.base = self.base.with_format(format);
        self
    }

    /// 设置压缩算法
    pub fn with_compression(
        mut self,
        compression: crate::common::compression::CompressionAlgorithm,
    ) -> Self {
        self.base = self.base.with_compression(compression);
        self
    }

    /// 强制指定序列化格式
    pub fn force_format(mut self, format: crate::common::protocol::SerializationFormat) -> Self {
        self.base = self.base.force_format(format);
        self
    }

    /// 强制指定压缩算法
    pub fn force_compression(
        mut self,
        compression: crate::common::compression::CompressionAlgorithm,
    ) -> Self {
        self.base = self.base.force_compression(compression);
        self
    }

    /// 设置设备信息
    pub fn with_device_info(mut self, device_info: crate::common::device::DeviceInfo) -> Self {
        self.base = self.base.with_device_info(device_info);
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

    /// 设置连接超时
    pub fn with_connect_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.base = self.base.with_connect_timeout(timeout);
        self
    }

    /// 设置协议竞速超时
    pub fn with_race_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.base = self.base.with_race_timeout(timeout);
        self
    }

    /// 设置重连间隔
    pub fn with_reconnect_interval(mut self, interval: std::time::Duration) -> Self {
        self.base = self.base.with_reconnect_interval(interval);
        self
    }

    /// 设置最大重连次数
    pub fn with_max_reconnect_attempts(mut self, attempts: Option<u32>) -> Self {
        self.base = self.base.with_max_reconnect_attempts(attempts);
        self
    }

    /// 设置 Token（用于认证）
    pub fn with_token(mut self, token: String) -> Self {
        self.base = self.base.with_token(token);
        self
    }

    /// 启用消息路由
    pub fn enable_router(mut self) -> Self {
        self.base = self.base.enable_router();
        self
    }

    /// 构建客户端（使用协议竞速）
    pub async fn build_with_race(self) -> Result<FlareClient> {
        let listener = self.listener.ok_or_else(|| {
            crate::common::error::FlareError::protocol_error(
                "MessageListener is required".to_string(),
            )
        })?;

        // 创建消息管道（使用默认 JSON 解析器，协商后会更新）
        let pipeline = Arc::new(Mutex::new(MessagePipeline::new(MessageParser::json())));

        // 添加用户提供的中间件
        for middleware in self.middlewares {
            pipeline.lock().await.add_middleware(middleware).await;
        }

        // 创建消息处理器（委托给 listener）
        let listener_processor = Arc::new(ListenerProcessor {
            listener: listener.clone(),
        });
        pipeline
            .lock()
            .await
            .add_processor(listener_processor)
            .await;

        // 添加用户提供的处理器
        for processor in self.processors {
            pipeline.lock().await.add_processor(processor).await;
        }

        // 直接使用 HybridClient，而不是通过 ObserverClient
        let client = HybridClient::connect_with_race(self.base.config.clone()).await?;
        let wrapper = ClientWrapper::new(client);

        // 创建观察者（集成消息管道）
        let observer = Arc::new(FlareObserver {
            pipeline: pipeline.clone(),
            listener: listener.clone(),
        });

        let observer_clone = observer.clone();

        // 添加观察者（FlareObserver 和用户提供的 observers）
        // 注意：统一使用 Observer 模式，不再使用 event_handler
        // 所有事件处理都通过 Observer 完成，更符合 IM SDK 的最佳实践
        {
            let mut client = wrapper.client().lock().await;
            client.add_observer(observer_clone);

            // 添加用户提供的 observers
            for observer in self.observers {
                client.add_observer(observer);
            }
        }

        Ok(FlareClient {
            wrapper,
            pipeline,
            listener,
        })
    }
}

/// Flare 客户端
pub struct FlareClient {
    wrapper: ClientWrapper,
    #[allow(dead_code)] // 保留用于未来扩展（如动态更新管道配置）
    pipeline: Arc<Mutex<MessagePipeline>>,
    #[allow(dead_code)] // 保留用于未来扩展（如动态更新监听器）
    listener: Arc<dyn MessageListener>,
}

impl FlareClient {
    /// 发送消息 Frame
    pub async fn send_frame(&self, frame: &Frame) -> Result<()> {
        self.wrapper.send_frame(frame).await
    }

    /// 发送并等待响应（按 message_id 匹配）
    pub async fn send_frame_and_wait(
        &self,
        frame: &Frame,
        timeout: std::time::Duration,
    ) -> Result<Frame> {
        self.wrapper.send_frame_and_wait(frame, timeout).await
    }

    /// 检查是否已连接
    pub fn is_connected(&self) -> bool {
        self.wrapper.is_connected()
    }

    /// 断开连接
    pub async fn disconnect(self) -> Result<()> {
        self.wrapper.disconnect().await
    }

    /// 获取连接 ID
    pub fn connection_id(&self) -> Option<String> {
        self.wrapper.connection_id()
    }

    /// 获取活动协议
    pub fn active_protocol(&self) -> crate::common::config_types::TransportProtocol {
        self.wrapper.active_protocol()
    }

    /// 更新消息管道解析器（协商完成后调用）
    pub async fn update_parser(&self, parser: MessageParser) {
        let mut pipeline = self.pipeline.lock().await;
        *pipeline = MessagePipeline::new(parser);
    }

    /// 添加连接观察者
    ///
    /// 观察者会收到连接事件（Connected、Disconnected、Error、Message）
    pub async fn add_observer(&self, observer: Arc<dyn ConnectionObserver>) {
        let mut client = self.wrapper.client().lock().await;
        client.add_observer(observer);
    }
}

/// Flare 客户端观察者
struct FlareObserver {
    pipeline: Arc<Mutex<MessagePipeline>>,
    listener: Arc<dyn MessageListener>,
}

impl ConnectionObserver for FlareObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Connected => {
                info!("[FlareClient] ✅ 已连接");
                // 在后台任务中调用 listener.on_connect
                let listener = self.listener.clone();
                tokio::spawn(async move {
                    if let Err(e) = listener.on_connect().await {
                        error!("[FlareClient] on_connect 失败: {}", e);
                    }
                });
            }

            ConnectionEvent::Disconnected(reason) => {
                let reason_clone = reason.clone();
                info!("[FlareClient] ❌ 连接断开: {}", reason_clone);
                let listener = self.listener.clone();
                tokio::spawn(async move {
                    if let Err(e) = listener.on_disconnect(Some(&reason_clone)).await {
                        error!("[FlareClient] on_disconnect 失败: {}", e);
                    }
                });
            }

            ConnectionEvent::Error(err) => {
                // 过滤协议竞速时关闭未选中连接导致的错误
                // 这些错误是正常的，因为协议竞速会选择最快的协议，其他协议会被关闭
                let err_str = format!("{:?}", err);
                // 协议竞速错误：关闭未选中连接时的正常错误
                let is_race_error = err_str.contains("Connection reset without closing handshake")
                    || (err_str.contains("ConnectionFailed")
                        && err_str.contains("WebSocket protocol error"));

                // 连接丢失错误：可能是网络问题或服务器关闭连接
                let is_connection_lost = err_str.contains("connection lost")
                    || err_str.contains("connection closed")
                    || err_str.contains("Connection reset");

                if is_race_error {
                    // 协议竞速相关的错误，只记录 debug 级别（不通知 listener）
                    debug!(
                        "[FlareClient] 协议竞速：未选中协议连接已关闭（这是正常的，协议竞速会选择最快的协议）"
                    );
                } else if is_connection_lost {
                    // 连接丢失错误，记录为 warn 级别并通知 listener
                    // 注意：底层客户端（WebSocketClient/QUICClient）会自动尝试重连
                    warn!("[FlareClient] 连接丢失: {:?}", err);
                    info!("[FlareClient] 💡 底层客户端将自动尝试重连（如果配置了重连）");
                    let listener = self.listener.clone();
                    let err_str_clone = err_str.clone();
                    tokio::spawn(async move {
                        if let Err(e) = listener.on_error(&err_str_clone).await {
                            error!("[FlareClient] on_error 失败: {}", e);
                        }
                    });
                } else {
                    // 其他错误，正常记录并通知 listener
                    warn!("[FlareClient] 连接错误: {:?}", err);
                    let listener = self.listener.clone();
                    let err_str_clone = err_str.clone();
                    tokio::spawn(async move {
                        if let Err(e) = listener.on_error(&err_str_clone).await {
                            error!("[FlareClient] on_error 失败: {}", e);
                        }
                    });
                }
            }

            ConnectionEvent::Message(data) => {
                // 使用消息管道处理消息
                let pipeline = self.pipeline.clone();
                let data = data.clone();
                tokio::spawn(async move {
                    let pipeline = pipeline.lock().await;
                    match pipeline.process_raw(&data, None).await {
                        Ok(Some(_response_data)) => {
                            debug!(
                                "[FlareClient] 消息管道返回响应，但客户端无法自动发送，需要用户手动处理"
                            );
                            // 注意：这里无法直接发送响应，因为需要访问 client
                            // 用户应该在 listener.on_message 中返回响应 Frame
                        }
                        Ok(None) => {
                            debug!("[FlareClient] 消息处理完成，无需响应");
                        }
                        Err(e) => {
                            error!("[FlareClient] 消息管道处理失败: {}", e);
                        }
                    }
                });
            }
        }
    }
}

/// 监听器处理器
struct ListenerProcessor {
    listener: Arc<dyn MessageListener>,
}

#[async_trait]
impl MessageProcessor for ListenerProcessor {
    async fn process(&self, ctx: &MessageContext) -> Result<Option<Frame>> {
        self.listener.on_message(&ctx.frame).await
    }

    fn name(&self) -> &str {
        "ListenerProcessor"
    }
}
