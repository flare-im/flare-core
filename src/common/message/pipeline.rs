//! 消息处理管道
//!
//! 提供统一的消息处理流程，支持中间件、观察者、自动序列化/压缩

use crate::common::MessageParser;
use crate::common::error::{FlareError, Result};
use crate::common::protocol::Frame;
use crate::transport::events::ConnectionEvent;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

/// 消息处理上下文
///
/// 包含消息处理所需的所有上下文信息
#[derive(Clone)]
pub struct MessageContext {
    /// 原始 Frame
    pub frame: Frame,
    /// 连接 ID（服务端）或 None（客户端）
    pub connection_id: Option<String>,
    /// 消息解析器（用于序列化/压缩）
    pub parser: MessageParser,
    /// 元数据（用于中间件传递数据）
    pub metadata: Arc<RwLock<std::collections::HashMap<String, Vec<u8>>>>,
}

impl MessageContext {
    /// 创建新的消息上下文
    pub fn new(frame: Frame, connection_id: Option<String>, parser: MessageParser) -> Self {
        Self {
            frame,
            connection_id,
            parser,
            metadata: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// 设置元数据
    pub async fn set_metadata(&self, key: String, value: Vec<u8>) {
        let mut meta = self.metadata.write().await;
        meta.insert(key, value);
    }

    /// 获取元数据
    pub async fn get_metadata(&self, key: &str) -> Option<Vec<u8>> {
        let meta = self.metadata.read().await;
        meta.get(key).cloned()
    }
}

/// 消息处理中间件
///
/// 支持在消息处理前后执行自定义逻辑
#[async_trait]
pub trait MessageMiddleware: Send + Sync {
    /// 处理消息（在业务处理之前）
    ///
    /// # 参数
    /// - `ctx`: 消息上下文
    ///
    /// # 返回
    /// - `Ok(Some(Frame))`: 提前返回响应，不再继续处理
    /// - `Ok(None)`: 继续处理
    /// - `Err`: 处理失败，停止管道
    async fn before(&self, ctx: &MessageContext) -> Result<Option<Frame>> {
        let _ = ctx;
        Ok(None)
    }

    /// 处理消息（在业务处理之后）
    ///
    /// # 参数
    /// - `ctx`: 消息上下文
    /// - `response`: 业务处理返回的响应（如果有）
    ///
    /// # 返回
    /// - `Ok(Some(Frame))`: 修改后的响应
    /// - `Ok(None)`: 使用原始响应
    /// - `Err`: 处理失败
    async fn after(&self, ctx: &MessageContext, response: Option<Frame>) -> Result<Option<Frame>> {
        let _ = (ctx, response);
        Ok(None)
    }

    /// 中间件名称（用于调试和日志）
    fn name(&self) -> &str {
        "UnknownMiddleware"
    }

    /// 中间件优先级（数字越小优先级越高）
    fn priority(&self) -> u32 {
        100
    }
}

/// 线程安全的中间件引用
pub type ArcMessageMiddleware = Arc<dyn MessageMiddleware>;

/// 消息处理器
///
/// 处理具体的业务逻辑
#[async_trait]
pub trait MessageProcessor: Send + Sync {
    /// 处理消息
    ///
    /// # 参数
    /// - `ctx`: 消息上下文
    ///
    /// # 返回
    /// - `Ok(Some(Frame))`: 需要发送的响应
    /// - `Ok(None)`: 不需要响应
    /// - `Err`: 处理失败
    async fn process(&self, ctx: &MessageContext) -> Result<Option<Frame>>;

    /// 处理器名称
    fn name(&self) -> &str {
        "UnknownProcessor"
    }
}

/// 线程安全的处理器引用
pub type ArcMessageProcessor = Arc<dyn MessageProcessor>;

/// 消息处理管道
///
/// 统一的消息处理流程：
/// 1. 原始数据 → 解析（自动解压、反序列化）→ Frame
/// 2. Frame → 中间件（before）→ 处理器 → 中间件（after）→ 响应 Frame
/// 3. 响应 Frame → 序列化（压缩、序列化）→ 原始数据
#[derive(Clone)]
pub struct MessagePipeline {
    /// 中间件列表（按优先级排序）
    middlewares: Arc<RwLock<Arc<Vec<ArcMessageMiddleware>>>>,
    /// 处理器列表
    processors: Arc<RwLock<Arc<Vec<ArcMessageProcessor>>>>,
    /// 消息解析器（使用 Arc 以便在运行时更新）
    parser: Arc<tokio::sync::Mutex<MessageParser>>,
}

impl MessagePipeline {
    /// 创建新的消息处理管道
    pub fn new(parser: MessageParser) -> Self {
        Self {
            middlewares: Arc::new(RwLock::new(Arc::new(Vec::new()))),
            processors: Arc::new(RwLock::new(Arc::new(Vec::new()))),
            parser: Arc::new(tokio::sync::Mutex::new(parser)),
        }
    }

    /// 更新消息解析器（协商完成后调用）
    pub async fn update_parser(&self, parser: MessageParser) {
        let mut p = self.parser.lock().await;
        *p = parser;
    }

    /// 添加中间件
    pub async fn add_middleware(&self, middleware: ArcMessageMiddleware) {
        let mut middlewares = self.middlewares.write().await;
        let mut next = (**middlewares).clone();
        next.push(middleware);
        // 按优先级排序
        next.sort_by_key(|m| m.priority());
        *middlewares = Arc::new(next);
    }

    /// 移除中间件
    pub async fn remove_middleware(&self, middleware: &ArcMessageMiddleware) {
        let mut middlewares = self.middlewares.write().await;
        let mut next = (**middlewares).clone();
        next.retain(|m| !Arc::ptr_eq(m, middleware));
        *middlewares = Arc::new(next);
    }

    /// 添加处理器
    pub async fn add_processor(&self, processor: ArcMessageProcessor) {
        let mut processors = self.processors.write().await;
        let mut next = (**processors).clone();
        next.push(processor);
        *processors = Arc::new(next);
    }

    /// 移除处理器
    pub async fn remove_processor(&self, processor: &ArcMessageProcessor) {
        let mut processors = self.processors.write().await;
        let mut next = (**processors).clone();
        next.retain(|p| !Arc::ptr_eq(p, processor));
        *processors = Arc::new(next);
    }

    /// 处理原始数据（自动解析）
    ///
    /// # 参数
    /// - `data`: 原始字节数据
    /// - `connection_id`: 连接 ID（服务端）或 None（客户端）
    ///
    /// # 返回
    /// - `Ok(Some(Vec<u8>))`: 需要发送的响应数据
    /// - `Ok(None)`: 不需要响应
    /// - `Err`: 处理失败
    pub async fn process_raw(
        &self,
        data: &[u8],
        connection_id: Option<&str>,
    ) -> Result<Option<Vec<u8>>> {
        // 1. 解析消息（自动解压、反序列化）
        let parser = self.parser.lock().await;
        let frame = parser.parse(data).map_err(|e| {
            FlareError::deserialization_error(format!("Failed to parse message: {}", e))
        })?;
        let parser_snapshot = parser.clone();
        drop(parser);

        // 2. 处理 Frame
        let response = self
            .process_frame_with_parser(&frame, connection_id, parser_snapshot.clone())
            .await?;

        // 3. 序列化响应（如果有）
        if let Some(response_frame) = response {
            let response_data = parser_snapshot.serialize(&response_frame).map_err(|e| {
                FlareError::encoding_error(format!("Failed to serialize response: {}", e))
            })?;
            Ok(Some(response_data))
        } else {
            Ok(None)
        }
    }

    async fn middleware_snapshot(&self) -> Arc<Vec<ArcMessageMiddleware>> {
        let middlewares = self.middlewares.read().await;
        Arc::clone(&middlewares)
    }

    async fn processor_snapshot(&self) -> Arc<Vec<ArcMessageProcessor>> {
        let processors = self.processors.read().await;
        Arc::clone(&processors)
    }

    /// 处理 Frame
    ///
    /// # 参数
    /// - `frame`: 消息 Frame
    /// - `connection_id`: 连接 ID（服务端）或 None（客户端）
    ///
    /// # 返回
    /// - `Ok(Some(Frame))`: 需要发送的响应 Frame
    /// - `Ok(None)`: 不需要响应
    /// - `Err`: 处理失败
    pub async fn process_frame(
        &self,
        frame: &Frame,
        connection_id: Option<&str>,
    ) -> Result<Option<Frame>> {
        let parser = self.parser.lock().await;
        let parser_snapshot = parser.clone();
        drop(parser);

        self.process_frame_with_parser(frame, connection_id, parser_snapshot)
            .await
    }

    async fn process_frame_with_parser(
        &self,
        frame: &Frame,
        connection_id: Option<&str>,
        parser: MessageParser,
    ) -> Result<Option<Frame>> {
        // 创建消息上下文
        let ctx = MessageContext::new(frame.clone(), connection_id.map(|s| s.to_string()), parser);

        // 1. 执行中间件（before）
        let middlewares = self.middleware_snapshot().await;
        for middleware in middlewares.iter() {
            if let Some(response) = middleware.before(&ctx).await? {
                // 中间件提前返回响应
                return Ok(Some(response));
            }
        }

        // 2. 执行处理器
        let processors = self.processor_snapshot().await;
        let mut response = None;
        for processor in processors.iter() {
            if let Some(resp) = processor.process(&ctx).await? {
                response = Some(resp);
                break; // 第一个返回响应的处理器生效
            }
        }

        // 3. 执行中间件（after）
        let middlewares = self.middleware_snapshot().await;
        for middleware in middlewares.iter() {
            if let Some(modified_response) = middleware.after(&ctx, response.clone()).await? {
                response = Some(modified_response);
            }
        }

        Ok(response)
    }

    /// 处理连接事件
    ///
    /// # 参数
    /// - `event`: 连接事件
    /// - `connection_id`: 连接 ID（服务端）或 None（客户端）
    pub async fn handle_connection_event(
        &self,
        _event: &ConnectionEvent,
        _connection_id: Option<&str>,
    ) -> Result<()> {
        // 连接事件可以传递给中间件处理
        let middlewares = self.middleware_snapshot().await;
        for _middleware in middlewares.iter() {
            // 如果中间件实现了连接事件处理，可以在这里调用
            // 目前先跳过，后续可以扩展
        }
        Ok(())
    }
}

impl Default for MessagePipeline {
    fn default() -> Self {
        Self::new(MessageParser::protobuf())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::{
        Command, FrameBuilder, Reliability, SerializationFormat, frame_with_system_command, ping,
        pong,
    };
    use tokio::sync::{Mutex, oneshot};

    struct BlockingMiddleware {
        entered_tx: Mutex<Option<oneshot::Sender<()>>>,
        release_rx: Mutex<Option<oneshot::Receiver<()>>>,
    }

    #[async_trait]
    impl MessageMiddleware for BlockingMiddleware {
        async fn before(&self, _ctx: &MessageContext) -> Result<Option<Frame>> {
            if let Some(tx) = self.entered_tx.lock().await.take() {
                let _ = tx.send(());
            }
            if let Some(rx) = self.release_rx.lock().await.take() {
                let _ = rx.await;
            }
            Ok(None)
        }
    }

    struct NoopMiddleware;

    #[async_trait]
    impl MessageMiddleware for NoopMiddleware {}

    struct ParserUpdatingProcessor {
        pipeline: MessagePipeline,
    }

    #[async_trait]
    impl MessageProcessor for ParserUpdatingProcessor {
        async fn process(&self, _ctx: &MessageContext) -> Result<Option<Frame>> {
            self.pipeline.update_parser(MessageParser::protobuf()).await;
            Ok(Some(frame_with_system_command(
                pong(),
                Reliability::BestEffort,
            )))
        }
    }

    #[tokio::test]
    async fn middleware_updates_do_not_wait_for_inflight_middleware_to_finish() {
        let pipeline = MessagePipeline::new(MessageParser::json());
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        pipeline
            .add_middleware(Arc::new(BlockingMiddleware {
                entered_tx: Mutex::new(Some(entered_tx)),
                release_rx: Mutex::new(Some(release_rx)),
            }))
            .await;

        let frame = FrameBuilder::new()
            .with_command(Command {
                r#type: Some(
                    crate::common::protocol::flare::core::commands::command::Type::System(ping()),
                ),
            })
            .with_reliability(Reliability::BestEffort)
            .build();
        let processing = {
            let pipeline = pipeline.clone();
            tokio::spawn(async move { pipeline.process_frame(&frame, Some("conn-1")).await })
        };

        entered_rx.await.expect("blocking middleware should start");
        let add_result = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            pipeline.add_middleware(Arc::new(NoopMiddleware)),
        )
        .await;

        let _ = release_tx.send(());
        processing
            .await
            .expect("pipeline task should not panic")
            .expect("pipeline should succeed");

        assert!(
            add_result.is_ok(),
            "middleware updates should use a copy-on-write snapshot and avoid waiting for in-flight middleware"
        );
    }

    #[tokio::test]
    async fn process_raw_serializes_response_with_request_parser_snapshot() {
        let json_parser = MessageParser::json();
        let pipeline = MessagePipeline::new(json_parser.clone());
        pipeline
            .add_processor(Arc::new(ParserUpdatingProcessor {
                pipeline: pipeline.clone(),
            }))
            .await;
        let request = frame_with_system_command(ping(), Reliability::BestEffort);
        let request_data = json_parser
            .serialize(&request)
            .expect("json request should serialize");

        let response_data = pipeline
            .process_raw(&request_data, Some("conn-1"))
            .await
            .expect("pipeline should process request")
            .expect("processor should produce response");

        let response = json_parser.parse_with_format(&response_data, SerializationFormat::Json).expect(
            "response should use the same parser snapshot as request even if parser is updated mid-pipeline",
        );
        let format = response
            .command
            .and_then(|command| command.r#type)
            .and_then(|kind| match kind {
                crate::common::protocol::flare::core::commands::command::Type::System(system) => {
                    SerializationFormat::try_from(system.format).ok()
                }
                _ => None,
            })
            .expect("response should be a system command");
        assert_eq!(format, SerializationFormat::Protobuf);
    }
}
