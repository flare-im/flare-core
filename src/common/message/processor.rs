//! 消息处理器实现
//!
//! 提供常用的消息处理器实现

use super::pipeline::{MessageContext, MessageProcessor};
use crate::common::error::Result;
use crate::common::protocol::Frame;
use async_trait::async_trait;
use std::sync::Arc;

/// 函数式消息处理器
///
/// 使用闭包处理消息，适合简单场景
pub struct FunctionProcessor {
    name: String,
    handler: Arc<
        dyn Fn(
                &MessageContext,
            )
                -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Frame>>> + Send>>
            + Send
            + Sync,
    >,
}

impl FunctionProcessor {
    /// 创建新的函数式处理器
    pub fn new<F, Fut>(name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(&MessageContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Option<Frame>>> + Send + 'static,
    {
        Self {
            name: name.into(),
            handler: Arc::new(move |ctx| Box::pin(handler(ctx))),
        }
    }
}

#[async_trait]
impl MessageProcessor for FunctionProcessor {
    async fn process(&self, ctx: &MessageContext) -> Result<Option<Frame>> {
        (self.handler)(ctx).await
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// 委托处理器
///
/// 将消息委托给其他处理器（如 ConnectionHandler）
pub struct DelegateProcessor {
    name: String,
    handler: Arc<
        dyn Fn(
                &Frame,
                Option<&str>,
            )
                -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Frame>>> + Send>>
            + Send
            + Sync,
    >,
}

impl DelegateProcessor {
    /// 创建新的委托处理器
    pub fn new<F, Fut>(name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(&Frame, Option<&str>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Option<Frame>>> + Send + 'static,
    {
        Self {
            name: name.into(),
            handler: Arc::new(move |frame, conn_id| Box::pin(handler(frame, conn_id))),
        }
    }
}

#[async_trait]
impl MessageProcessor for DelegateProcessor {
    async fn process(&self, ctx: &MessageContext) -> Result<Option<Frame>> {
        (self.handler)(&ctx.frame, ctx.connection_id.as_deref()).await
    }

    fn name(&self) -> &str {
        &self.name
    }
}
