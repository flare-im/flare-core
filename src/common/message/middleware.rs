//! 消息处理中间件
//!
//! 提供常用的中间件实现

use super::pipeline::{MessageContext, MessageMiddleware};
use crate::common::error::Result;
use crate::common::platform::wall_clock_ms;
use crate::common::protocol::Frame;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// 日志中间件
///
/// 记录所有消息的日志
pub struct LoggingMiddleware {
    name: String,
    log_level: LogLevel,
}

#[derive(Clone, Copy)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
}

impl LoggingMiddleware {
    /// 创建新的日志中间件
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            log_level: LogLevel::Info,
        }
    }

    /// 设置日志级别
    pub fn with_level(mut self, level: LogLevel) -> Self {
        self.log_level = level;
        self
    }
}

#[async_trait]
impl MessageMiddleware for LoggingMiddleware {
    async fn before(&self, ctx: &MessageContext) -> Result<Option<Frame>> {
        match self.log_level {
            LogLevel::Debug => {
                debug!(
                    connection_id = ?ctx.connection_id,
                    message_id = %ctx.frame.message_id,
                    "Processing message"
                );
            }
            LogLevel::Info => {
                info!(
                    connection_id = ?ctx.connection_id,
                    message_id = %ctx.frame.message_id,
                    "Processing message"
                );
            }
            LogLevel::Warn => {
                warn!(
                    connection_id = ?ctx.connection_id,
                    message_id = %ctx.frame.message_id,
                    "Processing message"
                );
            }
        }
        Ok(None)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn priority(&self) -> u32 {
        10 // 高优先级，最先执行
    }
}

/// 性能监控中间件
///
/// 记录消息处理耗时
pub struct MetricsMiddleware {
    name: String,
}

impl MetricsMiddleware {
    /// 创建新的性能监控中间件
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[async_trait]
impl MessageMiddleware for MetricsMiddleware {
    async fn before(&self, ctx: &MessageContext) -> Result<Option<Frame>> {
        // 记录开始时间
        ctx.set_metadata(
            "start_time".to_string(),
            wall_clock_ms().to_le_bytes().to_vec(),
        )
        .await;
        Ok(None)
    }

    async fn after(&self, ctx: &MessageContext, response: Option<Frame>) -> Result<Option<Frame>> {
        // 计算处理耗时
        if let Some(start_bytes) = ctx.get_metadata("start_time").await {
            let start_ms = u64::from_le_bytes(start_bytes.try_into().unwrap_or([0; 8]));
            let duration_ms = wall_clock_ms().saturating_sub(start_ms);

            debug!(
                connection_id = ?ctx.connection_id,
                message_id = %ctx.frame.message_id,
                duration_ms,
                "Message processed"
            );
        }
        Ok(response)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn priority(&self) -> u32 {
        20
    }
}

/// 验证中间件
///
/// 验证消息格式和内容
pub struct ValidationMiddleware {
    name: String,
    #[allow(clippy::type_complexity)]
    validator: Arc<dyn Fn(&Frame) -> Result<()> + Send + Sync>,
}

impl ValidationMiddleware {
    /// 创建新的验证中间件
    pub fn new<F>(name: impl Into<String>, validator: F) -> Self
    where
        F: Fn(&Frame) -> Result<()> + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            validator: Arc::new(validator),
        }
    }
}

#[async_trait]
impl MessageMiddleware for ValidationMiddleware {
    async fn before(&self, ctx: &MessageContext) -> Result<Option<Frame>> {
        (self.validator)(&ctx.frame)?;
        Ok(None)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn priority(&self) -> u32 {
        5 // 最高优先级，最先验证
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::MessageParser;
    use crate::common::platform::wall_clock_ms;
    use crate::common::protocol::{FrameBuilder, Reliability, ping};

    #[tokio::test]
    async fn metrics_middleware_records_absolute_start_time_millis() {
        let frame = FrameBuilder::new()
            .with_command(crate::common::protocol::Command {
                r#type: Some(
                    crate::common::protocol::flare::core::commands::command::Type::System(ping()),
                ),
            })
            .with_reliability(Reliability::BestEffort)
            .build();
        let ctx = MessageContext::new(frame, Some("conn-1".to_string()), MessageParser::json());
        let metrics = MetricsMiddleware::new("metrics");

        let before = wall_clock_ms();
        metrics.before(&ctx).await.expect("before should succeed");
        let after = wall_clock_ms();

        let start_bytes = ctx
            .get_metadata("start_time")
            .await
            .expect("metrics should store start_time");
        let start_ms = u64::from_le_bytes(
            start_bytes
                .try_into()
                .expect("start_time should be a u64 millis value"),
        );

        assert!(
            (before..=after).contains(&start_ms),
            "start_time should be an absolute wall-clock millis timestamp, got {start_ms}, expected within {before}..={after}"
        );
    }
}
