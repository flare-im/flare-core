//! 消息处理器

use crate::common::protocol::frame::Frame;
use crate::common::error::FlareError;
use std::sync::Arc;

/// 消息处理器trait
pub trait MessageHandler: Send + Sync {
    /// 处理消息
    fn handle_message(&self, frame: Frame) -> Result<(), FlareError>;
}

/// 异步消息处理器trait
#[async_trait::async_trait]
pub trait AsyncMessageHandler: Send + Sync {
    /// 异步处理消息
    async fn handle_message(&self, frame: Frame) -> Result<(), FlareError>;
}

/// 默认消息处理器
pub struct DefaultMessageHandler;

impl MessageHandler for DefaultMessageHandler {
    fn handle_message(&self, _frame: Frame) -> Result<(), FlareError> {
        // 默认实现：简单记录日志
        tracing::debug!("处理消息");
        Ok(())
    }
}

impl Default for DefaultMessageHandler {
    fn default() -> Self {
        Self
    }
}

/// 消息处理器包装器
pub struct MessageHandlerWrapper {
    sync_handler: Option<Arc<dyn MessageHandler>>,
    async_handler: Option<Arc<dyn AsyncMessageHandler>>,
}

impl MessageHandlerWrapper {
    pub fn new_sync(handler: Arc<dyn MessageHandler>) -> Self {
        Self {
            sync_handler: Some(handler),
            async_handler: None,
        }
    }
    
    pub fn new_async(handler: Arc<dyn AsyncMessageHandler>) -> Self {
        Self {
            sync_handler: None,
            async_handler: Some(handler),
        }
    }
    
    /// 设置同步消息处理器
    pub fn set_sync_handler(&mut self, handler: Arc<dyn MessageHandler>) {
        self.sync_handler = Some(handler);
        self.async_handler = None;
    }
    
    /// 设置异步消息处理器
    pub fn set_async_handler(&mut self, handler: Arc<dyn AsyncMessageHandler>) {
        self.async_handler = Some(handler);
        self.sync_handler = None;
    }
    
    /// 移除所有处理器
    pub fn clear_handler(&mut self) {
        self.sync_handler = None;
        self.async_handler = None;
    }
    
    pub fn handle_message(&self, frame: Frame) -> Result<(), FlareError> {
        if let Some(handler) = &self.sync_handler {
            handler.handle_message(frame)
        } else if let Some(_handler) = &self.async_handler {
            // 对于异步处理器，我们在同步上下文中需要特殊处理
            // 这里我们只是记录日志并返回Ok
            tracing::debug!("异步消息处理器被同步调用");
            Ok(())
        } else {
            // 默认处理
            DefaultMessageHandler::default().handle_message(frame)
        }
    }
}