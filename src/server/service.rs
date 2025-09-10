//! 服务模块
//!
//! 提供具体协议的服务实现
//!
//! # 核心组件
//!
//! - [MessageHandler](trait.MessageHandler.html): 消息处理器接口
//! - [ServerService](trait.ServerService.html): 服务接口
//! - [EchoMessageHandler](struct.EchoMessageHandler.html): 回显消息处理器实现

use std::sync::Arc;

use crate::common::{
    error::Result,
    protocol::Frame,
};

/// 消息处理器 trait
///
/// 定义消息处理接口，所有消息处理器都必须实现此 trait
#[async_trait::async_trait]
pub trait MessageHandler: Send + Sync {
    /// 处理接收到的消息
    ///
    /// # 参数
    ///
    /// * `connection_id` - 连接ID
    /// * `message` - 接收到的消息
    ///
    /// # 返回值
    ///
    /// 返回处理结果，如果需要响应消息则返回 Some(Frame)，否则返回 None
    async fn handle_message(&self, connection_id: String, message: Frame) -> Result<Option<Frame>>;
}

/// 服务 trait
///
/// 定义服务接口，所有服务都必须实现此 trait
#[async_trait::async_trait]
pub trait ServerService: Send + Sync {
    /// 启动服务
    ///
    /// # 返回值
    ///
    /// 返回操作结果
    async fn start(&self) -> Result<()>;
    
    /// 停止服务
    async fn stop(&self);
    
    /// 设置消息处理器
    ///
    /// # 参数
    ///
    /// * `handler` - 消息处理器
    async fn set_message_handler(&self, handler: Arc<dyn MessageHandler>);
}

/// 示例消息处理器实现
///
/// 简单地将接收到的消息回显回去
pub struct EchoMessageHandler;

#[async_trait::async_trait]
impl MessageHandler for EchoMessageHandler {
    async fn handle_message(&self, _connection_id: String, message: Frame) -> Result<Option<Frame>> {
        // 简单回显消息
        Ok(Some(message))
    }
}

/// 日志消息处理器实现
///
/// 记录接收到的消息并返回确认
pub struct LoggingEchoMessageHandler;

#[async_trait::async_trait]
impl MessageHandler for LoggingEchoMessageHandler {
    async fn handle_message(&self, connection_id: String, message: Frame) -> Result<Option<Frame>> {
        tracing::info!("收到消息 - 连接: {}, 消息类型: {:?}, 消息ID: {}, 可靠性: {:?}, 数据长度: {}", 
                      connection_id, 
                      message.message_type, 
                      message.message_id, 
                      message.reliability, 
                      message.payload.len());
        
        // 简单回显消息
        Ok(Some(message))
    }
}

/// 空消息处理器实现
///
/// 不处理消息，直接返回空响应
pub struct NullMessageHandler;

#[async_trait::async_trait]
impl MessageHandler for NullMessageHandler {
    async fn handle_message(&self, _connection_id: String, _message: Frame) -> Result<Option<Frame>> {
        // 不处理消息，返回空响应
        Ok(None)
    }
}

/// 错误消息处理器实现
///
/// 总是返回错误响应
pub struct ErrorMessageHandler {
    error_message: String,
}

impl ErrorMessageHandler {
    /// 创建新的错误消息处理器
    pub fn new(error_message: String) -> Self {
        Self {
            error_message,
        }
    }
}

#[async_trait::async_trait]
impl MessageHandler for ErrorMessageHandler {
    async fn handle_message(&self, _connection_id: String, _message: Frame) -> Result<Option<Frame>> {
        // 返回错误
        Err(crate::common::error::FlareError::general_error(self.error_message.clone()))
    }
}