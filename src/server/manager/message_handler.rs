//! 增强的消息处理器
//!
//! 提供更丰富的消息处理功能

use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tracing::{info, warn, debug};

use crate::common::{
    error::Result,
    protocol::{Frame, MessageType, Reliability},
};

use crate::server::service::MessageHandler;

/// 消息处理器类型
#[derive(Debug, Clone, PartialEq)]
pub enum HandlerType {
    /// 回显处理器
    Echo,
    /// 广播处理器
    Broadcast,
    /// 自定义处理器
    Custom(String),
}

/// 增强的消息处理器
pub struct EnhancedMessageHandler {
    /// 默认处理器
    default_handler: Arc<dyn MessageHandler>,
    /// 特定类型的消息处理器
    typed_handlers: Arc<RwLock<HashMap<String, Arc<dyn MessageHandler>>>>,
    /// 特定连接的消息处理器
    connection_handlers: Arc<RwLock<HashMap<String, Arc<dyn MessageHandler>>>>,
}

impl EnhancedMessageHandler {
    /// 创建新的增强消息处理器
    pub fn new(default_handler: Arc<dyn MessageHandler>) -> Self {
        Self {
            default_handler,
            typed_handlers: Arc::new(RwLock::new(HashMap::new())),
            connection_handlers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 注册特定类型的消息处理器
    pub async fn register_typed_handler(&self, message_type: String, handler: Arc<dyn MessageHandler>) {
        let mut handlers = self.typed_handlers.write().await;
        handlers.insert(message_type.clone(), handler);
        info!("已注册特定类型消息处理器: {}", message_type);
    }

    /// 注册特定连接的消息处理器
    pub async fn register_connection_handler(&self, connection_id: String, handler: Arc<dyn MessageHandler>) {
        let mut handlers = self.connection_handlers.write().await;
        handlers.insert(connection_id.clone(), handler);
        info!("已注册特定连接消息处理器: {}", connection_id);
    }

    /// 移除特定类型的消息处理器
    pub async fn unregister_typed_handler(&self, message_type: &str) -> bool {
        let mut handlers = self.typed_handlers.write().await;
        let removed = handlers.remove(message_type).is_some();
        if removed {
            info!("已移除特定类型消息处理器: {}", message_type);
        }
        removed
    }

    /// 移除特定连接的消息处理器
    pub async fn unregister_connection_handler(&self, connection_id: &str) -> bool {
        let mut handlers = self.connection_handlers.write().await;
        let removed = handlers.remove(connection_id).is_some();
        if removed {
            info!("已移除特定连接消息处理器: {}", connection_id);
        }
        removed
    }

    /// 获取处理器类型
    pub fn get_handler_type(&self) -> HandlerType {
        HandlerType::Custom("EnhancedMessageHandler".to_string())
    }
}

#[async_trait::async_trait]
impl MessageHandler for EnhancedMessageHandler {
    async fn handle_message(&self, connection_id: String, message: Frame) -> Result<Option<Frame>> {
        // 首先检查是否有特定连接的处理器
        {
            let handlers = self.connection_handlers.read().await;
            if let Some(handler) = handlers.get(&connection_id) {
                debug!("使用特定连接处理器处理消息: {}", connection_id);
                return handler.handle_message(connection_id, message).await;
            }
        }

        // 然后检查是否有特定类型的消息处理器
        {
            let handlers = self.typed_handlers.read().await;
            if let Some(handler) = handlers.get(&format!("{:?}", message.message_type)) {
                debug!("使用特定类型处理器处理消息: {:?}", message.message_type);
                return handler.handle_message(connection_id, message).await;
            }
        }

        // 使用默认处理器
        debug!("使用默认处理器处理消息");
        self.default_handler.handle_message(connection_id, message).await
    }
}

/// 日志消息处理器
///
/// 简单地记录接收到的消息并返回确认
pub struct LoggingMessageHandler;

#[async_trait::async_trait]
impl MessageHandler for LoggingMessageHandler {
    async fn handle_message(&self, connection_id: String, message: Frame) -> Result<Option<Frame>> {
        info!("收到消息 - 连接: {}, 消息类型: {:?}, 消息ID: {}, 可靠性: {:?}, 数据长度: {}", 
              connection_id, 
              message.message_type, 
              message.message_id, 
              message.reliability, 
              message.payload.len());
        
        // 返回确认消息
        let response = Frame::new(
            MessageType::DataAck,
            message.message_id,
            Reliability::AtLeastOnce,
            vec![],
        );
        
        Ok(Some(response))
    }
}

/// 广播消息处理器
///
/// 将接收到的消息广播到所有连接
pub struct BroadcastMessageHandler<T: crate::server::manager::traits::ConnectionManager> {
    /// 连接管理器
    connection_manager: Arc<T>,
}

impl<T: crate::server::manager::traits::ConnectionManager> BroadcastMessageHandler<T> {
    /// 创建新的广播消息处理器
    pub fn new(connection_manager: Arc<T>) -> Self {
        Self {
            connection_manager,
        }
    }
}

#[async_trait::async_trait]
impl<T: crate::server::manager::traits::ConnectionManager + 'static> MessageHandler for BroadcastMessageHandler<T> {
    async fn handle_message(&self, connection_id: String, message: Frame) -> Result<Option<Frame>> {
        info!("广播消息 - 来自连接: {}, 消息类型: {:?}, 数据长度: {}", 
              connection_id, 
              message.message_type, 
              message.payload.len());
        
        // 广播消息到所有连接（除了发送者）
        match self.connection_manager.broadcast_message(message.clone()).await {
            Ok(sent_count) => {
                info!("消息已广播到 {} 个连接", sent_count);
            }
            Err(e) => {
                warn!("广播消息失败: {}", e);
            }
        }
        
        // 返回确认消息给发送者
        let response = Frame::new(
            MessageType::DataAck,
            message.message_id,
            Reliability::AtLeastOnce,
            vec![],
        );
        
        Ok(Some(response))
    }
}