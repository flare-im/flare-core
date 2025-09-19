//! 消息处理器定义
//!
//! 用户需要实现此接口来处理业务消息

use async_trait::async_trait;
use tracing::info;
use crate::common::{
    error::Result,
};
use crate::common::protocol::commands::{CustomCommand, DataCommand, ErrorCommand, MessageSendCommand, NotificationCmd};

/// 连接事件类型
#[derive(Debug, Clone)]
pub enum ConnectionEventType {
    /// 连接已建立
    Connected,
    /// 连接已断开
    Disconnected,
    /// 连接发生错误
    Error,
}

/// 用户消息处理器
///
/// 用户需要实现此接口来处理业务消息
/// 这是简化版的事件处理接口，适用于快速开始的场景
#[async_trait]
pub trait MessageHandler: Send + Sync {
    /// 处理错误
    /// 
    /// # 参数
    /// * `connection_id` - 错误发生时的连接ID
    /// * `error` - 错误信息
    ///
    /// # 返回值
    /// * `Ok(())` - 处理成功
    /// * `Err(Error)` - 处理失败
    /// 
    async fn handle_error(&self, connection_id: &str, error: &ErrorCommand) -> Result<()>;

    /// 处理自定义控制命令
    /// 
    /// # 参数
    /// * `connection_id` - 错误发生时的连接ID
    /// * `command` - 自定义控制命令
    ///
    /// # 返回值
    /// * `Ok(())` - 处理成功
    /// * `Err(Error)` - 处理失败
    /// 
    async fn handle_custom_command(&self,user_id: &str, connection_id: &str, command: &CustomCommand) -> Result<()>;

    /// 处理用户消息
    ///
    /// # 参数
    /// * `user_id` - 用户ID
    /// * `connection_id` - 连接ID
    /// * `message` - 消息帧
    ///
    /// # 返回值
    /// * `Ok(())` - 处理成功
    /// * `Err(Error)` - 处理失败
    async fn handle_message(&self, user_id: &str, connection_id: &str, message: &MessageSendCommand) -> Result<()>;
    
    /// 处理数据消息
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `connection_id` - 连接ID
    /// * `message` - 消息帧
    ///    
     /// # 返回值
    /// * `Ok(Option<Vec<u8>>)` - 处理成功
    /// * `Err(Error)` - 处理失败
    async fn handle_data_message(&self,user_id: &str, connection_id: &str, message: &DataCommand) -> Result<Option<Vec<u8>>>;

    /// 处理自定义消息
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `message` - 消息帧
    ///
    /// # 返回值
    /// * `Ok(())` - 处理成功
    /// * `Err(Error)` - 处理失败
    async fn handle_custom_message(&self,user_id: &str, connection_id: &str, message: &CustomCommand) -> Result<()>;
    
    /// 处理通知
    /// 
    /// # 参数
    /// * `connection_id` - 错误发生时的连接ID
    /// * `message` - 通知消息
    ///
    /// # 返回值
    /// * `Ok(())` - 处理成功
    /// * `Err(Error)` - 处理失败
    /// 
    async fn handle_notification(&self, connection_id: &str, message: &NotificationCmd) -> Result<()> {
        let _ = connection_id;
        let _ = message;
        // 默认实现为空
        Ok(())
    }
    
    /// 处理自定义事件
    /// 
    /// # 参数
    /// * `connection_id` - 错误发生时的连接ID
    /// * `message` - 自定义事件消息
    ///
    /// # 返回值
    /// * `Ok(())` - 处理成功
    /// * `Err(Error)` - 处理失败
    /// 
    async fn handle_custom_event(&self, connection_id: &str, message: &CustomCommand) -> Result<()> {
        let _ = connection_id;
        let _ = message;
        // 默认实现为空
        Ok(())
    }

}

/// 默认消息处理器实现
#[derive(Debug)]
pub struct DefaultMessageHandler;

#[async_trait]
impl MessageHandler for DefaultMessageHandler {

    async fn handle_error(&self, connection_id: &str, error: &ErrorCommand) -> Result<()> {
        info!("connection_id: {}, error: {:?}", connection_id, error);
        Ok(())
    }

    async fn handle_custom_command(&self, user_id: &str, connection_id: &str, command: &CustomCommand) -> Result<()> {
        info!("user_id: {}, connection_id: {}, command: {:?}", user_id, connection_id, command);
        Ok(())
    }

    async fn handle_message(&self, user_id: &str, connection_id: &str, message: &MessageSendCommand) -> Result<()> {
        info!("user_id: {}, connection_id: {}, message: {:?}", user_id, connection_id, message);
        Ok(())
    }

    async fn handle_data_message(&self, user_id: &str, connection_id: &str, message: &DataCommand) -> Result<Option<Vec<u8>>> {
        info!("user_id: {}, connection_id: {}, message: {:?}", user_id, connection_id, message);
        Ok(None)
    }

    async fn handle_custom_message(&self, user_id: &str, connection_id: &str, message: &CustomCommand) -> Result<()> {
        info!("user_id: {}, connection_id: {}, message: {:?}", user_id, connection_id, message);
        Ok(())
    }
}

impl Default for DefaultMessageHandler {
    fn default() -> Self {
        Self
    }
}