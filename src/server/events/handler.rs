//! 服务端事件处理器 Trait
//!
//! 提供细化的命令处理方法，支持按命令类型处理

use crate::common::error::Result;
use crate::common::protocol::{
    Frame, MessageCommand, NotificationCommand,
    flare::core::commands::{
        message_command::Type as MessageType, notification_command::Type as NotificationType,
    },
};
use async_trait::async_trait;

/// 服务端事件处理器
///
/// 提供细化的命令处理方法，每个命令类型对应一个方法
/// 用户可以实现这个 trait 来自定义业务逻辑
#[async_trait]
pub trait ServerEventHandler: Send + Sync {
    /// 处理 CONNECT 系统命令（协商）
    ///
    /// 默认实现返回 None，表示使用默认处理
    async fn handle_connect(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        let _ = (frame, connection_id);
        Ok(None)
    }

    /// 处理 PING 系统命令
    ///
    /// 默认实现返回 None，表示使用默认处理（自动回复 PONG）
    async fn handle_ping(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        let _ = (frame, connection_id);
        Ok(None)
    }

    /// 处理 PONG 系统命令
    ///
    /// 默认实现返回 None，表示使用默认处理（更新连接活跃时间）
    async fn handle_pong(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        let _ = (frame, connection_id);
        Ok(None)
    }

    /// 处理消息命令
    ///
    /// # 参数
    /// - `command`: 消息命令
    /// - `connection_id`: 连接 ID
    ///
    /// # 返回
    /// 可选回复 Frame
    async fn handle_message_command(
        &self,
        command: &MessageCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        let _ = (command, connection_id);
        Ok(None)
    }

    /// 处理特定类型的消息命令
    ///
    /// 默认实现调用 `handle_message_command`
    async fn handle_message_command_by_type(
        &self,
        command: &MessageCommand,
        msg_type: MessageType,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        let _ = msg_type;
        self.handle_message_command(command, connection_id).await
    }

    /// 处理通知命令
    ///
    /// # 参数
    /// - `command`: 通知命令
    /// - `connection_id`: 连接 ID
    ///
    /// # 返回
    /// 可选回复 Frame
    async fn handle_notification_command(
        &self,
        command: &NotificationCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        let _ = (command, connection_id);
        Ok(None)
    }

    /// 处理特定类型的通知命令
    ///
    /// 默认实现调用 `handle_notification_command`
    async fn handle_notification_command_by_type(
        &self,
        command: &NotificationCommand,
        notif_type: NotificationType,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        let _ = notif_type;
        self.handle_notification_command(command, connection_id)
            .await
    }

    /// 处理连接断开事件
    ///
    /// # 参数
    /// - `connection_id`: 连接 ID
    /// - `reason`: 断开原因（如果有）
    async fn on_disconnect(&self, connection_id: &str, reason: Option<&str>) -> Result<()> {
        let _ = (connection_id, reason);
        Ok(())
    }

    /// 处理连接错误事件
    ///
    /// # 参数
    /// - `connection_id`: 连接 ID
    /// - `error`: 错误信息
    async fn on_error(&self, connection_id: &str, error: &str) -> Result<()> {
        let _ = (connection_id, error);
        Ok(())
    }
}
