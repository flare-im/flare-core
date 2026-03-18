//! 标准消息观察者接口
//!
//! 定义统一的消息观察者标准，支持服务端和客户端实现

use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::transport::events::ConnectionEvent;
use async_trait::async_trait;
use std::sync::Arc;

/// 消息观察者标准接口
///
/// 这是消息处理的核心接口，定义了如何处理不同类型的消息和事件。
/// 服务端和客户端都可以实现这个接口来自定义消息处理逻辑。
///
/// # 设计原则
/// 1. **分层处理**: 先处理系统命令，再处理业务命令
/// 2. **可扩展性**: 支持自定义命令类型处理
/// 3. **响应式**: 可以返回响应 Frame 用于请求-响应模式
#[async_trait]
pub trait MessageObserver: Send + Sync {
    /// 处理收到的消息 Frame
    ///
    /// # 参数
    /// - `frame`: 收到的消息 Frame
    /// - `connection_id`: 连接 ID（服务端）或 None（客户端）
    ///
    /// # 返回
    /// - `Ok(Some(Frame))`: 需要发送的响应 Frame（用于请求-响应模式）
    /// - `Ok(None)`: 不需要响应
    /// - `Err`: 处理失败
    ///
    /// # 说明
    /// 这个方法会处理所有类型的命令（System、Message、Notification、Custom）
    /// 默认实现会根据命令类型分发到对应的处理方法
    async fn handle_frame(
        &self,
        frame: &Frame,
        connection_id: Option<&str>,
    ) -> Result<Option<Frame>> {
        if let Some(cmd) = &frame.command {
            match &cmd.r#type {
                Some(crate::common::protocol::flare::core::commands::command::Type::System(
                    sys_cmd,
                )) => {
                    self.handle_system_command(sys_cmd, frame, connection_id)
                        .await
                }
                Some(crate::common::protocol::flare::core::commands::command::Type::Payload(
                    msg_cmd,
                )) => {
                    self.handle_message_command(msg_cmd, frame, connection_id)
                        .await
                }
                Some(
                    crate::common::protocol::flare::core::commands::command::Type::Notification(
                        notif_cmd,
                    ),
                ) => {
                    self.handle_notification_command(notif_cmd, frame, connection_id)
                        .await
                }
                Some(crate::common::protocol::flare::core::commands::command::Type::Custom(
                    custom_cmd,
                )) => {
                    self.handle_custom_command(custom_cmd, frame, connection_id)
                        .await
                }
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    /// 处理系统命令
    ///
    /// 默认实现返回 None，子类可以重写此方法
    async fn handle_system_command(
        &self,
        _sys_cmd: &crate::common::protocol::flare::core::commands::SystemCommand,
        _frame: &Frame,
        _connection_id: Option<&str>,
    ) -> Result<Option<Frame>> {
        Ok(None)
    }

    /// 处理消息命令
    ///
    /// 默认实现返回 None，子类可以重写此方法
    async fn handle_message_command(
        &self,
        _msg_cmd: &crate::common::protocol::PayloadCommand,
        _frame: &Frame,
        _connection_id: Option<&str>,
    ) -> Result<Option<Frame>> {
        Ok(None)
    }

    /// 处理通知命令
    ///
    /// 默认实现返回 None，子类可以重写此方法
    async fn handle_notification_command(
        &self,
        _notif_cmd: &crate::common::protocol::NotificationCommand,
        _frame: &Frame,
        _connection_id: Option<&str>,
    ) -> Result<Option<Frame>> {
        Ok(None)
    }

    /// 处理自定义命令（如 SyncMessages、ListSessions 等）
    ///
    /// 默认实现返回 None，子类可以重写此方法
    async fn handle_custom_command(
        &self,
        _custom_cmd: &crate::common::protocol::CustomCommand,
        _frame: &Frame,
        _connection_id: Option<&str>,
    ) -> Result<Option<Frame>> {
        Ok(None)
    }

    /// 处理连接事件
    ///
    /// 当连接状态发生变化时调用
    async fn handle_connection_event(
        &self,
        _event: &ConnectionEvent,
        _connection_id: Option<&str>,
    ) -> Result<()> {
        Ok(())
    }
}

/// 线程安全的消息观察者引用
pub type ArcMessageObserver = Arc<dyn MessageObserver>;
