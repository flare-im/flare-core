//! 客户端事件处理器
//!
//! 定义客户端事件处理接口，允许用户实现自定义业务逻辑

use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::common::protocol::flare::core::commands::notification_command::Type as NotifType;
use crate::common::protocol::flare::core::commands::payload_command::Type as PayloadType;
use crate::common::protocol::flare::core::commands::system_command::Type as SysType;
use crate::transport::events::ConnectionEvent;

/// 客户端事件处理器
///
/// 允许用户实现自定义逻辑来处理不同类型的客户端事件和命令
///
/// 所有方法都有默认实现，用户只需要实现需要的方法即可
#[async_trait::async_trait]
pub trait ClientEventHandler: Send + Sync {
    /// 处理系统命令
    ///
    /// # 参数
    /// - `command_type`: 系统命令类型（CONNECT, CONNECT_ACK, PING, PONG, KICKED, ERROR, CLOSE）
    /// - `frame`: 完整的消息帧
    ///
    /// # 返回
    /// - `Ok(Some(Frame))`: 需要发送的回复消息（可选）
    /// - `Ok(None)`: 不需要回复
    /// - `Err`: 处理失败
    async fn handle_system_command(
        &self,
        command_type: SysType,
        frame: &Frame,
    ) -> Result<Option<Frame>> {
        let _ = (command_type, frame);
        Ok(None)
    }

    /// 处理载荷命令（MESSAGE/EVENT/ACK/DATA）
    async fn handle_message_command(
        &self,
        command_type: PayloadType,
        frame: &Frame,
    ) -> Result<Option<Frame>> {
        let _ = (command_type, frame);
        Ok(None)
    }

    /// 处理通知命令
    ///
    /// # 参数
    /// - `command_type`: 通知命令类型
    /// - `frame`: 完整的消息帧
    ///
    /// # 返回
    /// - `Ok(Some(Frame))`: 需要发送的回复消息（可选）
    /// - `Ok(None)`: 不需要回复
    /// - `Err`: 处理失败
    async fn handle_notification_command(
        &self,
        command_type: NotifType,
        frame: &Frame,
    ) -> Result<Option<Frame>> {
        let _ = (command_type, frame);
        Ok(None)
    }

    /// 处理连接事件
    ///
    /// # 参数
    /// - `event`: 连接事件（Connected, Disconnected, Error）
    ///
    /// # 返回
    /// - `Ok(())`: 处理成功
    /// - `Err`: 处理失败
    async fn handle_connection_event(&self, event: &ConnectionEvent) -> Result<()> {
        let _ = event;
        Ok(())
    }
}
