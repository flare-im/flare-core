//! 消息发送器
//!
//! 提供统一的消息发送接口，支持发送控制命令、消息、通知和事件

use std::sync::Arc;

use crate::common::{
    error::Result,
    protocol::{
        Frame,
        commands::{
            ControlCmd, MessageCmd, NotificationCmd, EventCmd,
            ConnectCommand, ConnectAckCommand, DisconnectCommand,
            AuthRequestCommand, AuthResponseCommand,
            MessageSendCommand, MessageAckCommand, DataCommand,
            NotificationCommand,
        },
        Reliability,
        factory::FrameFactory,
    },
};
use crate::server::manager::{UserConnectionManager, traits::ServerConnectionManager};

/// 消息发送器
/// 
/// 提供统一的消息发送接口，支持发送控制命令、消息、通知和事件
pub struct MessageSender {
    /// 用户连接管理器
    user_connection_manager: Arc<UserConnectionManager>,
}

impl MessageSender {
    /// 创建新的消息发送器
    pub fn new(user_connection_manager: Arc<UserConnectionManager>) -> Self {
        Self {
            user_connection_manager,
        }
    }

    /// 发送控制命令
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `command` - 控制命令
    /// * `reliability` - 可靠性级别
    /// 
    /// # 返回值
    /// * `Ok(())` - 发送成功
    /// * `Err(Error)` - 发送失败
    pub async fn send_control_command(
        &self,
        connection_id: &str,
        command: ControlCmd,
        reliability: Reliability,
    ) -> Result<()> {
        let message_id = FrameFactory::generate_message_id();
        let frame = Frame::new(
            crate::common::protocol::commands::Command::Control(command),
            message_id,
            reliability,
        );
        
        self.user_connection_manager.send_message(connection_id, frame).await
    }

    /// 发送消息命令
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `command` - 消息命令
    /// * `reliability` - 可靠性级别
    /// 
    /// # 返回值
    /// * `Ok(())` - 发送成功
    /// * `Err(Error)` - 发送失败
    pub async fn send_message_command(
        &self,
        connection_id: &str,
        command: MessageCmd,
        reliability: Reliability,
    ) -> Result<()> {
        let message_id = FrameFactory::generate_message_id();
        let frame = Frame::new(
            crate::common::protocol::commands::Command::Message(command),
            message_id,
            reliability,
        );
        
        self.user_connection_manager.send_message(connection_id, frame).await
    }

    /// 发送通知命令
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `command` - 通知命令
    /// * `reliability` - 可靠性级别
    /// 
    /// # 返回值
    /// * `Ok(())` - 发送成功
    /// * `Err(Error)` - 发送失败
    pub async fn send_notification_command(
        &self,
        connection_id: &str,
        command: NotificationCmd,
        reliability: Reliability,
    ) -> Result<()> {
        let message_id = FrameFactory::generate_message_id();
        let frame = Frame::new(
            crate::common::protocol::commands::Command::Notification(command),
            message_id,
            reliability,
        );
        
        self.user_connection_manager.send_message(connection_id, frame).await
    }

    /// 发送事件命令
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `command` - 事件命令
    /// * `reliability` - 可靠性级别
    /// 
    /// # 返回值
    /// * `Ok(())` - 发送成功
    /// * `Err(Error)` - 发送失败
    pub async fn send_event_command(
        &self,
        connection_id: &str,
        command: EventCmd,
        reliability: Reliability,
    ) -> Result<()> {
        let message_id = FrameFactory::generate_message_id();
        let frame = Frame::new(
            crate::common::protocol::commands::Command::Event(command),
            message_id,
            reliability,
        );
        
        self.user_connection_manager.send_message(connection_id, frame).await
    }

    /// 向指定用户发送消息
    /// 
    /// # 参数
    /// * `user_id` - 用户ID
    /// * `frame` - 消息帧
    /// 
    /// # 返回值
    /// * `Ok(usize)` - 成功发送的连接数
    /// * `Err(Error)` - 发送失败
    pub async fn send_message_to_user(&self, user_id: &str, frame: Frame) -> Result<usize> {
        self.user_connection_manager.send_message_to_user(user_id, frame).await
    }

    /// 广播消息到所有用户
    /// 
    /// # 参数
    /// * `frame` - 消息帧
    /// 
    /// # 返回值
    /// * `Ok(usize)` - 成功发送的连接数
    /// * `Err(Error)` - 发送失败
    pub async fn broadcast_message(&self, frame: Frame) -> Result<usize> {
        self.user_connection_manager.broadcast_message_to_users(frame).await
    }

    /// 发送连接命令
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `client_id` - 客户端ID
    /// * `protocol` - 协议类型
    /// * `platform` - 平台信息
    /// * `version` - 客户端版本
    /// 
    /// # 返回值
    /// * `Ok(())` - 发送成功
    /// * `Err(Error)` - 发送失败
    pub async fn send_connect_command(
        &self,
        connection_id: &str,
        client_id: String,
        protocol: String,
        platform: String,
        version: String,
    ) -> Result<()> {
        let connect = ConnectCommand::new(client_id, protocol, platform, version);
        let command = ControlCmd::Connect(connect);
        self.send_control_command(connection_id, command, Reliability::AtLeastOnce).await
    }

    /// 发送连接确认命令
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `session_id` - 会话ID
    /// * `status` - 状态码
    /// * `status_message` - 状态消息
    /// 
    /// # 返回值
    /// * `Ok(())` - 发送成功
    /// * `Err(Error)` - 发送失败
    pub async fn send_connect_ack_command(
        &self,
        connection_id: &str,
        session_id: String,
        status: i32,
        status_message: Option<String>,
    ) -> Result<()> {
        let mut connect_ack = ConnectAckCommand::new(session_id);
        connect_ack.status = status;
        connect_ack.status_message = status_message;
        
        let command = ControlCmd::ConnectAck(connect_ack);
        self.send_control_command(connection_id, command, Reliability::AtLeastOnce).await
    }

    /// 发送断开连接命令
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `status` - 状态码
    /// * `reason` - 断开原因
    /// * `details` - 详细信息
    /// 
    /// # 返回值
    /// * `Ok(())` - 发送成功
    /// * `Err(Error)` - 发送失败
    pub async fn send_disconnect_command(
        &self,
        connection_id: &str,
        status: i32,
        reason: String,
        details: Option<String>,
    ) -> Result<()> {
        let disconnect = match details {
            Some(d) => DisconnectCommand::with_details(status, reason, d),
            None => DisconnectCommand::new(status, reason),
        };
        
        let command = ControlCmd::Disconnect(disconnect);
        self.send_control_command(connection_id, command, Reliability::AtLeastOnce).await
    }

    /// 发送认证请求命令
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `user_id` - 用户ID
    /// * `platform` - 平台
    /// * `token` - 认证令牌
    /// 
    /// # 返回值
    /// * `Ok(())` - 发送成功
    /// * `Err(Error)` - 发送失败
    pub async fn send_auth_request_command(
        &self,
        connection_id: &str,
        user_id: String,
        platform: String,
        token: String,
    ) -> Result<()> {
        let auth_request = AuthRequestCommand::new(user_id, platform, token);
        let command = ControlCmd::AuthRequest(auth_request);
        self.send_control_command(connection_id, command, Reliability::AtLeastOnce).await
    }

    /// 发送认证响应命令
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `success` - 是否成功
    /// * `status` - 状态码
    /// * `user_info` - 用户信息
    /// * `error_message` - 错误消息
    /// 
    /// # 返回值
    /// * `Ok(())` - 发送成功
    /// * `Err(Error)` - 发送失败
    pub async fn send_auth_response_command(
        &self,
        connection_id: &str,
        success: bool,
        status: i32,
        user_info: Option<Vec<u8>>,
        error_message: Option<String>,
    ) -> Result<()> {
        let auth_response = if success {
            AuthResponseCommand::success(user_info)
        } else {
            AuthResponseCommand::failure(status, error_message.unwrap_or_default())
        };
        
        let command = ControlCmd::AuthResponse(auth_response);
        self.send_control_command(connection_id, command, Reliability::AtLeastOnce).await
    }

    /// 发送消息发送命令
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `data` - 消息内容
    /// * `reliability` - 可靠性级别
    /// 
    /// # 返回值
    /// * `Ok(())` - 发送成功
    /// * `Err(Error)` - 发送失败
    pub async fn send_message_send_command(
        &self,
        connection_id: &str,
        data: Vec<u8>,
        reliability: Reliability,
    ) -> Result<()> {
        let message = MessageSendCommand::new(data);
        let command = MessageCmd::Send(message);
        self.send_message_command(connection_id, command, reliability).await
    }

    /// 发送消息确认命令
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `success` - 是否成功
    /// * `status` - 状态码
    /// * `ack_message_id` - 确认的消息ID
    /// * `error_code` - 错误码
    /// * `error_message` - 错误消息
    /// 
    /// # 返回值
    /// * `Ok(())` - 发送成功
    /// * `Err(Error)` - 发送失败
    pub async fn send_message_ack_command(
        &self,
        connection_id: &str,
        success: bool,
        status: i32,
        ack_message_id: Option<String>,
        error_code: Option<u32>,
        error_message: Option<String>,
    ) -> Result<()> {
        let mut ack = if success {
            MessageAckCommand::success()
        } else {
            MessageAckCommand::failure(status, error_code, error_message.clone())
        };
        
        ack.message_id = ack_message_id;
        
        let command = MessageCmd::Ack(ack);
        self.send_message_command(connection_id, command, Reliability::AtLeastOnce).await
    }

    /// 发送数据命令
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `data` - 数据内容
    /// * `reliability` - 可靠性级别
    /// 
    /// # 返回值
    /// * `Ok(())` - 发送成功
    /// * `Err(Error)` - 发送失败
    pub async fn send_data_command(
        &self,
        connection_id: &str,
        data: Vec<u8>,
        reliability: Reliability,
    ) -> Result<()> {
        let data_cmd = DataCommand::new(data);
        let command = MessageCmd::Data(data_cmd);
        self.send_message_command(connection_id, command, reliability).await
    }

    /// 发送系统通知命令
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `content` - 通知内容
    /// 
    /// # 返回值
    /// * `Ok(())` - 发送成功
    /// * `Err(Error)` - 发送失败
    pub async fn send_system_notification_command(
        &self,
        connection_id: &str,
        content: String,
    ) -> Result<()> {
        let notification = NotificationCommand::system(content);
        let command = NotificationCmd::System(notification);
        self.send_notification_command(connection_id, command, Reliability::AtLeastOnce).await
    }

    /// 发送广播通知命令
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `content` - 通知内容
    /// 
    /// # 返回值
    /// * `Ok(())` - 发送成功
    /// * `Err(Error)` - 发送失败
    pub async fn send_broadcast_notification_command(
        &self,
        connection_id: &str,
        content: String,
    ) -> Result<()> {
        let notification = NotificationCommand::broadcast(content);
        let command = NotificationCmd::Broadcast(notification);
        self.send_notification_command(connection_id, command, Reliability::AtLeastOnce).await
    }

    /// 发送警报通知命令
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `content` - 通知内容
    /// 
    /// # 返回值
    /// * `Ok(())` - 发送成功
    /// * `Err(Error)` - 发送失败
    pub async fn send_alert_notification_command(
        &self,
        connection_id: &str,
        content: String,
    ) -> Result<()> {
        let notification = NotificationCommand::alert(content);
        let command = NotificationCmd::Alert(notification);
        self.send_notification_command(connection_id, command, Reliability::AtLeastOnce).await
    }
}