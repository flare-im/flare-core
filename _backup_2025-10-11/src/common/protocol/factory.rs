//! 协议工厂
//! 
//! 提供简洁易用的API方法，用于快速构建协议帧

use std::collections::HashMap;

use crate::common::error::{FlareError, Result};
use crate::common::protocol::{
    commands::{
        Command, ControlCmd, MessageCmd, NotificationCmd, EventCmd,
        ConnectCommand, ConnectAckCommand, DisconnectCommand, 
        AuthRequestCommand, AuthResponseCommand, MessageSendCommand,
        MessageAckCommand, DataCommand, NotificationCommand, ErrorCommand,
        CustomCommand,
    },
    Reliability,
    Frame,
    frame::MessageIdGenerator
};

/// 协议工厂
/// 
/// 提供简洁易用的API方法，用于快速构建协议帧
pub struct FrameFactory;

impl FrameFactory {
    /// 创建心跳请求帧
    pub fn create_ping_frame(message_id: String) -> Result<Frame> {
        let command = Command::Control(ControlCmd::Ping);
        Self::create_frame(message_id, Reliability::BestEffort, command)
    }
    
    /// 创建心跳响应帧
    pub fn create_pong_frame(message_id: String) -> Result<Frame> {
        let command = Command::Control(ControlCmd::Pong);
        Self::create_frame(message_id, Reliability::BestEffort, command)
    }
    
    /// 创建连接请求帧
    pub fn create_connect_frame(
        message_id: String,
        client_id: String,
        protocol: String,
        platform: String,
        version: String,
    ) -> Result<Frame> {
        let connect = ConnectCommand::new(client_id, protocol, platform, version);
        let command = Command::Control(ControlCmd::Connect(connect));
        Self::create_frame(message_id, Reliability::AtLeastOnce, command)
    }
    
    /// 创建连接确认帧
    pub fn create_connect_ack_frame(
        message_id: String,
        session_id: String,
        status: i32,
        status_message: Option<String>,
    ) -> Result<Frame> {
        let mut connect_ack = ConnectAckCommand::new(session_id);
        connect_ack.status = status;
        connect_ack.status_message = status_message;
        
        let command = Command::Control(ControlCmd::ConnectAck(connect_ack));
        Self::create_frame(message_id, Reliability::AtLeastOnce, command)
    }
    
    /// 创建断开连接帧
    pub fn create_disconnect_frame(
        message_id: String,
        status: i32,
        reason: String,
        details: Option<String>,
    ) -> Result<Frame> {
        let disconnect = match details {
            Some(d) => DisconnectCommand::with_details(status, reason, d),
            None => DisconnectCommand::new(status, reason),
        };
        
        let command = Command::Control(ControlCmd::Disconnect(disconnect));
        Self::create_frame(message_id, Reliability::AtLeastOnce, command)
    }
    
    /// 创建认证请求帧
    pub fn create_auth_request_frame(
        message_id: String,
        user_id: String,
        platform: String,
        token: String,
    ) -> Result<Frame> {
        let auth_request = AuthRequestCommand::new(user_id, platform, token);
        let command = Command::Control(ControlCmd::AuthRequest(auth_request));
        Self::create_frame(message_id, Reliability::AtLeastOnce, command)
    }
    
    /// 创建认证响应帧
    pub fn create_auth_response_frame(
        message_id: String,
        success: bool,
        status: i32,
        user_info: Option<Vec<u8>>,
        error_message: Option<String>,
    ) -> Result<Frame> {
        let auth_response = if success {
            AuthResponseCommand::success(user_info)
        } else {
            AuthResponseCommand::failure(status, error_message.unwrap_or_default())
        };
        
        let command = Command::Control(ControlCmd::AuthResponse(auth_response));
        Self::create_frame(message_id, Reliability::AtLeastOnce, command)
    }
    
    /// 创建消息发送帧
    pub fn create_message_frame(
        message_id: String,
        data: Vec<u8>,
        reliability: Reliability,
    ) -> Result<Frame> {
        let message = MessageSendCommand::new(data);
        let command = Command::Message(MessageCmd::Send(message));
        Self::create_frame(message_id, reliability, command)
    }
    
    /// 创建消息确认帧
    pub fn create_message_ack_frame(
        message_id: String,
        success: bool,
        status: i32,
        ack_message_id: Option<String>,
        error_code: Option<u32>,
        error_message: Option<String>,
    ) -> Result<Frame> {
        let mut ack = if success {
            MessageAckCommand::success()
        } else {
            MessageAckCommand::failure(status, error_code, error_message.clone())
        };
        
        ack.message_id = ack_message_id;
        
        let command = Command::Message(MessageCmd::Ack(ack));
        Self::create_frame(message_id, Reliability::AtLeastOnce, command)
    }
    
    /// 创建数据帧
    pub fn create_data_frame(
        message_id: String,
        data: Vec<u8>,
        reliability: Reliability,
    ) -> Result<Frame> {
        let data_cmd = DataCommand::new(data);
        let command = Command::Message(MessageCmd::Data(data_cmd));
        Self::create_frame(message_id, reliability, command)
    }
    
    /// 创建系统通知帧
    pub fn create_system_notification_frame(
        message_id: String,
        content: String,
    ) -> Result<Frame> {
        let notification = NotificationCommand::system(content);
        let command = Command::Notification(NotificationCmd::System(notification));
        Self::create_frame(message_id, Reliability::AtLeastOnce, command)
    }
    
    /// 创建广播通知帧
    pub fn create_broadcast_notification_frame(
        message_id: String,
        content: String,
    ) -> Result<Frame> {
        let notification = NotificationCommand::broadcast(content);
        let command = Command::Notification(NotificationCmd::Broadcast(notification));
        Self::create_frame(message_id, Reliability::AtLeastOnce, command)
    }
    
    /// 创建警报通知帧
    pub fn create_alert_notification_frame(
        message_id: String,
        content: String,
    ) -> Result<Frame> {
        let notification = NotificationCommand::alert(content);
        let command = Command::Notification(NotificationCmd::Alert(notification));
        Self::create_frame(message_id, Reliability::AtLeastOnce, command)
    }
    
    /// 创建错误帧
    pub fn create_error_frame(
        message_id: String,
        status: i32,
        message: String,
        reason: Option<String>,
        details: Option<String>,
    ) -> Result<Frame> {
        let error = match (reason, details) {
            (Some(r), _) => ErrorCommand::with_reason(status,  message, r),
            (_, Some(d)) => ErrorCommand::with_details(status, message, d),
            _ => ErrorCommand::new(status, message),
        };
        
        let command = Command::Control(ControlCmd::Error(error));
        Self::create_frame(message_id, Reliability::AtLeastOnce, command)
    }
    
    /// 创建自定义命令帧
    pub fn create_custom_command_frame(
        message_id: String,
        command_type: &str,
        name: String,
        data: Vec<u8>,
        metadata: Option<HashMap<String, String>>,
        reliability: Reliability,
    ) -> Result<Frame> {
        let custom = match metadata {
            Some(m) => CustomCommand::with_metadata(name, data, m),
            None => CustomCommand::new(name, data),
        };
        
        let command = match command_type {
            "control" => Command::Control(ControlCmd::Custom(custom)),
            "message" => Command::Message(MessageCmd::Custom(custom)),
            "notification" => Command::Notification(NotificationCmd::Custom(custom)),
            "event" => Command::Event(EventCmd::Custom(custom)),
            _ => return Err(FlareError::ProtocolError(format!("Invalid command type: {}", command_type))),
        };
        
        Self::create_frame(message_id, reliability, command)
    }
    
    /// 创建事件帧
    pub fn create_event_frame(
        message_id: String,
        event_type: &str,
    ) -> Result<Frame> {
        let command = match event_type {
            "open" => Command::Event(EventCmd::Open),
            "close" => Command::Event(EventCmd::Close),
            "reconnect" => Command::Event(EventCmd::Reconnect),
            _ => return Err(FlareError::ProtocolError(format!("Invalid event type: {}", event_type))),
        };
        
        Self::create_frame(message_id, Reliability::AtLeastOnce, command)
    }
    
    /// 创建基础帧
    fn create_frame(
        message_id: String,
        reliability: Reliability,
        command: Command,
    ) -> Result<Frame> {
        Ok(Frame::new(command, message_id, reliability))
    }
    
    /// 从Proto帧中提取命令
    pub fn extract_command(frame: &Frame) -> Result<Command> {
        Ok(frame.get_command())
    }
    
    /// 设置帧的会话ID
    pub fn set_session_id(frame: &mut Frame, session_id: String) {
        frame.set_session_id(Some(session_id));
    }
    
    /// 设置帧的优先级
    pub fn set_priority(frame: &mut Frame, priority: u32) {
        frame.set_priority(priority as u8);
    }
    
    /// 设置帧的压缩算法
    pub fn set_compression(frame: &mut Frame, compression: u32) {
        frame.compression = Some(compression as u8);
    }
    
    /// 设置帧的加密标志
    pub fn set_encrypted(frame: &mut Frame, encrypted: bool) {
        frame.encrypted = encrypted;
    }
    
    /// 添加元数据
    pub fn add_metadata(frame: &mut Frame, key: String, value: Vec<u8>) {
        if frame.metadata.is_none() {
            frame.metadata = Some(HashMap::new());
        }
        if let Some(metadata) = &mut frame.metadata {
            metadata.insert(key, value);
        }
    }
    
    /// 生成新的消息ID
    pub fn generate_message_id() -> String {
        MessageIdGenerator::generate_uuid()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_create_ping_frame() {
        let message_id = FrameFactory::generate_message_id();
        let frame = FrameFactory::create_ping_frame(message_id.clone()).unwrap();
        
        assert_eq!(frame.get_message_id(), message_id);
        assert_eq!(frame.get_reliability(), Reliability::BestEffort);
        
        let command = FrameFactory::extract_command(&frame).unwrap();
        match command {
            Command::Control(ControlCmd::Ping) => {}, // 成功
            _ => panic!("Expected Ping command"),
        }
    }
    
    #[test]
    fn test_create_connect_frame() {
        let message_id = FrameFactory::generate_message_id();
        let frame = FrameFactory::create_connect_frame(
            message_id.clone(),
            "client123".to_string(),
            "websocket".to_string(),
            "ios".to_string(),
            "1.0.0".to_string(),
        ).unwrap();
        
        assert_eq!(frame.get_message_id(), message_id);
        assert_eq!(frame.get_reliability(), Reliability::AtLeastOnce);
        
        let command = FrameFactory::extract_command(&frame).unwrap();
        match command {
            Command::Control(ControlCmd::Connect(connect)) => {
                assert_eq!(connect.client_id, "client123");
                assert_eq!(connect.protocol, "websocket");
                assert_eq!(connect.platform, "ios");
                assert_eq!(connect.version, "1.0.0");
            },
            _ => panic!("Expected Connect command"),
        }
    }
    
    #[test]
    fn test_create_message_frame() {
        let message_id = FrameFactory::generate_message_id();
        let data = b"Hello, World!".to_vec();
        let frame = FrameFactory::create_message_frame(
            message_id.clone(),
            data.clone(),
            Reliability::ExactlyOnce,
        ).unwrap();
        
        assert_eq!(frame.get_message_id(), message_id);
        assert_eq!(frame.get_reliability(), Reliability::ExactlyOnce);
        
        let command = FrameFactory::extract_command(&frame).unwrap();
        match command {
            Command::Message(MessageCmd::Send(message)) => {
                assert_eq!(message.data, data);
            },
            _ => panic!("Expected Message command"),
        }
    }
    
    #[test]
    fn test_frame_metadata() {
        let message_id = FrameFactory::generate_message_id();
        let mut frame = FrameFactory::create_ping_frame(message_id).unwrap();
        
        FrameFactory::set_session_id(&mut frame, "session123".to_string());
        FrameFactory::set_priority(&mut frame, 10);
        FrameFactory::set_compression(&mut frame, 1);
        FrameFactory::set_encrypted(&mut frame, true);
        FrameFactory::add_metadata(&mut frame, "key1".to_string(), b"value1".to_vec());
        
        assert_eq!(frame.get_session_id(), &Some("session123".to_string()));
        assert_eq!(frame.get_priority(), 10);
        assert_eq!(frame.compression, Some(1));
        assert!(frame.encrypted);
        assert_eq!(frame.metadata.as_ref().unwrap().get("key1").unwrap(), &b"value1".to_vec());
    }
}