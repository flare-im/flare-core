//! 协议转换工具
//! 
//! 提供Proto生成的结构和Rust定义的命令结构之间的相互转换功能
//! 内部使用Rust定义的结构，外部接口采用Proto生成的格式


use crate::common::error::{FlareError, Result};
use crate::common::protocol::{commands::{
    Command, ControlCmd, MessageCmd, NotificationCmd, EventCmd,
    ConnectCommand, ConnectAckCommand, DisconnectCommand,
    AuthRequestCommand, AuthResponseCommand, MessageSendCommand,
    MessageAckCommand, DataCommand, NotificationCommand, ErrorCommand,
    CustomCommand,
}, ProtobufFrame, ProtobufReliability, Frame, Reliability};

// 修复模块引用路径 - 使用flare_proto而不是flare
use crate::common::protocol::flare_proto::commands as proto_commands;

/// 协议转换器
/// 
/// 提供Proto生成的结构和Rust定义的命令结构之间的相互转换功能
pub struct ProtocolConverter;

impl ProtocolConverter {
    /// 将Proto的Frame转换为Rust的Frame
    pub fn proto_to_rust_frame(proto_frame: &ProtobufFrame) -> Result<Frame> {
        // 确保command必须存在，否则返回错误
        let proto_command = proto_frame.command.as_ref()
            .ok_or_else(|| FlareError::ProtocolError("Missing command in proto frame".to_string()))?;
        
        let command = Self::proto_to_rust_command(proto_command)?;

        // 使用默认构造函数创建Frame
        let mut frame = Frame::new_command(command);
        frame.message_id = proto_frame.message_id.to_string();
        // 修复类型转换：从u64到u64 (应该没问题)
        frame.timestamp = proto_frame.timestamp;
        // 修复类型转换：从i32到Reliability
        frame.reliability = Self::proto_to_rust_reliability(proto_frame.reliability);
        frame.session_id = proto_frame.session_id.clone();
        // 修复类型转换：从u32到u8
        frame.priority = proto_frame.priority as u8;
        // 修复类型转换：从Option<u32>到Option<u8>
        frame.compression = proto_frame.compression.map(|c| c as u8);
        frame.encrypted = proto_frame.encrypted;

        // 转换metadata - 需要正确处理HashMap类型
        if frame.metadata.is_none() {
            frame.metadata = Some(std::collections::HashMap::new());
        }
        if let Some(metadata) = &mut frame.metadata {
            for (key, value) in &proto_frame.metadata {
                metadata.insert(key.clone(), value.clone());
            }
        }

        Ok(frame)
    }

    /// 将Rust的Frame转换为Proto的Frame
    pub fn rust_to_proto_frame(rust_frame: &Frame) -> Result<ProtobufFrame> {
        let mut proto_frame = ProtobufFrame::default();
        
        proto_frame.message_id = rust_frame.message_id.clone();
        proto_frame.timestamp = rust_frame.timestamp;
        // 修复类型转换：从Reliability到i32
        proto_frame.reliability = Self::rust_to_proto_reliability(rust_frame.reliability);
        
        if let Some(session_id) = &rust_frame.session_id {
            proto_frame.session_id = Some(session_id.clone());
        }
        
        // 修复类型转换：从u8到u32
        proto_frame.priority = rust_frame.priority as u32;
        
        // 修复类型转换：从Option<u8>到Option<u32>
        if let Some(compression) = rust_frame.compression {
            proto_frame.compression = Some(compression as u32);
        }
        
        proto_frame.encrypted = rust_frame.encrypted;
        
        // 转换metadata - 需要正确处理HashMap类型
        if let Some(metadata) = &rust_frame.metadata {
            for (key, value) in metadata {
                proto_frame.metadata.insert(key.clone(), value.clone());
            }
        }
        
        // 转换command字段
        let proto_command = Self::rust_to_proto_command(&rust_frame.command)?;
        proto_frame.command = Some(proto_command);
        
        Ok(proto_frame)
    }

    /// 将Proto的Command转换为Rust的Command
    pub fn proto_to_rust_command(proto_command: &proto_commands::Command) -> Result<Command> {
        match &proto_command.command_type {
            Some(proto_commands::command::CommandType::Control(ctrl)) => {
                Ok(Command::Control(Self::proto_to_rust_control_cmd(ctrl)?))
            },
            Some(proto_commands::command::CommandType::Message(msg)) => {
                Ok(Command::Message(Self::proto_to_rust_message_cmd(msg)?))
            },
            Some(proto_commands::command::CommandType::Notification(notify)) => {
                Ok(Command::Notification(Self::proto_to_rust_notification_cmd(notify)?))
            },
            Some(proto_commands::command::CommandType::Event(event)) => {
                Ok(Command::Event(Self::proto_to_rust_event_cmd(event)?))
            },
            None => Err(FlareError::ProtocolError("Missing command type in proto command".to_string())),
        }
    }

    /// 将Rust的Command转换为Proto的Command
    pub fn rust_to_proto_command(rust_command: &Command) -> Result<proto_commands::Command> {
        let mut proto_command = proto_commands::Command::default();
        
        match rust_command {
            Command::Control(ctrl) => {
                proto_command.command_type = Some(proto_commands::command::CommandType::Control(
                    Self::rust_to_proto_control_cmd(ctrl)?
                ));
            },
            Command::Message(msg) => {
                proto_command.command_type = Some(proto_commands::command::CommandType::Message(
                    Self::rust_to_proto_message_cmd(msg)?
                ));
            },
            Command::Notification(notify) => {
                proto_command.command_type = Some(proto_commands::command::CommandType::Notification(
                    Self::rust_to_proto_notification_cmd(notify)?
                ));
            },
            Command::Event(event) => {
                proto_command.command_type = Some(proto_commands::command::CommandType::Event(
                    Self::rust_to_proto_event_cmd(event)?
                ));
            },
        }
        
        Ok(proto_command)
    }

    // ControlCmd 转换
    fn proto_to_rust_control_cmd(proto_ctrl: &proto_commands::ControlCmd) -> Result<ControlCmd> {
        match &proto_ctrl.control_type {
            Some(proto_commands::control_cmd::ControlType::Connect(cmd)) => {
                Ok(ControlCmd::Connect(ConnectCommand::new(
                    cmd.client_id.clone(),
                    cmd.protocol.clone(),
                    cmd.platform.clone(),
                    cmd.version.clone(),
                )))
            },
            Some(proto_commands::control_cmd::ControlType::ConnectAck(cmd)) => {
                let mut ack = ConnectAckCommand::new(cmd.session_id.clone());
                ack.status = cmd.status;
                if !cmd.status_message.is_empty() {
                    ack.status_message = Some(cmd.status_message.clone());
                }
                Ok(ControlCmd::ConnectAck(ack))
            },
            Some(proto_commands::control_cmd::ControlType::Disconnect(cmd)) => {
                let disconnect = if !cmd.details.is_empty() {
                    DisconnectCommand::with_details(
                        cmd.status,
                        cmd.reason.clone(),
                        cmd.details.clone(),
                    )
                } else {
                    DisconnectCommand::new(
                        cmd.status,
                        cmd.reason.clone(),
                    )
                };
                Ok(ControlCmd::Disconnect(disconnect))
            },
            Some(proto_commands::control_cmd::ControlType::AuthRequest(cmd)) => {
                Ok(ControlCmd::AuthRequest(AuthRequestCommand::new(
                    cmd.user_id.clone(),
                    cmd.platform.clone(),
                    cmd.token.clone(),
                )))
            },
            Some(proto_commands::control_cmd::ControlType::AuthResponse(cmd)) => {
                let auth_response = if cmd.success {
                    let user_info = if !cmd.user_info.is_empty() {
                        Some(cmd.user_info.clone())
                    } else {
                        None
                    };
                    AuthResponseCommand::success(user_info)
                } else {
                    AuthResponseCommand::failure(
                        cmd.status,
                        cmd.error_message.clone(),
                    )
                };
                Ok(ControlCmd::AuthResponse(auth_response))
            },
            Some(proto_commands::control_cmd::ControlType::Ping(_)) => {
                Ok(ControlCmd::Ping)
            },
            Some(proto_commands::control_cmd::ControlType::Pong(_)) => {
                Ok(ControlCmd::Pong)
            },
            Some(proto_commands::control_cmd::ControlType::Error(cmd)) => {
                let error = if !cmd.details.is_empty() {
                    ErrorCommand::with_details(
                        cmd.status,
                        cmd.message.clone(),
                        cmd.details.clone(),
                    )
                } else if !cmd.reason.is_empty() {
                    ErrorCommand::with_reason(
                        cmd.status,
                        cmd.message.clone(),
                        cmd.reason.clone(),
                    )
                } else {
                    ErrorCommand::new(
                        cmd.status,
                        cmd.message.clone(),
                    )
                };
                Ok(ControlCmd::Error(error))
            },
            Some(proto_commands::control_cmd::ControlType::Custom(cmd)) => {
                let mut custom = CustomCommand::new(
                    cmd.name.clone(),
                    cmd.data.clone(),
                );
                for (key, value) in &cmd.metadata {
                    custom.add_metadata(key.clone(), value.clone());
                }
                Ok(ControlCmd::Custom(custom))
            },
            None => Err(FlareError::ProtocolError("Missing control type in proto control command".to_string())),
        }
    }

    fn rust_to_proto_control_cmd(rust_ctrl: &ControlCmd) -> Result<proto_commands::ControlCmd> {
        let mut proto_ctrl = proto_commands::ControlCmd::default();
        
        match rust_ctrl {
            ControlCmd::Connect(cmd) => {
                let mut connect = proto_commands::ConnectCommand::default();
                connect.client_id = cmd.client_id.clone();
                connect.protocol = cmd.protocol.clone();
                connect.platform = cmd.platform.clone();
                connect.version = cmd.version.clone();
                proto_ctrl.control_type = Some(proto_commands::control_cmd::ControlType::Connect(connect));
            },
            ControlCmd::ConnectAck(cmd) => {
                let mut connect_ack = proto_commands::ConnectAckCommand::default();
                connect_ack.status = cmd.status;
                if let Some(msg) = &cmd.status_message {
                    connect_ack.status_message = msg.clone();
                }
                connect_ack.session_id = cmd.session_id.clone();
                proto_ctrl.control_type = Some(proto_commands::control_cmd::ControlType::ConnectAck(connect_ack));
            },
            ControlCmd::Disconnect(cmd) => {
                let mut disconnect = proto_commands::DisconnectCommand::default();
                disconnect.status = cmd.status;
                disconnect.reason = cmd.reason.clone();
                if let Some(details) = &cmd.details {
                    disconnect.details = details.clone();
                }
                proto_ctrl.control_type = Some(proto_commands::control_cmd::ControlType::Disconnect(disconnect));
            },
            ControlCmd::AuthRequest(cmd) => {
                let mut auth_request = proto_commands::AuthRequestCommand::default();
                auth_request.user_id = cmd.user_id.clone();
                auth_request.platform = cmd.platform.clone();
                auth_request.token = cmd.token.clone();
                proto_ctrl.control_type = Some(proto_commands::control_cmd::ControlType::AuthRequest(auth_request));
            },
            ControlCmd::AuthResponse(cmd) => {
                let mut auth_response = proto_commands::AuthResponseCommand::default();
                auth_response.status = cmd.status;
                if let Some(msg) = &cmd.status_message {
                    auth_response.status_message = msg.clone();
                }
                auth_response.success = cmd.success;
                if let Some(user_info) = &cmd.user_info {
                    auth_response.user_info = user_info.clone();
                }
                if let Some(error_msg) = &cmd.error_message {
                    auth_response.error_message = error_msg.clone();
                }
                proto_ctrl.control_type = Some(proto_commands::control_cmd::ControlType::AuthResponse(auth_response));
            },
            ControlCmd::Ping => {
                proto_ctrl.control_type = Some(proto_commands::control_cmd::ControlType::Ping(proto_commands::PingCommand::default()));
            },
            ControlCmd::Pong => {
                proto_ctrl.control_type = Some(proto_commands::control_cmd::ControlType::Pong(proto_commands::PongCommand::default()));
            },
            ControlCmd::Error(cmd) => {
                let mut error = proto_commands::ErrorCommand::default();
                error.status = cmd.status;
                error.message = cmd.message.clone();
                if let Some(reason) = &cmd.reason {
                    error.reason = reason.clone();
                }
                if let Some(details) = &cmd.details {
                    error.details = details.clone();
                }
                proto_ctrl.control_type = Some(proto_commands::control_cmd::ControlType::Error(error));
            },
            ControlCmd::Custom(cmd) => {
                let mut custom = proto_commands::CustomCommand::default();
                custom.name = cmd.name.clone();
                custom.data = cmd.data.clone();
                for (key, value) in &cmd.metadata {
                    custom.metadata.insert(key.clone(), value.clone());
                }
                proto_ctrl.control_type = Some(proto_commands::control_cmd::ControlType::Custom(custom));
            },
        }
        
        Ok(proto_ctrl)
    }
    
    /// 将Rust的Reliability转换为Proto的Reliability
    pub fn rust_to_proto_reliability(reliability: Reliability) -> i32 {
        match reliability {
            Reliability::BestEffort => ProtobufReliability::BestEffort as i32,
            Reliability::AtLeastOnce => ProtobufReliability::AtLeastOnce as i32,
            Reliability::ExactlyOnce => ProtobufReliability::ExactlyOnce as i32,
            Reliability::Ordered => ProtobufReliability::Ordered as i32,
        }
    }
    
    /// 将Proto的Reliability转换为Rust的Reliability
    pub fn proto_to_rust_reliability(reliability: i32) -> Reliability {
        match reliability {
            0 => Reliability::BestEffort,
            1 => Reliability::AtLeastOnce,
            2 => Reliability::ExactlyOnce,
            3 => Reliability::Ordered,
            _ => Reliability::BestEffort, // 默认值
        }
    }
    
    // MessageCmd 转换
    fn proto_to_rust_message_cmd(proto_msg: &proto_commands::MessageCmd) -> Result<MessageCmd> {
        match &proto_msg.message_type {
            Some(proto_commands::message_cmd::MessageType::Send(cmd)) => {
                Ok(MessageCmd::Send(MessageSendCommand::new(cmd.data.clone())))
            },
            Some(proto_commands::message_cmd::MessageType::Ack(cmd)) => {
                let mut ack = if cmd.success {
                    MessageAckCommand::success()
                } else {
                    let error_code = if cmd.error_code != 0 {
                        Some(cmd.error_code)
                    } else {
                        None
                    };
                    let error_message = if !cmd.error_message.is_empty() {
                        Some(cmd.error_message.clone())
                    } else {
                        None
                    };
                    MessageAckCommand::failure(cmd.status, error_code, error_message)
                };
                
                if !cmd.message_id.is_empty() {
                    ack.message_id = Some(cmd.message_id.clone());
                }
                
                Ok(MessageCmd::Ack(ack))
            },
            Some(proto_commands::message_cmd::MessageType::Data(cmd)) => {
                Ok(MessageCmd::Data(DataCommand::new(cmd.data.clone())))
            },
            Some(proto_commands::message_cmd::MessageType::Custom(cmd)) => {
                let mut custom = CustomCommand::new(
                    cmd.name.clone(),
                    cmd.data.clone(),
                );
                for (key, value) in &cmd.metadata {
                    custom.add_metadata(key.clone(), value.clone());
                }
                Ok(MessageCmd::Custom(custom))
            },
            None => Err(FlareError::ProtocolError("Missing message type in proto message command".to_string())),
        }
    }

    fn rust_to_proto_message_cmd(rust_msg: &MessageCmd) -> Result<proto_commands::MessageCmd> {
        let mut proto_msg = proto_commands::MessageCmd::default();
        
        match rust_msg {
            MessageCmd::Send(cmd) => {
                let mut send = proto_commands::MessageSendCommand::default();
                send.data = cmd.data.clone();
                proto_msg.message_type = Some(proto_commands::message_cmd::MessageType::Send(send));
            },
            MessageCmd::Ack(cmd) => {
                let mut ack = proto_commands::MessageAckCommand::default();
                ack.status = cmd.status;
                if let Some(msg) = &cmd.status_message {
                    ack.status_message = msg.clone();
                }
                ack.success = cmd.success;
                if let Some(mid) = &cmd.message_id {
                    ack.message_id = mid.clone();
                }
                if let Some(code) = cmd.error_code {
                    ack.error_code = code;
                }
                if let Some(msg) = &cmd.error_message {
                    ack.error_message = msg.clone();
                }
                proto_msg.message_type = Some(proto_commands::message_cmd::MessageType::Ack(ack));
            },
            MessageCmd::Data(cmd) => {
                let mut data = proto_commands::DataCommand::default();
                data.data = cmd.data.clone();
                proto_msg.message_type = Some(proto_commands::message_cmd::MessageType::Data(data));
            },
            MessageCmd::Custom(cmd) => {
                let mut custom = proto_commands::CustomCommand::default();
                custom.name = cmd.name.clone();
                custom.data = cmd.data.clone();
                for (key, value) in &cmd.metadata {
                    custom.metadata.insert(key.clone(), value.clone());
                }
                proto_msg.message_type = Some(proto_commands::message_cmd::MessageType::Custom(custom));
            },
        }
        
        Ok(proto_msg)
    }

    // NotificationCmd 转换
    fn proto_to_rust_notification_cmd(proto_notify: &proto_commands::NotificationCmd) -> Result<NotificationCmd> {
        match &proto_notify.notification_type {
            Some(proto_commands::notification_cmd::NotificationType::System(cmd)) => {
                Ok(NotificationCmd::System(NotificationCommand::new(
                    cmd.content.clone(),
                    cmd.notification_type.clone(),
                )))
            },
            Some(proto_commands::notification_cmd::NotificationType::Broadcast(cmd)) => {
                Ok(NotificationCmd::Broadcast(NotificationCommand::new(
                    cmd.content.clone(),
                    cmd.notification_type.clone(),
                )))
            },
            Some(proto_commands::notification_cmd::NotificationType::Alert(cmd)) => {
                Ok(NotificationCmd::Alert(NotificationCommand::new(
                    cmd.content.clone(),
                    cmd.notification_type.clone(),
                )))
            },
            Some(proto_commands::notification_cmd::NotificationType::Custom(cmd)) => {
                let mut custom = CustomCommand::new(
                    cmd.name.clone(),
                    cmd.data.clone(),
                );
                for (key, value) in &cmd.metadata {
                    custom.add_metadata(key.clone(), value.clone());
                }
                Ok(NotificationCmd::Custom(custom))
            },
            None => Err(FlareError::ProtocolError("Missing notification type in proto notification command".to_string())),
        }
    }

    fn rust_to_proto_notification_cmd(rust_notify: &NotificationCmd) -> Result<proto_commands::NotificationCmd> {
        let mut proto_notify = proto_commands::NotificationCmd::default();
        
        match rust_notify {
            NotificationCmd::System(cmd) => {
                let mut system = proto_commands::NotificationCommand::default();
                system.content = cmd.content.clone();
                system.notification_type = cmd.notification_type.clone();
                proto_notify.notification_type = Some(proto_commands::notification_cmd::NotificationType::System(system));
            },
            NotificationCmd::Broadcast(cmd) => {
                let mut broadcast = proto_commands::NotificationCommand::default();
                broadcast.content = cmd.content.clone();
                broadcast.notification_type = cmd.notification_type.clone();
                proto_notify.notification_type = Some(proto_commands::notification_cmd::NotificationType::Broadcast(broadcast));
            },
            NotificationCmd::Alert(cmd) => {
                let mut alert = proto_commands::NotificationCommand::default();
                alert.content = cmd.content.clone();
                alert.notification_type = cmd.notification_type.clone();
                proto_notify.notification_type = Some(proto_commands::notification_cmd::NotificationType::Alert(alert));
            },
            NotificationCmd::Custom(cmd) => {
                let mut custom = proto_commands::CustomCommand::default();
                custom.name = cmd.name.clone();
                custom.data = cmd.data.clone();
                for (key, value) in &cmd.metadata {
                    custom.metadata.insert(key.clone(), value.clone());
                }
                proto_notify.notification_type = Some(proto_commands::notification_cmd::NotificationType::Custom(custom));
            },
        }
        
        Ok(proto_notify)
    }

    // EventCmd 转换
    fn proto_to_rust_event_cmd(proto_event: &proto_commands::EventCmd) -> Result<EventCmd> {
        match &proto_event.event_type {
            Some(proto_commands::event_cmd::EventType::Open(_)) => {
                Ok(EventCmd::Open)
            },
            Some(proto_commands::event_cmd::EventType::Close(_)) => {
                Ok(EventCmd::Close)
            },
            Some(proto_commands::event_cmd::EventType::Reconnect(_)) => {
                Ok(EventCmd::Reconnect)
            },
            Some(proto_commands::event_cmd::EventType::Custom(cmd)) => {
                let mut custom = CustomCommand::new(
                    cmd.name.clone(),
                    cmd.data.clone(),
                );
                for (key, value) in &cmd.metadata {
                    custom.add_metadata(key.clone(), value.clone());
                }
                Ok(EventCmd::Custom(custom))
            },
            None => Err(FlareError::ProtocolError("Missing event type in proto event command".to_string())),
        }
    }

    fn rust_to_proto_event_cmd(rust_event: &EventCmd) -> Result<proto_commands::EventCmd> {
        let mut proto_event = proto_commands::EventCmd::default();
        
        match rust_event {
            EventCmd::Open => {
                proto_event.event_type = Some(proto_commands::event_cmd::EventType::Open(proto_commands::OpenCommand::default()));
            },
            EventCmd::Close => {
                proto_event.event_type = Some(proto_commands::event_cmd::EventType::Close(proto_commands::CloseCommand::default()));
            },
            EventCmd::Reconnect => {
                proto_event.event_type = Some(proto_commands::event_cmd::EventType::Reconnect(proto_commands::ReconnectCommand::default()));
            },
            EventCmd::Custom(cmd) => {
                let mut custom = proto_commands::CustomCommand::default();
                custom.name = cmd.name.clone();
                custom.data = cmd.data.clone();
                for (key, value) in &cmd.metadata {
                    custom.metadata.insert(key.clone(), value.clone());
                }
                proto_event.event_type = Some(proto_commands::event_cmd::EventType::Custom(custom));
            },
        }
        
        Ok(proto_event)
    }
}

/// 简洁易用的API方法
/// 
/// 提供简单的方法来创建和转换Frame
pub struct FrameConverter;

impl FrameConverter {
    /// 创建一个新的Proto Frame
    pub fn create_proto_frame(
        message_id: u64,
        reliability: Reliability,
        command: Option<Command>,
    ) -> Result<ProtobufFrame> {
        let mut proto_frame = ProtobufFrame::default();
        
        // 设置基本属性
        proto_frame.message_id = message_id.to_string();
        proto_frame.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        proto_frame.reliability = ProtocolConverter::rust_to_proto_reliability(reliability);
        
        // 注意：Frame结构体中没有command字段，而是有单独的command字段
        if let Some(cmd) = command {
            let proto_command = ProtocolConverter::rust_to_proto_command(&cmd)?;
            proto_frame.command = Some(proto_command);
        }
        
        Ok(proto_frame)
    }
    
    /// 从Proto Frame中提取Rust Command
    pub fn extract_command_from_proto(proto_frame: &ProtobufFrame) -> Result<Option<Command>> {
        if proto_frame.command.is_some() {
            let proto_command = proto_frame.command.as_ref().ok_or_else(|| 
                FlareError::ProtocolError("Missing command in proto frame".to_string()))?;
            Ok(Some(ProtocolConverter::proto_to_rust_command(proto_command)?))
        } else {
            Ok(None)
        }
    }
    
    /// 将Rust Command转换为Proto Command并设置到Proto Frame中
    pub fn set_command_to_proto_frame(
        proto_frame: &mut ProtobufFrame,
        command: Command,
    ) -> Result<()> {
        let proto_command = ProtocolConverter::rust_to_proto_command(&command)?;
        proto_frame.command = Some(proto_command);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::common::protocol::Reliability;
    use super::*;
    
    #[test]
    fn test_convert_connect_command() {
        // 创建Rust的连接命令
        let rust_connect = ConnectCommand::new(
            "client123".to_string(),
            "websocket".to_string(),
            "ios".to_string(),
            "1.0.0".to_string(),
        );
        let rust_cmd = Command::Control(ControlCmd::Connect(rust_connect));
        
        // 转换为Proto命令
        let proto_cmd = ProtocolConverter::rust_to_proto_command(&rust_cmd).unwrap();
        
        // 再转换回Rust命令
        let converted_rust_cmd = ProtocolConverter::proto_to_rust_command(&proto_cmd).unwrap();
        
        // 验证转换后的命令与原始命令相同
        assert_eq!(rust_cmd, converted_rust_cmd);
    }
    
    #[test]
    fn test_create_proto_frame() {
        // 创建一个心跳命令
        let ping_cmd = Command::Control(ControlCmd::Ping);
        
        // 使用API创建Proto Frame
        let proto_frame = FrameConverter::create_proto_frame(
            123u64,
            Reliability::AtLeastOnce,
            Some(ping_cmd.clone()),
        ).unwrap();
        
        // 提取命令
        let extracted_cmd = FrameConverter::extract_command_from_proto(&proto_frame).unwrap();
        
        // 验证提取的命令与原始命令相同
        assert_eq!(Some(ping_cmd), extracted_cmd);
    }
}