//! 快速构建命令和 Frame 消息的辅助模块
//! 提供便捷方法创建各种类型的命令，自动生成消息 ID 和时间戳

use super::flare::core::commands::{
    command::Type as CommandType,
    message_command::Type as MessageType,
    notification_command::Type as NotificationType,
    system_command::{SerializationFormat, Type as SystemType},
};
use super::flare::core::{
    Frame, Reliability,
    commands::{Command, CustomCommand, MessageCommand, NotificationCommand, SystemCommand},
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// 生成唯一的消息 ID（基于时间戳和递增计数器）
pub fn generate_message_id() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{:016x}", timestamp, counter)
}

/// 获取当前时间戳（毫秒）
pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// ============================================================================
// SystemCommand 构建辅助函数
// ============================================================================

/// 创建基础 SystemCommand（内部辅助函数）
fn create_base_system_command(r#type: SystemType, format: SerializationFormat) -> SystemCommand {
    SystemCommand {
        r#type: r#type as i32,
        format: format as i32,
        message: String::new(),
        metadata: HashMap::new(),
        data: Vec::new(),
        compression: String::new(),
        encryption: String::new(),
    }
}

/// 创建带消息的 SystemCommand（内部辅助函数）
fn create_system_command_with_message(
    r#type: SystemType,
    format: SerializationFormat,
    message: impl Into<String>,
    metadata: Option<HashMap<String, Vec<u8>>>,
) -> SystemCommand {
    SystemCommand {
        r#type: r#type as i32,
        format: format as i32,
        message: message.into(),
        metadata: metadata.unwrap_or_default(),
        data: Vec::new(),
        compression: String::new(),
        encryption: String::new(),
    }
}

/// 创建带数据的 SystemCommand（内部辅助函数）
fn create_system_command_with_data(
    r#type: SystemType,
    format: SerializationFormat,
    message: impl Into<String>,
    metadata: Option<HashMap<String, Vec<u8>>>,
    data: Option<Vec<u8>>,
) -> SystemCommand {
    SystemCommand {
        r#type: r#type as i32,
        format: format as i32,
        message: message.into(),
        metadata: metadata.unwrap_or_default(),
        data: data.unwrap_or_default(),
        compression: String::new(),
        encryption: String::new(),
    }
}

// ============================================================================
// Frame 构建器
// ============================================================================

/// Frame 构建器
pub struct FrameBuilder {
    command: Option<Command>,
    message_id: Option<String>,
    reliability: Reliability,
    timestamp: Option<u64>,
    metadata: HashMap<String, Vec<u8>>,
}

impl FrameBuilder {
    /// 创建新的 Frame 构建器
    pub fn new() -> Self {
        Self {
            command: None,
            message_id: None,
            reliability: Reliability::BestEffort,
            timestamp: None,
            metadata: HashMap::new(),
        }
    }

    /// 设置命令
    #[must_use]
    pub fn with_command(mut self, command: Command) -> Self {
        self.command = Some(command);
        self
    }

    /// 设置消息 ID（不设置则自动生成）
    #[must_use]
    pub fn with_message_id(mut self, message_id: String) -> Self {
        self.message_id = Some(message_id);
        self
    }

    /// 设置可靠性等级
    #[must_use]
    pub fn with_reliability(mut self, reliability: Reliability) -> Self {
        self.reliability = reliability;
        self
    }

    /// 设置时间戳（不设置则使用当前时间）
    #[must_use]
    pub fn with_timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// 添加元数据
    #[must_use]
    pub fn with_metadata(mut self, key: String, value: Vec<u8>) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// 添加字符串元数据
    #[must_use]
    pub fn with_metadata_str(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value.into_bytes());
        self
    }

    /// 构建 Frame
    pub fn build(self) -> Frame {
        Frame {
            command: self.command,
            message_id: self.message_id.unwrap_or_else(generate_message_id),
            reliability: self.reliability as i32,
            timestamp: self.timestamp.unwrap_or_else(current_timestamp),
            metadata: self.metadata,
        }
    }
}

impl Default for FrameBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// 系统命令构建方法
// ============================================================================

/// 创建 PING 命令
pub fn ping() -> SystemCommand {
    create_base_system_command(SystemType::Ping, SerializationFormat::Protobuf)
}

/// 创建 PONG 命令
pub fn pong() -> SystemCommand {
    create_base_system_command(SystemType::Pong, SerializationFormat::Protobuf)
}

/// 创建 CONNECT 命令
pub fn connect(format: SerializationFormat, metadata: HashMap<String, Vec<u8>>) -> SystemCommand {
    SystemCommand {
        r#type: SystemType::Connect as i32,
        format: format as i32,
        message: String::new(),
        metadata,
        data: Vec::new(),
        compression: String::new(),
        encryption: String::new(),
    }
}

/// 创建 CONNECT_ACK 命令
pub fn connect_ack(
    format: SerializationFormat,
    compression: Option<&str>,
    encryption: Option<&str>,
    metadata: HashMap<String, Vec<u8>>,
) -> SystemCommand {
    SystemCommand {
        r#type: SystemType::ConnectAck as i32,
        format: format as i32,
        message: String::new(),
        metadata,
        data: Vec::new(),
        compression: compression.unwrap_or("none").to_string(),
        encryption: encryption.unwrap_or("none").to_string(),
    }
}

/// 创建 CLOSE 命令
pub fn close(message: Option<String>, metadata: Option<HashMap<String, Vec<u8>>>) -> SystemCommand {
    create_system_command_with_message(
        SystemType::Close,
        SerializationFormat::Protobuf,
        message.unwrap_or_default(),
        metadata,
    )
}

/// 创建 ERROR 命令
pub fn error(message: String, metadata: Option<HashMap<String, Vec<u8>>>) -> SystemCommand {
    create_system_command_with_message(
        SystemType::Error,
        SerializationFormat::Protobuf,
        message,
        metadata,
    )
}

/// 创建 EVENT 命令
pub fn event(
    message: String,
    metadata: Option<HashMap<String, Vec<u8>>>,
    data: Option<Vec<u8>>,
) -> SystemCommand {
    create_system_command_with_data(
        SystemType::Event,
        SerializationFormat::Protobuf,
        message,
        metadata,
        data,
    )
}

/// 创建 AUTH 命令
pub fn auth(metadata: HashMap<String, Vec<u8>>, data: Option<Vec<u8>>) -> SystemCommand {
    SystemCommand {
        r#type: SystemType::Auth as i32,
        format: SerializationFormat::Protobuf as i32,
        message: String::new(),
        metadata,
        data: data.unwrap_or_default(),
        compression: String::new(),
        encryption: String::new(),
    }
}

/// 创建 AUTH_ACK 命令
pub fn auth_ack(
    message: Option<String>,
    metadata: Option<HashMap<String, Vec<u8>>>,
) -> SystemCommand {
    create_system_command_with_message(
        SystemType::AuthAck,
        SerializationFormat::Protobuf,
        message.unwrap_or_default(),
        metadata,
    )
}

/// 创建 KICKED 命令（被踢下线）
///
/// # 参数
/// - `reason`: 被踢的原因（必需）
/// - `metadata`: 可选的元数据（如设备信息、冲突连接ID等）
///
/// # 示例
/// ```rust
/// use flare_core::common::protocol::builder::kicked;
/// use flare_core::common::protocol::frame_with_system_command;
/// use std::collections::HashMap;
///
/// let mut metadata = HashMap::new();
/// metadata.insert("conflict_device".to_string(), "device-123".as_bytes().to_vec());
///
/// let kick_cmd = kicked("设备冲突：同一平台已有其他设备在线", Some(metadata));
/// let frame = frame_with_system_command(kick_cmd, Reliability::AtLeastOnce);
/// ```
pub fn kicked(
    reason: impl Into<String>,
    metadata: Option<HashMap<String, Vec<u8>>>,
) -> SystemCommand {
    create_system_command_with_message(
        SystemType::Kicked,
        SerializationFormat::Protobuf,
        reason,
        metadata,
    )
}

// ============================================================================
// 消息命令构建方法
// ============================================================================

/// 创建消息命令（内部辅助函数）
fn create_message_command(
    r#type: MessageType,
    message_id: String,
    payload: Vec<u8>,
    metadata: Option<HashMap<String, Vec<u8>>>,
    seq: Option<u64>,
) -> MessageCommand {
    MessageCommand {
        r#type: r#type as i32,
        message_id,
        payload,
        metadata: metadata.unwrap_or_default(),
        seq: seq.unwrap_or(0),
    }
}

/// 创建 SEND 消息命令
pub fn send_message(
    message_id: String,
    payload: Vec<u8>,
    metadata: Option<HashMap<String, Vec<u8>>>,
    seq: Option<u64>,
) -> MessageCommand {
    create_message_command(MessageType::Send, message_id, payload, metadata, seq)
}

/// 创建 ACK 消息命令
pub fn ack_message(
    message_id: String,
    metadata: Option<HashMap<String, Vec<u8>>>,
) -> MessageCommand {
    MessageCommand {
        r#type: MessageType::Ack as i32,
        message_id,
        payload: Vec::new(),
        metadata: metadata.unwrap_or_default(),
        seq: 0,
    }
}

/// 创建 DATA 消息命令（无需 ACK）
pub fn data_message(
    message_id: String,
    payload: Vec<u8>,
    metadata: Option<HashMap<String, Vec<u8>>>,
    seq: Option<u64>,
) -> MessageCommand {
    create_message_command(MessageType::Data, message_id, payload, metadata, seq)
}

// ============================================================================
// 通知命令构建方法
// ============================================================================

/// 创建通知命令
pub fn notification(
    notification_type: NotificationType,
    title: String,
    content: Vec<u8>,
    metadata: Option<HashMap<String, Vec<u8>>>,
) -> NotificationCommand {
    NotificationCommand {
        r#type: notification_type as i32,
        title,
        content,
        metadata: metadata.unwrap_or_default(),
    }
}

// ============================================================================
// 自定义命令构建方法
// ============================================================================

/// 创建自定义命令
pub fn custom_command(
    name: String,
    data: Vec<u8>,
    metadata: Option<HashMap<String, Vec<u8>>>,
) -> CustomCommand {
    CustomCommand {
        name,
        data,
        metadata: metadata.unwrap_or_default(),
    }
}

// ============================================================================
// Frame 快速构建方法
// ============================================================================

/// 创建包含命令的 Frame（内部辅助函数）
fn create_frame_with_command(command_type: CommandType, reliability: Reliability) -> Frame {
    FrameBuilder::new()
        .with_command(Command {
            r#type: Some(command_type),
        })
        .with_reliability(reliability)
        .build()
}

/// 创建包含系统命令的 Frame
pub fn frame_with_system_command(system_command: SystemCommand, reliability: Reliability) -> Frame {
    create_frame_with_command(CommandType::System(system_command), reliability)
}

/// 创建包含消息命令的 Frame
///
/// 注意：Frame 的 message_id 会使用 MessageCommand.message_id，以确保客户端和服务端能正确匹配响应
/// 如果 MessageCommand.message_id 为空，则自动生成一个并更新 MessageCommand
pub fn frame_with_message_command(
    mut message_command: MessageCommand,
    reliability: Reliability,
) -> Frame {
    // 如果 message_id 为空，自动生成一个并更新 MessageCommand
    if message_command.message_id.is_empty() {
        message_command.message_id = generate_message_id();
    }

    // 使用 MessageCommand.message_id 作为 Frame 的 message_id，确保两者一致
    let message_id = message_command.message_id.clone();

    FrameBuilder::new()
        .with_command(Command {
            r#type: Some(CommandType::Message(message_command)),
        })
        .with_message_id(message_id)
        .with_reliability(reliability)
        .build()
}

/// 创建包含通知命令的 Frame
pub fn frame_with_notification_command(
    notification_command: NotificationCommand,
    reliability: Reliability,
) -> Frame {
    create_frame_with_command(CommandType::Notification(notification_command), reliability)
}

/// 创建包含自定义命令的 Frame
pub fn frame_with_custom_command(custom_command: CustomCommand, reliability: Reliability) -> Frame {
    create_frame_with_command(CommandType::Custom(custom_command), reliability)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping_pong() {
        let ping_cmd = ping();
        assert_eq!(ping_cmd.r#type, SystemType::Ping as i32);

        let pong_cmd = pong();
        assert_eq!(pong_cmd.r#type, SystemType::Pong as i32);
    }

    #[test]
    fn test_generate_message_id() {
        let id1 = generate_message_id();
        let id2 = generate_message_id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_frame_builder() {
        let frame = FrameBuilder::new()
            .with_command(Command {
                r#type: Some(CommandType::System(ping())),
            })
            .with_reliability(Reliability::AtLeastOnce)
            .build();

        assert!(!frame.message_id.is_empty());
        assert!(frame.timestamp > 0);
    }
}
