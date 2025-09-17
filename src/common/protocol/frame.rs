use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use crate::common::protocol::commands::{Command, ControlCmd};

/// 消息可靠性级别
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reliability {
    /// 尽力而为，不保证送达
    BestEffort = 0,
    /// 至少一次送达
    AtLeastOnce = 1,
    /// 恰好一次送达
    ExactlyOnce = 2,
    /// 有序送达
    Ordered = 3,
}

impl Reliability {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Reliability::BestEffort),
            1 => Some(Reliability::AtLeastOnce),
            2 => Some(Reliability::ExactlyOnce),
            3 => Some(Reliability::Ordered),
            _ => None,
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            Reliability::BestEffort => 0,
            Reliability::AtLeastOnce => 1,
            Reliability::ExactlyOnce => 2,
            Reliability::Ordered => 3,
        }
    }
}

/// 统一消息帧 - 核心消息结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    /// 消息ID (改为String类型以提升兼容性)
    pub message_id: String,
    /// 可靠性级别
    pub reliability: Reliability,
    /// 时间戳
    pub timestamp: u64,
    /// 命令
    pub command: Command,
    /// 会话ID
    pub session_id: Option<String>,
    /// 优先级
    pub priority: u8,
    /// 压缩算法
    pub compression: Option<u8>,
    /// 加密标志
    pub encrypted: bool,
    /// 元数据（用于传递额外信息，如平台信息）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, Vec<u8>>>,
}

impl Default for Frame {
    fn default() -> Self {
        Self {
            message_id: uuid::Uuid::new_v4().to_string(),
            reliability: Reliability::BestEffort,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            command: Command::Control(ControlCmd::Ping), // 默认使用Ping命令
            session_id: None,
            priority: 0,
            compression: None,
            encrypted: false,
            metadata: None,
        }
    }
}

impl Frame {
    /// 创建新的消息帧
    pub fn new(
        command: Command,
        message_id: String,
        reliability: Reliability,
    ) -> Self {
        Self {
            command,
            message_id,
            reliability,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            session_id: None,
            priority: 0,
            compression: None,
            encrypted: false,
            metadata: None,
        }
    }
    
    /// 创建无会话ID的消息帧
    pub fn new_command(
        command: Command,
    ) -> Self {
        Self::new_full(command, uuid::Uuid::new_v4().to_string(), Reliability::BestEffort, None, 0)
    }

    /// 创建完整参数的消息帧
    pub fn new_full(
        command: Command,
        message_id: String,
        reliability: Reliability,
        session_id: Option<String>,
        priority: u8,
    ) -> Self {
        Self {
            command,
            message_id,
            reliability,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            session_id,
            priority,
            compression: None,
            encrypted: false,
            metadata: None,
        }
    }

    /// 获取可靠性级别
    pub fn get_reliability(&self) -> Reliability {
        self.reliability
    }

    /// 获取消息类型
    pub fn get_command(&self) -> Command {
        self.command.clone()
    }
    
    /// 获取命令类型的字符串表示
    pub fn get_command_type_str(&self) -> &'static str {
        self.command.command_type()
    }

    /// 获取消息ID
    pub fn get_message_id(&self) -> String {
        self.message_id.clone()
    }

    /// 获取时间戳
    pub fn get_timestamp(&self) -> u64 {
        self.timestamp
    }

    /// 获取会话ID
    pub fn get_session_id(&self) -> &Option<String> {
        &self.session_id
    }

    /// 获取优先级
    pub fn get_priority(&self) -> u8 {
        self.priority
    }

    /// 设置会话ID
    pub fn set_session_id(&mut self, session_id: Option<String>) {
        self.session_id = session_id;
    }

    /// 设置优先级
    pub fn set_priority(&mut self, priority: u8) {
        self.priority = priority;
    }
    
    /// 创建心跳帧
    pub fn heartbeat(message_id: String) -> Self {
        let command = Command::Control(ControlCmd::Ping);
        Self::new(command, message_id, Reliability::BestEffort)
    }
    
    /// 创建错误帧
    pub fn error(message_id: String,message: String) -> Self {
        let error_cmd = crate::common::protocol::commands::ErrorCommand::new(500, message);
        let command = Command::Control(ControlCmd::Error(error_cmd));
        Self::new(command, message_id, Reliability::AtLeastOnce)
    }
    
    /// 创建认证响应帧
    pub fn auth_response(
        message_id: String,
        success: bool,
        status: i32,
        user_info: Option<Vec<u8>>,
        error_message: Option<String>,
    ) -> Self {
        let auth_response = if success {
            crate::common::protocol::commands::AuthResponseCommand::success(user_info)
        } else {
            crate::common::protocol::commands::AuthResponseCommand::failure(status, error_message.unwrap_or_default())
        };
        let command = Command::Control(ControlCmd::AuthResponse(auth_response));
        Self::new(command, message_id, Reliability::AtLeastOnce)
    }
}

// 消息ID生成器
pub struct MessageIdGenerator;

impl MessageIdGenerator {
    /// 生成UUID格式的消息ID
    pub fn generate_uuid() -> String {
        Uuid::new_v4().as_simple().to_string()
    }
    
    /// 生成时间戳格式的消息ID
    pub fn generate_timestamp() -> String {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .to_string()
    }
    
    /// 生成自定义格式的消息ID (前缀+时间戳)
    pub fn generate_with_prefix(prefix: &str) -> String {
        format!("{}_{}", prefix, Self::generate_timestamp())
    }
}