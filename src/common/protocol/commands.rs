//! 长连接协议命令定义
//! 
//! 本模块定义了长连接协议中的各种命令类型和结构

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// 一、核心命令枚举定义
// ============================================================================

/// 长链接大类命令
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// 控制类命令（心跳、鉴权、ACK、错误等）
    #[serde(rename = "c")]
    Control(ControlCmd),

    /// 消息类命令（需要ACK确认）
    #[serde(rename = "m")]
    Message(MessageCmd),

    /// 通知类命令（系统通知、广播等）
    #[serde(rename = "n")]
    Notification(NotificationCmd),

    /// 事件类命令（连接状态变化）
    #[serde(rename = "e")]
    Event(EventCmd),
}

impl Command {
    /// 获取命令的字符串表示（使用短字符）
    pub fn as_str(&self) -> &'static str {
        match self {
            Command::Control(_) => "c",
            Command::Message(_) => "m",
            Command::Notification(_) => "n",
            Command::Event(_) => "e",
        }
    }
    
    /// 获取命令类型名称
    pub fn command_type(&self) -> &'static str {
        match self {
            Command::Control(_) => "control",
            Command::Message(_) => "message",
            Command::Notification(_) => "notification",
            Command::Event(_) => "event",
        }
    }
}

// ============================================================================
// 二、控制类命令 (Control Commands)
// ============================================================================

/// 控制类子命令
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlCmd {
    // 核心控制命令
    #[serde(rename = "cn")]
    Connect(ConnectCommand),
    #[serde(rename = "ca")]
    ConnectAck(ConnectAckCommand),
    #[serde(rename = "dc")]
    Disconnect(DisconnectCommand),
    #[serde(rename = "ar")]
    AuthRequest(AuthRequestCommand),
    #[serde(rename = "as")]
    AuthResponse(AuthResponseCommand),
    
    // 心跳相关
    #[serde(rename = "p")]
    Ping,
    #[serde(rename = "po")]
    Pong,
    
    // 错误处理
    #[serde(rename = "er")]
    Error(ErrorCommand),
    
    // 自定义控制命令
    #[serde(rename = "cc")]
    Custom(CustomCommand),
}

impl ControlCmd {
    /// 获取控制命令的字符串表示（使用短字符）
    pub fn as_str(&self) -> &'static str {
        match self {
            ControlCmd::Connect(_) => "cn",
            ControlCmd::ConnectAck(_) => "ca",
            ControlCmd::Disconnect(_) => "dc",
            ControlCmd::AuthRequest(_) => "ar",
            ControlCmd::AuthResponse(_) => "as",
            ControlCmd::Ping => "p",
            ControlCmd::Pong => "po",
            ControlCmd::Error(_) => "er",
            ControlCmd::Custom(_) => "cc",
        }
    }
}


// ============================================================================
// 三、消息类命令 (Message Commands)
// ============================================================================

/// 消息类子命令（需要ACK确认）
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageCmd {
    // 核心消息命令（需要ACK确认）
    #[serde(rename = "s")]
    Send(MessageSendCommand),
    #[serde(rename = "a")]
    Ack(MessageAckCommand),
    
    // 数据传输作为消息的一个变体（需要ACK确认）
    #[serde(rename = "d")]
    Data(DataCommand),
    
    // 自定义消息命令
    #[serde(rename = "cm")]
    Custom(CustomCommand),
}

impl MessageCmd {
    /// 获取消息命令的字符串表示（使用短字符）
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageCmd::Send(_) => "s",
            MessageCmd::Ack(_) => "a",
            MessageCmd::Data(_) => "d",
            MessageCmd::Custom(_) => "cm",
        }
    }
}

// ============================================================================
// 四、通知类命令 (Notification Commands)
// ============================================================================

/// 通知类子命令
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotificationCmd {
    // 核心通知命令
    #[serde(rename = "s")]
    System(NotificationCommand),
    #[serde(rename = "b")]
    Broadcast(NotificationCommand),
    #[serde(rename = "a")]
    Alert(NotificationCommand),
    
    // 自定义通知命令
    #[serde(rename = "cn")]
    Custom(CustomCommand),
}

impl NotificationCmd {
    /// 获取通知命令的字符串表示（使用短字符）
    pub fn as_str(&self) -> &'static str {
        match self {
            NotificationCmd::System(_) => "s",
            NotificationCmd::Broadcast(_) => "b",
            NotificationCmd::Alert(_) => "a",
            NotificationCmd::Custom(_) => "cn",
        }
    }
}

// ============================================================================
// 五、事件类命令 (Event Commands)
// ============================================================================

/// 事件类子命令
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventCmd {
    // 核心事件命令
    #[serde(rename = "o")]
    Open,
    #[serde(rename = "c")]
    Close,
    #[serde(rename = "r")]
    Reconnect,
    
    // 自定义事件命令
    #[serde(rename = "ce")]
    Custom(CustomCommand),
}

impl EventCmd {
    /// 获取事件命令的字符串表示（使用短字符）
    pub fn as_str(&self) -> &'static str {
        match self {
            EventCmd::Open => "o",
            EventCmd::Close => "c",
            EventCmd::Reconnect => "r",
            EventCmd::Custom(_) => "ce",
        }
    }
}

// ============================================================================
// 六、通用命令结构 (Common Command Structures)
// ============================================================================

/// 自定义命令结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomCommand {
    /// 命令名称
    #[serde(rename = "n")]
    pub name: String,
    /// 命令数据
    #[serde(rename = "d")]
    pub data: Vec<u8>,
    /// 额外的元数据
    #[serde(rename = "m", skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
}

// 为 CustomCommand 手动实现 PartialEq 和 Eq
impl PartialEq for CustomCommand {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && 
        self.data == other.data && 
        self.metadata.len() == other.metadata.len() &&
        self.metadata.iter().all(|(k, v)| other.metadata.get(k) == Some(v))
    }
}

impl Eq for CustomCommand {}

impl CustomCommand {
    /// 创建新的自定义命令
    pub fn new(name: String, data: Vec<u8>) -> Self {
        Self {
            name,
            data,
            metadata: HashMap::new(),
        }
    }
    
    /// 创建带元数据的自定义命令
    pub fn with_metadata(name: String, data: Vec<u8>, metadata: HashMap<String, String>) -> Self {
        Self {
            name,
            data,
            metadata,
        }
    }
    
    /// 添加元数据
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }
}

// ============================================================================
// 七、控制类命令结构 (Control Command Structures)
// ============================================================================

/// 连接命令
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectCommand {
    /// 客户端ID
    #[serde(rename = "cid")]
    pub client_id: String,
    /// 协议类型
    #[serde(rename = "p")]
    pub protocol: String,
    /// 平台信息
    #[serde(rename = "pf")]
    pub platform: String,
    /// 客户端版本
    #[serde(rename = "v")]
    pub version: String,
}

impl ConnectCommand {
    /// 创建新的连接命令
    pub fn new(client_id: String, protocol: String, platform: String, version: String) -> Self {
        Self {
            client_id,
            protocol,
            platform,
            version,
        }
    }
}

/// 连接确认命令
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectAckCommand {
    /// 状态码
    #[serde(rename = "s")]
    pub status: i32,
    /// 状态消息
    #[serde(rename = "sm", skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    /// 会话ID
    #[serde(rename = "sid")]
    pub session_id: String,
}

impl ConnectAckCommand {
    /// 创建新的连接确认命令（成功）
    pub fn success(session_id: String) -> Self {
        Self {
            status: 200,
            status_message: Some("Connection established successfully".to_string()),
            session_id,
        }
    }
    
    /// 创建新的连接确认命令（失败）
    pub fn failure(status: i32, message: String) -> Self {
        Self {
            status,
            status_message: Some(message),
            session_id: String::new(),
        }
    }
    
    /// 创建新的连接确认命令
    pub fn new(session_id: String) -> Self {
        Self {
            status: 200,
            status_message: None,
            session_id,
        }
    }
}

/// 断开连接命令
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisconnectCommand {
    /// 状态码
    #[serde(rename = "s")]
    pub status: i32,
    /// 断开原因
    #[serde(rename = "r")]
    pub reason: String,
    /// 详细信息
    #[serde(rename = "d", skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

impl DisconnectCommand {
    /// 创建新的断开连接命令
    pub fn new(status: i32, reason: String) -> Self {
        Self {
            status,
            reason,
            details: None,
        }
    }
    
    /// 创建带详细信息的断开连接命令
    pub fn with_details(status: i32, reason: String, details: String) -> Self {
        Self {
            status,
            reason,
            details: Some(details),
        }
    }
}

/// 认证请求命令
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthRequestCommand {
    /// 用户ID
    #[serde(rename = "uid")]
    pub user_id: String,
    /// 平台
    #[serde(rename = "pf")]
    pub platform: String,
    /// 认证令牌
    #[serde(rename = "t")]
    pub token: String,
}

impl AuthRequestCommand {
    /// 创建新的认证请求命令
    pub fn new(user_id: String, platform: String, token: String) -> Self {
        Self {
            user_id,
            platform,
            token,
        }
    }
}

/// 认证响应命令
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthResponseCommand {
    /// 状态码
    #[serde(rename = "s")]
    pub status: i32,
    /// 状态消息
    #[serde(rename = "sm", skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    /// 是否成功
    #[serde(rename = "success")]
    pub success: bool,
    /// 用户信息
    #[serde(rename = "ui", skip_serializing_if = "Option::is_none")]
    pub user_info: Option<Vec<u8>>,
    /// 错误消息
    #[serde(rename = "em", skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

impl AuthResponseCommand {
    /// 创建新的认证响应命令（成功）
    pub fn success(user_info: Option<Vec<u8>>) -> Self {
        Self {
            status: 200,
            status_message: Some("Authentication successful".to_string()),
            success: true,
            user_info,
            error_message: None,
        }
    }

    /// 创建新的认证响应命令（失败）
    pub fn failure(status: i32, error_message: String) -> Self {
        Self {
            status,
            status_message: Some(error_message.clone()),
            success: false,
            user_info: None,
            error_message: Some(error_message),
        }
    }
}

// ============================================================================
// 八、消息类命令结构 (Message Command Structures)
// ============================================================================

/// 消息发送命令
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageSendCommand {
    /// 消息内容
    #[serde(rename = "d")]
    pub data: Vec<u8>,
}

impl MessageSendCommand {
    /// 创建新的消息发送命令
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
}

/// 消息确认命令
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageAckCommand {
    /// 状态码
    #[serde(rename = "s")]
    pub status: i32,
    /// 状态消息
    #[serde(rename = "sm", skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    /// 是否成功
    #[serde(rename = "success")]
    pub success: bool,
    /// 消息ID
    #[serde(rename = "mid", skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    /// 错误码  
    #[serde(rename = "ec", skip_serializing_if = "Option::is_none")]
    pub error_code: Option<u32>,
    /// 错误消息
    #[serde(rename = "em", skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

impl MessageAckCommand {
    /// 创建新的消息确认命令（成功）
    pub fn success() -> Self {
        Self {
            status: 200,
            status_message: Some("Message processed successfully".to_string()),
            success: true,
            message_id: None,
            error_code: None,
            error_message: None,
        }
    }

    /// 创建新的消息确认命令（失败）
    pub fn failure(status: i32, error_code: Option<u32>, error_message: Option<String>) -> Self {
        Self {
            status,
            status_message: error_message.clone(),
            success: false,
            message_id: None,
            error_code,
            error_message,
        }
    }
}
/// 数据命令
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataCommand {
    /// 数据内容
    #[serde(rename = "d")]
    pub data: Vec<u8>,
}

impl DataCommand {
    /// 创建新的数据命令
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
}
// ============================================================================
// 九、通知类命令结构 (Notification Command Structures)
// ============================================================================

/// 通知命令
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationCommand {
    /// 通知内容
    #[serde(rename = "c")]
    pub content: String,
    /// 通知类型
    #[serde(rename = "t")]
    pub notification_type: String,
}

impl NotificationCommand {
    /// 创建新的通知命令
    pub fn new(content: String, notification_type: String) -> Self {
        Self { content, notification_type }
    }
    
    /// 创建系统通知
    pub fn system(content: String) -> Self {
        Self::new(content, "system".to_string())
    }
    
    /// 创建广播通知
    pub fn broadcast(content: String) -> Self {
        Self::new(content, "broadcast".to_string())
    }
    
    /// 创建警报通知
    pub fn alert(content: String) -> Self {
        Self::new(content, "alert".to_string())
    }
}


// ============================================================================
// 十一、事件类命令结构 (Event Command Structures)
// ============================================================================

// 事件类命令通常不需要额外的数据结构，使用空结构体即可

// ============================================================================
// 十二、错误处理命令结构 (Error Handling Command Structures)
// ============================================================================

/// 错误命令
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorCommand {
    /// 状态码
    #[serde(rename = "s")]
    pub status: i32,
    /// 错误消息
    #[serde(rename = "m")]
    pub message: String,
    /// 错误原因
    #[serde(rename = "r", skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// 详细错误信息
    #[serde(rename = "d", skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

impl ErrorCommand {
    /// 创建新的错误命令
    pub fn new(status: i32,  message: String) -> Self {
        Self {
            status,
            message,
            reason: None,
            details: None,
        }
    }
    
    /// 创建带原因的错误命令
    pub fn with_reason(status: i32, message: String, reason: String) -> Self {
        Self {
            status,
            message,
            reason: Some(reason),
            details: None,
        }
    }
    
    /// 创建带详细信息的错误命令
    pub fn with_details(status: i32,message: String, details: String) -> Self {
        Self {
            status,
            message,
            reason: None,
            details: Some(details),
        }
    }
}

