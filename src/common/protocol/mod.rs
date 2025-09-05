//! 核心协议定义 - 专注于长连接可靠性
//! Author: Flare Core Team
//! Description: Core protocol definitions for reliable long-connection communication
//! This module contains both Serde-based and Protobuf-based message structures
//! for maximum compatibility and performance.

use serde::{Deserialize, Serialize};
use std::fmt;

// 引入Protobuf生成的代码
mod flare {
    pub mod core {
        include!("flare.core.rs");
    }
}

// 重新导出Protobuf生成的结构和枚举
pub use flare::core::{Frame as ProtobufFrame, MessageType as ProtobufMessageType, Reliability as ProtobufReliability};

/// 消息类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MessageType {
    // 核心消息类型
    Heartbeat = 1,
    HeartbeatAck = 2,
    Connect = 3,
    ConnectAck = 4,
    Disconnect = 5,
    DisconnectAck = 6,
    
    // 数据传输
    Data = 7,
    DataAck = 8,
    Retransmit = 9,
    
    // 协议控制
    ProtocolSwitch = 10,
    ProtocolTest = 11,
    
    // 错误处理
    Error = 12,
    Notification = 13,
    
    // 扩展类型
    CustomEvent = 17,
    CustomMessage = 18,
}

impl MessageType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(MessageType::Heartbeat),
            2 => Some(MessageType::HeartbeatAck),
            3 => Some(MessageType::Connect),
            4 => Some(MessageType::ConnectAck),
            5 => Some(MessageType::Disconnect),
            6 => Some(MessageType::DisconnectAck),
            7 => Some(MessageType::Data),
            8 => Some(MessageType::DataAck),
            9 => Some(MessageType::Retransmit),
            10 => Some(MessageType::ProtocolSwitch),
            11 => Some(MessageType::ProtocolTest),
            12 => Some(MessageType::Error),
            13 => Some(MessageType::Notification),
            17 => Some(MessageType::CustomEvent),
            18 => Some(MessageType::CustomMessage),
            _ => None,
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            MessageType::Heartbeat => 1,
            MessageType::HeartbeatAck => 2,
            MessageType::Connect => 3,
            MessageType::ConnectAck => 4,
            MessageType::Disconnect => 5,
            MessageType::DisconnectAck => 6,
            MessageType::Data => 7,
            MessageType::DataAck => 8,
            MessageType::Retransmit => 9,
            MessageType::ProtocolSwitch => 10,
            MessageType::ProtocolTest => 11,
            MessageType::Error => 12,
            MessageType::Notification => 13,
            MessageType::CustomEvent => 17,
            MessageType::CustomMessage => 18,
        }
    }

    pub fn is_heartbeat(&self) -> bool {
        matches!(self, MessageType::Heartbeat | MessageType::HeartbeatAck)
    }

    pub fn is_control(&self) -> bool {
        matches!(self, 
            MessageType::Connect | MessageType::ConnectAck |
            MessageType::Disconnect | MessageType::DisconnectAck |
            MessageType::ProtocolSwitch | MessageType::ProtocolTest
        )
    }

    pub fn is_data(&self) -> bool {
        matches!(self, MessageType::Data | MessageType::DataAck | MessageType::Retransmit)
    }
}

impl fmt::Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MessageType::Heartbeat => write!(f, "Heartbeat"),
            MessageType::HeartbeatAck => write!(f, "HeartbeatAck"),
            MessageType::Connect => write!(f, "Connect"),
            MessageType::ConnectAck => write!(f, "ConnectAck"),
            MessageType::Disconnect => write!(f, "Disconnect"),
            MessageType::DisconnectAck => write!(f, "DisconnectAck"),
            MessageType::Data => write!(f, "Data"),
            MessageType::DataAck => write!(f, "DataAck"),
            MessageType::Retransmit => write!(f, "Retransmit"),
            MessageType::ProtocolSwitch => write!(f, "ProtocolSwitch"),
            MessageType::ProtocolTest => write!(f, "ProtocolTest"),
            MessageType::Error => write!(f, "Error"),
            MessageType::Notification => write!(f, "Notification"),
            MessageType::CustomEvent => write!(f, "CustomEvent"),
            MessageType::CustomMessage => write!(f, "CustomMessage"),
        }
    }
}

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
    /// 消息类型
    pub message_type: MessageType,
    /// 消息ID
    pub message_id: u64,
    /// 可靠性级别
    pub reliability: Reliability,
    /// 时间戳
    pub timestamp: u64,
    /// 消息体
    pub payload: Vec<u8>,
    /// 会话ID
    pub session_id: Option<String>,
    /// 优先级
    pub priority: u8,
    /// 压缩算法
    pub compression: Option<u8>,
    /// 加密标志
    pub encrypted: bool,
}

impl Frame {
    /// 创建新的消息帧
    pub fn new(
        message_type: MessageType,
        message_id: u64,
        reliability: Reliability,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            message_type,
            message_id,
            reliability,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            payload,
            session_id: None,
            priority: 0,
            compression: None,
            encrypted: false,
        }
    }

    /// 创建完整参数的消息帧
    pub fn new_full(
        message_type: MessageType,
        message_id: u64,
        reliability: Reliability,
        payload: Vec<u8>,
        session_id: Option<String>,
        priority: u8,
    ) -> Self {
        Self {
            message_type,
            message_id,
            reliability,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            payload,
            session_id,
            priority,
            compression: None,
            encrypted: false,
        }
    }

    /// 创建心跳帧
    pub fn heartbeat() -> Self {
        Self::new(MessageType::Heartbeat, 0, Reliability::BestEffort, vec![])
    }

    /// 创建心跳确认帧
    pub fn heartbeat_ack() -> Self {
        Self::new(MessageType::HeartbeatAck, 0, Reliability::BestEffort, vec![])
    }

    /// 创建连接帧
    pub fn connect(client_id: &str) -> Self {
        let payload = serde_json::to_vec(&serde_json::json!({
            "client_id": client_id,
            "protocol": "auto"
        })).unwrap_or_default();
        Self::new(MessageType::Connect, 0, Reliability::ExactlyOnce, payload)
    }

    /// 创建数据帧
    pub fn data(message_id: u64, data: Vec<u8>) -> Self {
        Self::new(MessageType::Data, message_id, Reliability::AtLeastOnce, data)
    }

    /// 创建错误帧
    pub fn error(code: u32, message: &str) -> Self {
        let payload = serde_json::to_vec(&serde_json::json!({
            "code": code,
            "message": message
        })).unwrap_or_default();
        Self::new(MessageType::Error, 0, Reliability::ExactlyOnce, payload)
    }

    /// 获取可靠性级别
    pub fn get_reliability(&self) -> Reliability {
        self.reliability
    }

    /// 获取消息类型
    pub fn get_message_type(&self) -> MessageType {
        self.message_type
    }

    /// 获取消息ID
    pub fn get_message_id(&self) -> u64 {
        self.message_id
    }

    /// 获取时间戳
    pub fn get_timestamp(&self) -> u64 {
        self.timestamp
    }

    /// 获取负载
    pub fn get_payload(&self) -> &[u8] {
        &self.payload
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

    /// 检查是否为心跳消息
    pub fn is_heartbeat(&self) -> bool {
        self.message_type.is_heartbeat()
    }

    /// 检查是否为控制消息
    pub fn is_control(&self) -> bool {
        self.message_type.is_control()
    }

    /// 检查是否为数据消息
    pub fn is_data(&self) -> bool {
        self.message_type.is_data()
    }

    /// 检查是否为自定义消息
    pub fn is_custom(&self) -> bool {
        matches!(self.message_type, MessageType::CustomEvent | MessageType::CustomMessage)
    }

    /// 检查是否为断开连接消息
    pub fn is_disconnect(&self) -> bool {
        self.message_type == MessageType::Disconnect
    }

    /// 检查是否为通知消息
    pub fn is_notification(&self) -> bool {
        self.message_type == MessageType::Notification
    }

    /// 检查是否为错误消息
    pub fn is_error(&self) -> bool {
        self.message_type == MessageType::Error
    }

    /// 获取通知文本
    pub fn notification_text(&self) -> Option<String> {
        if self.message_type == MessageType::Notification {
            serde_json::from_slice(&self.payload).ok()
        } else {
            None
        }
    }

    /// 获取错误信息
    pub fn get_error(&self) -> Option<(u32, String)> {
        if self.message_type == MessageType::Error {
            let data: serde_json::Value = serde_json::from_slice(&self.payload).ok()?;
            let code = data["code"].as_u64()? as u32;
            let message = data["message"].as_str()?.to_string();
            Some((code, message))
        } else {
            None
        }
    }

    /// 创建自定义消息
    pub fn custom_message(_message_type: &str, data: Vec<u8>) -> Self {
        Self::new(
            MessageType::CustomMessage,
            0,
            Reliability::AtLeastOnce,
            data,
        )
    }

    /// 创建自定义事件
    pub fn custom_event(event_name: &str) -> Self {
        let payload = serde_json::to_vec(&serde_json::json!({
            "event": event_name
        })).unwrap_or_default();
        Self::new(
            MessageType::CustomEvent,
            0,
            Reliability::BestEffort,
            payload,
        )
    }

    /// 创建REST响应
    pub fn rest_response(status: u16, data: Vec<u8>) -> Self {
        let payload = serde_json::to_vec(&serde_json::json!({
            "status": status,
            "data": data
        })).unwrap_or_default();
        Self::new(
            MessageType::Data,
            0,
            Reliability::ExactlyOnce,
            payload,
        )
    }

    /// 创建断开连接消息
    pub fn disconnect(reason: &str) -> Self {
        let payload = serde_json::to_vec(&serde_json::json!({
            "reason": reason
        })).unwrap_or_default();
        Self::new(
            MessageType::Disconnect,
            0,
            Reliability::ExactlyOnce,
            payload,
        )
    }
}



/// 协议选择模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProtocolSelection {
    /// 仅使用 QUIC
    QuicOnly,
    /// 仅使用 WebSocket
    WebSocketOnly,
    /// 自动选择（协议竞速）
    Auto,
}

impl Default for ProtocolSelection {
    fn default() -> Self {
        ProtocolSelection::Auto
    }
}

/// 连接质量指标
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionQuality {
    /// 延迟（毫秒）
    pub latency_ms: u32,
    /// 抖动（毫秒）
    pub jitter_ms: u32,
    /// 丢包率（百分比）
    pub packet_loss_percent: f32,
    /// 带宽（字节/秒）
    pub bandwidth_bps: u64,
    /// 稳定性评分（0-100）
    pub stability_score: u8,
}

impl Default for ConnectionQuality {
    fn default() -> Self {
        Self {
            latency_ms: 0,
            jitter_ms: 0,
            packet_loss_percent: 0.0,
            bandwidth_bps: 0,
            stability_score: 100,
        }
    }
}

/// 协议测试结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolTestResult {
    /// 协议类型
    pub protocol: ProtocolSelection,
    /// 连接质量
    pub quality: ConnectionQuality,
    /// 连接时间（毫秒）
    pub connection_time_ms: u32,
    /// 是否成功
    pub success: bool,
}

impl ProtocolTestResult {
    /// 计算综合评分
    pub fn calculate_score(&self) -> f32 {
        if !self.success {
            return 0.0;
        }

        let latency_score = (1000.0 / (self.quality.latency_ms as f32 + 1.0)).min(100.0);
        let stability_score = self.quality.stability_score as f32;
        let connection_score = (1000.0 / (self.connection_time_ms as f32 + 1.0)).min(100.0);

        latency_score * 0.4 + stability_score * 0.4 + connection_score * 0.2
    }
}

/// 在Serde Frame和Protobuf Frame之间转换的工具函数
impl Frame {
    /// 将Serde Frame转换为Protobuf Frame
    pub fn to_protobuf(&self) -> ProtobufFrame {
        ProtobufFrame {
            message_type: self.message_type.to_u8() as i32,
            message_id: self.message_id,
            reliability: self.reliability.to_u8() as i32,
            timestamp: self.timestamp,
            payload: self.payload.clone(),
            session_id: self.session_id.clone(),
            priority: self.priority as u32,
            compression: self.compression.map(|c| c as u32),
            encrypted: self.encrypted,
        }
    }

    /// 从Protobuf Frame创建Serde Frame
    pub fn from_protobuf(proto_frame: ProtobufFrame) -> Self {
        Self {
            message_type: MessageType::from_u8(proto_frame.message_type as u8).unwrap_or(MessageType::Heartbeat),
            message_id: proto_frame.message_id,
            reliability: Reliability::from_u8(proto_frame.reliability as u8).unwrap_or(Reliability::BestEffort),
            timestamp: proto_frame.timestamp,
            payload: proto_frame.payload,
            session_id: proto_frame.session_id,
            priority: proto_frame.priority as u8,
            compression: proto_frame.compression.map(|c| c as u8),
            encrypted: proto_frame.encrypted,
        }
    }
}
