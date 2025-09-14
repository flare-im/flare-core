//! 连接枚举定义
//! 
//! 定义连接相关的枚举类型

use serde::{Deserialize, Serialize};
use std::fmt;

/// Transport  连接类型
#[derive(Debug, Clone,Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Transport {
    /// WebSocket
    WebSocket,
    /// QUIC
    Quic,
    /// TCP
    Tcp,
    /// UDP
    Udp,
}

/// 用户平台信息
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Platform {
    /// iOS平台
    IOS,
    /// Android平台
    Android,
    /// Web平台
    Web,
    /// Windows桌面
    Windows,
    /// macOS桌面
    MacOS,
    /// Linux桌面
    Linux,
    /// 其他平台
    Other(String),
}

impl Platform {
    /// 从字符串创建平台类型
    pub fn from_str(platform: &str) -> Self {
        match platform.to_lowercase().as_str() {
            "ios" => Platform::IOS,
            "android" => Platform::Android,
            "web" => Platform::Web,
            "windows" => Platform::Windows,
            "macos" => Platform::MacOS,
            "linux" => Platform::Linux,
            _ => Platform::Other(platform.to_string()),
        }
    }
    
    /// 转换为字符串
    pub fn as_str(&self) -> &str {
        match self {
            Platform::IOS => "ios",
            Platform::Android => "android",
            Platform::Web => "web",
            Platform::Windows => "windows",
            Platform::MacOS => "macos",
            Platform::Linux => "linux",
            Platform::Other(s) => s.as_str(),
        }
    }
}

impl fmt::Display for Platform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Platform::IOS => write!(f, "iOS"),
            Platform::Android => write!(f, "Android"),
            Platform::Web => write!(f, "Web"),
            Platform::Windows => write!(f, "Windows"),
            Platform::MacOS => write!(f, "macOS"),
            Platform::Linux => write!(f, "Linux"),
            Platform::Other(s) => write!(f, "{}", s),
        }
    }
}


/// 连接角色
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionRole {
    Client,
    Server,
}

/// 连接状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionState {
    /// 初始化
    Initializing,
    /// 连接中
    Connecting,
    /// 已连接
    Connected,
    /// 就绪（可以开始通信）
    Ready,
    /// 断开中
    Disconnecting,
    /// 已断开
    Disconnected,
    /// 连接失败
    Failed,
    /// 重连中
    Reconnecting,
    /// 错误状态
    Error,
}

/// 协议特性
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolFeature {
    /// 支持双向通信
    Bidirectional,
    /// 支持流式传输
    Streaming,
    /// 支持可靠传输
    Reliable,
    /// 支持有序传输
    Ordered,
    /// 支持 TLS 加密
    Tls,
    /// 支持心跳
    Heartbeat,
    /// 支持重连
    Reconnection,
}

/// 连接质量等级
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ConnectionQuality {
    /// 优秀 (90-100)
    Excellent = 100,
    /// 良好 (80-89)
    Good = 89,
    /// 一般 (70-79)
    Fair = 79,
    /// 较差 (60-69)
    Poor = 69,
    /// 很差 (0-59)
    VeryPoor = 59,
}

impl From<u8> for ConnectionQuality {
    fn from(score: u8) -> Self {
        match score {
            90..=100 => ConnectionQuality::Excellent,
            80..=89 => ConnectionQuality::Good,
            70..=79 => ConnectionQuality::Fair,
            60..=69 => ConnectionQuality::Poor,
            _ => ConnectionQuality::VeryPoor,
        }
    }
}

impl ConnectionQuality {
    /// 获取质量描述
    pub fn description(&self) -> &'static str {
        match self {
            ConnectionQuality::Excellent => "优秀",
            ConnectionQuality::Good => "良好",
            ConnectionQuality::Fair => "一般",
            ConnectionQuality::Poor => "较差",
            ConnectionQuality::VeryPoor => "很差",
        }
    }
    
    /// 获取质量评分
    pub fn score(&self) -> u8 {
        *self as u8
    }
}