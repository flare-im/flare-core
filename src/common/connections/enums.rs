//! 连接相关枚举类型定义

use serde::{Deserialize, Serialize};

/// 连接状态枚举
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConnectionState {
    /// 初始化中
    Initializing,
    /// 已就绪
    Ready,
    /// 连接已建立
    Connected,
    /// 连接已断开
    Disconnected,
    /// 连接错误
    Error,
    /// 连接失败
    Failed,
    /// 重连中
    Reconnecting,
    /// 等待认证
    WaitingAuth,
    /// 认证失败
    AuthFailed,
    /// 认证成功
    Authenticated,
    /// 自定义状态
    Custom(String),
}

/// 传输协议类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Transport {
    /// WebSocket协议
    WebSocket,
    /// QUIC协议
    Quic,
}