//! 客户端事件处理
//!
//! 定义客户端事件处理相关的 trait 与默认实现
//! 设计理念：提供客户端特有的高级事件回调，基础连接事件由 Client 内部处理

use async_trait::async_trait;

use crate::common::{
    protocol::{
        commands::{ControlCmd, MessageCmd, NotificationCmd, EventCmd}
    },
    connections::{
        traits::ConnectionStats,
    },
};

/// 客户端事件处理器
/// 
/// 提供客户端特有的高级事件回调
/// 基础连接事件（连接、断开、错误等）由 Client 内部处理
/// 用户只需要关注业务相关的消息处理
#[async_trait::async_trait]
pub trait ClientEvent: Send + Sync {
    /// 处理控制消息
    /// 
    /// 当收到控制消息时调用，包括认证、心跳等控制命令
    async fn on_control_command(&self, cmd: &ControlCmd);

    /// 处理业务消息
    /// 
    /// 当收到业务消息时调用，用于处理应用层的消息通信
    async fn on_message_command(&self, message: &MessageCmd);

    /// 处理通知消息
    /// 
    /// 当收到通知消息时调用，用于处理系统通知
    async fn on_notification_command(&self, notification: &NotificationCmd);

    /// 处理事件消息
    /// 
    /// 当收到事件消息时调用，用于处理系统事件
    async fn on_event_command(&self, event: &EventCmd);
    
    /// 连接建立完成
    /// 
    /// 当连接成功建立并准备就绪时调用
    async fn on_connected(&self, connection_id: &str);
    
    /// 连接断开
    /// 
    /// 当连接断开时调用，包括正常断开和异常断开
    async fn on_disconnected(&self, connection_id: &str, reason: &str);
    
    /// 连接错误
    /// 
    /// 当连接发生错误时调用
    async fn on_error(&self, connection_id: &str, error: &str);
    
    /// 连接质量变化
    /// 
    /// 当连接质量发生变化时调用
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8);
    
    /// 统计信息更新
    /// 
    /// 当连接统计信息更新时调用
    async fn on_statistics_updated(&self, connection_id: &str, stats: &ConnectionStats);
    
    /// 重连开始
    /// 
    /// 当开始重连时调用，用户可以决定是否允许重连
    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) -> bool;
    
    /// 重连成功
    /// 
    /// 当重连成功时调用
    async fn on_reconnected(&self, connection_id: &str, attempt: u32);
    
    /// 重连失败
    /// 
    /// 当重连失败时调用，用户可以决定是否继续重连
    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) -> bool;
    
    /// 协议切换
    /// 
    /// 当协议切换时调用，告知用户当前使用的协议
    async fn on_protocol_switched(&self, connection_id: &str, from_protocol: &str, to_protocol: &str);
    
    /// 心跳超时
    /// 
    /// 当心跳超时时调用，用户可以决定如何处理
    async fn on_heartbeat_timeout(&self, connection_id: &str) -> bool;
    
    /// 心跳ping
    /// 
    /// 当收到心跳ping时调用
    async fn on_heartbeat_ping(&self, connection_id: &str);
    
    /// 心跳pong
    /// 
    /// 当收到心跳pong时调用
    async fn on_heartbeat_pong(&self, connection_id: &str);
}

/// 默认客户端事件处理器
#[derive(Debug)]
pub struct DefClientEventHandler;

#[async_trait]
impl ClientEvent for DefClientEventHandler {
    async fn on_control_command(&self, cmd: &ControlCmd) {
        tracing::info!("收到控制消息: {}", cmd.as_str());
    }

    async fn on_message_command(&self, message: &MessageCmd) {
        tracing::info!("收到业务消息: {}", message.as_str());
    }

    async fn on_notification_command(&self, notification: &NotificationCmd) {
        tracing::info!("收到通知消息: {}", notification.as_str());
    }

    async fn on_event_command(&self, event: &EventCmd) {
        tracing::info!("收到事件消息: {}", event.as_str());
    }
    
    async fn on_connected(&self, connection_id: &str) {
        tracing::info!("客户端连接已建立: {}", connection_id);
    }
    
    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        tracing::info!("客户端连接已断开: {} - 原因: {}", connection_id, reason);
    }
    
    async fn on_error(&self, connection_id: &str, error: &str) {
        tracing::error!("客户端连接错误: {} - 错误: {}", connection_id, error);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        tracing::info!("连接质量变化: {} - 评分: {}", connection_id, quality_score);
    }
    
    async fn on_statistics_updated(&self, connection_id: &str, stats: &ConnectionStats) {
        tracing::debug!("统计信息更新: {} - 收到: {} - 发送: {} - 质量: {}", 
                       connection_id, stats.messages_received, stats.messages_sent, stats.quality_score);
    }
    
    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) -> bool {
        tracing::info!("重连开始: {} - 尝试次数: {}", connection_id, attempt);
        true // 默认允许重连
    }
    
    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        tracing::info!("重连成功: {} - 尝试次数: {}", connection_id, attempt);
    }
    
    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) -> bool {
        tracing::warn!("重连失败: {} - 尝试次数: {} - 错误: {}", connection_id, attempt, error);
        attempt < 5 // 默认最多重连5次
    }
    
    async fn on_protocol_switched(&self, connection_id: &str, from_protocol: &str, to_protocol: &str) {
        tracing::info!("协议切换: {} - 从 {} 切换到 {}", connection_id, from_protocol, to_protocol);
    }
    
    async fn on_heartbeat_timeout(&self, connection_id: &str) -> bool {
        tracing::warn!("心跳超时: {}", connection_id);
        true // 默认允许重连
    }
    
    async fn on_heartbeat_ping(&self, connection_id: &str) {
        tracing::debug!("收到心跳ping: {}", connection_id);
    }
    
    async fn on_heartbeat_pong(&self, connection_id: &str) {
        tracing::debug!("收到心跳pong: {}", connection_id);
    }
}

impl Default for DefClientEventHandler {
    fn default() -> Self {
        Self
    }
}