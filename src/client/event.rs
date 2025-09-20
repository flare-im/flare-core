//! 客户端连接事件处理
//!
//! 定义客户端连接事件处理相关的 trait 与默认实现
//! 设计理念：扩展基础连接事件，提供客户端特有的事件回调

use async_trait::async_trait;

use crate::common::{
    protocol::{
        Frame,
        commands::{ControlCmd, MessageCmd, NotificationCmd, EventCmd}
    },
    connections::{
        traits::ConnectionStats,
        event::ConnectionEvent,
    },
};

/// 客户端连接事件处理器
/// 
/// 扩展基础连接事件，提供客户端特有的事件回调
/// 设计目标：在基础连接事件基础上，增加客户端功能所需的事件回调
#[async_trait::async_trait]
pub trait ClientEvent: ConnectionEvent + Send + Sync {
    /// 处理控制消息
    async fn on_control_command(&self, cmd: &ControlCmd);

    /// 处理消息
    async fn on_message_command(&self, message: &MessageCmd);

    /// 处理通知
    async fn on_notification_command(&self, notification: &NotificationCmd);

    /// 处理事件
    async fn on_event_command(&self, event: &EventCmd);
    
    /// 认证成功
    async fn on_authenticated(&self);
    
    /// 认证失败
    async fn on_authentication_failed(&self, error: &str);
}

/// 默认客户端连接事件处理器
#[derive(Debug)]
pub struct DefClientEventHandler;

#[async_trait]
impl ClientEvent for DefClientEventHandler {
    async fn on_control_command(&self, cmd: &ControlCmd) {
        tracing::info!("客户端: 收到控制消息 - 类型: {}", cmd.as_str());
    }

    async fn on_message_command(&self, message: &MessageCmd) {
        tracing::info!("客户端: 收到消息 - 类型: {}", message.as_str());
    }

    async fn on_notification_command(&self, notification: &NotificationCmd) {
        tracing::info!("客户端: 收到通知 - 类型: {}", notification.as_str());
    }

    async fn on_event_command(&self, event: &EventCmd) {
        tracing::info!("客户端: 收到事件 - 类型: {}", event.as_str());
    }
    
    async fn on_authenticated(&self) {
        tracing::info!("客户端: 认证成功");
    }
    
    async fn on_authentication_failed(&self, error: &str) {
        tracing::info!("客户端: 认证失败 - 错误: {}", error);
    }
}

#[async_trait]
impl ConnectionEvent for DefClientEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        tracing::info!("客户端: 连接已建立: {}", connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        tracing::info!("客户端: 连接已断开: {} - 原因: {}", connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        tracing::info!("客户端: 连接错误: {} - 错误: {}", connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        tracing::info!("客户端: 收到消息: {} - 类型: {}", connection_id, message.get_command_type_str());
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        tracing::info!("客户端: 发送消息: {} - 类型: {}", connection_id, message.get_command_type_str());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        tracing::info!("客户端: 心跳超时: {}", connection_id);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        tracing::info!("客户端: 收到心跳的ping: {}", connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        tracing::info!("客户端: 收到心跳的pong: {}", connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        tracing::info!("客户端: 连接质量变化: {} - 评分: {}", connection_id, quality_score);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        tracing::info!("客户端: 开始重连: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        tracing::info!("客户端: 重连成功: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        tracing::info!("客户端: 重连失败: {} - 尝试次数: {} - 错误: {}", connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &ConnectionStats) {
        tracing::info!("客户端: 统计信息更新: {} - 收到: {} - 发送: {} - 质量: {}", 
                     connection_id, stats.messages_received, stats.messages_sent, stats.quality_score);
    }
}

impl Default for DefClientEventHandler {
    fn default() -> Self {
        Self
    }
}