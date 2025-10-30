//! FastClient 事件处理
//!
//! 定义 FastClient 特有的事件处理 trait，提供更丰富的事件回调

use async_trait::async_trait;

use crate::common::{
    protocol::{
        commands::{ControlCmd, MessageCmd, NotificationCmd, EventCmd}
    },
    connections::{
        traits::ConnectionStats,
    },
};

/// FastClient 事件处理器
/// 
/// 提供 FastClient 特有的高级事件回调
/// 包括认证、心跳、重连等高级功能的事件处理
/// 基础连接事件由 FastClient 内部处理，用户只需要关注业务相关的消息处理
#[async_trait::async_trait]
pub trait FastEvent: Send + Sync {
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
    
    /// 心跳发送成功
    async fn on_heartbeat_sent(&self);
    
    /// 心跳发送失败
    async fn on_heartbeat_failed(&self, error: &str);
    
    /// 开始自动重连
    async fn on_auto_reconnect_started(&self, attempt: u32);
    
    /// 自动重连成功
    async fn on_auto_reconnect_success(&self, attempt: u32);
    
    /// 自动重连失败
    async fn on_auto_reconnect_failed(&self, attempt: u32, error: &str);
    
    /// 连接质量监控
    async fn on_connection_quality_monitored(&self, quality_score: u8, latency_ms: u64);
    
    /// 连接状态变化
    async fn on_connection_state_changed(&self, old_state: &str, new_state: &str);
    
    /// 协议切换
    async fn on_protocol_switched(&self, connection_id: &str, from_protocol: &str, to_protocol: &str);
    
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

/// 默认 FastClient 事件处理器
#[derive(Debug)]
pub struct DefFastEventHandler;

#[async_trait]
impl FastEvent for DefFastEventHandler {
    async fn on_control_command(&self, cmd: &ControlCmd) {
        tracing::info!("FastClient: 收到控制消息 - 类型: {}", cmd.as_str());
    }

    async fn on_message_command(&self, message: &MessageCmd) {
        tracing::info!("FastClient: 收到消息 - 类型: {}", message.as_str());
    }

    async fn on_notification_command(&self, notification: &NotificationCmd) {
        tracing::info!("FastClient: 收到通知 - 类型: {}", notification.as_str());
    }

    async fn on_event_command(&self, event: &EventCmd) {
        tracing::info!("FastClient: 收到事件 - 类型: {}", event.as_str());
    }
    
    async fn on_authenticated(&self) {
        tracing::info!("FastClient: 认证成功");
    }
    
    async fn on_authentication_failed(&self, error: &str) {
        tracing::info!("FastClient: 认证失败 - 错误: {}", error);
    }
    
    async fn on_heartbeat_sent(&self) {
        tracing::debug!("FastClient: 心跳发送成功");
    }
    
    async fn on_heartbeat_failed(&self, error: &str) {
        tracing::warn!("FastClient: 心跳发送失败 - 错误: {}", error);
    }
    
    async fn on_auto_reconnect_started(&self, attempt: u32) {
        tracing::info!("FastClient: 开始自动重连 - 尝试次数: {}", attempt);
    }
    
    async fn on_auto_reconnect_success(&self, attempt: u32) {
        tracing::info!("FastClient: 自动重连成功 - 尝试次数: {}", attempt);
    }
    
    async fn on_auto_reconnect_failed(&self, attempt: u32, error: &str) {
        tracing::error!("FastClient: 自动重连失败 - 尝试次数: {} - 错误: {}", attempt, error);
    }
    
    async fn on_connection_quality_monitored(&self, quality_score: u8, latency_ms: u64) {
        tracing::debug!("FastClient: 连接质量监控 - 评分: {} - 延迟: {}ms", quality_score, latency_ms);
    }
    
    async fn on_connection_state_changed(&self, old_state: &str, new_state: &str) {
        tracing::info!("FastClient: 连接状态变化 - {} -> {}", old_state, new_state);
    }
    
    async fn on_protocol_switched(&self, connection_id: &str, from_protocol: &str, to_protocol: &str) {
        tracing::info!("FastClient: 协议切换: {} - 从 {} 切换到 {}", connection_id, from_protocol, to_protocol);
    }
    
    async fn on_connected(&self, connection_id: &str) {
        tracing::info!("FastClient: 连接已建立: {}", connection_id);
    }
    
    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        tracing::info!("FastClient: 连接已断开: {} - 原因: {}", connection_id, reason);
    }
    
    async fn on_error(&self, connection_id: &str, error: &str) {
        tracing::error!("FastClient: 连接错误: {} - 错误: {}", connection_id, error);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        tracing::info!("FastClient: 连接质量变化: {} - 评分: {}", connection_id, quality_score);
    }
    
    async fn on_statistics_updated(&self, connection_id: &str, stats: &ConnectionStats) {
        tracing::debug!("FastClient: 统计信息更新: {} - 收到: {} - 发送: {} - 质量: {}", 
                       connection_id, stats.messages_received, stats.messages_sent, stats.quality_score);
    }
    
    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) -> bool {
        tracing::info!("FastClient: 开始重连: {} - 尝试次数: {}", connection_id, attempt);
        true // 默认允许重连
    }
    
    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        tracing::info!("FastClient: 重连成功: {} - 尝试次数: {}", connection_id, attempt);
    }
    
    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) -> bool {
        tracing::warn!("FastClient: 重连失败: {} - 尝试次数: {} - 错误: {}", connection_id, attempt, error);
        attempt < 5 // 默认最多重连5次
    }
    
    async fn on_heartbeat_timeout(&self, connection_id: &str) -> bool {
        tracing::warn!("FastClient: 心跳超时: {}", connection_id);
        true // 默认允许重连
    }
    
    async fn on_heartbeat_ping(&self, connection_id: &str) {
        tracing::debug!("FastClient: 收到心跳ping: {}", connection_id);
    }
    
    async fn on_heartbeat_pong(&self, connection_id: &str) {
        tracing::debug!("FastClient: 收到心跳pong: {}", connection_id);
    }
}


impl Default for DefFastEventHandler {
    fn default() -> Self {
        Self
    }
}
