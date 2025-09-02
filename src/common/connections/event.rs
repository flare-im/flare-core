//! 连接事件处理
//!
//! 定义连接事件处理相关的 trait 与默认实现
//! 设计理念：简洁、易用、稳定

use async_trait::async_trait;

use crate::common::protocol::Frame;

/// 连接事件处理器
/// 
/// 处理连接生命周期中的各种事件
/// 设计目标：简洁易用，提供核心必需的事件回调
#[async_trait]
pub trait ConnectionEvent: Send + Sync {
    /// 连接建立事件
    async fn on_connected(&self, connection_id: &str);
    
    /// 连接断开事件
    async fn on_disconnected(&self, connection_id: &str, reason: &str);
    
    /// 连接错误事件
    async fn on_error(&self, connection_id: &str, error: &str);
    
    /// 消息接收事件
    async fn on_message_received(&self, connection_id: &str, message: &Frame);
    
    /// 消息发送事件
    async fn on_message_sent(&self, connection_id: &str, message: &Frame);
    
    /// 心跳超时事件
    async fn on_heartbeat_timeout(&self, connection_id: &str);
    
    /// 心跳发送事件
    async fn on_heartbeat_sent(&self, connection_id: &str);
    
    /// 心跳接收事件
    async fn on_heartbeat_received(&self, connection_id: &str);
    
    /// 连接质量变化事件
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8);
    
    /// 重连开始事件
    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32);
    
    /// 重连成功事件
    async fn on_reconnected(&self, connection_id: &str, attempt: u32);
    
    /// 重连失败事件
    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str);
    
    /// 连接统计更新事件
    async fn on_statistics_updated(&self, connection_id: &str, stats: &crate::common::connections::traits::ConnectionStats);
}

/// 默认连接事件处理器
/// 
/// 提供基本的日志记录功能，只打印info级别日志
/// 设计目标：简洁易用，适合生产环境使用
pub struct DefConnectionEventHandler;

#[async_trait]
impl ConnectionEvent for DefConnectionEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        tracing::info!("连接已建立: {}", connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        tracing::info!("连接已断开: {} - 原因: {}", connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        tracing::info!("连接错误: {} - 错误: {}", connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        tracing::info!("收到消息: {} - 类型: {:?}", connection_id, message.get_message_type());
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        tracing::info!("发送消息: {} - 类型: {:?}", connection_id, message.get_message_type());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        tracing::info!("心跳超时: {}", connection_id);
    }

    async fn on_heartbeat_sent(&self, connection_id: &str) {
        tracing::info!("心跳已发送: {}", connection_id);
    }

    async fn on_heartbeat_received(&self, connection_id: &str) {
        tracing::info!("收到心跳: {}", connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        tracing::info!("连接质量变化: {} - 评分: {}", connection_id, quality_score);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        tracing::info!("开始重连: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        tracing::info!("重连成功: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        tracing::info!("重连失败: {} - 尝试次数: {} - 错误: {}", connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &crate::common::connections::traits::ConnectionStats) {
        tracing::info!("统计信息更新: {} - 收到: {} - 发送: {} - 质量: {}", 
                     connection_id, stats.messages_received, stats.messages_sent, stats.quality_score);
    }
}

impl Default for DefConnectionEventHandler {
    fn default() -> Self {
        Self
    }
}
