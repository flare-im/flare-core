//! 连接事件处理
//!
//! 定义连接事件处理相关的 trait 与默认实现

use async_trait::async_trait;

use crate::common::protocol::UnifiedProtocolMessage;

/// 连接事件处理器
/// 
/// 处理连接生命周期中的各种事件
#[async_trait]
pub trait ConnectionEventHandler: Send + Sync {
    /// 连接建立事件
    async fn on_connected(&self, connection_id: &str);
    
    /// 连接断开事件
    async fn on_disconnected(&self, connection_id: &str, reason: &str);
    
    /// 连接错误事件
    async fn on_error(&self, connection_id: &str, error: &str);
    
    /// 消息接收事件
    async fn on_message_received(&self, connection_id: &str, message: &UnifiedProtocolMessage);
    
    /// 心跳超时事件
    async fn on_heartbeat_timeout(&self, connection_id: &str);
    
    /// 连接质量变化事件
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8);
}

/// 默认连接事件处理器
/// 
/// 提供基本的日志记录功能
pub struct DefaultConnectionEventHandler;

#[async_trait]
impl ConnectionEventHandler for DefaultConnectionEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        tracing::info!("连接已建立: {}", connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        tracing::info!("连接已断开: {} - 原因: {}", connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        tracing::error!("连接错误: {} - 错误: {}", connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &UnifiedProtocolMessage) {
        tracing::debug!("收到消息: {} - 类型: {:?}", connection_id, message.get_message_type());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        tracing::warn!("心跳超时: {}", connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        tracing::info!("连接质量变化: {} - 评分: {}", connection_id, quality_score);
    }
}

impl Default for DefaultConnectionEventHandler {
    fn default() -> Self {
        Self
    }
}


