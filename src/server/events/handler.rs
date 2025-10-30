//! 增强事件处理器

use crate::common::connections::traits::ConnectionEvent;
use crate::common::connections::types::ConnectionStats;
use crate::common::error::FlareError;
use crate::common::protocol::frame::Frame;
use std::sync::Arc;
use std::any::Any;

/// 增强事件处理器trait
pub trait EnhancedEventHandler: Send + Sync {
    /// 连接建立时触发
    fn on_connected(&self, _connection_id: String) {}
    
    /// 连接断开时触发
    fn on_disconnected(&self, _connection_id: String, _reason: Option<String>) {}
    
    /// 发生错误时触发
    fn on_error(&self, _connection_id: String, _err: FlareError) {}
    
    /// 接收到消息时触发
    fn on_message_received(&self, _connection_id: String, _frame: Frame) {}
    
    /// 消息发送成功时触发
    fn on_message_sent(&self, _connection_id: String, _frame: Frame) {}
    
    /// 发送心跳 Ping 时触发
    fn on_heartbeat_ping(&self, _connection_id: String) {}
    
    /// 接收到心跳 Pong 时触发
    fn on_heartbeat_pong(&self, _connection_id: String, _rtt_ms: u32) {}
    
    /// 心跳超时时触发
    fn on_heartbeat_timeout(&self, _connection_id: String) {}
    
    /// 连接质量变化时触发
    fn on_quality_changed(&self, _connection_id: String, _quality: u8) {}
    
    /// 统计信息更新时触发
    fn on_statistics_updated(&self, _connection_id: String, _stats: ConnectionStats) {}
}

/// 事件处理器适配器
pub struct EventHandlerAdapter {
    handler: Option<Arc<dyn EnhancedEventHandler>>,
}

impl EventHandlerAdapter {
    pub fn new() -> Self {
        Self { handler: None }
    }
    
    pub fn with_handler(handler: Arc<dyn EnhancedEventHandler>) -> Self {
        Self { handler: Some(handler) }
    }
    
    /// 设置事件处理器
    pub fn set_handler(&mut self, handler: Arc<dyn EnhancedEventHandler>) {
        self.handler = Some(handler);
    }
    
    /// 移除事件处理器
    pub fn remove_handler(&mut self) {
        self.handler = None;
    }
    
    /// 带连接ID的消息接收方法
    pub fn on_message_received_with_id(&self, connection_id: String, frame: Frame) {
        if let Some(handler) = &self.handler {
            handler.on_message_received(connection_id, frame);
        } else {
            tracing::debug!("收到消息: {:?}", frame);
        }
    }
    
    /// 带连接ID的连接建立方法
    pub fn on_connected_with_id(&self, connection_id: String) {
        if let Some(handler) = &self.handler {
            handler.on_connected(connection_id);
        } else {
            tracing::info!("连接建立");
        }
    }
    
    /// 带连接ID的连接断开方法
    pub fn on_disconnected_with_id(&self, connection_id: String, reason: Option<String>) {
        if let Some(handler) = &self.handler {
            handler.on_disconnected(connection_id, reason);
        } else {
            tracing::info!("连接断开: {:?}", reason);
        }
    }
    
    /// 带连接ID的错误处理方法
    pub fn on_error_with_id(&self, connection_id: String, err: FlareError) {
        if let Some(handler) = &self.handler {
            handler.on_error(connection_id, err);
        } else {
            tracing::error!("发生错误: {:?}", err);
        }
    }
    
    /// 带连接ID的消息发送成功方法
    pub fn on_message_sent_with_id(&self, connection_id: String, frame: Frame) {
        if let Some(handler) = &self.handler {
            handler.on_message_sent(connection_id, frame);
        } else {
            tracing::debug!("消息发送成功: {:?}", frame);
        }
    }
    
    /// 带连接ID的心跳Ping方法
    pub fn on_heartbeat_ping_with_id(&self, connection_id: String) {
        if let Some(handler) = &self.handler {
            handler.on_heartbeat_ping(connection_id);
        } else {
            tracing::trace!("心跳Ping");
        }
    }
    
    /// 带连接ID的心跳Pong方法
    pub fn on_heartbeat_pong_with_id(&self, connection_id: String, rtt_ms: u32) {
        if let Some(handler) = &self.handler {
            handler.on_heartbeat_pong(connection_id, rtt_ms);
        } else {
            tracing::trace!("心跳Pong，RTT: {}ms", rtt_ms);
        }
    }
    
    /// 带连接ID的心跳超时方法
    pub fn on_heartbeat_timeout_with_id(&self, connection_id: String) {
        if let Some(handler) = &self.handler {
            handler.on_heartbeat_timeout(connection_id);
        } else {
            tracing::warn!("心跳超时");
        }
    }
    
    /// 带连接ID的连接质量变化方法
    pub fn on_quality_changed_with_id(&self, connection_id: String, quality: u8) {
        if let Some(handler) = &self.handler {
            handler.on_quality_changed(connection_id, quality);
        } else {
            tracing::info!("连接质量变化: {}", quality);
        }
    }
    
    /// 带连接ID的统计信息更新方法
    pub fn on_statistics_updated_with_id(&self, connection_id: String, stats: ConnectionStats) {
        if let Some(handler) = &self.handler {
            handler.on_statistics_updated(connection_id, stats);
        } else {
            tracing::debug!("统计信息更新: {:?}", stats);
        }
    }
}

impl ConnectionEvent for EventHandlerAdapter {
    fn on_connected(&self) {
        // 注意：基础ConnectionEvent没有connection_id参数
        // 在实际使用中，需要通过其他方式获取连接ID
        if let Some(handler) = &self.handler {
            handler.on_connected("unknown".to_string());
        } else {
            tracing::info!("连接建立");
        }
    }
    
    fn on_disconnected(&self, reason: Option<String>) {
        if let Some(handler) = &self.handler {
            handler.on_disconnected("unknown".to_string(), reason);
        } else {
            tracing::info!("连接断开: {:?}", reason);
        }
    }
    
    fn on_error(&self, err: FlareError) {
        if let Some(handler) = &self.handler {
            handler.on_error("unknown".to_string(), err);
        } else {
            tracing::error!("发生错误: {:?}", err);
        }
    }
    
    fn on_message_received(&self, frame: Frame) {
        if let Some(handler) = &self.handler {
            handler.on_message_received("unknown".to_string(), frame);
        } else {
            tracing::debug!("收到消息: {:?}", frame);
        }
    }
    
    fn on_message_sent(&self, frame: Frame) {
        if let Some(handler) = &self.handler {
            handler.on_message_sent("unknown".to_string(), frame);
        } else {
            tracing::debug!("消息发送成功: {:?}", frame);
        }
    }
    
    fn on_heartbeat_ping(&self) {
        if let Some(handler) = &self.handler {
            handler.on_heartbeat_ping("unknown".to_string());
        } else {
            tracing::trace!("心跳Ping");
        }
    }
    
    fn on_heartbeat_pong(&self, rtt_ms: u32) {
        if let Some(handler) = &self.handler {
            handler.on_heartbeat_pong("unknown".to_string(), rtt_ms);
        } else {
            tracing::trace!("心跳Pong，RTT: {}ms", rtt_ms);
        }
    }
    
    fn on_heartbeat_timeout(&self) {
        if let Some(handler) = &self.handler {
            handler.on_heartbeat_timeout("unknown".to_string());
        } else {
            tracing::warn!("心跳超时");
        }
    }
    
    fn on_quality_changed(&self, quality: u8) {
        if let Some(handler) = &self.handler {
            handler.on_quality_changed("unknown".to_string(), quality);
        } else {
            tracing::info!("连接质量变化: {}", quality);
        }
    }
    
    fn on_statistics_updated(&self, stats: ConnectionStats) {
        if let Some(handler) = &self.handler {
            handler.on_statistics_updated("unknown".to_string(), stats);
        } else {
            tracing::debug!("统计信息更新: {:?}", stats);
        }
    }
    
    /// 为类型转换提供支持
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Default for EventHandlerAdapter {
    fn default() -> Self {
        Self::new()
    }
}