//! FastServer事件处理器

use crate::common::connections::traits::ConnectionEvent;
use crate::common::connections::types::ConnectionStats;
use crate::common::error::FlareError;
use crate::common::protocol::frame::Frame;
use std::sync::Arc;

/// FastServer事件处理器trait
#[async_trait::async_trait]
pub trait FastServerEventHandlerTrait: Send + Sync {
    /// 连接建立时触发
    fn on_connected(&self) {}
    
    /// 连接断开时触发
    fn on_disconnected(&self, _reason: Option<String>) {}
    
    /// 发生错误时触发
    fn on_error(&self, _err: FlareError) {}
    
    /// 接收到消息时触发
    fn on_message_received(&self, _frame: Frame) {}
    
    /// 消息发送成功时触发
    fn on_message_sent(&self, _frame: Frame) {}
    
    /// 发送心跳 Ping 时触发
    fn on_heartbeat_ping(&self) {}
    
    /// 接收到心跳 Pong 时触发
    fn on_heartbeat_pong(&self, _rtt_ms: u32) {}
    
    /// 心跳超时时触发
    fn on_heartbeat_timeout(&self) {}
    
    /// 连接质量变化时触发
    fn on_quality_changed(&self, _quality: u8) {}
    
    /// 统计信息更新时触发
    fn on_statistics_updated(&self, _stats: ConnectionStats) {}
    
    /// 处理传入消息
    async fn process_incoming_message(&self, _user_id: String, _frame: Frame) -> Result<(), FlareError> {
        Ok(())
    }
}

/// FastServer事件处理器
pub struct FastServerEventHandler {
    handler: Option<Arc<dyn FastServerEventHandlerTrait>>,
}

impl FastServerEventHandler {
    pub fn new() -> Self {
        Self { handler: None }
    }
    
    pub fn with_handler(handler: Arc<dyn FastServerEventHandlerTrait>) -> Self {
        Self { handler: Some(handler) }
    }
    
    /// 设置事件处理器
    pub fn set_handler(&mut self, handler: Arc<dyn FastServerEventHandlerTrait>) {
        self.handler = Some(handler);
    }
    
    /// 移除事件处理器
    pub fn remove_handler(&mut self) {
        self.handler = None;
    }
}

impl ConnectionEvent for FastServerEventHandler {
    fn on_connected(&self) {
        if let Some(handler) = &self.handler {
            handler.on_connected();
        } else {
            tracing::info!("连接建立");
        }
    }
    
    fn on_disconnected(&self, reason: Option<String>) {
        if let Some(handler) = &self.handler {
            handler.on_disconnected(reason);
        } else {
            tracing::info!("连接断开: {:?}", reason);
        }
    }
    
    fn on_error(&self, err: FlareError) {
        if let Some(handler) = &self.handler {
            handler.on_error(err);
        } else {
            tracing::error!("发生错误: {:?}", err);
        }
    }
    
    fn on_message_received(&self, frame: Frame) {
        if let Some(handler) = &self.handler {
            handler.on_message_received(frame);
        } else {
            tracing::debug!("收到消息: {:?}", frame);
        }
    }
    
    fn on_message_sent(&self, frame: Frame) {
        if let Some(handler) = &self.handler {
            handler.on_message_sent(frame);
        } else {
            tracing::debug!("消息发送成功: {:?}", frame);
        }
    }
    
    fn on_heartbeat_ping(&self) {
        if let Some(handler) = &self.handler {
            handler.on_heartbeat_ping();
        } else {
            tracing::trace!("心跳Ping");
        }
    }
    
    fn on_heartbeat_pong(&self, rtt_ms: u32) {
        if let Some(handler) = &self.handler {
            handler.on_heartbeat_pong(rtt_ms);
        } else {
            tracing::trace!("心跳Pong，RTT: {}ms", rtt_ms);
        }
    }
    
    fn on_heartbeat_timeout(&self) {
        if let Some(handler) = &self.handler {
            handler.on_heartbeat_timeout();
        } else {
            tracing::warn!("心跳超时");
        }
    }
    
    fn on_quality_changed(&self, quality: u8) {
        if let Some(handler) = &self.handler {
            handler.on_quality_changed(quality);
        } else {
            tracing::info!("连接质量变化: {}", quality);
        }
    }
    
    fn on_statistics_updated(&self, stats: ConnectionStats) {
        if let Some(handler) = &self.handler {
            handler.on_statistics_updated(stats);
        } else {
            tracing::debug!("统计信息更新: {:?}", stats);
        }
    }
}

impl Default for FastServerEventHandler {
    fn default() -> Self {
        Self::new()
    }
}