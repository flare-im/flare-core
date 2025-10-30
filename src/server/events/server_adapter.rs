//! 服务端事件适配器
//!
//! 提供服务端专用的事件适配器实现

use crate::common::connections::traits::ConnectionEvent;
use crate::common::connections::types::ConnectionStats;
use crate::common::error::FlareError;
use crate::common::protocol::frame::Frame;
use std::sync::Arc;

/// 服务端事件适配器
/// 
/// 为服务端提供默认的事件处理实现，所有方法默认为空实现
/// 服务端可以根据需要覆盖特定方法
pub struct ServerEventAdapter {
    /// FastServer事件处理器
    pub(crate) fast_server_handler: Option<Arc<dyn crate::server::fast::event_handler::FastServerEventHandlerTrait>>,
}

impl ServerEventAdapter {
    pub fn new() -> Self {
        Self {
            fast_server_handler: None,
        }
    }
    
    /// 设置FastServer事件处理器
    pub fn set_fast_server_handler(&mut self, handler: Arc<dyn crate::server::fast::event_handler::FastServerEventHandlerTrait>) {
        self.fast_server_handler = Some(handler);
    }
}

impl Default for ServerEventAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionEvent for ServerEventAdapter {
    fn on_connected(&self) {
        if let Some(handler) = &self.fast_server_handler {
            handler.on_connected();
        }
    }
    
    fn on_disconnected(&self, reason: Option<String>) {
        if let Some(handler) = &self.fast_server_handler {
            handler.on_disconnected(reason);
        }
    }
    
    fn on_error(&self, err: FlareError) {
        if let Some(handler) = &self.fast_server_handler {
            handler.on_error(err);
        }
    }
    
    fn on_message_received(&self, frame: Frame) {
        if let Some(handler) = &self.fast_server_handler {
            handler.on_message_received(frame);
        }
    }
    
    fn on_message_sent(&self, frame: Frame) {
        if let Some(handler) = &self.fast_server_handler {
            handler.on_message_sent(frame);
        }
    }
    
    fn on_heartbeat_ping(&self) {
        if let Some(handler) = &self.fast_server_handler {
            handler.on_heartbeat_ping();
        }
    }
    
    fn on_heartbeat_pong(&self, rtt_ms: u32) {
        if let Some(handler) = &self.fast_server_handler {
            handler.on_heartbeat_pong(rtt_ms);
        }
    }
    
    fn on_heartbeat_timeout(&self) {
        if let Some(handler) = &self.fast_server_handler {
            handler.on_heartbeat_timeout();
        }
    }
    
    fn on_quality_changed(&self, quality: u8) {
        if let Some(handler) = &self.fast_server_handler {
            handler.on_quality_changed(quality);
        }
    }
    
    fn on_statistics_updated(&self, stats: ConnectionStats) {
        if let Some(handler) = &self.fast_server_handler {
            handler.on_statistics_updated(stats);
        }
    }
}