//! 事件路由器

use crate::common::connections::traits::ConnectionEvent;
use crate::common::connections::types::ConnectionStats;
use crate::common::error::FlareError;
use crate::common::protocol::frame::Frame;
use std::sync::Arc;
use dashmap::DashMap;

/// 事件路由规则
pub enum EventRouteRule {
    /// 按连接ID路由
    ByConnectionId(String),
    /// 按事件类型路由
    ByEventType(String),
    /// 自定义路由规则
    Custom(Box<dyn Fn(&str) -> bool + Send + Sync>),
}

/// 事件路由器
pub struct EventRouter {
    /// 事件处理器映射
    handlers: DashMap<String, Arc<dyn ConnectionEvent>>,
    /// 路由规则
    routes: DashMap<String, EventRouteRule>,
}

impl EventRouter {
    pub fn new() -> Self {
        Self {
            handlers: DashMap::new(),
            routes: DashMap::new(),
        }
    }
    
    /// 注册事件处理器
    pub fn register_handler(&self, id: String, handler: Arc<dyn ConnectionEvent>) {
        self.handlers.insert(id, handler);
    }
    
    /// 移除事件处理器
    pub fn unregister_handler(&self, id: &str) {
        self.handlers.remove(id);
    }
    
    /// 添加路由规则
    pub fn add_route(&self, id: String, rule: EventRouteRule) {
        self.routes.insert(id, rule);
    }
    
    /// 移除路由规则
    pub fn remove_route(&self, id: &str) {
        self.routes.remove(id);
    }
    
    /// 根据连接ID获取事件处理器
    pub fn get_handler_by_connection_id(&self, connection_id: &str) -> Option<Arc<dyn ConnectionEvent>> {
        self.handlers.get(connection_id).map(|h| h.clone())
    }
}

impl ConnectionEvent for EventRouter {
    fn on_connected(&self) {
        // 广播到所有处理器
        for handler in self.handlers.iter() {
            handler.on_connected();
        }
    }
    
    fn on_disconnected(&self, reason: Option<String>) {
        // 广播到所有处理器
        for handler in self.handlers.iter() {
            handler.on_disconnected(reason.clone());
        }
    }
    
    fn on_error(&self, err: FlareError) {
        // 广播到所有处理器
        for handler in self.handlers.iter() {
            handler.on_error(err.clone());
        }
    }
    
    fn on_message_received(&self, frame: Frame) {
        // 广播到所有处理器
        for handler in self.handlers.iter() {
            handler.on_message_received(frame.clone());
        }
    }
    
    fn on_message_sent(&self, frame: Frame) {
        // 广播到所有处理器
        for handler in self.handlers.iter() {
            handler.on_message_sent(frame.clone());
        }
    }
    
    fn on_heartbeat_ping(&self) {
        // 广播到所有处理器
        for handler in self.handlers.iter() {
            handler.on_heartbeat_ping();
        }
    }
    
    fn on_heartbeat_pong(&self, rtt_ms: u32) {
        // 广播到所有处理器
        for handler in self.handlers.iter() {
            handler.on_heartbeat_pong(rtt_ms);
        }
    }
    
    fn on_heartbeat_timeout(&self) {
        // 广播到所有处理器
        for handler in self.handlers.iter() {
            handler.on_heartbeat_timeout();
        }
    }
    
    fn on_quality_changed(&self, quality: u8) {
        // 广播到所有处理器
        for handler in self.handlers.iter() {
            handler.on_quality_changed(quality);
        }
    }
    
    fn on_statistics_updated(&self, stats: ConnectionStats) {
        // 广播到所有处理器
        for handler in self.handlers.iter() {
            handler.on_statistics_updated(stats.clone());
        }
    }
}

impl Default for EventRouter {
    fn default() -> Self {
        Self::new()
    }
}