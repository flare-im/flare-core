use std::sync::Arc;
use async_trait::async_trait;
use crate::{ConnectionEvent, Frame, MessageType};
use crate::common::connections::traits::ConnectionStats;
use crate::server::ServerEvent;

/// 连接事件处理器
pub struct ConnectionEventHandler {
    /// 服务端事件处理器
    server_event_handler: Arc<dyn ServerEvent>,
}

impl ConnectionEventHandler {
    /// 创建新的连接事件处理器
    pub fn new(server_event_handler: Arc<dyn ServerEvent>) -> Self {
        Self {
            server_event_handler,
        }
    }
    
    /// 获取内部的服务端事件处理器
    pub fn get_server_event_handler(&self) -> Arc<dyn ServerEvent> {
        self.server_event_handler.clone()
    }
    /// 消息处理器
    async fn message_handler(&self, connection_id: &str, message: &Frame) {
        match message.message_type {
            MessageType::Data => {
               // 处理数据消息
            }
            MessageType::Message => {
                // 处理用户消息
            }
            MessageType::CustomMessage => {
                // 处理命令消息
            }
            _ => {
                // 处理其他消息类型
            }
        }
    }
}

/// 连接事件处理器实现
#[async_trait]
impl ConnectionEvent for ConnectionEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        self.server_event_handler.on_connected(connection_id).await;
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        self.server_event_handler.on_disconnected(connection_id, reason).await;
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        self.server_event_handler.on_error(connection_id, error).await;
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        self.message_handler(connection_id, message).await;
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        self.server_event_handler.on_message_sent(connection_id, message).await;
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        self.server_event_handler.on_heartbeat_timeout(connection_id).await;
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        self.server_event_handler.on_heartbeat_ping(connection_id).await;
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        self.server_event_handler.on_heartbeat_pong(connection_id).await;
    }

    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        self.server_event_handler.on_quality_changed(connection_id, quality_score).await;
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        self.server_event_handler.on_reconnect_started(connection_id, attempt).await;
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        self.server_event_handler.on_reconnected(connection_id, attempt).await;
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        self.server_event_handler.on_reconnect_failed(connection_id, attempt, error).await;
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &ConnectionStats) {
        self.server_event_handler.on_statistics_updated(connection_id, stats).await;
    }
}
