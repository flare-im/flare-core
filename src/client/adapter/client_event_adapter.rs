//! 客户端事件适配器
//!
//! 专门用于适配客户端事件处理的模块，未来可扩展以适配其他客户端功能

use std::sync::Arc;
use async_trait::async_trait;

use crate::{
    common::{
        connections::{
            event::ConnectionEvent,
            traits::ConnectionStats,
        },
        protocol::{
            Frame,
            commands::{Command, ControlCmd, MessageCmd, NotificationCmd, EventCmd},
        },
    },
    client::ClientEvent,
};

/// 客户端事件适配器
/// 
/// 专门用于适配客户端事件处理的模块，未来可扩展以适配其他客户端功能
pub struct ClientEventAdapter {
    /// 客户端事件处理器
    pub(crate) client_event_handler: Arc<dyn ClientEvent>,
}

impl ClientEventAdapter {
    /// 创建新的客户端事件适配器
    pub fn new(client_event_handler: Arc<dyn ClientEvent>) -> Self {
        Self {
            client_event_handler,
        }
    }
    
    /// 获取内部的客户端事件处理器
    pub fn get_client_event_handler(&self) -> Arc<dyn ClientEvent> {
        self.client_event_handler.clone()
    }
    
    /// 消息处理器
    pub async fn handle_message(&self, message: &Frame) {
        // 根据消息中的命令类型进行处理
        match &message.command {
            Command::Control(control_cmd) => {
                self.client_event_handler.on_control_command(control_cmd).await;
            },
            Command::Message(message_cmd) => {
                self.client_event_handler.on_message_command(message_cmd).await;
            },
            Command::Notification(notification_cmd) => {
               self.client_event_handler.on_notification_command(notification_cmd).await;
            },
            Command::Event(event_cmd) => {
              self.client_event_handler.on_event_command(event_cmd).await;
            }
        }
    }
}

/// 连接事件处理器实现
#[async_trait]
impl ConnectionEvent for ClientEventAdapter {
    async fn on_connected(&self, connection_id: &str) {
        self.client_event_handler.on_connected(connection_id).await;
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        self.client_event_handler.on_disconnected(connection_id, reason).await;
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        self.client_event_handler.on_error(connection_id, error).await;
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        // 对于客户端事件适配器，我们直接处理消息而不转发给客户端事件处理器
        self.handle_message(message).await;
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        self.client_event_handler.on_message_sent(connection_id, message).await;
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        self.client_event_handler.on_heartbeat_timeout(connection_id).await;
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        self.client_event_handler.on_heartbeat_ping(connection_id).await;
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        self.client_event_handler.on_heartbeat_pong(connection_id).await;
    }

    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        self.client_event_handler.on_quality_changed(connection_id, quality_score).await;
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        self.client_event_handler.on_reconnect_started(connection_id, attempt).await;
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        self.client_event_handler.on_reconnected(connection_id, attempt).await;
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        self.client_event_handler.on_reconnect_failed(connection_id, attempt, error).await;
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &ConnectionStats) {
        self.client_event_handler.on_statistics_updated(connection_id, stats).await;
    }
}

#[async_trait::async_trait]
impl ClientEvent for ClientEventAdapter {
    async fn on_control_command(&self, cmd: &ControlCmd) {
        self.client_event_handler.on_control_command(cmd).await;
    }

    async fn on_message_command(&self, message: &MessageCmd) {
        self.client_event_handler.on_message_command(message).await;
    }

    async fn on_notification_command(&self, notification: &NotificationCmd) {
        self.client_event_handler.on_notification_command(notification).await;
    }

    async fn on_event_command(&self, event: &EventCmd) {
        self.client_event_handler.on_event_command(event).await;
    }
    
    async fn on_authenticated(&self) {
        self.client_event_handler.on_authenticated().await;
    }
    
    async fn on_authentication_failed(&self, error: &str) {
        self.client_event_handler.on_authentication_failed(error).await;
    }
}