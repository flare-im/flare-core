use std::sync::Arc;
use async_trait::async_trait;
use crate::{ConnectionEvent, Frame};
use crate::common::connections::traits::ConnectionStats;
use crate::server::event::ServerEvent;
use crate::common::protocol::commands::{Command, ControlCmd, MessageCmd, NotificationCmd, EventCmd};

/// 服务端事件适配器
/// 
/// 专门用于适配服务端事件处理的模块，未来可扩展以适配其他服务端功能
pub struct ServerEventAdapter {
    /// 服务端事件处理器
    server_event_handler: Arc<dyn ServerEvent>,
}

impl ServerEventAdapter {
    /// 创建新的服务端事件适配器
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
    async fn handle_message(&self, connection_id: &str, message: &Frame) {
        // 根据消息中的命令类型进行处理
        match &message.command {
            Command::Control(control_cmd) => {
                self.server_event_handler.on_control_command(connection_id, control_cmd).await;
            },
            Command::Message(message_cmd) => {
                self.server_event_handler.on_message_command(connection_id, message_cmd).await;
            },
            Command::Notification(notification_cmd) => {
               self.server_event_handler.on_notification_command(connection_id, notification_cmd).await;
            },
            Command::Event(event_cmd) => {
              self.server_event_handler.on_event_command(connection_id, event_cmd).await;
            }
        }
    }

}

/// 连接事件处理器实现
#[async_trait]
impl ConnectionEvent for ServerEventAdapter {
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
        // 对于服务端事件适配器，我们直接处理消息而不转发给服务端事件处理器
        self.handle_message(connection_id, message).await;
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

#[async_trait::async_trait]
impl ServerEvent for ServerEventAdapter {
    async fn on_control_command(&self, connection_id: &str, cmd: &ControlCmd) {
        self.server_event_handler.on_control_command(connection_id, cmd).await;
    }

    async fn on_message_command(&self, connection_id: &str, message: &MessageCmd) {
        self.server_event_handler.on_message_command(connection_id, message).await;
    }

    async fn on_notification_command(&self, connection_id: &str, notification: &NotificationCmd) {
        self.server_event_handler.on_notification_command(connection_id, notification).await;
    }

    async fn on_event_command(&self, connection_id: &str, event: &EventCmd) {
        self.server_event_handler.on_event_command(connection_id, event).await;
    }
}