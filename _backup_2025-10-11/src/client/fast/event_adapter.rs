//! FastClient 事件适配器
//!
//! 将 ClientEvent 和 ConnectionEvent 转换为 FastEvent，让 FastClient 能够利用基础 Client 的事件系统

use std::sync::Arc;
use async_trait::async_trait;
use tracing::{debug, warn};

use crate::common::{
    protocol::Frame,
    connections::{
        traits::ConnectionStats,
        event::ConnectionEvent,
    },
};

use super::{
    event::FastEvent,
    auth::FastAuthManager,
};

use crate::client::event::ClientEvent;

/// FastClient 事件适配器
/// 
/// 将 ClientEvent 和 ConnectionEvent 转换为 FastEvent，让 FastClient 能够利用基础 Client 的事件系统
pub struct FastClientEventAdapter {
    fast_event_handler: Arc<dyn FastEvent>,
    auth_manager: Arc<FastAuthManager>,
}

impl FastClientEventAdapter {
    /// 创建新的适配器
    pub fn new(fast_event_handler: Arc<dyn FastEvent>, auth_manager: Arc<FastAuthManager>) -> Self {
        Self {
            fast_event_handler,
            auth_manager,
        }
    }
}

#[async_trait]
impl ClientEvent for FastClientEventAdapter {
    async fn on_control_command(&self, cmd: &crate::common::protocol::commands::ControlCmd) {
        self.fast_event_handler.on_control_command(cmd).await;
    }

    async fn on_message_command(&self, message: &crate::common::protocol::commands::MessageCmd) {
        self.fast_event_handler.on_message_command(message).await;
    }

    async fn on_notification_command(&self, notification: &crate::common::protocol::commands::NotificationCmd) {
        self.fast_event_handler.on_notification_command(notification).await;
    }

    async fn on_event_command(&self, event: &crate::common::protocol::commands::EventCmd) {
        self.fast_event_handler.on_event_command(event).await;
    }
    
    async fn on_connected(&self, connection_id: &str) {
        self.fast_event_handler.on_connected(connection_id).await;
    }
    
    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        self.fast_event_handler.on_disconnected(connection_id, reason).await;
    }
    
    async fn on_error(&self, connection_id: &str, error: &str) {
        self.fast_event_handler.on_error(connection_id, error).await;
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        self.fast_event_handler.on_quality_changed(connection_id, quality_score).await;
    }
    
    async fn on_statistics_updated(&self, connection_id: &str, stats: &ConnectionStats) {
        self.fast_event_handler.on_statistics_updated(connection_id, stats).await;
    }
    
    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) -> bool {
        // FastClient 有自己的重连逻辑，这里返回 true 让基础 Client 继续
        self.fast_event_handler.on_reconnect_started(connection_id, attempt).await;
        true
    }
    
    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        self.fast_event_handler.on_reconnected(connection_id, attempt).await;
    }
    
    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) -> bool {
        // FastClient 有自己的重连逻辑，这里返回 false 让 FastClient 处理
        self.fast_event_handler.on_reconnect_failed(connection_id, attempt, error).await;
        false
    }
    
    async fn on_protocol_switched(&self, connection_id: &str, from_protocol: &str, to_protocol: &str) {
        self.fast_event_handler.on_protocol_switched(connection_id, from_protocol, to_protocol).await;
    }
    
    async fn on_heartbeat_timeout(&self, connection_id: &str) -> bool {
        // FastClient 有自己的心跳逻辑，这里返回 false 让 FastClient 处理
        self.fast_event_handler.on_heartbeat_timeout(connection_id).await;
        false
    }
    
    async fn on_heartbeat_ping(&self, connection_id: &str) {
        self.fast_event_handler.on_heartbeat_ping(connection_id).await;
    }
    
    async fn on_heartbeat_pong(&self, connection_id: &str) {
        self.fast_event_handler.on_heartbeat_pong(connection_id).await;
    }
}

// 实现 ConnectionEvent trait，用于处理基础连接事件
#[async_trait]
impl ConnectionEvent for FastClientEventAdapter {
    async fn on_connected(&self, connection_id: &str) {
        self.fast_event_handler.on_connected(connection_id).await;
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        self.fast_event_handler.on_disconnected(connection_id, reason).await;
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        self.fast_event_handler.on_error(connection_id, error).await;
    }

    async fn on_message_received(&self, _connection_id: &str, message: &Frame) {
        // 处理认证响应
        if let Err(e) = self.auth_manager.handle_auth_response(message).await {
            warn!("处理认证响应失败: {}", e);
        }
        
        // 根据消息中的命令类型进行处理
        match &message.command {
            crate::common::protocol::commands::Command::Control(control_cmd) => {
                self.fast_event_handler.on_control_command(control_cmd).await;
            },
            crate::common::protocol::commands::Command::Message(message_cmd) => {
                self.fast_event_handler.on_message_command(message_cmd).await;
            },
            crate::common::protocol::commands::Command::Notification(notification_cmd) => {
                self.fast_event_handler.on_notification_command(notification_cmd).await;
            },
            crate::common::protocol::commands::Command::Event(event_cmd) => {
                self.fast_event_handler.on_event_command(event_cmd).await;
            }
        }
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        // FastEvent 没有 on_message_sent 方法，这里可以添加日志或其他处理
        debug!("FastClient: 发送消息: {} - 类型: {}", connection_id, message.get_command_type_str());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        self.fast_event_handler.on_heartbeat_timeout(connection_id).await;
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        self.fast_event_handler.on_heartbeat_ping(connection_id).await;
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        self.fast_event_handler.on_heartbeat_pong(connection_id).await;
    }

    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        self.fast_event_handler.on_quality_changed(connection_id, quality_score).await;
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        self.fast_event_handler.on_reconnect_started(connection_id, attempt).await;
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        self.fast_event_handler.on_reconnected(connection_id, attempt).await;
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        self.fast_event_handler.on_reconnect_failed(connection_id, attempt, error).await;
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &ConnectionStats) {
        self.fast_event_handler.on_statistics_updated(connection_id, stats).await;
    }
}
