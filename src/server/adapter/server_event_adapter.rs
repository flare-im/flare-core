use std::sync::Arc;
use async_trait::async_trait;
use crate::{ConnectionEvent, Frame};
use crate::common::connections::traits::ConnectionStats;
use crate::server::ServerEvent;
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
    async fn handle_message(&self, _connection_id: &str, message: &Frame) {
        // 根据消息中的命令类型进行处理
        // 注意：已经在message_parser中处理过的命令（如心跳、连接、断开连接等）这里只打印日志
        match &message.command {
            Command::Control(control_cmd) => {
                match control_cmd {
                    ControlCmd::Connect(connect_cmd) => {
                        // 连接请求已在message_parser中处理，这里只打印日志
                        tracing::info!("收到连接请求: client_id={}, protocol={}", connect_cmd.client_id, connect_cmd.protocol);
                    },
                    ControlCmd::ConnectAck(_) => {
                        // 连接确认已在message_parser中处理，这里只打印日志
                        tracing::info!("收到连接确认");
                    },
                    ControlCmd::Disconnect(disconnect_cmd) => {
                        // 断开连接已在message_parser中处理，这里只打印日志
                        tracing::info!("收到断开连接: reason={}", disconnect_cmd.reason);
                    },
                    ControlCmd::AuthRequest(auth_cmd) => {
                        // 认证请求需要在这里处理业务逻辑
                        tracing::info!("处理认证请求: user_id={}", auth_cmd.user_id);
                        // TODO: 添加认证逻辑
                    },
                    ControlCmd::AuthResponse(_) => {
                        // 认证响应需要在这里处理业务逻辑
                        tracing::info!("处理认证响应");
                        // TODO: 添加认证响应处理逻辑
                    },
                    ControlCmd::Ping => {
                        // 心跳请求已在message_parser中处理，这里只打印日志
                        tracing::info!("收到心跳请求");
                    },
                    ControlCmd::Pong => {
                        // 心跳响应已在message_parser中处理，这里只打印日志
                        tracing::info!("收到心跳响应");
                    },
                    ControlCmd::Error(error_cmd) => {
                        // 错误消息已在message_parser中处理，这里只打印日志
                        tracing::info!("收到错误消息: status={}, message={}", error_cmd.status, error_cmd.message);
                    },
                    ControlCmd::Custom(custom_cmd) => {
                        // 自定义控制命令需要在这里处理业务逻辑
                        tracing::info!("处理自定义控制命令: name={}", custom_cmd.name);
                        // TODO: 添加自定义控制命令处理逻辑
                    }
                }
            },
            Command::Message(message_cmd) => {
                match message_cmd {
                    MessageCmd::Send(send_cmd) => {
                        // 发送消息需要在这里处理业务逻辑
                        tracing::info!("处理发送消息: data_len={}", send_cmd.data.len());
                        // TODO: 添加消息处理逻辑
                    },
                    MessageCmd::Ack(ack_cmd) => {
                        // 消息确认需要在这里处理业务逻辑
                        tracing::info!("处理消息确认: success={}, status={}", ack_cmd.success, ack_cmd.status);
                        // TODO: 添加确认处理逻辑
                    },
                    MessageCmd::Data(data_cmd) => {
                        // 数据消息需要在这里处理业务逻辑
                        tracing::info!("处理数据消息: data_len={}", data_cmd.data.len());
                        // TODO: 添加数据处理逻辑
                    },
                    MessageCmd::Custom(custom_cmd) => {
                        // 自定义消息命令需要在这里处理业务逻辑
                        tracing::info!("处理自定义消息命令: name={}", custom_cmd.name);
                        // TODO: 添加自定义消息处理逻辑
                    }
                }
            },
            Command::Notification(notification_cmd) => {
                match notification_cmd {
                    NotificationCmd::System(system_cmd) => {
                        // 系统通知需要在这里处理业务逻辑
                        tracing::info!("处理系统通知: content={}, type={}", system_cmd.content, system_cmd.notification_type);
                        // TODO: 添加系统通知处理逻辑
                    },
                    NotificationCmd::Broadcast(broadcast_cmd) => {
                        // 广播通知需要在这里处理业务逻辑
                        tracing::info!("处理广播通知: content={}, type={}", broadcast_cmd.content, broadcast_cmd.notification_type);
                        // TODO: 添加广播通知处理逻辑
                    },
                    NotificationCmd::Alert(alert_cmd) => {
                        // 警报通知需要在这里处理业务逻辑
                        tracing::info!("处理警报通知: content={}, type={}", alert_cmd.content, alert_cmd.notification_type);
                        // TODO: 添加警报通知处理逻辑
                    },
                    NotificationCmd::Custom(custom_cmd) => {
                        // 自定义通知命令需要在这里处理业务逻辑
                        tracing::info!("处理自定义通知命令: name={}", custom_cmd.name);
                        // TODO: 添加自定义通知处理逻辑
                    }
                }
            },
            Command::Event(event_cmd) => {
                match event_cmd {
                    EventCmd::Open => {
                        // 连接打开事件已在message_parser中处理，这里只打印日志
                        tracing::info!("连接已打开");
                    },
                    EventCmd::Close => {
                        // 连接关闭事件已在message_parser中处理，这里只打印日志
                        tracing::info!("连接已关闭");
                    },
                    EventCmd::Reconnect => {
                        // 重连事件已在message_parser中处理，这里只打印日志
                        tracing::info!("连接已重连");
                    },
                    EventCmd::Custom(custom_cmd) => {
                        // 自定义事件命令需要在这里处理业务逻辑
                        tracing::info!("处理自定义事件命令: name={}", custom_cmd.name);
                        // TODO: 添加自定义事件处理逻辑
                    }
                }
            }
        }
    }
    
    // 未来可以在这里添加其他适配方法
    // 例如：
    // - 用户管理适配
    // - 连接管理适配
    // - 消息路由适配
    // - 认证授权适配
    // - 统计信息适配
    //
    // 示例方法签名（未实现）：
    // pub async fn adapt_user_management(&self, user_id: &str) -> Result<()> { ... }
    // pub async fn adapt_connection_management(&self, connection_id: &str) -> Result<()> { ... }
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