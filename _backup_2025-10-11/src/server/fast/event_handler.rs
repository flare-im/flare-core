use std::sync::Arc;
use async_trait::async_trait;
use tracing::{info, warn, debug};
use crate::server::fast::AuthProvider;
use crate::{ConnectionEvent, Frame};
use crate::common::connections::traits::ConnectionStats;
use crate::common::protocol::commands::{Command, ControlCmd, CustomCommand, ErrorCommand, EventCmd, MessageCmd, NotificationCmd};
use crate::server::{ServerEvent, UserConnectionManager};
use crate::server::fast::message_handler::MessageHandler;

/// 快速服务端事件处理器
pub struct FastServerEventHandler {
    /// 连接管理器
    connection_manager: Arc<UserConnectionManager>,
    /// 消息处理器
    message_handler: Arc<dyn MessageHandler>,
    /// 认证提供者
    auth_provider: Arc<dyn AuthProvider>,
}

impl FastServerEventHandler {
    /// 创建新的快速服务端事件处理器
    pub fn new(
        connection_manager: Arc<UserConnectionManager>,
        message_handler: Arc<dyn MessageHandler>,
        auth_provider: Arc<dyn AuthProvider>,
    ) -> Self {
        Self {
            connection_manager,
            message_handler,
            auth_provider,
        }
    }
}

#[async_trait]
impl ConnectionEvent for FastServerEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        debug!("连接已建立: {}", connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        debug!("连接已断开: {} - 原因: {}", connection_id, reason);
        // 从连接管理器中移除连接
        if let Err(e) = self.connection_manager.remove_connection(connection_id, Some(reason.to_string())).await {
            warn!("移除连接失败: {} - 错误: {}", connection_id, e);
        }
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        warn!("连接错误: {} - 错误: {}", connection_id, error);
        
        // 创建一个ErrorCommand来传递给消息处理器
        let error_cmd = ErrorCommand::new(500, error.to_string());
        
        // 通知消息处理器
        if let Err(e) = self.message_handler.handle_error(connection_id, &error_cmd).await {
            warn!("处理错误事件失败: {} - 错误: {}", connection_id, e);
        }
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        debug!("收到消息: {} - 类型: {}", connection_id, message.get_command_type_str());
        // 根据消息类型调用相应的消息处理器方法
        match &message.command {
            Command::Message(msg_cmd) => {
                self.on_message_command(connection_id, msg_cmd).await;
            },
            Command::Control(ctrl_cmd) => {
               self.on_control_command(connection_id, ctrl_cmd).await;
            },
            Command::Notification(notify_cmd) => {
               self.on_notification_command(connection_id, notify_cmd).await;
            },
            Command::Event(event_cmd) => {
                self.on_event_command(connection_id, event_cmd).await;
            },
        }
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        debug!("发送消息: {} - 类型: {}", connection_id, message.get_command_type_str());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("心跳超时: {}", connection_id);
        
        // 断开连接
        if let Err(e) = self.connection_manager.remove_connection(connection_id, Some("心跳超时".to_string())).await {
            warn!("移除心跳超时连接失败: {} - 错误: {}", connection_id, e);
        }
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        debug!("收到心跳的ping: {}", connection_id);
        
        // 创建一个自定义命令来表示心跳事件
        let mut custom_cmd = CustomCommand::new(
            "heartbeat_ping".to_string(), 
            vec![]
        );
        custom_cmd.add_metadata("type".to_string(), "ping".to_string());
        
        // 通知消息处理器
        if let Err(e) = self.message_handler.handle_custom_event(connection_id, &custom_cmd).await {
            warn!("处理心跳ping事件失败: {} - 错误: {}", connection_id, e);
        }
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        debug!("收到心跳的pong: {}", connection_id);
        
        // 创建一个自定义命令来表示心跳事件
        let mut custom_cmd = CustomCommand::new(
            "heartbeat_pong".to_string(), 
            vec![]
        );
        custom_cmd.add_metadata("type".to_string(), "pong".to_string());
        
        // 通知消息处理器
        if let Err(e) = self.message_handler.handle_custom_event(connection_id, &custom_cmd).await {
            warn!("处理心跳pong事件失败: {} - 错误: {}", connection_id, e);
        }
    }

    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        debug!("连接质量变化: {} - 评分: {}", connection_id, quality_score);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        debug!("开始重连: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        debug!("重连成功: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        warn!("重连失败: {} - 尝试次数: {} - 错误: {}", connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &ConnectionStats) {
        debug!("统计信息更新: {} - 收到: {} - 发送: {} - 质量: {}", 
               connection_id, stats.messages_received, stats.messages_sent, stats.quality_score);
    }
}

#[async_trait]
impl ServerEvent for FastServerEventHandler {
    async fn on_control_command(&self, connection_id: &str, cmd: &ControlCmd) {
        info!("收到控制消息: {} - 类型: {}", connection_id, cmd.as_str());
        
        match cmd {
            ControlCmd::AuthRequest(auth_req) => {
                // 使用认证提供者验证令牌
                match self.auth_provider.validate_token(
                    &auth_req.user_id,
                    &auth_req.platform,
                    &auth_req.token,
                ).await {
                    Ok(true) => {
                        // 认证成功，获取用户信息
                        let user_info = self.auth_provider.get_user_info(&auth_req.user_id).await.unwrap_or(None);
                        
                        // 处理认证结果
                        if let Err(e) = self.connection_manager.process_authentication_result(
                            connection_id.to_string(),
                            auth_req.user_id.clone(),
                            auth_req.platform.clone(),
                            true, // success
                            None, // error_message
                            user_info, // user_info
                        ).await {
                            warn!("处理认证结果失败: {} - 错误: {}", connection_id, e);
                        } else {
                            info!("连接认证成功: {}", connection_id);
                        }
                    }
                    Ok(false) => {
                        // 认证失败
                        if let Err(e) = self.connection_manager.process_authentication_result(
                            connection_id.to_string(),
                            auth_req.user_id.clone(),
                            auth_req.platform.clone(),
                            false, // success
                            Some("认证失败".to_string()), // error_message
                            None, // user_info
                        ).await {
                            warn!("处理认证结果失败: {} - 错误: {}", connection_id, e);
                        } else {
                            info!("连接认证失败: {}", connection_id);
                        }
                    }
                    Err(e) => {
                        // 验证过程中发生错误
                        warn!("验证令牌时发生错误: {} - 错误: {}", connection_id, e);
                        
                        if let Err(e) = self.connection_manager.process_authentication_result(
                            connection_id.to_string(),
                            auth_req.user_id.clone(),
                            auth_req.platform.clone(),
                            false, // success
                            Some(format!("验证错误: {}", e)), // error_message
                            None, // user_info
                        ).await {
                            warn!("处理认证结果失败: {} - 错误: {}", connection_id, e);
                        }
                    }
                }
            }
            _ => {
                debug!("未处理的控制命令: {} - 类型: {}", connection_id, cmd.as_str());
            }
        }
    }

    async fn on_message_command(&self, connection_id: &str, message: &MessageCmd) {
        info!("收到消息: {} - 类型: {}", connection_id, message.as_str());
        
        // 获取用户信息
        if let Some((user_id, _platform)) = self.connection_manager.get_user_by_connection(connection_id).await {
            // 根据消息类型调用相应的消息处理器方法
            match message {
                MessageCmd::Send(send_cmd) => {
                    if let Err(e) = self.message_handler.handle_message(&user_id, connection_id, send_cmd).await {
                        warn!("处理用户消息失败: 用户={} 连接={} - 错误: {}", user_id, connection_id, e);
                    }
                },
                MessageCmd::Data(data_cmd) => {
                    if let Err(e) = self.message_handler.handle_data_message(&user_id, connection_id, data_cmd).await {
                        warn!("处理用户数据消息失败: 用户={} 连接={} - 错误: {}", user_id, connection_id, e);
                    }
                },
                MessageCmd::Custom(custom_cmd) => {
                    if let Err(e) = self.message_handler.handle_custom_message(&user_id, connection_id, custom_cmd).await {
                        warn!("处理用户自定义消息失败: 用户={} 连接={} - 错误: {}", user_id, connection_id, e);
                    }
                },
                _ => {
                    debug!("未处理的消息命令类型: {}", message.as_str());
                }
            }
        } else {
            debug!("收到未认证连接的消息命令: {} - 类型: {}", connection_id, message.as_str());
        }
    }

    async fn on_notification_command(&self, connection_id: &str, notification: &NotificationCmd) {
        info!("收到通知: {} - 类型: {}", connection_id, notification.as_str());
        
        // 获取用户信息
        if let Some((user_id, _platform)) = self.connection_manager.get_user_by_connection(connection_id).await {
            // 直接调用消息处理器处理通知
            if let Err(e) = self.message_handler.handle_notification(connection_id, notification).await {
                warn!("处理用户通知失败: 用户={} 连接={} - 错误: {}", user_id, connection_id, e);
            }
        } else {
            debug!("收到未认证连接的通知命令: {} - 类型: {}", connection_id, notification.as_str());
        }
    }

    async fn on_event_command(&self, connection_id: &str, event: &EventCmd) {
        info!("收到事件: {} - 类型: {}", connection_id, event.as_str());
        
        // 获取用户信息
        if let Some((user_id, _platform)) = self.connection_manager.get_user_by_connection(connection_id).await {
            // 根据事件类型调用相应的消息处理器方法
            match event {
                EventCmd::Custom(custom_cmd) => {
                    if let Err(e) = self.message_handler.handle_custom_event(connection_id, custom_cmd).await {
                        warn!("处理用户自定义事件失败: 用户={} 连接={} - 错误: {}", user_id, connection_id, e);
                    }
                },
                _ => {
                    // 对于其他事件类型，创建一个自定义命令来表示事件
                    let mut custom_cmd = CustomCommand::new(
                        "event".to_string(), 
                        vec![]
                    );
                    custom_cmd.add_metadata("type".to_string(), event.as_str().to_string());
                    
                    if let Err(e) = self.message_handler.handle_custom_event(connection_id, &custom_cmd).await {
                        warn!("处理用户事件失败: 用户={} 连接={} - 错误: {}", user_id, connection_id, e);
                    }
                }
            }
        } else {
            debug!("收到未认证连接的事件命令: {} - 类型: {}", connection_id, event.as_str());
        }
    }
}