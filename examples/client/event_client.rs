//! 客户端事件处理示例
//!
//! 演示如何使用客户端事件处理机制

use std::sync::Arc;
use async_trait::async_trait;
use tracing::{info, warn};

use flare_core::{
    client::{
        Client, ClientConfig, ClientEvent, AuthConfig,
        DefClientEventHandler, ClientEventAdapter,
    },
    common::{
        connections::{
            event::ConnectionEvent,
            traits::ConnectionStats,
        },
        protocol::{
            Frame,
            commands::{ControlCmd, MessageCmd, NotificationCmd, EventCmd},
        },
    },
};

/// 自定义客户端事件处理器
#[derive(Debug)]
pub struct CustomClientEventHandler;

#[async_trait]
impl ClientEvent for CustomClientEventHandler {
    async fn on_control_command(&self, cmd: &ControlCmd) {
        info!("[自定义处理器] 收到控制消息 - 类型: {}", cmd.as_str());
    }

    async fn on_message_command(&self, message: &MessageCmd) {
        info!("[自定义处理器] 收到消息 - 类型: {}", message.as_str());
    }

    async fn on_notification_command(&self, notification: &NotificationCmd) {
        info!("[自定义处理器] 收到通知 - 类型: {}", notification.as_str());
    }

    async fn on_event_command(&self, event: &EventCmd) {
        info!("[自定义处理器] 收到事件 - 类型: {}", event.as_str());
    }
    
    async fn on_authenticated(&self) {
        info!("[自定义处理器] 客户端认证成功");
    }
    
    async fn on_authentication_failed(&self, error: &str) {
        warn!("[自定义处理器] 客户端认证失败 - 错误: {}", error);
    }
}

#[async_trait]
impl ConnectionEvent for CustomClientEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[自定义处理器] 连接已建立: {}", connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[自定义处理器] 连接已断开: {} - 原因: {}", connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        warn!("[自定义处理器] 连接错误: {} - 错误: {}", connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        info!("[自定义处理器] 收到消息: {} - 类型: {}", connection_id, message.get_command_type_str());
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        info!("[自定义处理器] 发送消息: {} - 类型: {}", connection_id, message.get_command_type_str());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        warn!("[自定义处理器] 心跳超时: {}", connection_id);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        info!("[自定义处理器] 收到心跳的ping: {}", connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        info!("[自定义处理器] 收到心跳的pong: {}", connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[自定义处理器] 连接质量变化: {} - 评分: {}", connection_id, quality_score);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        info!("[自定义处理器] 开始重连: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        info!("[自定义处理器] 重连成功: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        warn!("[自定义处理器] 重连失败: {} - 尝试次数: {} - 错误: {}", connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &ConnectionStats) {
        info!("[自定义处理器] 统计信息更新: {} - 收到: {} - 发送: {} - 质量: {}", 
              connection_id, stats.messages_received, stats.messages_sent, stats.quality_score);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    // 创建客户端配置
    let mut config = ClientConfig::default();
    
    // 配置认证（如果需要）
    let auth_config = AuthConfig {
        enabled: false, // 暂时禁用认证以简化示例
        user_id: Some("test_user".to_string()),
        platform: Some("test_platform".to_string()),
        token: Some("test_token".to_string()),
        timeout_ms: 5000,
    };
    config = config.with_auth_config(auth_config);
    
    // 创建自定义事件处理器
    let custom_handler = Arc::new(CustomClientEventHandler);
    
    // 创建客户端（使用自定义事件处理器）
    let mut client = Client::with_event_handler(config, custom_handler);
    
    // 或者使用默认事件处理器
    // let mut client = Client::new(config);
    
    // 连接到服务器
    match client.connect().await {
        Ok(_) => {
            info!("客户端连接成功");
            
            // 检查连接状态
            let state = client.get_state().await;
            info!("当前连接状态: {:?}", state);
            
            // 等待一段时间
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            
            // 断开连接
            client.disconnect().await?;
            info!("客户端已断开连接");
        }
        Err(e) => {
            eprintln!("连接失败: {}", e);
        }
    }
    
    Ok(())
}