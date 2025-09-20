//! 基础客户端连接示例
//!
//! 展示如何使用flare-core的基础Client创建客户端并进行通信

use std::time::Instant;
use tracing::{info, error};

use flare_core::{
    client::{
        Client, 
        ClientConfig, 
        ClientEvent, 
    },
    common::{
        connections::{
            types::{Transport},
        },
        protocol::{Reliability},
        serialization::{SerializationFormat, SerializationConfig},
    },
    common::protocol::factory::FrameFactory,
};

/// 基础客户端事件处理器
#[derive(Debug)]
pub struct BasicClientEventHandler {
    pub name: String,
}

impl BasicClientEventHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait::async_trait]
impl ClientEvent for BasicClientEventHandler {
    async fn on_control_command(&self, cmd: &flare_core::common::protocol::commands::ControlCmd) {
        info!("[{}] 收到控制命令: {}", self.name, cmd.as_str());
    }

    async fn on_message_command(&self, message: &flare_core::common::protocol::commands::MessageCmd) {
        info!("[{}] 收到消息命令: {}", self.name, message.as_str());
    }

    async fn on_notification_command(&self, notification: &flare_core::common::protocol::commands::NotificationCmd) {
        info!("[{}] 收到通知命令: {}", self.name, notification.as_str());
    }

    async fn on_event_command(&self, event: &flare_core::common::protocol::commands::EventCmd) {
        info!("[{}] 收到事件命令: {}", self.name, event.as_str());
    }
    
    async fn on_authenticated(&self) {
        info!("[{}] 客户端认证成功", self.name);
    }
    
    async fn on_authentication_failed(&self, error: &str) {
        error!("[{}] 客户端认证失败: {}", self.name, error);
    }
}

#[async_trait::async_trait]
impl flare_core::common::connections::event::ConnectionEvent for BasicClientEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] 客户端连接已建立: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] 客户端连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] 客户端连接错误: {} - 错误: {}", self.name, connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &flare_core::common::protocol::Frame) {
        info!("[{}] 收到服务器消息: {} - 类型: {}", 
              self.name, connection_id, message.get_command_type_str());
    }

    async fn on_message_sent(&self, connection_id: &str, message: &flare_core::common::protocol::Frame) {
        info!("[{}] 数据消息已发送: {} - 类型: {}", 
              self.name, connection_id, message.get_command_type_str());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("[{}] 心跳超时: {}", self.name, connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] 连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        info!("[{}] 心跳已发送: {}", self.name, connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        info!("[{}] 收到心跳响应: {}", self.name, connection_id);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        info!("[{}] 开始重连: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        info!("[{}] 重连成功: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        info!("[{}] 重连失败: {} - 尝试次数: {} - 错误: {}", self.name, connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) {
        info!("[{}] 统计信息更新: {} - 收到消息: {} - 发送消息: {}", 
             self.name, connection_id, stats.messages_received, stats.messages_sent);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("启动基础客户端示例");
    
    // 创建客户端配置
    let config = ClientConfig::default()
        .with_websocket_only()  // 仅使用WebSocket协议
        .with_server_address(Transport::WebSocket, "ws://127.0.0.1:8080".to_string())
        .with_heartbeat(5000, 2000)  // 5秒心跳，2秒超时
        .with_serialization(SerializationFormat::Protobuf, SerializationConfig::default());
    
    // 创建客户端（使用默认事件处理器）
    let mut client = Client::new(config);
    
    // 或者创建带有自定义事件处理器的客户端
    // let event_handler = Arc::new(BasicClientEventHandler::new("基础客户端".to_string()));
    // let mut client = Client::with_event_handler(config, event_handler);
    
    // 连接到服务器
    info!("正在连接服务端...");
    let connect_start = Instant::now();
    client.connect().await?;
    let connect_time = connect_start.elapsed();
    info!("✅ 已连接到服务端！连接耗时: {:.2}ms", connect_time.as_secs_f64() * 1000.0);
    
    // 发送认证消息（简化示例，实际应用中应该使用真实的认证数据）
    info!("发送认证消息...");
    let message_id = FrameFactory::generate_message_id();
    let auth_message = FrameFactory::create_connect_frame(
        message_id,
        "basic_client".to_string(),
        "websocket".to_string(),
        "web".to_string(),
        "1.0.0".to_string(),
    )?;
    
    client.send_message(auth_message).await?;
    info!("认证消息已发送");
    
    // 等待一小段时间确保认证完成
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // 发送测试消息
    info!("发送测试消息...");
    let message_id = FrameFactory::generate_message_id();
    let test_message = FrameFactory::create_message_frame(
        message_id,
        "Hello from basic client!".as_bytes().to_vec(),
        Reliability::AtLeastOnce,
    )?;
    
    client.send_message(test_message).await?;
    info!("测试消息已发送");
    
    // 等待一段时间以接收响应
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    
    // 断开连接
    info!("正在断开连接...");
    client.disconnect().await?;
    info!("连接已断开");
    
    Ok(())
}