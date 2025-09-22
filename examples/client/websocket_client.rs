//! WebSocket 客户端连接示例 (FastClient版本)
//!
//! 展示如何使用flare-core的FastClient创建WebSocket客户端并进行通信

use std::sync::Arc;
use std::time::Instant;
use tracing::{info, error};

use flare_core::{
    client::{
        FastClientBuilder, 
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

/// WebSocket客户端事件处理器
#[derive(Debug)]
pub struct WebSocketClientEventHandler {
    pub name: String,
}

impl WebSocketClientEventHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait::async_trait]
impl ClientEvent for WebSocketClientEventHandler {
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
impl flare_core::common::connections::event::ConnectionEvent for WebSocketClientEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] WebSocket连接已建立: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] WebSocket连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] WebSocket连接错误: {} - 错误: {}", self.name, connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &flare_core::common::protocol::Frame) {
        // 获取消息内容长度
        let content_length = match &message.command {
            flare_core::common::protocol::commands::Command::Message(msg_cmd) => {
                match msg_cmd {
                    flare_core::common::protocol::commands::MessageCmd::Send(send_cmd) => send_cmd.data.len(),
                    flare_core::common::protocol::commands::MessageCmd::Data(data_cmd) => data_cmd.data.len(),
                    _ => 0,
                }
            },
            _ => 0,
        };
        
        info!("[{}] 收到WebSocket服务器消息: {} - 类型: {} - 内容长度: {}", 
              self.name, connection_id, message.get_command_type_str(), content_length);
    }

    async fn on_message_sent(&self, connection_id: &str, message: &flare_core::common::protocol::Frame) {
        info!("[{}] WebSocket数据消息已发送: {} - 类型: {}", 
              self.name, connection_id, message.get_command_type_str());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("[{}] WebSocket心跳超时: {}", self.name, connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] WebSocket连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        info!("[{}] WebSocket心跳已发送: {}", self.name, connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        info!("[{}] 收到WebSocket心跳响应: {}", self.name, connection_id);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        info!("[{}] WebSocket开始重连: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        info!("[{}] WebSocket重连成功: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        error!("[{}] WebSocket重连失败: {} - 尝试次数: {} - 错误: {}", self.name, connection_id, attempt, error);
        // 当重连失败时，可以在这里添加终止程序的逻辑
        if attempt >= 5 {
            error!("[{}] 重连尝试次数已达到上限，程序将退出", self.name);
            std::process::exit(1);
        }
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) {
        info!("[{}] WebSocket统计信息更新: {} - 收到消息: {} - 发送消息: {}", 
             self.name, connection_id, stats.messages_received, stats.messages_sent);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("启动FastClient WebSocket客户端示例");
    
    // 创建序列化配置
    let serialization_config = SerializationConfig::builder()
        .format(SerializationFormat::Protobuf)
        .build();
    
    // 设置事件处理器
    let event_handler = Arc::new(WebSocketClientEventHandler::new("FastClient WebSocket客户端".to_string()));
    
    // 创建带有自定义事件处理器的客户端
    let mut client = FastClientBuilder::new()
        .with_websocket_only()  // 仅使用WebSocket协议
        .with_server_address(Transport::WebSocket, "ws://127.0.0.1:8080".to_string())
        .with_heartbeat(5000, 2000)  // 5秒心跳，2秒超时
        .with_serialization(serialization_config)
        .with_event_handler(event_handler)
        .build();
    
    // 启动客户端
    info!("正在连接WebSocket服务端...");
    let connect_start = Instant::now();
    
    // 使用更好的错误处理
    match client.start().await {
        Ok(()) => {
            let connect_time = connect_start.elapsed();
            info!("✅ 已连接到WebSocket服务端！连接耗时: {:.2}ms", connect_time.as_secs_f64() * 1000.0);
        }
        Err(e) => {
            error!("❌ 连接WebSocket服务端失败: {}", e);
            error!("请确保服务端已启动并监听在 ws://127.0.0.1:8080");
            // 等待一段时间让事件处理器处理错误
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            return Err(e.into());
        }
    }
    
    // 发送认证消息（简化示例，实际应用中应该使用真实的认证数据）
    info!("发送认证消息...");
    let message_id = FrameFactory::generate_message_id();
    let auth_message = FrameFactory::create_connect_frame(
        message_id,
        "websocket_client".to_string(),
        "websocket".to_string(),
        "web".to_string(),
        "1.0.0".to_string(),
    )?;
    
    if let Err(e) = client.send_message(auth_message).await {
        error!("发送认证消息失败: {}", e);
    } else {
        info!("认证消息已发送");
    }
    
    // 等待一小段时间确保认证完成
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // 发送测试消息
    info!("发送测试消息...");
    let message_id = FrameFactory::generate_message_id();
    let test_message = FrameFactory::create_message_frame(
        message_id,
        "Hello from FastClient WebSocket client!".as_bytes().to_vec(),
        Reliability::AtLeastOnce,
    )?;
    
    if let Err(e) = client.send_message(test_message).await {
        error!("发送测试消息失败: {}", e);
    } else {
        info!("测试消息已发送");
    }
    
    // 等待一段时间以接收响应
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    
    // 停止客户端
    info!("正在停止客户端...");
    if let Err(e) = client.stop().await {
        error!("停止客户端时发生错误: {}", e);
    } else {
        info!("客户端已停止");
    }
    
    Ok(())
}