//! QUIC 客户端连接示例 (FastClient版本)
//!
//! 展示如何使用flare-core的FastClient创建QUIC客户端并进行通信

use std::sync::Arc;
use std::time::Instant;
use tracing::{info, error};

// 修改rustls的引用
use rustls::crypto::ring;

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

/// QUIC客户端事件处理器
#[derive(Debug)]
pub struct QuicClientEventHandler {
    pub name: String,
}

impl QuicClientEventHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait::async_trait]
impl ClientEvent for QuicClientEventHandler {
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
impl flare_core::common::connections::event::ConnectionEvent for QuicClientEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] QUIC连接已建立: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] QUIC连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] QUIC连接错误: {} - 错误: {}", self.name, connection_id, error);
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
        
        info!("[{}] 收到QUIC服务器消息: {} - 类型: {} - 内容长度: {}", 
              self.name, connection_id, message.get_command_type_str(), content_length);
    }

    async fn on_message_sent(&self, connection_id: &str, message: &flare_core::common::protocol::Frame) {
        info!("[{}] QUIC数据消息已发送: {} - 类型: {}", 
              self.name, connection_id, message.get_command_type_str());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("[{}] QUIC心跳超时: {}", self.name, connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] QUIC连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        info!("[{}] QUIC心跳已发送: {}", self.name, connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        info!("[{}] 收到QUIC心跳响应: {}", self.name, connection_id);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        info!("[{}] QUIC开始重连: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        info!("[{}] QUIC重连成功: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        error!("[{}] QUIC重连失败: {} - 尝试次数: {} - 错误: {}", self.name, connection_id, attempt, error);
        // 当重连失败时，可以在这里添加终止程序的逻辑
        if attempt >= 5 {
            error!("[{}] 重连尝试次数已达到上限，程序将退出", self.name);
            std::process::exit(1);
        }
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) {
        info!("[{}] QUIC统计信息更新: {} - 收到消息: {} - 发送消息: {}", 
             self.name, connection_id, stats.messages_received, stats.messages_sent);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    // 初始化CryptoProvider
    rustls::crypto::CryptoProvider::install_default(ring::default_provider()).unwrap();
    
    info!("启动FastClient QUIC客户端示例");
    
    // 创建序列化配置
    let serialization_config = SerializationConfig::builder()
        .format(SerializationFormat::Protobuf)
        .build();
    
    // 设置事件处理器
    let event_handler = Arc::new(QuicClientEventHandler::new("FastClient QUIC客户端".to_string()));
    
    // 创建带有自定义事件处理器的客户端
    let mut client = FastClientBuilder::new()
        .with_quic_only()  // 仅使用QUIC协议
        .with_server_address(Transport::Quic, "127.0.0.1:8081".to_string())
        .with_heartbeat(5000, 2000)  // 5秒心跳，2秒超时
        .with_serialization(serialization_config)
        .with_event_handler(event_handler)
        .build();
    
    // 启动客户端
    info!("正在连接QUIC服务端...");
    let connect_start = Instant::now();
    
    // 使用更好的错误处理
    match client.start().await {
        Ok(()) => {
            let connect_time = connect_start.elapsed();
            info!("✅ 已连接到QUIC服务端！连接耗时: {:.2}ms", connect_time.as_secs_f64() * 1000.0);
        }
        Err(e) => {
            error!("❌ 连接QUIC服务端失败: {}", e);
            error!("请确保服务端已启动并监听在 127.0.0.1:8081");
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
        "quic_client".to_string(),
        "quic".to_string(),
        "desktop".to_string(),
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
        "Hello from FastClient QUIC client!".as_bytes().to_vec(),
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