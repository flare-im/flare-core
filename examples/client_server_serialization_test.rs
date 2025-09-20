//! 客户端和服务端序列化配置一致性测试
//!
//! 演示如何统一配置客户端和服务端的序列化方式

use std::sync::Arc;
use tokio::time::sleep;
use std::time::Duration;
use tracing::{info, error};

use flare_core::{
    server::{
        config::{ServerConfig, ProtocolConfig},
        fast::server::FastServer,
    },
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

/// 测试客户端事件处理器
#[derive(Debug)]
pub struct TestClientEventHandler {
    pub name: String,
}

impl TestClientEventHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait::async_trait]
impl ClientEvent for TestClientEventHandler {
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
impl flare_core::common::connections::event::ConnectionEvent for TestClientEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] 连接已建立: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] 连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] 连接错误: {} - 错误: {}", self.name, connection_id, error);
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
    
    info!("启动客户端和服务端序列化配置一致性测试");
    
    // 创建统一的序列化配置
    let serialization_config = SerializationConfig::builder()
        .format(SerializationFormat::Protobuf)
        .build();
    
    info!("使用序列化格式: {:?}", serialization_config.format);
    
    // 创建服务端配置 - 使用Protobuf序列化
    let mut server_config = ServerConfig::default_websocket();
    server_config = server_config.with_websocket_config(
        ProtocolConfig::new()
            .with_listen_addr("127.0.0.1:8080".to_string())
            .with_max_connections(1000)
    );
    server_config = server_config.with_connection_timeout_ms(30000);
    server_config = server_config.with_heartbeat_interval_ms(10000);
    server_config = server_config.with_auth_timeout_ms(30000);
    // 设置使用Protobuf序列化
    server_config = server_config.with_serialization_config(serialization_config.clone());
    
    // 创建FastServer实例
    let server = FastServer::new_with_config(server_config);
    
    // 启动服务端
    server.start().await?;
    info!("✅ WebSocket服务端已启动，监听地址: 127.0.0.1:8080");
    
    // 等待服务端完全启动
    sleep(Duration::from_secs(1)).await;
    
    // 创建客户端配置 - 使用Protobuf序列化
    let event_handler = Arc::new(TestClientEventHandler::new("测试客户端".to_string()));
    let mut client = FastClientBuilder::new()
        .with_websocket_only()  // 仅使用WebSocket协议
        .with_server_address(Transport::WebSocket, "ws://127.0.0.1:8080".to_string())
        .with_heartbeat(5000, 2000)  // 5秒心跳，2秒超时
        .with_serialization(serialization_config)
        .with_event_handler(event_handler)
        .build();
    
    // 启动客户端
    info!("正在连接WebSocket服务端...");
    client.start().await?;
    info!("✅ 已连接到WebSocket服务端！");
    
    // 发送测试消息
    info!("发送测试消息...");
    let message_id = FrameFactory::generate_message_id();
    let test_message = FrameFactory::create_message_frame(
        message_id,
        "Hello from client-server serialization test!".as_bytes().to_vec(),
        Reliability::AtLeastOnce,
    )?;
    
    client.send_message(test_message).await?;
    info!("测试消息已发送");
    
    // 等待一段时间以接收响应
    sleep(Duration::from_secs(5)).await;
    
    // 停止客户端和服务端
    info!("正在停止客户端...");
    client.stop().await?;
    info!("客户端已停止");
    
    info!("正在停止服务端...");
    server.stop().await;
    info!("服务端已停止");
    
    info!("✅ 客户端和服务端序列化配置一致性测试完成！");
    
    Ok(())
}