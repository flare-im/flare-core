//! WebSocket 客户端连接示例
//!
//! 展示如何使用flare-core的WebSocket连接功能创建客户端并进行通信

use std::sync::Arc;
use std::time::Instant;
use tracing::{info, error};

use flare_core::{
    common::{
        connections::{
            factory::ConnectionFactory,
            traits::ConnectionEvent,
            types::{ConnectionConfig, Transport, WebSocketConfig},
        },
        protocol::{Frame, Reliability},
    },
};

/// WebSocket客户端事件处理器
#[derive(Debug)]
pub struct WebSocketClientEventHandler {
    pub name: String,
}

#[async_trait::async_trait]
impl ConnectionEvent for WebSocketClientEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] WebSocket连接已建立: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] WebSocket连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] WebSocket连接错误: {} - 错误: {}", self.name, connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        info!("[{}] 收到WebSocket消息: {} - 类型: {}", self.name, connection_id, message.get_command_type_str());
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        info!("[{}] WebSocket消息已发送: {} - 类型: {}", self.name, connection_id, message.get_command_type_str());
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
        info!("[{}] WebSocket重连失败: {} - 尝试次数: {} - 错误: {}", self.name, connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) {
        info!("[{}] WebSocket统计信息更新: {} - 收到消息: {} - 发送消息: {}", 
             self.name, connection_id, stats.messages_received, stats.messages_sent);
    }
}

impl WebSocketClientEventHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("启动WebSocket客户端示例");
    
    // 创建WebSocket客户端配置
    let mut config = ConnectionConfig::client(
        "websocket_client_example".to_string(),
        "ws://127.0.0.1:8080".to_string()  // WebSocket服务端地址
    );
    config.transport = Transport::WebSocket;
    config.heartbeat_interval_ms = 5000;  // 5秒心跳
    config.heartbeat_timeout_ms = 2000;   // 2秒超时
    
    // 配置 WebSocket 特定设置
    config.protocol_config.websocket = WebSocketConfig {
        subprotocols: vec!["flare-protocol".to_string()],
        extensions: vec![],
        compression_threshold: Some(1024),
    };
    config.serialization_config = Some(flare_core::common::serialization::SerializationConfig {
        format: flare_core::common::serialization::SerializationFormat::Protobuf,
        enable_encryption: false,
        enable_compression: false,
        compression_level: Some(0),
        pretty_format: false,
        max_message_size: Some(1024 * 1024), // 1MB
        custom_params: std::collections::HashMap::new(),
    });
    
    info!("WebSocket客户端配置: {:?}", config);
    info!("连接地址: {}", config.remote_addr);
    
    // 使用ConnectionFactory创建客户端连接
    let mut client_connection = ConnectionFactory::create_client(config).await?;
    
    // 设置事件处理器
    let event_handler = Arc::new(WebSocketClientEventHandler::new("WebSocket客户端".to_string()));
    client_connection.set_event_handler(event_handler).await;
    
    // 建立连接
    info!("正在连接WebSocket服务端...");
    let connect_start = Instant::now();
    client_connection.connect().await?;
    let connect_time = connect_start.elapsed();
    info!("✅ 已连接到WebSocket服务端！连接耗时: {:.2}ms", connect_time.as_secs_f64() * 1000.0);
    
    // 发送认证消息（简化示例，实际应用中应该使用真实的认证数据）
    info!("发送认证消息...");
    let connect_cmd = flare_core::common::protocol::commands::ConnectCommand::new(
        "websocket_client_example".to_string(),
        "websocket".to_string(),
        "rust".to_string(),
        "1.0.0".to_string()
    );
    let command = flare_core::common::protocol::commands::Command::Control(
        flare_core::common::protocol::commands::ControlCmd::Connect(connect_cmd)
    );
    let auth_message = Frame::new(
        command,
        uuid::Uuid::new_v4().to_string(),
        Reliability::AtLeastOnce,
    );
    
    client_connection.send_message(auth_message).await?;
    info!("认证消息已发送");
    
    // 等待一小段时间确保认证完成
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // 发送测试消息
    info!("发送测试消息...");
    let send_cmd = flare_core::common::protocol::commands::MessageSendCommand::new(
        "Hello from WebSocket client with Protobuf!".as_bytes().to_vec()
    );
    let command = flare_core::common::protocol::commands::Command::Message(
        flare_core::common::protocol::commands::MessageCmd::Send(send_cmd)
    );
    let test_message = Frame::new(
        command,
        uuid::Uuid::new_v4().to_string(),
        Reliability::AtLeastOnce,
    );
    
    client_connection.send_message(test_message).await?;
    info!("测试消息已发送");
    
    // 等待一段时间以接收响应
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    
    // 断开连接
    info!("正在断开连接...");
    client_connection.disconnect(Some("客户端主动断开".to_string())).await?;
    info!("连接已断开");
    
    Ok(())
}