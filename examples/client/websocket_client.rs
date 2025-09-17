//! WebSocket 客户端连接示例
//!
//! 展示如何使用flare-core的WebSocket连接功能创建客户端并进行通信
//! 支持单独QUIC、单独WebSocket和协议竞速模式

use std::sync::Arc;
use std::time::Instant;
use tracing::{info, error};

use flare_core::{
    common::{
        connections::{
            factory::ConnectionFactory,
            traits::{ConnectionFactory as ConnectionFactoryTrait, ConnectionEvent},
            types::{ConnectionConfig, Transport, WebSocketConfig},
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

    async fn on_message_received(&self, connection_id: &str, _message: &flare_core::common::protocol::Frame) {
        info!("[{}] 收到WebSocket服务器消息: {}", self.name, connection_id);
    }

    async fn on_message_sent(&self, connection_id: &str, _message: &flare_core::common::protocol::Frame) {
        info!("[{}] WebSocket数据消息已发送: {}", self.name, connection_id);
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
        "websocket_client".to_string(),  // 更新为websocket_client
        "ws://127.0.0.1:8080".to_string()  // WebSocket服务端地址 (修改为8080端口)
    );
    
    // 设置传输类型为WebSocket
    config.transport = Transport::WebSocket;
    
    // 设置WebSocket配置
    config.protocol_config.websocket = WebSocketConfig {
         subprotocols: vec!["binary".to_string()],
         extensions: vec![],
         compression_threshold: Some(128),
    };
    
    // 设置心跳和序列化
    config = config
     .with_heartbeat(5000, 2000)  // 5秒心跳，2秒超时
     .with_serialization_config(SerializationConfig {
         format: SerializationFormat::Protobuf,  // 使用Protobuf序列化
         ..Default::default()
     });
    
    info!("WebSocket客户端配置: {:?}", config);
    info!("连接地址: {}", config.remote_addr);
    
    // 创建连接工厂
    let factory = ConnectionFactory::new();
    
    // 创建客户端连接
    let mut client_connection = factory.create_client_connection(config).await?;
    
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
    let message_id = FrameFactory::generate_message_id();
    let auth_message = FrameFactory::create_connect_frame(
        message_id,
        "websocket_client".to_string(),
        "websocket".to_string(),
        "web".to_string(),
        "1.0.0".to_string(),
    )?;
    
    client_connection.send_message(auth_message).await?;
    info!("认证消息已发送");
    
    // 等待一小段时间确保认证完成
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // 发送测试消息
    info!("发送测试消息...");
    let message_id = FrameFactory::generate_message_id();
    let test_message = FrameFactory::create_message_frame(
        message_id,
        "Hello from WebSocket client!".as_bytes().to_vec(),
        Reliability::AtLeastOnce,
    )?;
    
    client_connection.send_message(test_message).await?;
    info!("测试消息已发送");
    
    // 等待一段时间以接收响应
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    
    // 断开连接
    info!("正在断开连接...");
    client_connection.disconnect(Some("Client disconnect".to_string())).await?;
    info!("连接已断开");
    
    Ok(())
}