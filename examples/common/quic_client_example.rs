//! QUIC 客户端连接示例
//!
//! 展示如何使用flare-core的QUIC连接功能创建客户端并进行通信

use std::sync::Arc;
use std::time::Instant;
use tracing::{info, error};

use flare_core::{
    common::{
        connections::{
            factory::ConnectionFactory,
            traits::{ConnectionFactory as ConnectionFactoryTrait, ConnectionEvent},
            types::{ConnectionConfig, ConnectionType, QuicConfig},
        },
        protocol::{Frame, MessageType, Reliability},
    },
};

/// QUIC客户端事件处理器
#[derive(Debug)]
pub struct QuicClientEventHandler {
    pub name: String,
}

#[async_trait::async_trait]
impl ConnectionEvent for QuicClientEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] QUIC连接已建立: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] QUIC连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] QUIC连接错误: {} - 错误: {}", self.name, connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        if message.is_heartbeat() {
            info!("[{}] 收到QUIC心跳消息: {}", self.name, connection_id);
        } else {
            let payload = message.get_payload();
            if let Ok(text) = String::from_utf8(payload.to_vec()) {
                info!("[{}] 收到QUIC服务器消息: {} - 内容: {}", self.name, connection_id, text);
            } else {
                info!("[{}] 收到QUIC二进制消息: {} - 长度: {}", self.name, connection_id, payload.len());
            }
        }
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        if message.is_heartbeat() {
            info!("[{}] QUIC心跳消息已发送: {}", self.name, connection_id);
        } else {
            info!("[{}] QUIC数据消息已发送: {}", self.name, connection_id);
        }
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
        info!("[{}] QUIC重连失败: {} - 尝试次数: {} - 错误: {}", self.name, connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) {
        info!("[{}] QUIC统计信息更新: {} - 收到消息: {} - 发送消息: {}", 
             self.name, connection_id, stats.messages_received, stats.messages_sent);
    }
}

impl QuicClientEventHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化 TLS 加密提供程序
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("无法初始化 TLS 加密提供程序");
    
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("启动QUIC客户端示例");
    
    // 创建QUIC客户端配置
    let mut config = ConnectionConfig::client(
        "quic_client_example".to_string(),
        "127.0.0.1:4433".to_string()  // QUIC服务端地址
    ).with_type(ConnectionType::Quic)
     .with_quic_config(QuicConfig {
         max_concurrent_streams: 100,
         initial_stream_window: 1048576,
         connection_window: 4194304,
         congestion_control: "bbr".to_string(),
     })
     .with_heartbeat(5000, 2000)  // 5秒心跳，2秒超时
     .with_tls()  // QUIC强制使用TLS
     .with_json_serialization(); // 使用JSON序列化格式，与服务端保持一致
    
    // 使用默认的JSON序列化
    
    info!("QUIC客户端配置: {:?}", config);
    info!("连接地址: {}", config.remote_addr);
    
    // 创建连接工厂
    let factory = ConnectionFactory::new();
    
    // 创建客户端连接
    let mut client_connection = factory.create_client_connection(config).await?;
    
    // 设置事件处理器
    let event_handler = Arc::new(QuicClientEventHandler::new("QUIC客户端".to_string()));
    client_connection.set_connection_event_handler(event_handler as Arc<dyn ConnectionEvent>).await;
    
    // 建立连接
    info!("正在连接QUIC服务端...");
    let connect_start = Instant::now();
    client_connection.connect().await?;
    let connect_time = connect_start.elapsed();
    info!("✅ 已连接到QUIC服务端！连接耗时: {:.2}ms", connect_time.as_secs_f64() * 1000.0);
    
    // 发送测试消息
    info!("发送测试消息...");
    let test_message = Frame::new(
        MessageType::Data,
        1,
        Reliability::AtLeastOnce,
        "Hello from QUIC client!".as_bytes().to_vec(),
    );
    
    client_connection.send_message(test_message).await?;
    info!("测试消息已发送");
    
    // 等待一段时间以接收响应
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    
    // 断开连接
    info!("正在断开连接...");
    client_connection.disconnect().await?;
    info!("连接已断开");
    
    Ok(())
}