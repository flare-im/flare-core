//! QUIC Protobuf客户端测试
//! 
//! 专门用于测试QUIC连接中使用Protobuf序列化的客户端

use tracing::{info, error};
use std::sync::Arc;

use flare_core::{
    ConnectionConfig, ConnectionType,
    ConnectionEvent, Frame,
    FlareError,
};
use flare_core::common::connections::{
    ConnectionFactory, QuicConfig, ConnectionFactoryTrait
};
use flare_core::common::protocol::{MessageType, Reliability};
use flare_core::common::serialization::SerializationFormat;

type Result<T> = std::result::Result<T, FlareError>;

/// 简单事件处理器
#[derive(Debug)]
pub struct SimpleEventHandler {
    pub name: String,
}

#[async_trait::async_trait]
impl ConnectionEvent for SimpleEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] 连接已建立: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] 连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] 连接错误: {} - 错误: {}", self.name, connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        let payload = message.get_payload();
        if let Ok(text) = String::from_utf8(payload.to_vec()) {
            println!("📨 [服务器回复] {}", text);
            info!("[{}] 📨 收到服务器消息: {} - 内容: {}", self.name, connection_id, text);
        } else {
            println!("📦 [服务器回复] 二进制数据 ({} bytes)", payload.len());
            info!("[{}] 📦 收到二进制消息: {} - 长度: {}", self.name, connection_id, payload.len());
        }
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        let payload = message.get_payload();
        if let Ok(text) = String::from_utf8(payload.to_vec()) {
            info!("[{}] 📤 数据消息已发送 (ID: {}): '{}'", self.name, message.get_message_id(), text);
        } else {
            info!("[{}] 📦 二进制消息已发送 (ID: {}): {} bytes", self.name, message.get_message_id(), payload.len());
        }
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("[{}] 心跳超时: {}", self.name, connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] 连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
    }

    async fn on_heartbeat_sent(&self, connection_id: &str) {
        info!("[{}] 心跳已发送: {}", self.name, connection_id);
    }

    async fn on_heartbeat_received(&self, connection_id: &str) {
        info!("[{}] 收到心跳: {}", self.name, connection_id);
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

impl SimpleEventHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化 TLS 加密提供程序
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("无法初始化 TLS 加密提供程序");
    
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("🚀 启动QUIC Protobuf客户端");
    
    // 创建客户端配置，使用Protobuf序列化
    let mut config = ConnectionConfig::client(
        "quic_protobuf_client".to_string(),
        "127.0.0.1:4433".to_string()
    ).with_type(ConnectionType::Quic)
     .with_quic_config(QuicConfig {
         max_concurrent_streams: 20,
         initial_stream_window: 65536,
         connection_window: 262144,
         congestion_control: "bbr".to_string(),
     })
     .with_heartbeat(5000, 2000)
     .with_tls();
     
    // 设置使用Protobuf序列化
    config.serialization_format = Some(SerializationFormat::Protobuf);
    
    // 创建连接工厂和客户端连接
    let factory = ConnectionFactory::new();
    let mut client_connection = factory.create_client_connection(config).await?;
    
    // 设置事件处理器
    let event_handler = Arc::new(SimpleEventHandler::new("QUIC Protobuf客户端".to_string()));
    client_connection.set_connection_event_handler(event_handler as Arc<dyn ConnectionEvent>).await;
    
    // 建立连接
    info!("📡 正在连接到QUIC服务端...");
    client_connection.connect().await?;
    
    // 等待连接稳定
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    info!("⚡ 开始QUIC Protobuf消息传输测试...");
    
    // 发送测试消息
    let test_messages = vec![
        "Protobuf测试消息1: 高效二进制序列化",
        "Protobuf测试消息2: 跨语言兼容性",
        "Protobuf测试消息3: 强类型模式验证",
    ];
    
    for (i, text) in test_messages.iter().enumerate() {
        let message = Frame::new(
            MessageType::Data,
            (i + 1) as u64,
            Reliability::AtLeastOnce,
            text.as_bytes().to_vec(),
        );
        
        match client_connection.send_message(message).await {
            Ok(_) => {
                info!("⚡ Protobuf消息 {} 已发送: '{}'", i + 1, text);
            }
            Err(e) => {
                error!("❌ Protobuf消息 {} 发送失败: {}", i + 1, e);
                break;
            }
        }
        
        // 等待服务端回复
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
    
    // 等待接收回显消息
    info!("⏱️ 等待接收服务端回显消息...");
    tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;
    
    // 断开连接
    info!("🔌 断开QUIC连接...");
    client_connection.disconnect().await?;
    
    info!("✅ QUIC Protobuf客户端测试完成！");
    Ok(())
}