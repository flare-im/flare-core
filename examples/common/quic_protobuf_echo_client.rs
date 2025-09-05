//! QUIC Protobuf回显客户端测试
//! 
//! 专门用于测试QUIC连接中使用Protobuf序列化的回显客户端

use tracing::{info, error, warn};
use std::sync::Arc;

use flare_core::{
    ConnectionConfig, ConnectionType,
    ConnectionEvent, Frame,
    FlareError,
};
use flare_core::common::connections::{
    types::QuicConfig
};
use flare_core::common::connections::traits::{ConnectionFactory, ConnectionFactory as ConnectionFactoryTrait};
use flare_core::common::protocol::{MessageType, Reliability};
use flare_core::common::serialization::SerializationFormat;

type Result<T> = std::result::Result<T, FlareError>;

/// 回显事件处理器
#[derive(Debug)]
pub struct EchoClientEventHandler {
    pub name: String,
    pub messages_received: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub messages_sent: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub expected_count: usize,
}

impl EchoClientEventHandler {
    pub fn new(name: String, expected_count: usize) -> Self {
        Self {
            name,
            messages_received: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            messages_sent: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            expected_count,
        }
    }
    
    pub fn get_received_count(&self) -> usize {
        self.messages_received.load(std::sync::atomic::Ordering::SeqCst)
    }
    
    pub fn get_sent_count(&self) -> usize {
        self.messages_sent.load(std::sync::atomic::Ordering::SeqCst)
    }
    
    pub fn is_all_received(&self) -> bool {
        self.get_received_count() >= self.expected_count
    }
}

#[async_trait::async_trait]
impl ConnectionEvent for EchoClientEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] 连接已建立: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] 连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] 连接错误: {} - 错误: {}", self.name, connection_id, error);
    }

    async fn on_message_received(&self, _connection_id: &str, message: &Frame) {
        let count = self.messages_received.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        let payload = message.get_payload();
        if let Ok(text) = String::from_utf8(payload.to_vec()) {
            println!("📨 [服务器回显] {}", text);
            info!("[{}] 📨 收到服务器回显 {}/{}: '{}'", self.name, count, self.expected_count, text);
        } else {
            println!("📦 [服务器回显] 二进制数据 ({} bytes)", payload.len());
            info!("[{}] 📦 收到二进制回显 {}/{}: {} 字节", self.name, count, self.expected_count, payload.len());
        }
    }

    async fn on_message_sent(&self, _connection_id: &str, message: &Frame) {
        let count = self.messages_sent.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        let payload = message.get_payload();
        if let Ok(text) = String::from_utf8(payload.to_vec()) {
            info!("[{}] 📤 消息已发送 {}/{}: '{}'", self.name, count, self.expected_count, text);
        } else {
            info!("[{}] 📦 二进制消息已发送 {}/{}: {} 字节", self.name, count, self.expected_count, payload.len());
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
    
    info!("🚀 启动QUIC Protobuf回显客户端");
    
    // 创建客户端配置，使用Protobuf序列化
    let mut config = ConnectionConfig::client(
        "quic_protobuf_echo_client".to_string(),
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
    let factory = flare_core::common::connections::ConnectionFactory::new();
    let mut client_connection = factory.create_client_connection(config).await?;
    
    // 准备测试消息
    let test_messages = vec![
        "Protobuf测试消息1: 高效二进制序列化",
        "Protobuf测试消息2: 跨语言兼容性",
        "Protobuf测试消息3: 强类型模式验证",
        "Protobuf测试消息4: 高性能通信",
        "Protobuf测试消息5: 最后一条测试消息"
    ];
    
    // 设置事件处理器
    let event_handler = Arc::new(EchoClientEventHandler::new(
        "QUIC Protobuf回显客户端".to_string(), 
        test_messages.len() // 期望接收与发送相同数量的回显消息
    ));
    client_connection.set_connection_event_handler(event_handler.clone() as Arc<dyn ConnectionEvent>).await;
    
    // 建立连接
    info!("📡 正在连接到QUIC服务端...");
    client_connection.connect().await?;
    
    // 等待连接稳定
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    info!("⚡ 开始QUIC Protobuf回显测试...");
    let start_time = std::time::Instant::now();
    
    // 发送测试消息
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
        
        tokio::task::yield_now().await;
    }
    
    let send_duration = start_time.elapsed();
    info!("⚡ Protobuf消息发送完成，耗时: {:?}", send_duration);
    
    // 等待接收回显消息
    info!("⏱️ 等待接收服务端回显消息...");
    let mut wait_count = 0;
    const MAX_WAIT_MS: u64 = 5000;
    const CHECK_INTERVAL_MS: u64 = 100;
    
    while !event_handler.is_all_received() && wait_count < (MAX_WAIT_MS / CHECK_INTERVAL_MS) {
        tokio::time::sleep(tokio::time::Duration::from_millis(CHECK_INTERVAL_MS)).await;
        wait_count += 1;
        
        if wait_count % 10 == 0 { // 每秒输出一次进度
            info!("⏱️ 已接收回显: {}/{} 条消息...", 
                  event_handler.get_received_count(), test_messages.len());
        }
    }
    
    let total_duration = start_time.elapsed();
    
    if event_handler.is_all_received() {
        info!("✅ 所有Protobuf回显消息测试完成！总耗时: {:?}", total_duration);
        info!("📊 发送消息数: {}", event_handler.get_sent_count());
        info!("📊 接收回显数: {}", event_handler.get_received_count());
        
        // 计算每秒消息数
        let messages_per_second = (test_messages.len() as f64) / total_duration.as_secs_f64();
        info!("🚀 Protobuf消息吞吐量: {:.0} 条/秒", messages_per_second);
        
        // 计算平均延迟（毫秒）
        let avg_latency_ms = total_duration.as_millis() as f64 / test_messages.len() as f64;
        info!("📈 Protobuf平均延迟: {:.2}ms", avg_latency_ms);
    } else {
        warn!("⚠️ 超时！只接收了 {}/{} 条Protobuf回显消息", 
              event_handler.get_received_count(), test_messages.len());
    }
    
    // 等待服务端处理所有消息
    info!("⏱️ 等待 1 秒让服务端处理所有Protobuf消息...");
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    
    // 断开连接
    info!("🔌 断开QUIC连接...");
    client_connection.disconnect().await?;
    
    info!("✅ QUIC Protobuf回显客户端测试完成！");
    Ok(())
}