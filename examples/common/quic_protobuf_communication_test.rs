//! QUIC Protobuf通信测试
//! 
//! 测试QUIC连接中使用Protobuf序列化的完整双向通信流程

use tracing::{info, error, warn};
use flare_core::{
    ConnectionConfig, ConnectionType,
    ConnectionEvent, Frame,
    FlareError,
};
use flare_core::common::connections::{
    ConnectionFactory, types::QuicConfig
};
use flare_core::common::connections::traits::ConnectionFactory as ConnectionFactoryTrait;
use flare_core::common::protocol::{MessageType, Reliability};
use flare_core::common::serialization::{SerializationFormat};
use std::sync::Arc;

type Result<T> = std::result::Result<T, FlareError>;

/// 测试事件处理器
#[derive(Debug)]
pub struct TestEventHandler {
    pub messages_received: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub messages_sent: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub expected_count: usize,
    pub echo_messages_received: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub expected_echo_count: usize,
}

impl TestEventHandler {
    pub fn new(expected_echo_count: usize) -> Self {
        Self {
            messages_received: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            messages_sent: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            expected_count: expected_echo_count, // 只期望接收回显消息
            echo_messages_received: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            expected_echo_count,
        }
    }
    
    pub fn get_received_count(&self) -> usize {
        self.messages_received.load(std::sync::atomic::Ordering::SeqCst)
    }
    
    pub fn get_sent_count(&self) -> usize {
        self.messages_sent.load(std::sync::atomic::Ordering::SeqCst)
    }
    
    pub fn get_echo_received_count(&self) -> usize {
        self.echo_messages_received.load(std::sync::atomic::Ordering::SeqCst)
    }
    
    pub fn is_all_received(&self) -> bool {
        self.get_echo_received_count() >= self.expected_echo_count
    }
}

#[async_trait::async_trait]
impl ConnectionEvent for TestEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("✅ [QUIC Protobuf测试] 连接已建立: {}", connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("❌ [QUIC Protobuf测试] 连接已断开: {} - 原因: {}", connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("⚠️  [QUIC Protobuf测试] 连接错误: {} - 错误: {}", connection_id, error);
    }

    async fn on_message_received(&self, _connection_id: &str, message: &Frame) {
        let count = self.messages_received.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        let payload = message.get_payload();
        if let Ok(text) = String::from_utf8(payload.to_vec()) {
            // 检查是否是回显消息（以"ECHO: "开头）
            if text.starts_with("ECHO: ") {
                let echo_count = self.echo_messages_received.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                info!("📩 [QUIC Protobuf测试] 收到回显消息 {}/{}: '{}'", echo_count, self.expected_echo_count, text);
            } else {
                info!("📩 [QUIC Protobuf测试] 收到普通消息 {}/{}: '{}'", count, self.expected_count, text);
            }
        } else {
            info!("📦 [QUIC Protobuf测试] 收到二进制消息 {}/{}: {} 字节", count, self.expected_count, payload.len());
        }
    }

    async fn on_message_sent(&self, _connection_id: &str, message: &Frame) {
        let count = self.messages_sent.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        let payload = message.get_payload();
        if let Ok(text) = String::from_utf8(payload.to_vec()) {
            info!("📤 [QUIC Protobuf测试] 消息已发送 {}/{}: '{}'", count, self.expected_count, text);
        } else {
            info!("📤 [QUIC Protobuf测试] 二进制消息已发送 {}/{}: {} 字节", count, self.expected_count, payload.len());
        }
    }

    async fn on_heartbeat_timeout(&self, _connection_id: &str) {}
    async fn on_heartbeat_sent(&self, _connection_id: &str) {}
    async fn on_heartbeat_received(&self, _connection_id: &str) {}
    async fn on_quality_changed(&self, _connection_id: &str, _quality_score: u8) {}
    async fn on_reconnect_started(&self, _connection_id: &str, _attempt: u32) {}
    async fn on_reconnected(&self, _connection_id: &str, _attempt: u32) {}
    async fn on_reconnect_failed(&self, _connection_id: &str, _attempt: u32, _error: &str) {}
    async fn on_statistics_updated(&self, _connection_id: &str, _stats: &flare_core::common::connections::traits::ConnectionStats) {}
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
    
    info!("🚀 启动QUIC Protobuf双向通信测试");
    
    // 准备测试消息
    let test_messages = vec![
        "Protobuf测试消息1: 高效二进制序列化",
        "Protobuf测试消息2: 跨语言兼容性",
        "Protobuf测试消息3: 强类型模式验证",
        "Protobuf测试消息4: 高性能通信",
        "Protobuf测试消息5: 最后一条测试消息"
    ];
    
    info!("📊 计划发送 {} 条QUIC消息，使用Protobuf序列化", test_messages.len());
    
    // 创建客户端配置，使用Protobuf序列化
    let mut config = ConnectionConfig::client(
        "quic_protobuf_test_client".to_string(),
        "127.0.0.1:4433".to_string()
    ).with_type(ConnectionType::Quic)
     .with_quic_config(QuicConfig {
         max_concurrent_streams: 20,
         initial_stream_window: 65536,
         connection_window: 262144,
         congestion_control: "bbr".to_string(),
     })
     .with_heartbeat(15000, 5000)
     .with_tls();
     
    // 设置使用Protobuf序列化
    config.serialization_format = Some(SerializationFormat::Protobuf);
    
    // 创建连接工厂和客户端连接
    let factory = ConnectionFactory::new();
    let mut client_connection = factory.create_client_connection(config).await?;
    
    // 设置事件处理器
    let event_handler = Arc::new(TestEventHandler::new(test_messages.len())); // 只期望接收回显消息数量
    let event_handler_clone = Arc::clone(&event_handler);
    client_connection.set_connection_event_handler(event_handler_clone as Arc<dyn ConnectionEvent>).await;
    
    // 建立连接
    info!("📡 正在连接到QUIC服务端...");
    client_connection.connect().await?;
    
    // 等待连接稳定
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    info!("⚡ 开始QUIC Protobuf消息传输测试...");
    let start_time = std::time::Instant::now();
    
    // 发送消息
    for (i, text) in test_messages.iter().enumerate() {
        let message = Frame::new(
            MessageType::Data,
            (i + 1) as u64,
            Reliability::AtLeastOnce,
            text.as_bytes().to_vec(),
        );
        
        match client_connection.send_message(message).await {
            Ok(_) => {
                info!("⚡ Protobuf消息 {} 已投递: '{}'", i + 1, text);
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
            info!("⏱️ 已接收回显消息: {}/{} 条...", 
                  event_handler.get_echo_received_count(), test_messages.len());
        }
    }
    
    let total_duration = start_time.elapsed();
    
    if event_handler.is_all_received() {
        info!("✅ 所有Protobuf消息测试完成！总耗时: {:?}", total_duration);
        info!("📊 发送消息数: {}", event_handler.get_sent_count());
        info!("📊 接收回显消息数: {}", event_handler.get_echo_received_count());
        
        // 计算每秒消息数
        let messages_per_second = (test_messages.len() as f64) / total_duration.as_secs_f64();
        info!("🚀 Protobuf消息吞吐量: {:.0} 条/秒", messages_per_second);
        
        // 计算平均延迟（毫秒）
        let avg_latency_ms = total_duration.as_millis() as f64 / test_messages.len() as f64;
        info!("📈 Protobuf平均延迟: {:.2}ms", avg_latency_ms);
    } else {
        warn!("⚠️ 超时！只接收了 {}/{} 条Protobuf回显消息", 
              event_handler.get_echo_received_count(), test_messages.len());
    }
    
    // 等待服务端处理所有消息
    info!("⏱️ 等待 1 秒让服务端处理所有Protobuf消息...");
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    
    // 断开连接
    info!("🔌 断开QUIC连接...");
    client_connection.disconnect().await?;
    
    info!("✅ QUIC Protobuf序列化测试完成！");
    Ok(())
}