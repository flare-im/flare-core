//! 快速测试QUIC客户端
//! 
//! 用于测试QUIC消息发送和接收

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

/// 高性能事件处理器 - 监控消息发送完成状态
#[derive(Debug)]
pub struct TestEventHandler {
    pub message_sent_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub expected_count: usize,
}

impl TestEventHandler {
    pub fn new(expected_count: usize) -> Self {
        Self {
            message_sent_count: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            expected_count,
        }
    }
    
    pub fn get_sent_count(&self) -> usize {
        self.message_sent_count.load(std::sync::atomic::Ordering::SeqCst)
    }
    
    pub fn is_all_sent(&self) -> bool {
        self.get_sent_count() >= self.expected_count
    }
}

#[async_trait::async_trait]
impl ConnectionEvent for TestEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("✅ [QUIC高性能客户端] 连接已建立: {}", connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("❌ [QUIC高性能客户端] 连接已断开: {} - 原因: {}", connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("⚠️  [QUIC高性能客户端] 连接错误: {} - 错误: {}", connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        let payload = message.get_payload();
        if let Ok(text) = String::from_utf8(payload.to_vec()) {
            info!("📩 [QUIC高性能客户端] 收到服务器回复: {} - 内容: '{}'", connection_id, text);
        } else {
            info!("📦 [QUIC高性能客户端] 收到二进制消息: {} - 长度: {}", connection_id, payload.len());
        }
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        let payload = message.get_payload();
        if let Ok(text) = String::from_utf8(payload.to_vec()) {
            let count = self.message_sent_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            info!("📤 [QUIC高性能客户端] 消息已真正发送 {}/{}: {} - 内容: '{}'", 
                  count, self.expected_count, connection_id, text);
        } else {
            let count = self.message_sent_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            info!("📤 [QUIC高性能客户端] 二进制消息已发送 {}/{}: {} - 长度: {}", 
                  count, self.expected_count, connection_id, payload.len());
        }
    }

    async fn on_heartbeat_timeout(&self, _connection_id: &str) {}
    async fn on_heartbeat_ping(&self, _connection_id: &str) {}
    async fn on_heartbeat_pong(&self, _connection_id: &str) {}
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
    
    info!("🚀 启动QUIC超低延迟高性能测试客户端 (使用Protobuf序列化)");
    
    // 准备测试消息
    let test_messages = vec![
        "QUIC高性能消息1: 超低延迟测试",
        "QUIC高性能消息2: 快速双向通信",
        "QUIC高性能消息3: 实时数据传输",
        "QUIC高性能消息4: 多路复用优势",
        "QUIC高性能消息5: 内置可靠传输",
        "QUIC高性能消息6: 最后一条测试消息"
    ];
    
    info!("📊 计划发送 {} 条QUIC消息，使用超低延迟策略", test_messages.len());
    
    // 创建客户端配置
    let mut config = ConnectionConfig::client(
        "quic_test_client".to_string(),
        "127.0.0.1:4433".to_string()
    ).with_type(ConnectionType::Quic)
     .with_quic_config(QuicConfig {
         max_concurrent_streams: 20,
         initial_stream_window: 65536,
         connection_window: 262144,
         congestion_control: "bbr".to_string(),
     })
     .with_heartbeat(15000, 5000)  // 更短的心跳间隔用于高性能
     .with_tls();
     
    // 设置使用Protobuf序列化
    config.serialization_format = Some(SerializationFormat::Protobuf);
    
    // 创建连接工厂和客户端连接
    let factory = ConnectionFactory::new();
    let mut client_connection = factory.create_client_connection(config).await?;
    
    // 设置事件处理器，监控消息发送状态
    let event_handler = Arc::new(TestEventHandler::new(test_messages.len()));
    let event_handler_clone = Arc::clone(&event_handler);
    client_connection.set_connection_event_handler(event_handler_clone as Arc<dyn ConnectionEvent>).await;
    
    // 建立连接
    info!("📡 正在连接到QUIC服务端...");
    client_connection.connect().await?;
    
    // 最小化连接稳定时间 - QUIC连接建立更快
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    info!("⚡ 开始QUIC极速消息传输 (使用Protobuf序列化)...");
    let start_time = std::time::Instant::now();
    
    // 一次性快速批量发送所有消息，利用QUIC多路复用
    for (i, text) in test_messages.iter().enumerate() {
        let message = Frame::new(
            MessageType::Data,
            (i + 1) as u64,
            Reliability::AtLeastOnce,
            text.as_bytes().to_vec(),
        );
        
        match client_connection.send_message(message).await {
            Ok(_) => {
                info!("⚡ QUIC消息 {} 已投递: '{}'", i + 1, text);
            }
            Err(e) => {
                error!("❌ QUIC消息 {} 发送失败: {}", i + 1, e);
                break;
            }
        }
        
        // 给系统一个微小的处理时间，QUIC可以更快
        tokio::task::yield_now().await;
    }
    
    let send_duration = start_time.elapsed();
    info!("⚡ QUIC极速投递完成，耗时: {:?}", send_duration);
    
    // 极短等待消息真正发送完成
    info!("⏱️ 等待QUIC消息发送完成...");
    let mut wait_count = 0;
    const MAX_WAIT_MS: u64 = 2000; // QUIC更快，等待时间更短
    const CHECK_INTERVAL_MS: u64 = 5; // 每5ms检查一次
    
    while !event_handler.is_all_sent() && wait_count < (MAX_WAIT_MS / CHECK_INTERVAL_MS) {
        tokio::time::sleep(tokio::time::Duration::from_millis(CHECK_INTERVAL_MS)).await;
        wait_count += 1;
        
        if wait_count % 100 == 0 { // 每0.5秒输出一次进度
            info!("⏱️ QUIC已发送: {}/{} 条消息...", 
                  event_handler.get_sent_count(), test_messages.len());
        }
    }
    
    if event_handler.is_all_sent() {
        let total_duration = start_time.elapsed();
        info!("✅ 所有QUIC消息发送完成！总耗时: {:?}", total_duration);
        info!("📊 平均每条QUIC消息延迟: {:?}", total_duration / test_messages.len() as u32);
        
        // 计算每秒消息数
        let messages_per_second = (test_messages.len() as f64) / total_duration.as_secs_f64();
        info!("🚀 QUIC消息吞吐量: {:.0} 条/秒", messages_per_second);
        
        // 计算平均延迟（毫秒）
        let avg_latency_ms = total_duration.as_millis() as f64 / test_messages.len() as f64;
        info!("📈 QUIC平均延迟: {:.2}ms (目标: <15ms)", avg_latency_ms);
        
        if avg_latency_ms < 15.0 {
            info!("🎯 延迟目标达成！QUIC性能优秀");
        } else {
            warn!("⚠️ 延迟超过目标值，当前: {:.2}ms", avg_latency_ms);
        }
    } else {
        error!("⚠️ 超时！只发送了 {}/{} 条QUIC消息", 
               event_handler.get_sent_count(), test_messages.len());
    }
    
    // 等待服务端处理所有消息
    info!("⏱️ 等待 0.5 秒让服务端处理所有QUIC消息...");
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    // 断开连接
    info!("🔌 断开QUIC连接...");
    client_connection.disconnect().await?;
    
    info!("✅ QUIC极速测试完成！展现多路复用和内置可靠性优势");
    Ok(())
}