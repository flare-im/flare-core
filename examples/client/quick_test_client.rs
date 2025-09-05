//! 快速测试客户端
//! 
//! 用于测试消息发送和接收

use tracing::{info, error};
use flare_core::{
    ConnectionConfig, ConnectionType,
    ConnectionEvent, Frame,
    FlareError,
};
use flare_core::common::connections::{
    ConnectionFactory, types::WebSocketConfig
};
use flare_core::common::connections::traits::ConnectionFactory as ConnectionFactoryTrait;
use flare_core::common::protocol::{MessageType, Reliability};
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
        info!("✅ [高性能客户端] 连接已建立: {}", connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("❌ [高性能客户端] 连接已断开: {} - 原因: {}", connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("⚠️  [高性能客户端] 连接错误: {} - 错误: {}", connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        let payload = message.get_payload();
        if let Ok(text) = String::from_utf8(payload.to_vec()) {
            info!("📩 [高性能客户端] 收到服务器回复: {} - 内容: '{}'", connection_id, text);
        } else {
            info!("📦 [高性能客户端] 收到二进制消息: {} - 长度: {}", connection_id, payload.len());
        }
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        let payload = message.get_payload();
        if let Ok(text) = String::from_utf8(payload.to_vec()) {
            let count = self.message_sent_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            info!("📤 [高性能客户端] 消息已真正发送 {}/{}: {} - 内容: '{}'", 
                  count, self.expected_count, connection_id, text);
        } else {
            let count = self.message_sent_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            info!("📤 [高性能客户端] 二进制消息已发送 {}/{}: {} - 长度: {}", 
                  count, self.expected_count, connection_id, payload.len());
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
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("🚀 启动超低延迟高性能测试客户端");
    
    // 准备测试消息
    let test_messages = vec![
        "高性能消息1: 超低延迟测试",
        "高性能消息2: 快速双向通信",
        "高性能消息3: 实时数据传输",
        "高性能消息4: 最后一条测试消息"
    ];
    
    info!("📊 计划发送 {} 条消息，使用超低延迟策略", test_messages.len());
    
    // 创建客户端配置
    let config = ConnectionConfig::client(
        "quick_test_client".to_string(),
        "ws://127.0.0.1:8080".to_string()
    ).with_type(ConnectionType::WebSocket)
     .with_websocket_config(WebSocketConfig {
         subprotocols: vec!["text".to_string()],
         extensions: vec![],
         compression_threshold: None,
     });
    
    // 创建连接工厂和客户端连接
    let factory = ConnectionFactory::new();
    let mut client_connection = factory.create_client_connection(config).await?;
    
    // 设置事件处理器，监控消息发送状态
    let event_handler = Arc::new(TestEventHandler::new(test_messages.len()));
    let event_handler_clone = Arc::clone(&event_handler);
    client_connection.set_connection_event_handler(event_handler_clone as Arc<dyn ConnectionEvent>).await;
    
    // 建立连接
    info!("📡 正在连接到服务端...");
    client_connection.connect().await?;
    
    // 最小化连接稳定时间
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    info!("⚡ 开始极速消息传输...");
    let start_time = std::time::Instant::now();
    
    // 一次性快速批量发送所有消息
    for (i, text) in test_messages.iter().enumerate() {
        let message = Frame::new(
            MessageType::Data,
            (i + 1) as u64,
            Reliability::AtLeastOnce,
            text.as_bytes().to_vec(),
        );
        
        match client_connection.send_message(message).await {
            Ok(_) => {
                info!("⚡ 消息 {} 已投递: '{}'", i + 1, text);
            }
            Err(e) => {
                error!("❌ 消息 {} 发送失败: {}", i + 1, e);
                break;
            }
        }
        
        // 给系统一个微小的处理时间
        tokio::task::yield_now().await;
    }
    
    let send_duration = start_time.elapsed();
    info!("⚡ 极速投递完成，耗时: {:?}", send_duration);
    
    // 极短等待消息真正发送完成
    info!("⏱️ 等待消息发送完成...");
    let mut wait_count = 0;
    const MAX_WAIT_MS: u64 = 3000; // 最多等待3秒
    const CHECK_INTERVAL_MS: u64 = 5; // 每5ms检查一次，更频繁
    
    while !event_handler.is_all_sent() && wait_count < (MAX_WAIT_MS / CHECK_INTERVAL_MS) {
        tokio::time::sleep(tokio::time::Duration::from_millis(CHECK_INTERVAL_MS)).await;
        wait_count += 1;
        
        if wait_count % 200 == 0 { // 每1秒输出一次进度
            info!("⏱️ 已发送: {}/{} 条消息...", 
                  event_handler.get_sent_count(), test_messages.len());
        }
    }
    
    if event_handler.is_all_sent() {
        let total_duration = start_time.elapsed();
        info!("✅ 所有消息发送完成！总耗时: {:?}", total_duration);
        info!("📊 平均每条消息延迟: {:?}", total_duration / test_messages.len() as u32);
        
        // 计算每秒消息数
        let messages_per_second = (test_messages.len() as f64) / total_duration.as_secs_f64();
        info!("🚀 消息吞吐量: {:.0} 条/秒", messages_per_second);
    } else {
        error!("⚠️ 超时！只发送了 {}/{} 条消息", 
               event_handler.get_sent_count(), test_messages.len());
    }
    
    // 等待服务端处理所有消息
    info!("⏱️ 等待 1 秒让服务端处理所有消息...");
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    
    // 断开连接
    info!("🔌 断开连接...");
    client_connection.disconnect().await?;
    
    info!("✅ 极速测试完成！本地通信最优化版本");
    Ok(())
}