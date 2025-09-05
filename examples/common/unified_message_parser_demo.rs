// 统一消息解析器演示
// 演示如何使用统一消息解析器处理来自不同协议的消息

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{info};

use flare_core::common::{
    connections::{
        event::DefConnectionEventHandler,
        traits::ConnectionStats,
    },
    messaging::MessageParser,
    protocol::{Frame, MessageType, Reliability},
    serialization::factory,
};

// 演示事件处理器
#[derive(Debug, Clone, Default)]
struct DemoEventHandler;

#[async_trait::async_trait]
impl flare_core::common::connections::event::ConnectionEvent for DemoEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("连接已建立: {}", connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("连接已断开: {} 原因: {}", connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        info!("连接错误: {} 错误: {}", connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        info!("收到消息: {} 类型: {:?} 内容长度: {}", 
              connection_id, message.get_message_type(), message.get_payload().len());
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        info!("发送消息: {} 类型: {:?}", connection_id, message.get_message_type());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("心跳超时: {}", connection_id);
    }

    async fn on_heartbeat_sent(&self, connection_id: &str) {
        info!("发送心跳: {}", connection_id);
    }

    async fn on_heartbeat_received(&self, connection_id: &str) {
        info!("收到心跳: {}", connection_id);
    }

    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("质量变化: {} 新质量: {}", connection_id, quality_score);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        info!("开始重连: {} 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        info!("重连成功: {} 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        info!("重连失败: {} 尝试次数: {} 错误: {}", connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &ConnectionStats) {
        info!("统计更新: {} 消息接收: {} 消息发送: {}", 
              connection_id, stats.messages_received, stats.messages_sent);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    info!("开始统一消息解析器演示");
    
    // 创建事件处理器
    let handler = Arc::new(DemoEventHandler::default());
    
    // 创建统计信息
    let stats = Arc::new(RwLock::new(ConnectionStats::default()));
    
    // 创建序列化器
    let serializer = Arc::new(factory::json_serializer());
    
    // 为QUIC连接创建消息解析器
    let quic_parser = MessageParser::new(
        "quic-connection-1".to_string(),
        Arc::clone(&handler) as Arc<dyn flare_core::common::connections::event::ConnectionEvent>,
        Arc::clone(&stats),
        Arc::clone(&serializer),
    );
    
    // 为WebSocket连接创建消息解析器
    let ws_parser = MessageParser::new(
        "websocket-connection-1".to_string(),
        Arc::clone(&handler) as Arc<dyn flare_core::common::connections::event::ConnectionEvent>,
        Arc::clone(&stats),
        Arc::clone(&serializer),
    );
    
    // 演示1: QUIC连接解析普通数据消息
    info!("演示1: QUIC连接解析普通数据消息");
    let data_frame = Frame::new(
        MessageType::Data,
        1,
        Reliability::AtLeastOnce,
        b"Hello from QUIC connection!".to_vec(),
    );
    
    let serialized_data = serializer.serialize(&data_frame).await?;
    quic_parser.parse_and_handle(serialized_data).await;
    
    // 等待事件处理完成
    sleep(Duration::from_millis(100)).await;
    
    // 演示2: WebSocket连接解析心跳消息
    info!("演示2: WebSocket连接解析心跳消息");
    let heartbeat_frame = Frame::heartbeat();
    let serialized_heartbeat = serializer.serialize(&heartbeat_frame).await?;
    ws_parser.parse_and_handle(serialized_heartbeat).await;
    
    // 等待事件处理完成
    sleep(Duration::from_millis(100)).await;
    
    // 演示3: WebSocket连接处理Ping消息（模拟）
    info!("演示3: WebSocket连接处理Ping消息");
    ws_parser.handle_websocket_ping().await;
    
    // 等待事件处理完成
    sleep(Duration::from_millis(100)).await;
    
    // 演示4: WebSocket连接处理Pong消息（模拟）
    info!("演示4: WebSocket连接处理Pong消息");
    ws_parser.handle_websocket_pong().await;
    
    // 等待事件处理完成
    sleep(Duration::from_millis(100)).await;
    
    // 检查统计信息
    let stats_snapshot = stats.read().await;
    info!("最终统计信息: 收到消息数: {}", stats_snapshot.messages_received);
    
    info!("统一消息解析器演示完成");
    Ok(())
}