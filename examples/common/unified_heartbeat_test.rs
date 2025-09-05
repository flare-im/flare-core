//! 统一心跳测试示例
//! 
//! 测试QUIC和WebSocket连接的心跳消息统一处理

use std::sync::Arc;
use tracing::{info, error};
use tokio::time::{Duration, sleep};

use flare_core::{
    common::{
        connections::{
            event::ConnectionEvent,
            types::{ConnectionConfig, ConnectionType},
            traits::ConnectionFactory,
        },
        protocol::{Frame, MessageType},
    },
};

/// 统一的心跳事件处理器
struct UnifiedHeartbeatHandler {
    name: String,
}

#[async_trait::async_trait]
impl ConnectionEvent for UnifiedHeartbeatHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] 连接已建立: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] 连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        info!("[{}] 连接错误: {} - 错误: {}", self.name, connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        if message.is_heartbeat() {
            match message.get_message_type() {
                MessageType::Heartbeat => {
                    info!("[{}] ❤️  收到心跳: {}", self.name, connection_id);
                }
                MessageType::HeartbeatAck => {
                    info!("[{}] 💗 收到心跳确认: {}", self.name, connection_id);
                }
                _ => {
                    info!("[{}] 💓 收到其他心跳消息: {} - 类型: {:?}", self.name, connection_id, message.get_message_type());
                }
            }
        } else {
            let payload = message.get_payload();
            if let Ok(text) = String::from_utf8(payload.to_vec()) {
                info!("[{}] 📨 收到消息: {} - 内容: {}", self.name, connection_id, text);
            } else {
                info!("[{}] 📦 收到二进制消息: {} - 长度: {}", self.name, connection_id, payload.len());
            }
        }
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        if message.is_heartbeat() {
            info!("[{}] ❤️  心跳消息已发送: {} - 类型: {:?}", self.name, connection_id, message.get_message_type());
        } else {
            let payload = message.get_payload();
            if let Ok(text) = String::from_utf8(payload.to_vec()) {
                info!("[{}] 📤 数据消息已发送 (ID: {}): '{}'", self.name, message.get_message_id(), text);
            } else {
                info!("[{}] 📦 二进制消息已发送 (ID: {}): {} bytes", self.name, message.get_message_id(), payload.len());
            }
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
        info!("[{}] 统计信息更新: {} - 收到: {} - 发送: {} - 质量: {}", 
              self.name, connection_id, stats.messages_received, stats.messages_sent, stats.quality_score);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    info!("开始统一心跳测试");
    
    // 创建WebSocket连接配置
    let ws_config = ConnectionConfig::client(
        "ws-test-connection".to_string(),
        "ws://127.0.0.1:8080/ws".to_string(), // WebSocket服务器地址
    ).with_heartbeat(5000, 15000)  // 心跳间隔5秒，心跳超时15秒
     .with_reconnect(3, 1000);     // 最大重连次数3，重连延迟1秒
    
    // 创建QUIC连接配置
    let quic_config = ConnectionConfig::client(
        "quic-test-connection".to_string(),
        "127.0.0.1:4433".to_string(), // QUIC服务器地址
    ).with_type(ConnectionType::Quic)
     .with_heartbeat(5000, 15000)  // 心跳间隔5秒，心跳超时15秒
     .with_reconnect(3, 1000)      // 最大重连次数3，重连延迟1秒
     .with_tls();                  // QUIC需要TLS
    
    // 创建连接工厂
    let factory = flare_core::common::connections::ConnectionFactory::new();
    
    // 创建QUIC连接
    let mut quic_connection = factory.create_client_connection(quic_config).await?;
    
    // 创建WebSocket连接
    let mut ws_connection = factory.create_client_connection(ws_config).await?;
    
    // 设置事件处理器
    let quic_handler = Arc::new(UnifiedHeartbeatHandler { name: "QUIC客户端".to_string() });
    quic_connection.set_connection_event_handler(Arc::clone(&quic_handler) as Arc<dyn ConnectionEvent>).await;
    
    let ws_handler = Arc::new(UnifiedHeartbeatHandler { name: "WebSocket客户端".to_string() });
    ws_connection.set_connection_event_handler(Arc::clone(&ws_handler) as Arc<dyn ConnectionEvent>).await;
    
    // 尝试连接（这里仅作示例，实际需要服务器运行）
    info!("尝试建立QUIC连接...");
    match quic_connection.connect().await {
        Ok(_) => {
            info!("QUIC连接建立成功");
            
            // 发送心跳测试
            info!("发送QUIC心跳测试...");
            if let Err(e) = quic_connection.send_heartbeat().await {
                error!("QUIC心跳发送失败: {}", e);
            }
            
            // 等待一段时间以接收心跳响应
            sleep(Duration::from_secs(2)).await;
            
            // 断开连接
            if let Err(e) = quic_connection.disconnect().await {
                error!("QUIC断开连接失败: {}", e);
            }
        }
        Err(e) => {
            error!("QUIC连接建立失败: {}", e);
        }
    }
    
    info!("尝试建立WebSocket连接...");
    match ws_connection.connect().await {
        Ok(_) => {
            info!("WebSocket连接建立成功");
            
            // 发送心跳测试
            info!("发送WebSocket心跳测试...");
            if let Err(e) = ws_connection.send_heartbeat().await {
                error!("WebSocket心跳发送失败: {}", e);
            }
            
            // 等待一段时间以接收心跳响应
            sleep(Duration::from_secs(2)).await;
            
            // 断开连接
            if let Err(e) = ws_connection.disconnect().await {
                error!("WebSocket断开连接失败: {}", e);
            }
        }
        Err(e) => {
            error!("WebSocket连接建立失败: {}", e);
        }
    }
    
    info!("统一心跳测试完成");
    Ok(())
}