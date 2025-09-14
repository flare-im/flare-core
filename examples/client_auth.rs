//! 客户端认证示例

use flare_core::common::{
    connections::{
        websocket::WebSocketConnection,
        traits::{ClientConnection, Connection},
        types::{ConnectionConfig, ConnectionRole, ClientSpecificConfig},
        event::ConnectionEvent,
    },
    protocol::Frame,
};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// 自定义客户端事件处理器
struct ClientEventHandler;

#[async_trait::async_trait]
impl ConnectionEvent for ClientEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        println!("客户端连接已建立: {}", connection_id);
        
        // 连接建立后发送认证请求
        // 注意：在实际应用中，你可能需要通过其他方式获取WebSocketConnection实例
        // 这里仅作示例展示
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        println!("客户端连接已断开: {} - 原因: {}", connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        println!("客户端连接错误: {} - 错误: {}", connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        println!("客户端收到消息: {} - 类型: {:?}", connection_id, message.get_message_type());
        
        // 处理认证响应
        if message.is_auth_response() {
            if let Some((success, user_info, error_message)) = message.get_auth_response_data() {
                if success {
                    println!("客户端认证成功");
                    if let Some(info) = user_info {
                        println!("用户信息长度: {}", info.len());
                    }
                } else {
                    println!("客户端认证失败: {:?}", error_message);
                }
            }
        }
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        println!("客户端发送消息: {} - 类型: {:?}", connection_id, message.get_message_type());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        println!("客户端心跳超时: {}", connection_id);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        println!("客户端收到心跳的ping: {}", connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        println!("客户端收到心跳的pong: {}", connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        println!("客户端连接质量变化: {} - 评分: {}", connection_id, quality_score);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        println!("客户端开始重连: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        println!("客户端重连成功: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        println!("客户端重连失败: {} - 尝试次数: {} - 错误: {}", connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) {
        println!("客户端统计信息更新: {} - 收到: {} - 发送: {} - 质量: {}", 
                 connection_id, stats.messages_received, stats.messages_sent, stats.quality_score);
    }
    
    async fn on_authentication_response(&self, connection_id: &str, success: bool, user_info: Option<Vec<u8>>, error_message: Option<String>) {
        if success {
            println!("客户端认证成功: {} - 用户信息长度: {:?}", connection_id, user_info.as_ref().map(|v| v.len()));
        } else {
            println!("客户端认证失败: {} - 错误信息: {:?}", connection_id, error_message);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    // 创建客户端连接配置
    let config = ConnectionConfig {
        id: "client-001".to_string(),
        role: ConnectionRole::Client,
        remote_addr: "ws://127.0.0.1:8080/ws".to_string(), // 示例地址
        local_addr: None,
        serialization_config: None,
        client_config: Some(ClientSpecificConfig {
            max_reconnect_attempts: 3,
            reconnect_delay_ms: 1000,
        }),
        server_config: None,
        heartbeat_interval_ms: 30000,
        heartbeat_timeout_ms: 60000,
        auto_heartbeat_response: true,
    };
    
    // 创建WebSocket连接
    let mut connection = WebSocketConnection::new(config);
    
    // 设置事件处理器
    let event_handler = Arc::new(ClientEventHandler);
    connection.set_event_handler(event_handler).await;
    
    // 连接服务器
    if let Err(e) = connection.connect().await {
        println!("连接失败: {}", e);
        return Ok(());
    }
    
    // 等待一段时间确保连接建立
    sleep(Duration::from_secs(1)).await;
    
    // 发送认证请求
    if let Err(e) = connection.send_auth_request("user123", "web", "token123").await {
        println!("发送认证请求失败: {}", e);
    }
    
    // 保持程序运行一段时间以观察结果
    sleep(Duration::from_secs(10)).await;
    
    // 断开连接
    let _ = connection.disconnect().await;
    
    Ok(())
}