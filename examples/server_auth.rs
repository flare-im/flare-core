//! 服务端认证示例

use flare_core::common::{
    connections::{
        websocket::WebSocketConnection,
        traits::{ServerConnection, Connection},
        types::{ConnectionConfig, ConnectionRole, ServerSpecificConfig},
        event::ConnectionEvent,
    },
    protocol::Frame,
};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// 自定义服务端事件处理器
struct ServerEventHandler;

#[async_trait::async_trait]
impl ConnectionEvent for ServerEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        println!("服务端连接已建立: {}", connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        println!("服务端连接已断开: {} - 原因: {}", connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        println!("服务端连接错误: {} - 错误: {}", connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        println!("服务端收到消息: {} - 类型: {:?}", connection_id, message.get_message_type());
        
        // 处理认证请求
        if message.is_auth_request() {
            if let Some((user_id, platform, token)) = message.get_auth_request_data() {
                println!("收到认证请求 - 用户ID: {}, 平台: {}, Token: {}", user_id, platform, token);
                
                // 这里应该实现实际的认证逻辑
                // 例如验证token，检查用户是否存在等
                // 简化处理，假设认证总是成功
                
                // 注意：在实际应用中，你可能需要通过其他方式获取WebSocketConnection实例
                // 这里仅作示例展示
            }
        }
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        println!("服务端发送消息: {} - 类型: {:?}", connection_id, message.get_message_type());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        println!("服务端心跳超时: {}", connection_id);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        println!("服务端收到心跳的ping: {}", connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        println!("服务端收到心跳的pong: {}", connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        println!("服务端连接质量变化: {} - 评分: {}", connection_id, quality_score);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        println!("服务端开始重连: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        println!("服务端重连成功: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        println!("服务端重连失败: {} - 尝试次数: {} - 错误: {}", connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) {
        println!("服务端统计信息更新: {} - 收到: {} - 发送: {} - 质量: {}", 
                 connection_id, stats.messages_received, stats.messages_sent, stats.quality_score);
    }
    
    async fn on_authentication_request(&self, connection_id: &str, user_id: &str, platform: &str, token: &str) {
        println!("服务端处理认证请求: {} - 用户ID: {} - 平台: {} - Token: {}", 
                 connection_id, user_id, platform, token);
        
        // 这里应该实现实际的认证逻辑
        // 例如验证token，检查用户是否存在等
        // 简化处理，假设认证总是成功
        
        // 注意：在实际应用中，你可能需要通过其他方式获取WebSocketConnection实例
        // 并调用handle_auth_request方法来处理认证请求
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    // 创建服务端连接配置
    let config = ConnectionConfig {
        id: "server-001".to_string(),
        role: ConnectionRole::Server,
        remote_addr: "127.0.0.1:8080".to_string(), // 示例地址
        local_addr: None,
        serialization_config: None,
        client_config: None,
        server_config: Some(ServerSpecificConfig {
            max_connections: 1000,
            heartbeat_monitor_interval_ms: 30000,
            heartbeat_monitor_timeout_ms: 60000,
        }),
        heartbeat_interval_ms: 30000,
        heartbeat_timeout_ms: 60000,
        auto_heartbeat_response: true,
    };
    
    // 创建WebSocket连接
    let mut connection = WebSocketConnection::new(config);
    
    // 设置事件处理器
    let event_handler = Arc::new(ServerEventHandler);
    connection.set_event_handler(event_handler).await;
    
    // 接受连接
    if let Err(e) = connection.accept().await {
        println!("接受连接失败: {}", e);
        return Ok(());
    }
    
    // 保持程序运行一段时间以观察结果
    sleep(Duration::from_secs(10)).await;
    
    // 关闭连接
    let _ = connection.close().await;
    
    Ok(())
}