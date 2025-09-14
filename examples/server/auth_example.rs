//! 服务端认证处理示例
//!
//! 展示如何在服务端处理客户端的认证请求

use flare_core::{
    common::{
        connections::{
            websocket::WebSocketConnection,
            traits::{ServerConnection, Connection},
            types::{ConnectionConfig, ConnectionRole, ServerSpecificConfig},
            event::ConnectionEvent,
        },
        protocol::Frame,
    },
    server::{
        manager::{
            ConnectionManager,
            UserConnectionManager,
        },
        auth::SimpleAuthHandler,
        auth_handler::ServerAuthHandler,
        auth_event_handler::AuthEventHandler,
    },
};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// 自定义服务端事件处理器
struct ServerAuthEventHandler {
    /// 认证事件处理器
    auth_event_handler: Arc<AuthEventHandler>,
}

#[async_trait::async_trait]
impl ConnectionEvent for ServerAuthEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        println!("服务端连接已建立: {}", connection_id);
        // 调用认证事件处理器的on_connected方法
        self.auth_event_handler.on_connected(connection_id).await;
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        println!("服务端连接已断开: {} - 原因: {}", connection_id, reason);
        // 调用认证事件处理器的on_disconnected方法
        self.auth_event_handler.on_disconnected(connection_id, reason).await;
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        println!("服务端连接错误: {} - 错误: {}", connection_id, error);
        // 调用认证事件处理器的on_error方法
        self.auth_event_handler.on_error(connection_id, error).await;
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        println!("服务端收到消息: {} - 类型: {:?}", connection_id, message.get_message_type());
        // 调用认证事件处理器的on_message_received方法
        self.auth_event_handler.on_message_received(connection_id, message).await;
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        println!("服务端发送消息: {} - 类型: {:?}", connection_id, message.get_message_type());
        // 调用认证事件处理器的on_message_sent方法
        self.auth_event_handler.on_message_sent(connection_id, message).await;
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        println!("服务端心跳超时: {}", connection_id);
        // 调用认证事件处理器的on_heartbeat_timeout方法
        self.auth_event_handler.on_heartbeat_timeout(connection_id).await;
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        println!("服务端收到心跳的ping: {}", connection_id);
        // 调用认证事件处理器的on_heartbeat_ping方法
        self.auth_event_handler.on_heartbeat_ping(connection_id).await;
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        println!("服务端收到心跳的pong: {}", connection_id);
        // 调用认证事件处理器的on_heartbeat_pong方法
        self.auth_event_handler.on_heartbeat_pong(connection_id).await;
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        println!("服务端连接质量变化: {} - 评分: {}", connection_id, quality_score);
        // 调用认证事件处理器的on_quality_changed方法
        self.auth_event_handler.on_quality_changed(connection_id, quality_score).await;
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        println!("服务端开始重连: {} - 尝试次数: {}", connection_id, attempt);
        // 调用认证事件处理器的on_reconnect_started方法
        self.auth_event_handler.on_reconnect_started(connection_id, attempt).await;
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        println!("服务端重连成功: {} - 尝试次数: {}", connection_id, attempt);
        // 调用认证事件处理器的on_reconnected方法
        self.auth_event_handler.on_reconnected(connection_id, attempt).await;
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        println!("服务端重连失败: {} - 尝试次数: {} - 错误: {}", connection_id, attempt, error);
        // 调用认证事件处理器的on_reconnect_failed方法
        self.auth_event_handler.on_reconnect_failed(connection_id, attempt, error).await;
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) {
        println!("服务端统计信息更新: {} - 收到: {} - 发送: {} - 质量: {}", 
                 connection_id, stats.messages_received, stats.messages_sent, stats.quality_score);
        // 调用认证事件处理器的on_statistics_updated方法
        self.auth_event_handler.on_statistics_updated(connection_id, stats).await;
    }
    
    async fn on_authentication_request(&self, connection_id: &str, user_id: &str, platform: &str, token: &str) {
        println!("服务端收到认证请求: {} - 用户ID: {} - 平台: {} - Token: {}", 
                 connection_id, user_id, platform, token);
        // 调用认证事件处理器的on_authentication_request方法
        self.auth_event_handler.on_authentication_request(connection_id, user_id, platform, token).await;
    }
    
    async fn on_authentication_response(&self, connection_id: &str, success: bool, user_info: Option<Vec<u8>>, error_message: Option<String>) {
        println!("服务端收到认证响应: {} - 成功: {} - 用户信息长度: {:?} - 错误信息: {:?}", 
                 connection_id, success, user_info.as_ref().map(|v| v.len()), error_message);
        // 调用认证事件处理器的on_authentication_response方法
        self.auth_event_handler.on_authentication_response(connection_id, success, user_info, error_message).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    // 创建基础连接管理器
    let base_manager = Arc::new(ConnectionManager::new());
    
    // 创建用户连接管理器
    let user_connection_manager = Arc::new(UserConnectionManager::new(base_manager));
    
    // 创建认证处理器
    let auth_handler = Arc::new(SimpleAuthHandler::new());
    
    // 添加一些测试用户
    auth_handler.add_user("token123".to_string(), "user123".to_string()).await;
    auth_handler.add_user("token456".to_string(), "user456".to_string()).await;
    
    // 创建服务端认证处理器
    let server_auth_handler = Arc::new(ServerAuthHandler::new(
        Arc::clone(&user_connection_manager),
        Arc::clone(&auth_handler),
    ));
    
    // 创建认证事件处理器
    let auth_event_handler = Arc::new(AuthEventHandler::new(Arc::clone(&user_connection_manager)));
    
    // 创建服务端事件处理器
    let event_handler = Arc::new(ServerAuthEventHandler {
        auth_event_handler: Arc::clone(&auth_event_handler),
    });
    
    // 创建服务端连接配置
    let config = ConnectionConfig::server(
        "server-001".to_string(),
        "127.0.0.1:8080".to_string(), // 示例地址
    )
    .with_heartbeat(30000, 10000)
    .with_heartbeat_monitoring(60000, 300000);
    
    // 创建WebSocket连接
    let mut connection = WebSocketConnection::new(config);
    
    // 设置事件处理器
    connection.set_event_handler(event_handler).await;
    
    // 接受连接
    if let Err(e) = connection.accept().await {
        println!("接受连接失败: {}", e);
        return Ok(());
    }
    
    // 保持程序运行一段时间以观察结果
    sleep(Duration::from_secs(10)).await;
    
    // 关闭连接
    let _ = connection.close(None).await;
    
    Ok(())
}