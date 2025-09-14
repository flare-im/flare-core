//! 服务端连接事件示例
//!
//! 展示如何使用flare-core的服务端连接事件功能

use std::sync::Arc;
use tracing::{info, error};

use flare_core::{
    server::{
        server::{ServerImpl, ServerConfig},
        ServerConnectionEvent,
        ConnectionManager,
        manager::traits::ServerConnectionManager,
    },
    common::{
        protocol::{Frame, Platform},
        connections::{
            config::ConnectionConfig,
            event::ConnectionEvent,
        },
    },
};

/// 自定义服务端事件处理器
#[derive(Debug)]
pub struct CustomServerEventHandler {
    pub name: String,
}

#[async_trait::async_trait]
impl ConnectionEvent for CustomServerEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] 客户端已连接: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] 客户端已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] 连接错误: {} - 错误: {}", self.name, connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        if message.is_heartbeat() {
            info!("[{}] 收到心跳消息: {}", self.name, connection_id);
        } else {
            let payload = message.get_payload();
            if let Ok(text) = String::from_utf8(payload.to_vec()) {
                info!("[{}] 收到客户端消息: {} - 内容: {}", self.name, connection_id, text);
            } else {
                info!("[{}] 收到二进制消息: {} - 长度: {}", self.name, connection_id, payload.len());
            }
        }
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        if message.is_heartbeat() {
            info!("[{}] 心跳消息已发送: {}", self.name, connection_id);
        } else {
            info!("[{}] 数据消息已发送: {}", self.name, connection_id);
        }
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("[{}] 心跳超时: {}", self.name, connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] 连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        info!("[{}] 心跳已发送: {}", self.name, connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        info!("[{}] 收到心跳响应: {}", self.name, connection_id);
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

#[async_trait::async_trait]
impl ServerConnectionEvent for CustomServerEventHandler {
    async fn on_user_authenticated(&self, connection_id: &str, user_id: &str, platform: &Platform) {
        info!("[{}] 用户认证成功: 连接={} 用户={} 平台={:?}", self.name, connection_id, user_id, platform);
    }
    
    async fn on_user_disconnected(&self, connection_id: &str, user_id: &str, reason: &str) {
        info!("[{}] 用户连接断开: 连接={} 用户={} 原因={}", self.name, connection_id, user_id, reason);
    }
    
    async fn on_authentication_failed(&self, connection_id: &str, error: &str) {
        info!("[{}] 连接认证失败: 连接={} 错误={}", self.name, connection_id, error);
    }
    
    async fn on_authentication_timeout(&self, connection_id: &str) {
        info!("[{}] 连接认证超时: 连接={}", self.name, connection_id);
    }
    
    async fn on_user_message(&self, connection_id: &str, user_id: &str, message: &Frame) -> bool {
        info!("[{}] 收到用户消息: 连接={} 用户={} 类型={:?}", self.name, connection_id, user_id, message.get_message_type());
        // 继续处理消息
        true
    }
    
    async fn on_connection_count_changed(&self, total_connections: usize, authenticated_users: usize) {
        info!("[{}] 连接数量变化: 总连接数={} 已认证用户数={}", self.name, total_connections, authenticated_users);
    }
    
    async fn on_user_online(&self, user_id: &str, platform: &Platform, connection_id: &str) {
        info!("[{}] 用户上线: 用户={} 平台={:?} 连接={}", self.name, user_id, platform, connection_id);
    }
    
    async fn on_user_offline(&self, user_id: &str, platform: &Platform) {
        info!("[{}] 用户下线: 用户={} 平台={:?}", self.name, user_id, platform);
    }
}

impl CustomServerEventHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    // 创建连接管理器
    let connection_manager = Arc::new(ConnectionManager::new());
    
    // 创建自定义事件处理器
    let event_handler = Arc::new(CustomServerEventHandler::new("服务端".to_string()));
    
    // 创建服务器配置
    let config = ServerConfig::new()
        .with_local_addr("127.0.0.1:8084".to_string())
        .with_connection_timeout_ms(30000)
        .with_heartbeat_interval_ms(10000)
        .with_max_connections(100);
    
    // 创建服务器实例
    let server = ServerImpl::with_event_handler(
        config,
        connection_manager,
        event_handler,
    );
    
    // 启动服务器
    info!("正在启动服务器...");
    server.start().await?;
    info!("服务器已启动，监听地址: 127.0.0.1:8084");
    
    // 模拟运行一段时间
    info!("服务器正在运行，按 Ctrl+C 停止...");
    tokio::signal::ctrl_c().await?;
    
    // 停止服务器
    server.stop().await;
    info!("服务器已停止");
    
    Ok(())
}