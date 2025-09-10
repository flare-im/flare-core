//! QUIC 服务端连接示例
//!
//! 展示如何使用flare-core的QUIC连接功能创建服务端并进行通信

use std::sync::Arc;
use tracing::info;

use flare_core::{
    common::{
        connections::{
            traits::ConnectionEvent,
        },
        protocol::Frame,
    },
    server::{
        Server, ServerConfig, ConnectionBasedManager,
        EchoMessageHandler,
    },
};

/// QUIC服务端事件处理器
#[derive(Debug)]
pub struct QuicServerEventHandler {
    pub name: String,
}

#[async_trait::async_trait]
impl ConnectionEvent for QuicServerEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] QUIC客户端已连接: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] QUIC客户端已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        tracing::error!("[{}] QUIC连接错误: {} - 错误: {}", self.name, connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        if message.is_heartbeat() {
            info!("[{}] 收到QUIC心跳消息: {}", self.name, connection_id);
        } else {
            let payload = message.get_payload();
            if let Ok(text) = String::from_utf8(payload.to_vec()) {
                info!("[{}] 收到QUIC客户端消息: {} - 内容: {}", self.name, connection_id, text);
            } else {
                info!("[{}] 收到QUIC二进制消息: {} - 长度: {}", self.name, connection_id, payload.len());
            }
        }
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        if message.is_heartbeat() {
            info!("[{}] QUIC心跳消息已发送: {}", self.name, connection_id);
        } else {
            info!("[{}] QUIC数据消息已发送: {}", self.name, connection_id);
        }
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("[{}] QUIC心跳超时: {}", self.name, connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] QUIC连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        info!("[{}] QUIC心跳已发送: {}", self.name, connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        info!("[{}] 收到QUIC心跳响应: {}", self.name, connection_id);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        info!("[{}] QUIC开始重连: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        info!("[{}] QUIC重连成功: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        info!("[{}] QUIC重连失败: {} - 尝试次数: {} - 错误: {}", self.name, connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) {
        info!("[{}] QUIC统计信息更新: {} - 收到消息: {} - 发送消息: {}", 
             self.name, connection_id, stats.messages_received, stats.messages_sent);
    }
}

impl QuicServerEventHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化 TLS 加密提供程序
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("无法初始化 TLS 加密提供程序");
    
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    // 创建连接管理器
    let connection_manager = Arc::new(ConnectionBasedManager::new());
    
    // 创建服务器配置（仅启用 QUIC）
    let config = ServerConfig {
        websocket_addr: None,
        quic_addr: Some("127.0.0.1:4433".to_string()),
        enable_tls: true,
        tls_cert_path: Some("certs/server.crt".to_string()),
        tls_key_path: Some("certs/server.key".to_string()),
        max_connections: 1000,
        connection_timeout_ms: 30000,
        heartbeat_interval_ms: 10000,
        enable_auto_cleanup: true,
    };
    
    // 创建服务器实例
    let mut server = Server::new(config, connection_manager);
    
    // 注册消息处理器
    let echo_handler = Arc::new(EchoMessageHandler);
    server.register_message_handler(echo_handler).await;
    
    // 启动服务器
    server.start().await?;
    
    println!("QUIC 服务器已启动:");
    println!("  QUIC地址: 127.0.0.1:4433");
    println!("按 Ctrl+C 停止服务器");
    
    // 等待中断信号
    tokio::signal::ctrl_c().await?;
    
    // 停止服务器
    server.stop().await?;
    
    println!("服务器已停止");
    Ok(())
}