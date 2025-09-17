//! WebSocket 服务端测试
//!
//! 测试WebSocket服务端连接处理的修复

use std::sync::Arc;
use std::net::SocketAddr;
use tracing::{info, error};
use tokio::net::TcpListener;

use flare_core::{
    common::{
        connections::{
            traits::{ConnectionEvent, ServerConnection},
            types::{ConnectionConfig, ConnectionType},
            factory::RawConnectionHandler,
        },
        protocol::{Frame, MessageType},
        serialization::SerializationFormat,
    },
};

/// WebSocket服务端事件处理器
#[derive(Debug)]
pub struct WebSocketServerTestHandler {
    pub name: String,
}

#[async_trait::async_trait]
impl ConnectionEvent for WebSocketServerTestHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] WebSocket客户端已连接: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] WebSocket客户端已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] WebSocket连接错误: {} - 错误: {}", self.name, connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        if message.is_heartbeat() {
            info!("[{}] 收到WebSocket心跳消息: {}", self.name, connection_id);
        } else {
            let payload = message.get_payload();
            if let Ok(text) = String::from_utf8(payload.to_vec()) {
                info!("[{}] 收到WebSocket客户端消息: {} - 内容: {}", self.name, connection_id, text);
            } else {
                info!("[{}] 收到WebSocket二进制消息: {} - 长度: {}", self.name, connection_id, payload.len());
            }
        }
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        if message.is_heartbeat() {
            info!("[{}] WebSocket心跳消息已发送: {}", self.name, connection_id);
        } else {
            info!("[{}] WebSocket数据消息已发送: {}", self.name, connection_id);
        }
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("[{}] WebSocket心跳超时: {}", self.name, connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] WebSocket连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        info!("[{}] WebSocket心跳已发送: {}", self.name, connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        info!("[{}] 收到WebSocket心跳响应: {}", self.name, connection_id);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        info!("[{}] WebSocket开始重连: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        info!("[{}] WebSocket重连成功: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        info!("[{}] WebSocket重连失败: {} - 尝试次数: {} - 错误: {}", self.name, connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) {
        info!("[{}] WebSocket统计信息更新: {} - 收到消息: {} - 发送消息: {}", 
             self.name, connection_id, stats.messages_received, stats.messages_sent);
    }
}

impl WebSocketServerTestHandler {
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
    
    // 服务端地址 (使用不同的端口避免冲突)
    let addr: SocketAddr = "127.0.0.1:8085".parse()?;
    
    // 创建TCP监听器
    let listener = TcpListener::bind(addr).await?;
    info!("WebSocket 测试服务器已启动，监听地址: {}", addr);
    
    // 创建事件处理器
    let event_handler = Arc::new(WebSocketServerTestHandler::new("WebSocket测试服务端".to_string()));
    
    // 等待一个客户端连接进行测试
    let (tcp_stream, client_addr) = listener.accept().await?;
    info!("新客户端连接: {}", client_addr);
    
    // 克隆事件处理器
    let handler = Arc::clone(&event_handler);
    
    // 创建连接配置
    let config = ConnectionConfig::server(
        format!("ws_test_{}", client_addr).replace(":", "_"),
        addr.to_string(),
    )
    .with_type(ConnectionType::WebSocket)
    .with_serialization_format(SerializationFormat::Json); // 使用JSON序列化
    
    // 从原始TCP连接创建WebSocket服务端连接
    match RawConnectionHandler::from_websocket_with_handler_arc(
        tcp_stream,
        config,
        handler as Arc<dyn ConnectionEvent>,
    ).await {
        Ok(connection) => {
            let connection_id = connection.id().to_string();
            info!("WebSocket连接已建立: {} (ID: {})", client_addr, connection_id);
            
            // 接受连接以正确初始化状态
            if let Err(e) = connection.accept().await {
                error!("接受连接失败: {}", e);
                return Ok(());
            }
            
            info!("连接已接受并准备就绪: {}", connection_id);
            
            // 等待一段时间以观察连接状态
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            
            info!("测试完成，关闭连接");
        }
        Err(e) => {
            error!("创建WebSocket连接失败: {} - {}", client_addr, e);
        }
    }
    
    Ok(())
}