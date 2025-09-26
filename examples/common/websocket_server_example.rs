//! WebSocket 服务端连接示例
//!
//! 展示如何使用flare-core的WebSocket连接功能创建服务端并进行通信
//! 本示例只使用 src/common 模块，不依赖 src/server 模块

use std::sync::Arc;
use std::net::SocketAddr;
use tracing::{info, error};
use tokio::net::TcpListener;

use flare_core::{
    common::{
        connections::{
            traits::ConnectionEvent,
            types::{ConnectionConfig, Transport},
            factory::ConnectionFactory,
        },
        protocol::Frame,
        serialization::SerializationFormat,
    },
};

/// WebSocket服务端事件处理器
#[derive(Debug)]
pub struct WebSocketServerEventHandler {
    pub name: String,
}

#[async_trait::async_trait]
impl ConnectionEvent for WebSocketServerEventHandler {
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
        info!("[{}] 收到WebSocket消息: {} - 类型: {}", self.name, connection_id, message.get_command_type_str());
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        info!("[{}] WebSocket消息已发送: {} - 类型: {}", self.name, connection_id, message.get_command_type_str());
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

impl WebSocketServerEventHandler {
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
    
    // 服务端地址 (使用标准 WebSocket 端口)
    let addr: SocketAddr = "127.0.0.1:8080".parse()?;
    
    // 创建TCP监听器
    let listener = TcpListener::bind(addr).await?;
    info!("WebSocket 服务器已启动，监听地址: {}", addr);
    
    // 创建事件处理器
    let event_handler = Arc::new(WebSocketServerEventHandler::new("WebSocket服务端".to_string()));
    
    // 等待客户端连接
    loop {
        let (tcp_stream, client_addr) = listener.accept().await?;
        info!("新客户端连接: {}", client_addr);
        
        // 克隆事件处理器
        let handler = Arc::clone(&event_handler);
        
        // 为每个连接创建独立的任务
        tokio::spawn(async move {
            // 创建连接配置
            let mut config = ConnectionConfig::server(
                format!("ws_server_{}", client_addr).replace(":", "_"),
                addr.to_string(),
            );
            // 设置远程地址为客户端地址
            config.remote_addr = client_addr.to_string();
            config.transport = Transport::WebSocket;
            
            // 配置 WebSocket 特定设置
            config.protocol_config.websocket = flare_core::common::connections::types::WebSocketConfig {
                subprotocols: vec!["flare-protocol".to_string()],
                extensions: vec![],
                compression_threshold: Some(1024),
            };
            
            config.serialization_config = Some(flare_core::common::serialization::SerializationConfig {
                format: SerializationFormat::Protobuf,
                enable_encryption: false,
                enable_compression: false,
                compression_level: Some(0),
                pretty_format: false,
                max_message_size: Some(1024 * 1024), // 1MB
                custom_params: std::collections::HashMap::new(),
            });
            
            // 从原始TCP连接创建WebSocket服务端连接
            match ConnectionFactory::from_websocket_with_handler_arc(
                tcp_stream,
                config,
                handler as Arc<dyn ConnectionEvent>,
            ).await {
                Ok(connection) => {
                    let connection_id = connection.id();
                    info!("WebSocket连接已建立: {} (ID: {})", client_addr, connection_id);
                    
                    // 接受连接以正确初始化状态
                    if let Err(e) = connection.accept().await {
                        error!("接受连接失败: {}", e);
                        return;
                    }
                    
                    info!("连接已接受并准备就绪: {}", connection_id);
                    
                    // 发送欢迎消息
                    let welcome_cmd = flare_core::common::protocol::commands::MessageSendCommand::new(
                        format!("欢迎连接到 WebSocket 服务端! 客户端: {}", client_addr).into_bytes()
                    );
                    let command = flare_core::common::protocol::commands::Command::Message(
                        flare_core::common::protocol::commands::MessageCmd::Send(welcome_cmd)
                    );
                    let welcome_message = flare_core::common::protocol::Frame::new(
                        command,
                        uuid::Uuid::new_v4().to_string(),
                        flare_core::common::protocol::Reliability::AtLeastOnce,
                    );
                    
                    if let Err(e) = connection.send_message(welcome_message).await {
                        error!("发送欢迎消息失败: {}", e);
                    } else {
                        info!("欢迎消息已发送给客户端: {}", client_addr);
                    }
                    
                    // 保持连接活跃，等待消息处理
                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        
                        // 检查连接状态
                        let state = connection.state();
                        if matches!(state, flare_core::common::connections::types::ConnectionState::Disconnected) {
                            info!("连接已断开: {}", client_addr);
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("创建WebSocket连接失败: {} - {}", client_addr, e);
                }
            }
        });
    }
}