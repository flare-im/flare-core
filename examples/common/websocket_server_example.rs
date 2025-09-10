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
        if message.is_heartbeat() {
            info!("[{}] 收到WebSocket心跳消息: {}", self.name, connection_id);
        } else {
            let payload = message.get_payload();
            if let Ok(text) = String::from_utf8(payload.to_vec()) {
                info!("[{}] 收到WebSocket客户端消息: {} - 内容: {}", self.name, connection_id, text);
                
                // 如果是数据消息，可以发送响应
                if message.get_message_type() == MessageType::Data {
                    info!("[{}] 准备发送响应消息", self.name);
                }
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
    
    // 服务端地址 (使用不同的端口避免冲突)
    let addr: SocketAddr = "127.0.0.1:8083".parse()?;
    
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
            let config = ConnectionConfig::server(
                format!("ws_server_{}", client_addr).replace(":", "_"),
                addr.to_string(),
            )
            .with_type(ConnectionType::WebSocket)
            .with_serialization_format(SerializationFormat::Protobuf); // 使用Protobuf序列化
            
            // 从原始TCP连接创建WebSocket服务端连接
            match RawConnectionHandler::from_websocket_with_handler_arc(
                tcp_stream,
                config,
                handler as Arc<dyn ConnectionEvent>,
            ).await {
                Ok(connection) => {
                    let connection_id = connection.get_id().to_string();
                    info!("WebSocket连接已建立: {} (ID: {})", client_addr, connection_id);
                    
                    // 接受连接以正确初始化状态
                    if let Err(e) = connection.accept().await {
                        error!("接受连接失败: {}", e);
                        return;
                    }
                    
                    info!("连接已接受并准备就绪: {}", connection_id);
                    
                    // 启动消息处理循环
                    loop {
                        match connection.receive_message().await {
                            Ok(Some(frame)) => {
                                // 处理接收到的消息
                                info!("收到消息: {:?}", frame.get_message_type());
                                
                                // 如果是数据消息，可以发送响应
                                if frame.get_message_type() == MessageType::Data {
                                    let response_payload = b"Hello from WebSocket server with Protobuf!".to_vec();
                                    let response = Frame::new(
                                        MessageType::Data,
                                        frame.get_message_id(),
                                        frame.get_reliability(),
                                        response_payload,
                                    );
                                    
                                    if let Err(e) = connection.send_message(response).await {
                                        error!("发送响应消息失败: {}", e);
                                    } else {
                                        info!("响应消息已发送");
                                    }
                                }
                            }
                            Ok(None) => {
                                // 连接已关闭
                                info!("连接已关闭: {}", client_addr);
                                break;
                            }
                            Err(e) => {
                                error!("接收消息失败: {}", e);
                                break;
                            }
                        }
                        
                        // 检查连接是否还活跃
                        if !connection.is_active().await {
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