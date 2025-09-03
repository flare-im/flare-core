//! WebSocket 服务端示例
//! 
//! 演示如何使用 common 模块的 ConnectionFactory 和 RawConnectionHandler
//! 创建 WebSocket 服务端连接

use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{info, error, warn};

use flare_core::{
    ConnectionConfig, ConnectionType,
    ConnectionEvent, Frame,
    DefConnectionEventHandler,
    FlareError,
};
use flare_core::common::connections::{
    WebSocketConfig, RawConnectionHandler,
};

type Result<T> = std::result::Result<T, FlareError>;

/// 简单事件处理器 - 用于更好的消息可见性
#[derive(Debug)]
pub struct SimpleEventHandler {
    pub name: String,
}

#[async_trait::async_trait]
impl ConnectionEvent for SimpleEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] 连接已建立: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] 连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] 连接错误: {} - 错误: {}", self.name, connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        if message.is_heartbeat() {
            let message_type = message.get_message_type();
            match message_type {
                flare_core::common::protocol::MessageType::HeartbeatAck => {
                    info!("[{}] 💗 收到心跳确认: {}", self.name, connection_id);
                }
                flare_core::common::protocol::MessageType::Heartbeat => {
                    info!("[{}] ❤️  收到客户端心跳: {}", self.name, connection_id);
                }
                _ => {
                    info!("[{}] 💓 收到其他心跳消息: {} - 类型: {:?}", self.name, connection_id, message_type);
                }
            }
        } else {
            let payload = message.get_payload();
            if let Ok(text) = String::from_utf8(payload.to_vec()) {
                println!("📨 [客户端消息] {}", text);
                info!("[{}] 📨 收到客户端消息: {} - 内容: '{}'", self.name, connection_id, text);
            } else {
                println!("📦 [客户端消息] 二进制数据 ({} bytes)", payload.len());
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
        info!("[{}] 统计信息更新: {} - 收到消息: {} - 发送消息: {}", 
             self.name, connection_id, stats.messages_received, stats.messages_sent);
    }
}

impl SimpleEventHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}


/// WebSocket 服务端
#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志，指定 info 级别
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("启动 WebSocket 服务端");
    info!("=== WebSocket 服务端启动 ===");
    
    // 创建服务端配置
    let config = ConnectionConfig::server(
        "websocket_server".to_string(),
        "127.0.0.1:8080".to_string()
    ).with_type(ConnectionType::WebSocket)
     .with_websocket_config(WebSocketConfig {
         subprotocols: vec!["text".to_string()],
         extensions: vec![],
         compression_threshold: None,
     })
     .with_heartbeat_monitoring(30000, 60000);
    
    info!("服务端配置: {:?}", config);
    info!("监听地址: {}", config.local_addr.as_ref().unwrap());
    
    // 注意：事件处理器现在为每个连接单独创建
    
    // 创建 TCP 监听器
    let listener = TcpListener::bind("127.0.0.1:8080").await
        .map_err(|e| FlareError::connection_failed(format!("绑定端口失败: {}", e)))?;
    
    info!("服务端监听地址: 127.0.0.1:8080");
    info!("等待客户端连接...");
    info!("按 Ctrl+C 停止服务端");
    
    // 使用 select! 来同时监听连接和中断信号
    loop {
        tokio::select! {
            // 监听新的客户端连接
            result = listener.accept() => {
                match result {
                    Ok((tcp_stream, addr)) => {
                        info!("客户端已连接: {}", addr);
                        
                        // 使用新的SimpleEventHandler来处理和显示消息
                        let connection_event_handler = Arc::new(SimpleEventHandler::new(format!("服务端-{}", addr)));
                        
                        // 克隆配置用于新连接
                        let connection_config = config.clone();
                        
                        // 为每个连接创建独立的任务
                        tokio::spawn(async move {
                            match RawConnectionHandler::from_websocket_with_handler(
                                tcp_stream, 
                                connection_config, 
                                Arc::clone(&connection_event_handler) as Arc<dyn ConnectionEvent>
                            ).await {
                                Ok(mut server_connection) => {
                                    info!("WebSocket 服务端连接已建立: {}", addr);
                                    
                                    // 接受连接
                                    if let Err(e) = server_connection.accept().await {
                                        error!("接受连接失败: {}", e);
                                        return;
                                    }
                                    
                                    // SimpleEventHandler 不需要设置连接实例，直接保持连接活跃
                                    // 如果使用 EchoConnectionEventHandler 或 HeartbeatConnectionEventHandler，需要设置连接
                                    // let server_conn_arc = Arc::new(tokio::sync::Mutex::new(server_connection));
                                    // connection_event_handler.set_connection(server_conn_arc).await;
                                    
                                    // 保持连接活跃，等待客户端断开
                                    info!("连接已就绪，等待消息...");
                                    loop {
                                        tokio::task::yield_now().await; // 使用超低延迟策略
                                        // 检查连接是否还活跃
                                        if !server_connection.is_active().await {
                                            info!("连接已断开: {}", addr);
                                            break;
                                        }
                                        // 给系统一个微小的处理时间
                                        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                                    }
                                }
                                Err(e) => {
                                    error!("创建服务端连接失败: {} - {}", addr, e);
                                }
                            }
                        });
                    }
                    Err(e) => {
                        error!("接受连接失败: {}", e);
                        // 短暂等待后继续监听
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
            }
            
            // 监听 Ctrl+C 信号
            _ = signal::ctrl_c() => {
                warn!("收到中断信号 (Ctrl+C)，正在优雅关闭服务端...");
                info!("服务端已停止");
                break;
            }
        }
    }
    
    Ok(())
}
