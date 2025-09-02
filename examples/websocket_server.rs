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
    ConnectionEventHandler, UnifiedProtocolMessage,
    FlareError,
};
use flare_core::common::connections::{
    WebSocketConfig, RawConnectionHandler,
};

type Result<T> = std::result::Result<T, FlareError>;

/// 简单事件处理器
#[derive(Debug)]
pub struct SimpleEventHandler {
    pub name: String,
}

#[async_trait::async_trait]
impl ConnectionEventHandler for SimpleEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] 连接已建立: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] 连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] 连接错误: {} - 错误: {}", self.name, connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &UnifiedProtocolMessage) {
        let payload = message.get_payload();
        if let Ok(text) = String::from_utf8(payload.to_vec()) {
            info!("[{}] 收到消息: {} - 内容: {}", self.name, connection_id, text);
        } else {
            info!("[{}] 收到二进制消息: {} - 长度: {}", self.name, connection_id, payload.len());
        }
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("[{}] 心跳超时: {}", self.name, connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] 连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
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
                        
                        // 为每个连接创建独立的事件处理器
                        let connection_event_handler = Arc::new(SimpleEventHandler::new(
                            format!("服务端-{}", addr)
                        ));
                        
                        // 克隆配置用于新连接
                        let connection_config = config.clone();
                        
                        // 为每个连接创建独立的任务
                        tokio::spawn(async move {
                            match RawConnectionHandler::from_websocket_with_handler(
                                tcp_stream, 
                                connection_config, 
                                connection_event_handler
                            ).await {
                                Ok(mut server_connection) => {
                                    // 接受连接
                                    if let Err(e) = server_connection.accept().await {
                                        error!("接受连接失败: {}", e);
                                        return;
                                    }
                                    
                                    info!("WebSocket 服务端连接已建立: {}", addr);
                                    
                                    // 等待直到连接不健康
                                    while server_connection.is_healthy().await {
                                        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                                    }
                                    
                                    // 关闭连接
                                    if let Err(e) = server_connection.close().await {
                                        error!("关闭连接失败: {} - {}", addr, e);
                                    } else {
                                        info!("客户端已断开: {}", addr);
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
