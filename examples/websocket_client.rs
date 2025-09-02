//! WebSocket 客户端示例
//! 
//! 演示如何使用 common 模块的 Connection trait 和 ConnectionFactory
//! 创建 WebSocket 客户端连接

use tracing::{info, error};

use flare_core::{
    ConnectionConfig, ConnectionType,
    ConnectionEventHandler, UnifiedProtocolMessage,
    FlareError,
};
use flare_core::common::connections::{
    ConnectionFactory, WebSocketConfig, ConnectionFactoryTrait
};
use flare_core::common::protocol::{Frame, MessageType, Reliability};

use std::sync::Arc;

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

/// WebSocket 客户端
#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志，指定 info 级别
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("启动 WebSocket 客户端");
    info!("=== WebSocket 客户端启动 ===");
    
    // 创建客户端配置
    let config = ConnectionConfig::client(
        "websocket_client".to_string(),
        "ws://127.0.0.1:8080".to_string()
    ).with_type(ConnectionType::WebSocket)
     .with_websocket_config(WebSocketConfig {
         subprotocols: vec!["text".to_string()],
         extensions: vec![],
         compression_threshold: None,
     })
     .with_heartbeat(30000, 10000);
    
    info!("客户端配置: {:?}", config);
    info!("连接地址: {}", config.remote_addr);
    
    // 创建连接工厂
    let factory = ConnectionFactory::new();
    
    // 创建客户端连接
    let mut client_connection = factory.create_client_connection(config).await?;
    
    // 注意：set_event_handler 方法不在 ClientConnection trait 中
    // 事件处理需要在具体的连接实现中设置
    let _event_handler = Arc::new(SimpleEventHandler::new("客户端".to_string()));
    
    // 建立连接
    info!("正在连接服务端...");
    client_connection.connect().await?;
    info!("已连接到服务端！");
    
    // 启动用户输入处理
    info!("请输入消息 (输入 'quit' 退出):");
    
    // 使用 tokio::task::spawn_blocking 处理阻塞的输入操作
    loop {
        // 在阻塞任务中处理用户输入
        let input = tokio::task::spawn_blocking(|| {
            let mut input = String::new();
            print!("> ");
            use std::io::{self, Write};
            io::stdout().flush().ok();
            std::io::stdin().read_line(&mut input).ok();
            input.trim().to_string()
        }).await.map_err(|e| FlareError::general_error(format!("输入任务失败: {}", e)))?;
        
        if input == "quit" {
            info!("用户请求退出");
            break;
        }
        
        if !input.is_empty() {
            // 创建统一协议消息
            let frame = Frame::new(
                MessageType::Data,
                0,
                Reliability::AtLeastOnce,
                input.as_bytes().to_vec(),
            );
            let message = UnifiedProtocolMessage::new(frame, None, 1);
            
            // 通过Connection trait发送消息
            if let Err(e) = client_connection.send_message(message).await {
                error!("发送消息失败: {}", e);
                break;
            } else {
                info!("消息已发送: {}", input);
            }
        }
    }
    
    // 断开连接
    info!("正在断开连接...");
    client_connection.disconnect().await?;
    
    info!("客户端已断开");
    Ok(())
}
