//! WebSocket 客户端示例
//! 
//! 演示如何使用 common 模块的 Connection trait 和 ConnectionFactory
//! 创建 WebSocket 客户端连接

use tracing::{info, error};

use flare_core::{
    ConnectionConfig, ConnectionType,
    ConnectionEvent, Frame,
    FlareError,
};
use flare_core::common::connections::{
    ConnectionFactory, WebSocketConfig, ConnectionFactoryTrait
};
use flare_core::common::protocol::{MessageType, Reliability};

use std::sync::Arc;

type Result<T> = std::result::Result<T, FlareError>;

/// 简单事件处理器
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
                MessageType::HeartbeatAck => {
                    info!("[{}] 收到心跳确认: {}", self.name, connection_id);
                }
                MessageType::Heartbeat => {
                    info!("[{}] 收到服务端心跳: {}", self.name, connection_id);
                }
                _ => {
                    info!("[{}] 收到其他心跳消息: {} - 类型: {:?}", self.name, connection_id, message_type);
                }
            }
        } else {
            let payload = message.get_payload();
            if let Ok(text) = String::from_utf8(payload.to_vec()) {
                info!("[{}] 收到消息: {} - 内容: {}", self.name, connection_id, text);
            } else {
                info!("[{}] 收到二进制消息: {} - 长度: {}", self.name, connection_id, payload.len());
            }
        }
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("[{}] 心跳超时: {}", self.name, connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] 连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        if message.is_heartbeat() {
            info!("[{}] 心跳消息已发送: {} - 类型: {:?}", self.name, connection_id, message.get_message_type());
        } else {
            info!("[{}] 消息已发送: {} - 类型: {:?}", self.name, connection_id, message.get_message_type());
        }
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
    
    // 设置事件处理器
    let event_handler = Arc::new(SimpleEventHandler::new("客户端".to_string()));
    client_connection.set_connection_event_handler(event_handler as Arc<dyn ConnectionEvent>).await;
    
    // 建立连接
    info!("正在连接服务端...");
    client_connection.connect().await?;
    info!("已连接到服务端！");
    
    // 等待一下让连接稳定
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    // 启动心跳任务
    let client_connection_heartbeat = Arc::new(tokio::sync::Mutex::new(client_connection));
    let heartbeat_connection = Arc::clone(&client_connection_heartbeat);
    let heartbeat_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(30000)); // 30秒心跳间隔
        loop {
            interval.tick().await;
            
            let mut conn = heartbeat_connection.lock().await;
            
            // 发送心跳消息
            let heartbeat_frame = Frame::heartbeat();
            if let Err(e) = conn.send_message(heartbeat_frame).await {
                error!("心跳发送失败: {}", e);
                break;
            } else {
                info!("心跳已发送");
            }
            
            // 调用连接的心跳方法更新活跃状态
            if let Err(e) = conn.send_heartbeat().await {
                error!("心跳状态更新失败: {}", e);
            }
        }
    });
    
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
            let message = Frame::new(
                MessageType::Data,
                0,
                Reliability::AtLeastOnce,
                input.as_bytes().to_vec(),
            );
            
            // 通过Connection trait发送消息
            let mut conn = client_connection_heartbeat.lock().await;
            if let Err(e) = conn.send_message(message).await {
                error!("发送消息失败: {}", e);
                break;
            } else {
                info!("消息已发送: {}", input);
            }
        }
    }
    
    // 停止心跳任务
    heartbeat_task.abort();
    
    // 断开连接
    info!("正在断开连接...");
    let mut conn = client_connection_heartbeat.lock().await;
    conn.disconnect().await?;
    
    info!("客户端已断开");
    Ok(())
}
