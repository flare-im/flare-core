//! 心跳演示
//! 
//! 展示新的统一心跳处理设计

use std::time::Duration;
use tokio::time::interval;
use tracing::{info, debug};

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
pub struct HeartbeatEventHandler {
    pub name: String,
}

#[async_trait::async_trait]
impl ConnectionEventHandler for HeartbeatEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] 连接已建立: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] 连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        info!("[{}] 连接错误: {} - 错误: {}", self.name, connection_id, error);
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

impl HeartbeatEventHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

/// 心跳演示
#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    info!("=== 心跳演示开始 ===");
    
    // 创建客户端配置
    let config = ConnectionConfig::client(
        "heartbeat_demo_client".to_string(),
        "ws://127.0.0.1:8080".to_string()
    ).with_type(ConnectionType::WebSocket)
     .with_websocket_config(WebSocketConfig {
         subprotocols: vec!["text".to_string()],
         extensions: vec![],
         compression_threshold: None,
     })
     .with_heartbeat(5000, 2000); // 5秒心跳间隔，2秒超时
    
    info!("客户端配置: {:?}", config);
    
    // 创建连接工厂
    let factory = ConnectionFactory::new();
    
    // 创建客户端连接
    let mut client_connection = factory.create_client_connection(config).await?;
    
    // 设置事件处理器
    let event_handler = Arc::new(HeartbeatEventHandler::new("心跳演示客户端".to_string()));
    // 注意：set_event_handler 方法需要在实际的连接实现中调用
    // 这里只是演示，实际使用时需要根据具体的连接类型来设置
    
    // 设置心跳响应处理器
    client_connection.set_heartbeat_response_handler(Some(Box::new(|data| {
        info!("收到心跳响应数据: {:?}", data);
        Ok(())
    }))).await;
    
    // 建立连接
    info!("正在连接服务端...");
    client_connection.connect().await?;
    info!("已连接到服务端！");
    
    // 启动心跳发送任务
    let connection_clone = Arc::new(tokio::sync::Mutex::new(client_connection));
    let heartbeat_connection = Arc::clone(&connection_clone);
    
    let heartbeat_task = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(5));
        
        loop {
            interval.tick().await;
            
            if let Ok(connection) = heartbeat_connection.lock().await.send_heartbeat().await {
                debug!("心跳发送成功");
            } else {
                info!("心跳发送失败");
            }
        }
    });
    
    // 启动心跳监控任务
    let monitor_connection = Arc::clone(&connection_clone);
    let monitor_task = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(3));
        
        loop {
            interval.tick().await;
            
            if monitor_connection.lock().await.has_received_heartbeat().await {
                debug!("连接状态正常");
            } else {
                info!("连接可能已断开");
            }
        }
    });
    
    // 运行一段时间
    info!("运行心跳演示 30 秒...");
    tokio::time::sleep(Duration::from_secs(30)).await;
    
    // 停止任务
    heartbeat_task.abort();
    monitor_task.abort();
    
    // 断开连接
    info!("正在断开连接...");
    let mut connection = connection_clone.lock().await;
    if let Ok(_) = connection.disconnect().await {
        info!("连接已断开");
    }
    
    info!("=== 心跳演示结束 ===");
    Ok(())
}
