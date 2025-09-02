//! WebSocket 双向通信测试
//! 
//! 演示完整的 WebSocket 双向通信功能

use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::time::Duration;
use tracing::{info, error, warn};

use flare_core::{
    ConnectionConfig, ConnectionType,
    ConnectionEventHandler, Frame,
    FlareError,
};
use flare_core::common::{
    EchoConnectionEventHandler,
    connections::{
        ConnectionFactory, WebSocketConfig, RawConnectionHandler, ConnectionFactoryTrait
    },
};
use flare_core::common::protocol::{MessageType, Reliability};

type Result<T> = std::result::Result<T, FlareError>;

/// 客户端事件处理器
#[derive(Debug)]
pub struct ClientEventHandler {
    pub name: String,
}

#[async_trait::async_trait]
impl ConnectionEventHandler for ClientEventHandler {
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
        let payload = message.get_payload();
        if let Ok(text) = String::from_utf8(payload.to_vec()) {
            info!("[{}] 收到回显消息: {} - 内容: {}", self.name, connection_id, text);
        } else {
            info!("[{}] 收到回显二进制消息: {} - 长度: {}", self.name, connection_id, payload.len());
        }
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        warn!("[{}] 心跳超时: {}", self.name, connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] 连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
    }
}

impl ClientEventHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

/// 启动服务端
async fn start_server() -> Result<()> {
    info!("=== 启动 WebSocket 服务端 ===");
    
    // 创建服务端配置
    let config = ConnectionConfig::server(
        "echo_server".to_string(),
        "127.0.0.1:8081".to_string()
    ).with_type(ConnectionType::WebSocket)
     .with_websocket_config(WebSocketConfig {
         subprotocols: vec!["echo".to_string()],
         extensions: vec![],
         compression_threshold: None,
     })
     .with_heartbeat_monitoring(30000, 60000);
    
    // 创建 TCP 监听器
    let listener = TcpListener::bind("127.0.0.1:8081").await
        .map_err(|e| FlareError::connection_failed(format!("绑定端口失败: {}", e)))?;
    
    info!("服务端监听地址: 127.0.0.1:8081");
    
    // 监听连接
    loop {
        match listener.accept().await {
            Ok((tcp_stream, addr)) => {
                info!("客户端已连接: {}", addr);
                
                // 使用回显事件处理器
                let connection_event_handler = Arc::new(EchoConnectionEventHandler::new());
                
                // 克隆配置用于新连接
                let connection_config = config.clone();
                
                // 为每个连接创建独立的任务
                tokio::spawn(async move {
                    match RawConnectionHandler::from_websocket_with_handler(
                        tcp_stream, 
                        connection_config, 
                        Arc::clone(&connection_event_handler) as Arc<dyn ConnectionEventHandler>
                    ).await {
                        Ok(mut server_connection) => {
                            info!("WebSocket 回显服务连接已建立: {}", addr);
                            
                            // 接受连接
                            if let Err(e) = server_connection.accept().await {
                                error!("接受连接失败: {}", e);
                                return;
                            }
                            
                            // 将连接设置到回显事件处理器中
                            connection_event_handler.set_connection(Arc::new(tokio::sync::Mutex::new(server_connection))).await;
                            
                            // 保持连接活跃，等待客户端断开
                            loop {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                // 这里可以添加连接健康检查
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
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// 运行客户端测试
async fn run_client_test() -> Result<()> {
    info!("=== 启动 WebSocket 客户端测试 ===");
    
    // 等待服务端启动
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // 创建客户端配置
    let config = ConnectionConfig::client(
        "test_client".to_string(),
        "ws://127.0.0.1:8081".to_string()
    ).with_type(ConnectionType::WebSocket)
     .with_websocket_config(WebSocketConfig {
         subprotocols: vec!["echo".to_string()],
         extensions: vec![],
         compression_threshold: None,
     })
     .with_heartbeat(30000, 10000);
    
    // 创建连接工厂
    let factory = ConnectionFactory::new();
    
    // 创建客户端连接
    let mut client_connection = factory.create_client_connection(config).await?;
    
    // 设置事件处理器
    let event_handler = Arc::new(ClientEventHandler::new("测试客户端".to_string()));
    client_connection.set_connection_event_handler(event_handler as Arc<dyn ConnectionEventHandler>).await;
    
    // 建立连接
    info!("正在连接到回显服务端...");
    client_connection.connect().await?;
    info!("已连接到回显服务端！");
    
    // 等待连接稳定
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // 发送测试消息
    let test_messages = vec![
        "Hello Echo Server!",
        "这是一个中文消息",
        "Message 1",
        "Message 2", 
        "Message 3",
        "Final test message!",
    ];
    
    for (i, msg_text) in test_messages.iter().enumerate() {
        info!("发送测试消息 {}: {}", i + 1, msg_text);
        
        // 创建统一协议消息
        let message = Frame::new(
            MessageType::Data,
            (i + 1) as u64,
            Reliability::AtLeastOnce,
            msg_text.as_bytes().to_vec(),
        );
        
        // 发送消息
        if let Err(e) = client_connection.send_message(message).await {
            error!("发送消息失败: {}", e);
        } else {
            info!("消息已发送: {}", msg_text);
        }
        
        // 间隔发送
        tokio::time::sleep(Duration::from_millis(2000)).await;
    }
    
    // 等待一段时间接收回显消息
    info!("等待接收回显消息...");
    tokio::time::sleep(Duration::from_secs(8)).await;
    
    // 断开连接
    info!("正在断开连接...");
    client_connection.disconnect().await?;
    
    info!("客户端测试完成");
    Ok(())
}

/// WebSocket 双向通信测试
#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("启动 WebSocket 双向通信测试");
    
    // 同时启动服务端和客户端
    tokio::select! {
        server_result = start_server() => {
            if let Err(e) = server_result {
                error!("服务端错误: {}", e);
            }
        }
        client_result = run_client_test() => {
            if let Err(e) = client_result {
                error!("客户端测试错误: {}", e);
            }
            
            // 客户端测试完成后退出
            info!("双向通信测试完成");
        }
    }
    
    Ok(())
}