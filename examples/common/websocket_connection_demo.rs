//! WebSocket 连接演示
//! 
//! 简单的服务端和客户端联动演示
//! 客户端运行后用户可以输入信息，服务端接收到后打印

use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use futures_util::{StreamExt, SinkExt};
use tracing::{info, error, warn};

use flare_core::{
    ConnectionConfig, ConnectionType,
    ConnectionEventHandler, Frame,
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

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        let payload = message.get_payload();
        if let Ok(text) = String::from_utf8(payload.to_vec()) {
            info!("[{}] 收到消息: {} - 内容: {}", self.name, connection_id, text);
        } else {
            info!("[{}] 收到二进制消息: {} - 长度: {}", self.name, connection_id, payload.len());
        }
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        warn!("[{}] 心跳超时: {}", self.name, connection_id);
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
pub async fn run_websocket_server() -> Result<()> {
    info!("=== WebSocket 服务端启动 ===");
    
    // 创建 TCP 监听器
    let listener = TcpListener::bind("127.0.0.1:8080").await
        .map_err(|e| FlareError::connection_failed(format!("绑定端口失败: {}", e)))?;
    
    info!("服务端监听地址: 127.0.0.1:8080");
    info!("等待客户端连接...");
    info!("按 Ctrl+C 停止服务端");
    
    // 等待客户端连接
    if let Ok((stream, addr)) = listener.accept().await {
        info!("客户端已连接: {}", addr);
        
        // 接受 WebSocket 握手
        let ws_stream = accept_async(stream).await
            .map_err(|e| FlareError::connection_failed(format!("WebSocket 握手失败: {}", e)))?;
        
        info!("WebSocket 连接已建立，开始接收消息...");
        
        // 分离读写流
        let (mut write, mut read) = ws_stream.split();
        
        // 处理接收到的消息
        while let Some(msg) = read.next().await {
            match msg {
                Ok(msg) => {
                    info!("服务端收到消息: {:?}", msg);
                    
                    // 回显消息给客户端
                    if let Err(e) = write.send(msg).await {
                        error!("发送消息失败: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    error!("接收消息错误: {}", e);
                    break;
                }
            }
        }
        
        info!("客户端已断开，服务端退出");
    }
    
    Ok(())
}

/// WebSocket 客户端
pub async fn run_websocket_client() -> Result<()> {
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
    let mut input = String::new();
    let stdin = std::io::stdin();
    
    info!("请输入消息 (输入 'quit' 退出):");
    
    loop {
        input.clear();
        if stdin.read_line(&mut input).is_ok() {
            let input = input.trim();
            
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
                let message = frame;
                
                // 通过Connection trait发送消息
                if let Err(e) = client_connection.send_message(message).await {
                    error!("发送消息失败: {}", e);
                    break;
                } else {
                    info!("消息已发送: {}", input);
                }
            }
        }
    }
    
    // 断开连接
    info!("正在断开连接...");
    client_connection.disconnect().await?;
    
    info!("客户端已断开");
    Ok(())
}

/// 主函数 - 根据命令行参数决定运行服务端还是客户端
#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    let args: Vec<String> = std::env::args().collect();
    
    match args.get(1).map(|s| s.as_str()) {
        Some("server") => {
            info!("启动 WebSocket 服务端");
            run_websocket_server().await?;
        }
        Some("client") => {
            info!("启动 WebSocket 客户端");
            run_websocket_client().await?;
        }
        _ => {
            println!("用法:");
            println!("  {} server  - 启动服务端", args[0]);
            println!("  {} client  - 启动客户端", args[0]);
            println!();
            println!("示例:");
            println!("  1. 先启动服务端: {} server", args[0]);
            println!("  2. 再启动客户端: {} client", args[0]);
            println!("  3. 在客户端输入消息，服务端会收到并打印");
        }
    }
    
    Ok(())
}
