//! 聊天室客户端示例
//!
//! 使用 FastClient 和 ProtocolRacer 实现协议竞速功能
//! 支持用户认证流程，能够发送和接收聊天消息

use std::sync::Arc;
use tokio::io::{self, AsyncBufReadExt};
use tracing::{info, error};

use flare_core::{
    client::{
        fast::{
            client::FastClient,
        },
    },
    common::{
        connections::{
            config::ConnectionConfig,
            enums::Transport,
            traits::ConnectionEvent,
        },
        protocol::{
            frame::Frame,
            factory::FrameFactory,
            reliability::Reliability,
            commands::{ControlCmd, Command},
        },
        parsing::{MessageParser, PayloadCodec},
        error::FlareError,
    },
};

/// 聊天室消息结构
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ChatMessage {
    /// 发送者用户ID
    sender: String,
    /// 消息内容
    content: String,
    /// 时间戳
    timestamp: u64,
}

impl ChatMessage {
    fn new(sender: String, content: String) -> Self {
        Self {
            sender,
            content,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

/// 聊天室客户端事件处理器
struct ChatClientEventHandler {
    /// 消息解析器
    parser: MessageParser,
}

impl ChatClientEventHandler {
    fn new() -> Self {
        Self {
            parser: MessageParser::new(PayloadCodec::Json),
        }
    }
}

impl ConnectionEvent for ChatClientEventHandler {
    fn on_connected(&self) {
        info!("✅ 已连接到聊天室服务器");
        println!("✅ 已连接到聊天室服务器");
    }
    
    fn on_disconnected(&self, reason: Option<String>) {
        info!("❌ 与聊天室服务器断开连接: {:?}", reason);
        println!("❌ 与聊天室服务器断开连接: {:?}", reason);
    }
    
    fn on_message_received(&self, frame: Frame) {
        tracing::debug!("收到消息，message_id: {}, payload大小: {} 字节", frame.message_id, frame.payload.len());
        // 处理控制命令
        if let Command::Control(control_cmd) = &frame.command {
            match control_cmd {
                ControlCmd::AuthResponse(success, message) => {
                    if *success {
                        println!("🔐 认证成功: {}", message);
                    } else {
                        println!("❌ 认证失败: {}", message);
                    }
                    return;
                }
                _ => {
                    // 其他控制命令可以在这里处理
                }
            }
        }
        
        // 解析消息内容
        let payload = frame.payload.to_vec();
        match self.parser.codec().decode::<ChatMessage>(&payload) {
            Ok(chat_msg) => {
                tracing::debug!("消息解析成功: 发送者={}, 内容={}", chat_msg.sender, chat_msg.content);
                let time_str = if let Some(dt) = chrono::DateTime::from_timestamp(chat_msg.timestamp as i64, 0) {
                    dt.naive_local().format("%H:%M:%S").to_string()
                } else {
                    "未知时间".to_string()
                };
                println!("[{}] {}: {}", 
                    time_str,
                    chat_msg.sender, 
                    chat_msg.content
                );
            }
            Err(e) => {
                error!("解析消息失败: {:?}", e);
                // 尝试以文本形式显示
                if let Ok(text) = String::from_utf8(payload) {
                    tracing::debug!("原始消息内容: {}", text);
                    println!("📥 收到消息: {}", text);
                }
            }
        }
    }
    
    fn on_heartbeat_ping(&self) {
        // println!("💓 心跳 Ping");
    }
    
    fn on_heartbeat_pong(&self, _rtt_ms: u32) {
        // println!("💚 心跳 Pong (RTT: {}ms)", rtt_ms);
    }
    
    fn on_error(&self, err: FlareError) {
        error!("❌ 客户端错误: {:?}", err);
        println!("❌ 客户端错误: {:?}", err);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 获取命令行参数
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("用法: cargo run --example chat_room_client <用户名> <认证令牌>");
        eprintln!("示例: cargo run --example chat_room_client user1 token_user1");
        std::process::exit(1);
    }
    
    let username = args[1].clone();
    let token = args[2].clone();
    
    // 初始化日志，设置为debug级别
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    
    println!("🚀 启动聊天室客户端，用户名: {}", username);
    
    // 创建客户端配置
    let mut config = ConnectionConfig::default();
    config.heartbeat_interval_ms = Some(10000); // 10秒心跳
    
    // 定义要连接的地址和协议
    let addresses = vec!["127.0.0.1:9005".to_string(), "127.0.0.1:9006".to_string()];
    let protocols = vec![Transport::WebSocket, Transport::Quic];
    
    // 创建 FastClient
    let fast_client = Arc::new(FastClient::new(config));
    
    // 使用FastClient的协议竞速功能连接到最快的服务器
    println!("🔌 正在使用协议竞速连接到聊天室服务器...");
    fast_client.connect_with_race(addresses, protocols).await?;
    
    // 发送认证请求
    println!("🔐 正在进行认证...");
    send_authentication_request(&fast_client, &username, &token).await?;
    
    // 创建事件处理器
    let event_handler = Arc::new(ChatClientEventHandler::new());
    
    // 设置事件处理器
    fast_client.set_event_handler(event_handler).await?;
    
    println!("\n💬 欢迎来到聊天室!");
    println!("📝 支持的命令:");
    println!("   /quit - 退出聊天室");
    println!("   /users - 显示在线用户");
    println!("   其他内容将作为聊天消息发送\n");
    
    // 创建 stdin 读取器
    let stdin = io::stdin();
    let reader = io::BufReader::new(stdin);
    let mut lines = reader.lines();
    
    // 读取用户输入并发送消息
    loop {
        if let Some(line) = lines.next_line().await? {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            
            // 处理特殊命令
            if line == "/quit" {
                println!("👋 正在退出聊天室...");
                break;
            } else if line == "/users" {
                // 发送系统命令获取用户列表
                send_chat_message(&fast_client, "/system users".to_string()).await?;
            } else {
                // 发送普通聊天消息
                send_chat_message(&fast_client, line.to_string()).await?;
            }
        }
    }
    
    // 断开连接
    println!("🛑 正在断开连接...");
    fast_client.disconnect().await?;
    
    println!("✅ 客户端已断开连接");
    
    Ok(())
}

/// 发送认证请求的辅助函数
async fn send_authentication_request(client: &Arc<FastClient>, username: &str, token: &str) -> Result<(), Box<dyn std::error::Error>> {
    // 创建控制命令
    let control_cmd = ControlCmd::AuthRequest(
        username.to_string(),
        "chat_room".to_string(), // platform
        token.to_string(), // token
    );
    let command = Command::Control(control_cmd);
    
    // 创建帧
    let frame = Frame::new(
        command,
        FrameFactory::generate_message_id(),
        Reliability::AtLeastOnce,
    );
    
    // 发送认证请求
    client.send_message(frame).await?;
    
    // 等待一小段时间确保认证完成
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    println!("🔐 用户 {} 使用令牌 {} 认证成功", username, token);
    
    Ok(())
}

/// 发送聊天消息的辅助函数
async fn send_chat_message(client: &Arc<FastClient>, content: String) -> Result<(), Box<dyn std::error::Error>> {
    tracing::debug!("准备发送消息: {}", content);
    
    // 创建消息，使用实际的用户名作为发送者
    let args: Vec<String> = std::env::args().collect();
    let username = if args.len() > 1 { args[1].clone() } else { "Unknown".to_string() };
    
    let chat_msg = ChatMessage::new(username, content);
    
    // 序列化消息
    let parser = MessageParser::new(PayloadCodec::Json);
    let payload = parser.codec().encode(&chat_msg)?;
    
    tracing::debug!("消息序列化完成，payload大小: {} 字节", payload.len());
    
    // 创建帧
    let frame = FrameFactory::create_data_frame(
        FrameFactory::generate_message_id(),
        payload,
        Reliability::AtLeastOnce
    )?;
    
    tracing::debug!("帧创建完成，message_id: {}", frame.message_id);
    
    // 发送消息
    client.send_message(frame).await?;
    
    tracing::debug!("消息发送完成");
    
    Ok(())
}