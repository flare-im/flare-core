//! 简单QUIC聊天室客户端示例
//!
//! 直接连接到QUIC服务器

use std::sync::Arc;
use tokio::io::{self, AsyncBufReadExt};
use tracing::{info, error};

use flare_core::{
    common::{
        connections::{
            config::{ConnectionConfig, QuicClientConfig, QuicConfig, ProtocolConfig},
            enums::Transport,
            traits::{ConnectionEvent, BaseConnection, ClientConnection},  // 添加必要的trait
            quic::QuicClientConn,  // 使用QUIC客户端连接
        },
        protocol::{
            frame::Frame,
            factory::FrameFactory,
            reliability::Reliability,
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
    if args.len() < 2 {
        eprintln!("用法: cargo run --example simple_quic_chat_client <用户名>");
        eprintln!("示例: cargo run --example simple_quic_chat_client user1");
        std::process::exit(1);
    }
    
    let username = args[1].clone();
    
    // 初始化日志，设置为info级别
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    println!("🚀 启动QUIC聊天室客户端，用户名: {}", username);
    
    // 创建客户端配置
    let mut config = ConnectionConfig::default();
    config.transport = Transport::Quic;
    config.remote_addr = Some("127.0.0.1:9006".to_string());
    config.enable_tls = true; // 启用TLS
    
    // 创建协议配置
    let mut protocol_config = ProtocolConfig::default();
    
    // QUIC客户端配置
    let mut quic_client_config = QuicClientConfig::default();
    // 设置服务器证书路径
    quic_client_config.server_cert_path = Some("certs/server.crt".to_string());
    // 不跳过服务器验证
    quic_client_config.skip_server_verification = false;
    
    // 创建QUIC配置
    let mut quic_config = QuicConfig::default();
    quic_config.client = Some(quic_client_config);
    
    // 设置协议配置
    protocol_config.quic = Some(quic_config);
    config.protocol_config = Some(protocol_config);
    
    // 创建事件处理器
    let event_handler = Arc::new(ChatClientEventHandler::new());
    
    // 创建QUIC客户端连接
    let client = QuicClientConn::from_config(config);
    
    // 设置事件处理器
    client.set_event_handler(event_handler.clone());
    
    // 连接到服务器
    println!("🔌 正在连接到聊天室服务器...");
    client.connect()?;
    
    println!("\n💬 欢迎来到聊天室!");
    println!("📝 支持的命令:");
    println!("   /quit - 退出聊天室");
    println!("   其他内容将作为聊天消息发送\n");
    
    // 添加连接确认和等待时间
    println!("⏳ 等待连接稳定...");
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    
    println!("✅ 连接已建立，可以开始聊天\n");
    
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
            } else {
                // 发送普通聊天消息
                send_chat_message(&client, username.clone(), line.to_string())?;
            }
        }
        
        // 添加一个小延迟
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    
    // 断开连接
    println!("🛑 正在断开连接...");
    client.disconnect(None)?;
    
    println!("✅ 客户端已断开连接");
    
    Ok(())
}

/// 发送聊天消息的辅助函数
fn send_chat_message(client: &QuicClientConn, sender: String, content: String) -> Result<(), Box<dyn std::error::Error>> {
    tracing::debug!("准备发送消息: {}", content);
    
    let chat_msg = ChatMessage::new(sender, content);
    
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
    match client.send_message(frame) {
        Ok(_) => {
            tracing::debug!("消息发送完成");
            println!("📤 消息已发送");
            // 添加一个小延迟，确保消息被正确处理
            std::thread::sleep(std::time::Duration::from_millis(100));
            Ok(())
        }
        Err(e) => {
            error!("消息发送失败: {:?}", e);
            println!("⚠️  消息发送失败: {:?}", e);
            Ok(()) // 不返回错误，让客户端继续运行
        }
    }
}