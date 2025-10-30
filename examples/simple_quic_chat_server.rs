//! 简单QUIC聊天室服务端示例
//!
//! 使用 AggregationServer 实现仅支持 QUIC 协议的聊天室服务端
//! 使用基础的连接管理器，不包含认证功能

use std::sync::Arc;
use tokio::io::{self, AsyncBufReadExt};
use tracing::{info, warn};

use flare_core::{
    server::{
        config::{ServerConfig, ProtocolConfig, TlsConfig, ServerType},
        server::AggregationServer,
        manager::connection_manager::ConnectionManagerImpl,
        manager::traits::ConnectionManager, // 导入 ConnectionManager trait
        events::handler::EnhancedEventHandler, // 导入 EnhancedEventHandler trait
    },
    common::{
        protocol::{
            frame::Frame,
            factory::FrameFactory,
            reliability::Reliability,
            commands::{Command, ControlCmd},
        },
        error::FlareError,
        connections::{
            types::ConnectionStats,
            enums::ConnectionState,
        },
        parsing::{MessageParser, PayloadCodec},
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

/// 聊天室服务端事件处理器
struct ChatServerEventHandler {
    /// 消息解析器
    parser: MessageParser,
    /// 服务端引用
    server: Arc<AggregationServer>,
    /// 连接管理器
    connection_manager: Arc<ConnectionManagerImpl>,
}

impl ChatServerEventHandler {
    fn new(server: Arc<AggregationServer>, connection_manager: Arc<ConnectionManagerImpl>) -> Self {
        Self {
            parser: MessageParser::new(PayloadCodec::Json),
            server,
            connection_manager,
        }
    }
    
    /// 广播消息给除指定连接外的所有其他连接
    fn broadcast_to_others(&self, exclude_connection_id: &str, frame: Frame) -> Result<(), FlareError> {
        let mut success_count = 0;
        let mut failed_count = 0;
        
        // 获取所有连接ID
        let connection_ids = self.connection_manager.all_connection_ids();
        
        tracing::debug!("开始广播消息，排除连接: {}，总连接数: {}", exclude_connection_id, connection_ids.len());
        
        for connection_id in connection_ids {
            // 跳过发送者
            if connection_id == exclude_connection_id {
                tracing::debug!("跳过发送者连接: {}", connection_id);
                continue;
            }
            
            // 获取连接并发送消息
            if let Some(conn) = self.connection_manager.get_connection(&connection_id) {
                // 检查连接状态
                let state = conn.state();
                tracing::debug!("连接 {} 状态: {:?}", connection_id, state);
                
                // 只向已连接的客户端发送消息
                if matches!(state, ConnectionState::Connected) {
                    match conn.send_message(frame.clone()) {
                        Ok(()) => {
                            success_count += 1;
                            tracing::debug!("成功发送消息到连接: {}", connection_id);
                        }
                        Err(e) => {
                            failed_count += 1;
                            warn!("发送消息到连接 {} 失败: {:?}", connection_id, e);
                        }
                    }
                } else {
                    tracing::debug!("跳过非连接状态的连接: {} (状态: {:?})", connection_id, state);
                }
            } else {
                tracing::warn!("找不到连接: {}", connection_id);
            }
        }
        
        tracing::debug!("广播完成: 成功 {} 个，失败 {} 个", success_count, failed_count);
        
        if failed_count > 0 {
            Err(FlareError::other(format!("广播消息失败: {} 个连接失败", failed_count)))
        } else {
            Ok(())
        }
    }
}

impl EnhancedEventHandler for ChatServerEventHandler {
    fn on_connected(&self, connection_id: String) {
        info!("✅ 有新客户端连接: {}", connection_id);
        println!("✅ 有新客户端连接: {}", connection_id);
    }
    
    fn on_disconnected(&self, connection_id: String, reason: Option<String>) {
        info!("❌ 客户端断开连接: {} 原因: {:?}", connection_id, reason);
        println!("❌ 客户端断开连接: {} 原因: {:?}", connection_id, reason);
    }
    
    fn on_error(&self, connection_id: String, err: FlareError) {
        warn!("❌ 服务端错误: {} 错误: {:?}", connection_id, err);
        println!("❌ 服务端错误: {} 错误: {:?}", connection_id, err);
    }
    
    fn on_message_received(&self, connection_id: String, frame: Frame) {
        // 通过连接ID前缀推断协议类型
        let protocol_info = if connection_id.starts_with("ws_") {
            "WebSocket"
        } else if connection_id.starts_with("quic_") {
            "QUIC"
        } else {
            "Unknown"
        };
        
        tracing::debug!("收到消息，connection_id: {}, message_id: {}, payload大小: {} 字节, 协议: {}", 
            connection_id, frame.message_id, frame.payload.len(), protocol_info);
        
        // 处理控制命令
        if let Command::Control(control_cmd) = &frame.command {
            match control_cmd {
                ControlCmd::AuthRequest(user_id, platform, token) => {
                    println!("🔐 用户 {} 使用平台 {} 和令牌 {} 请求认证", user_id, platform, token);
                    // 发送认证成功响应
                    let auth_response = ControlCmd::AuthResponse(true, "认证成功".to_string());
                    let response_frame = Frame::new(
                        Command::Control(auth_response),
                        FrameFactory::generate_message_id(),
                        Reliability::AtLeastOnce,
                    );
                    // 广播认证成功消息给所有连接
                    if let Err(e) = self.connection_manager.broadcast_message(response_frame) {
                        warn!("发送认证响应失败: {:?}", e);
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
                println!("[{}] [{}] {}: {}", 
                    time_str,
                    protocol_info,
                    chat_msg.sender, 
                    chat_msg.content
                );
                
                // 创建新的帧用于广播，避免发送者收到自己的消息
                let mut broadcast_frame = Frame::new(
                    frame.command.clone(),
                    FrameFactory::generate_message_id(),
                    frame.reliability,
                );
                broadcast_frame.payload = frame.payload.clone();
                
                // 广播消息给所有连接（除了发送者）
                if let Err(e) = self.broadcast_to_others(&connection_id, broadcast_frame) {
                    warn!("广播消息失败: {:?}", e);
                }
            }
            Err(e) => {
                warn!("解析消息失败: {:?}", e);
                // 尝试以文本形式显示
                if let Ok(text) = String::from_utf8(payload) {
                    tracing::debug!("原始消息内容: {}", text);
                    println!("📥 [{}] 收到消息: {}", protocol_info, text);
                    
                    // 创建新的帧用于广播
                    let mut broadcast_frame = Frame::new(
                        frame.command.clone(),
                        FrameFactory::generate_message_id(),
                        frame.reliability,
                    );
                    broadcast_frame.payload = frame.payload.clone();
                    
                    // 广播原始消息给其他连接
                    if let Err(e) = self.broadcast_to_others(&connection_id, broadcast_frame) {
                        warn!("广播消息失败: {:?}", e);
                    }
                }
            }
        }
    }
    
    fn on_message_sent(&self, _connection_id: String, _frame: Frame) {
        // 消息发送成功，可以在这里处理
    }
    
    fn on_heartbeat_ping(&self, _connection_id: String) {
        // println!("💓 心跳 Ping");
    }
    
    fn on_heartbeat_pong(&self, _connection_id: String, _rtt_ms: u32) {
        // println!("💚 心跳 Pong (RTT: {}ms)", rtt_ms);
    }
    
    fn on_heartbeat_timeout(&self, _connection_id: String) {
        // 心跳超时处理
    }
    
    fn on_quality_changed(&self, _connection_id: String, _quality: u8) {
        // 连接质量变化处理
    }
    
    fn on_statistics_updated(&self, _connection_id: String, _stats: ConnectionStats) {
        // 统计信息更新处理
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志，设置为debug级别
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    
    println!("🚀 启动简单QUIC聊天室服务端...");
    
    // 创建 QUIC 配置（需要 TLS 证书）
    let tls_config = TlsConfig::new(
        "certs/server.crt".to_string(),
        "certs/server.key".to_string(),
    );
    let quic_config = ProtocolConfig::new()
        .with_listen_addr("127.0.0.1:9006".to_string())
        .with_max_connections(100)
        .with_tls_config(tls_config);
    
    // 创建服务端配置（仅QUIC协议模式）
    let config = ServerConfig::new()
        .with_server_type(ServerType::Quic)  // 明确设置为QUIC服务器类型
        .with_quic_config(quic_config)
        .with_heartbeat_interval_ms(30000) // 30秒心跳（增加间隔减少频繁检查）
        .with_heartbeat_monitoring(60000, 90000); // 心跳监控超时60秒，清理间隔90秒（增加时间避免误判）
    
    // 创建连接管理器
    let connection_manager = Arc::new(ConnectionManagerImpl::new());
    
    // 创建聚合型服务端
    let server = Arc::new(AggregationServer::new_with_connection_manager(config, connection_manager.clone()));
    
    // 创建事件处理器
    let event_handler = Arc::new(ChatServerEventHandler::new(server.clone(), connection_manager.clone()));
    
    // 设置事件处理器
    server.set_event_handler(event_handler.clone()).await;
    
    // 启动服务端
    server.start().await?;
    
    println!("✅ 简单QUIC聊天室服务端启动成功！");
    println!("🌐 QUIC 地址: 127.0.0.1:9006");
    println!("📝 输入 '/quit' 退出服务端\n");
    
    // 创建 stdin 读取器
    let stdin = io::stdin();
    let reader = io::BufReader::new(stdin);
    let mut lines = reader.lines();
    
    // 读取终端输入
    loop {
        if let Some(line) = lines.next_line().await? {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            
            // 处理特殊命令
            if line == "/quit" {
                println!("👋 正在关闭聊天室服务端...");
                break;
            } else if line == "/users" {
                // 显示在线用户
                let connection_ids = connection_manager.all_connection_ids();
                println!("👥 在线用户 ({}/{}): {:?}", 
                    connection_ids.len(), connection_manager.connection_count(), connection_ids);
            } else if line == "/help" {
                println!("📋 可用命令:");
                println!("  /users - 显示在线用户");
                println!("  /help  - 显示帮助");
                println!("  /quit  - 退出服务端");
            } else {
                println!("❓ 未知命令，输入 '/help' 查看帮助");
            }
        }
        
        // 添加一个小延迟
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    
    // 停止服务端
    server.stop().await?;
    println!("✅ 聊天室服务端已关闭");
    
    Ok(())
}