//! 聊天室服务端示例
//!
//! 使用 FastServer 实现支持 WebSocket 和 QUIC 双协议的聊天室服务端
//! 集成带认证功能的连接管理器，确保只有认证通过的用户才能参与聊天

use std::sync::Arc;
use tokio::io::{self, AsyncBufReadExt};
use tracing::{info, warn};

use flare_core::{
    server::{
        config::{ServerConfig, ProtocolConfig, TlsConfig},
        fast::{
            server::FastServer,
            message_handler::AsyncMessageHandler,
        },
        manager::traits::ConnectionManager, // 导入 ConnectionManager trait
    },
    common::{
        protocol::{
            frame::Frame,
            factory::FrameFactory,
            reliability::Reliability,
            commands::{Command, ControlCmd},
        },
        error::FlareError,
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

/// 聊天室消息处理器
struct ChatRoomMessageHandler {
    /// 消息解析器
    parser: MessageParser,
    /// 服务端引用
    server: Arc<FastServer>,
}

impl ChatRoomMessageHandler {
    fn new(server: Arc<FastServer>) -> Self {
        Self {
            parser: MessageParser::new(PayloadCodec::Json),
            server,
        }
    }
    
    /// 处理消息的公共方法，可以被外部调用
    async fn handle_incoming_message(&self, user_id: String, frame: Frame) -> Result<(), FlareError> {
        tracing::debug!("处理用户 {} 的消息，message_id: {}", user_id, frame.message_id);
        self.handle_message(user_id, frame).await
    }
}

#[async_trait::async_trait]
impl AsyncMessageHandler for ChatRoomMessageHandler {
    async fn handle_message(&self, user_id: String, frame: Frame) -> Result<(), FlareError> {
        tracing::debug!("收到用户 {} 的消息，message_id: {}", user_id, frame.message_id);
        tracing::debug!("帧信息: reliability={:?}, payload大小={}字节", frame.reliability, frame.payload.len());
        tracing::debug!("命令类型: {:?}", frame.command);
        
        // 处理控制命令
        if let Command::Control(control_cmd) = &frame.command {
            tracing::debug!("收到控制命令: {:?}", control_cmd);
            match control_cmd {
                ControlCmd::AuthRequest(user_id, platform, token) => {
                    // 处理认证请求
                    self.handle_authentication(&user_id, platform, token).await?;
                    return Ok(());
                }
                _ => {
                    tracing::debug!("未处理的控制命令: {:?}", control_cmd);
                }
            }
        }
        
        // 解析消息内容
        let payload = frame.payload.to_vec();
        tracing::debug!("消息payload大小: {} 字节", payload.len());
        
        // 尝试解析为聊天消息
        match self.parser.codec().decode::<ChatMessage>(&payload) {
            Ok(chat_message) => {
                tracing::debug!("消息解析成功: 发送者={}, 内容={}", chat_message.sender, chat_message.content);
                
                // 如果是系统命令
                if chat_message.content.starts_with("/system") {
                    tracing::debug!("处理系统命令: {}", chat_message.content);
                    // 只允许系统管理员发送系统命令
                    if user_id == "admin" {
                        let parts: Vec<&str> = chat_message.content.splitn(2, ' ').collect();
                        if parts.len() == 2 {
                            let command = parts[1];
                            match command {
                                "users" => {
                                    // 获取在线用户列表
                                    if let Some(auth_manager) = self.server.auth_connection_manager() {
                                        let connection_ids = auth_manager.all_connection_ids();
                                        let mut users = Vec::new();
                                        for conn_id in &connection_ids {
                                            if let Some(user_id) = auth_manager.get_connection_user_id(conn_id) {
                                                users.push(user_id);
                                            }
                                        }
                                        
                                        let response = format!("在线用户 ({}/{}): {:?}", 
                                            users.len(), auth_manager.connection_count(), users);
                                        self.send_system_message(&user_id, response).await?;
                                    }
                                }
                                _ => {
                                    self.send_system_message(&user_id, "未知系统命令".to_string()).await?;
                                }
                            }
                        }
                    } else {
                        self.send_system_message(&user_id, "权限不足".to_string()).await?;
                    }
                    return Ok(());
                }
                
                tracing::debug!("广播普通消息: 发送者={}, 内容={}", chat_message.sender, chat_message.content);
                
                // 广播消息给所有已认证用户
                self.broadcast_message(&user_id, &chat_message).await?;
            }
            Err(e) => {
                tracing::warn!("解析用户 {} 的消息失败: {:?}", user_id, e);
                // 即使解析失败，也尝试以文本形式显示
                if let Ok(text) = String::from_utf8(payload) {
                    tracing::debug!("原始消息内容: {}", text);
                    // 创建一个简单的聊天消息
                    let simple_msg = ChatMessage::new(user_id.clone(), text);
                    self.broadcast_message(&user_id, &simple_msg).await?;
                }
            }
        }
        
        Ok(())
    }
}

impl ChatRoomMessageHandler {
    /// 处理认证请求
    async fn handle_authentication(&self, user_id: &str, platform: &str, token: &str) -> Result<(), FlareError> {
        info!("处理用户 {} 的认证请求，平台: {}, 令牌: {}", user_id, platform, token);
        
        // 简单的认证逻辑：检查用户ID和令牌不为空
        if user_id.is_empty() || token.is_empty() {
            // 发送认证失败响应
            let response_msg = format!("认证失败：用户ID或令牌不能为空");
            self.send_auth_response(user_id, false, response_msg).await?;
            return Ok(());
        }
        
        // 在实际应用中，这里应该验证令牌的有效性
        // 为简化示例，我们假设所有非空令牌都是有效的
        
        // 认证成功，更新连接的用户ID
        // 注意：我们需要实际调用认证管理器来认证连接
        let response_msg = format!("认证成功");
        self.send_auth_response(user_id, true, response_msg).await?;
        
        Ok(())
    }
    
    /// 发送认证响应
    async fn send_auth_response(&self, user_id: &str, success: bool, message: String) -> Result<(), FlareError> {
        tracing::debug!("发送认证响应给用户 {}: success={}, message={}", user_id, success, message);
        
        // 创建认证响应命令
        let control_cmd = ControlCmd::AuthResponse(success, message);
        let command = Command::Control(control_cmd);
        
        // 创建帧
        let frame = Frame::new(
            command,
            FrameFactory::generate_message_id(),
            Reliability::AtLeastOnce,
        );
        
        tracing::debug!("认证响应帧创建完成，message_id: {}", frame.message_id);
        
        // 发送消息给指定用户
        if let Some(auth_manager) = self.server.auth_connection_manager() {
            // 查找用户的连接并发送消息
            let connection_ids = auth_manager.all_connection_ids();
            for conn_id in &connection_ids {
                if let Some(conn_user_id) = auth_manager.get_connection_user_id(conn_id) {
                    if &conn_user_id == user_id {
                        // 获取连接并发送消息
                        if let Some(connection) = auth_manager.get_connection(conn_id) {
                            tracing::debug!("向用户 {} 发送认证响应", user_id);
                            if let Err(e) = connection.send_message(frame.clone()) {
                                warn!("向用户 {} 发送认证响应失败: {:?}", user_id, e);
                            } else {
                                tracing::debug!("认证响应发送成功");
                            }
                        }
                        break;
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// 广播消息给所有用户
    async fn broadcast_message(&self, sender_id: &str, message: &ChatMessage) -> Result<(), FlareError> {
        info!("广播消息: {} - {}", message.sender, message.content);
        
        // 创建广播消息，确保发送者ID正确
        let broadcast_msg = ChatMessage::new(
            if message.sender.is_empty() { sender_id.to_string() } else { message.sender.clone() },
            message.content.clone()
        );
        
        tracing::debug!("广播消息内容: {:?}", broadcast_msg);
        
        // 序列化消息
        let payload = self.parser.codec().encode(&broadcast_msg)
            .map_err(|e| FlareError::serialization_error(format!("序列化失败: {}", e)))?;
        
        tracing::debug!("广播消息序列化完成，payload大小: {} 字节", payload.len());
        
        // 创建数据帧
        let frame = FrameFactory::create_data_frame(
            FrameFactory::generate_message_id(),
            payload,
            Reliability::AtLeastOnce,
        ).map_err(|e| FlareError::general_error(e))?;
        
        tracing::debug!("广播消息帧创建完成，message_id: {}", frame.message_id);
        
        // 广播消息
        if let Some(auth_manager) = self.server.auth_connection_manager() {
            let stats = auth_manager.broadcast_message(frame)?;
            tracing::debug!("消息广播完成，成功: {}，失败: {}", stats.success, stats.failed);
        }
        
        Ok(())
    }
    
    /// 发送系统消息给指定用户
    async fn send_system_message(&self, user_id: &str, content: String) -> Result<(), FlareError> {
        tracing::debug!("发送系统消息给用户 {}: {}", user_id, content);
        
        // 创建系统消息
        let system_msg = ChatMessage::new(
            "System".to_string(),
            content
        );
        
        // 序列化消息
        let payload = self.parser.codec().encode(&system_msg)
            .map_err(|e| FlareError::serialization_error(format!("序列化失败: {}", e)))?;
        
        tracing::debug!("系统消息序列化完成，payload大小: {} 字节", payload.len());
        
        // 创建数据帧
        let frame = FrameFactory::create_data_frame(
            FrameFactory::generate_message_id(),
            payload,
            Reliability::AtLeastOnce,
        ).map_err(|e| FlareError::general_error(e))?;
        
        tracing::debug!("系统消息帧创建完成，message_id: {}", frame.message_id);
        
        // 发送消息给指定用户
        if let Some(auth_manager) = self.server.auth_connection_manager() {
            // 查找用户的连接并发送消息
            let connection_ids = auth_manager.all_connection_ids();
            for conn_id in &connection_ids {
                if let Some(conn_user_id) = auth_manager.get_connection_user_id(conn_id) {
                    if &conn_user_id == user_id {
                        // 获取连接并发送消息
                        if let Some(connection) = auth_manager.get_connection(conn_id) {
                            tracing::debug!("向用户 {} 发送系统消息", user_id);
                            if let Err(e) = connection.send_message(frame.clone()) {
                                warn!("向用户 {} 发送系统消息失败: {:?}", user_id, e);
                            } else {
                                tracing::debug!("系统消息发送成功");
                            }
                        }
                        break;
                    }
                }
            }
        }
        
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志，设置为debug级别
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    
    println!("🚀 启动聊天室服务端...");
    
    // 创建 WebSocket 配置
    let ws_config = ProtocolConfig::new()
        .with_listen_addr("127.0.0.1:9005".to_string())
        .with_max_connections(100);
    
    // 创建 QUIC 配置（需要 TLS 证书）
    let tls_config = TlsConfig::new(
        "certs/server.crt".to_string(),
        "certs/server.key".to_string(),
    );
    let quic_config = ProtocolConfig::new()
        .with_listen_addr("127.0.0.1:9006".to_string())
        .with_max_connections(100)
        .with_tls_config(tls_config);
    
    // 创建服务端配置（双协议模式）
    let config = ServerConfig::new()
        .with_dual_protocol_config(ws_config, quic_config);
    
    // 创建带认证功能的 FastServer
    let server = Arc::new(FastServer::new_with_auth(config, 30000));
    
    // 创建消息处理器
    let message_handler = Arc::new(ChatRoomMessageHandler::new(server.clone()));
    
    // 设置异步消息处理器
    server.set_async_message_handler(message_handler.clone()).await;
    
    // 启动服务端
    server.start().await?;
    
    println!("✅ 聊天室服务端启动成功！");
    println!("🌐 WebSocket 地址: 127.0.0.1:9005");
    println!("🌐 QUIC 地址: 127.0.0.1:9006");
    println!("🔐 认证超时: 30秒");
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
                if let Some(auth_manager) = server.auth_connection_manager() {
                    let connection_ids = auth_manager.all_connection_ids();
                    let mut users = Vec::new();
                    for conn_id in &connection_ids {
                        if let Some(user_id) = auth_manager.get_connection_user_id(conn_id) {
                            users.push(user_id);
                        }
                    }
                    
                    println!("👥 在线用户 ({}/{}): {:?}", 
                        users.len(), auth_manager.connection_count(), users);
                }
            } else if line == "/help" {
                println!("📋 可用命令:");
                println!("  /users - 显示在线用户");
                println!("  /help  - 显示帮助");
                println!("  /quit  - 退出服务端");
            } else {
                println!("❓ 未知命令，输入 '/help' 查看帮助");
            }
        }
    }
    
    // 停止服务端
    server.stop().await?;
    println!("✅ 聊天室服务端已关闭");
    
    Ok(())
}