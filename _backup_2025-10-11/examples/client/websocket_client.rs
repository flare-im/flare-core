//! WebSocket 客户端连接示例 (FastClient版本)
//!
//! 展示如何使用flare-core的FastClient创建WebSocket客户端并进行通信

use std::sync::Arc;
use std::time::Instant;
use tokio::io::{self, AsyncBufReadExt};
use tracing::{info, error, warn, debug};

use flare_core::{
    client::{
        config::{ClientConfig, ProtocolSelection},
        event::ClientEvent,
        ClientBuilder,
    },
    common::{
        connections::{
            types::{Transport},
        },
        protocol::{Reliability, commands::{Command, MessageCmd, MessageSendCommand}},
        serialization::{SerializationFormat, SerializationConfig},
    },
};

/// WebSocket客户端事件处理器
#[derive(Debug)]
pub struct WebSocketClientEventHandler {
    pub name: String,
}

impl WebSocketClientEventHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait::async_trait]
impl ClientEvent for WebSocketClientEventHandler {
    async fn on_control_command(&self, cmd: &flare_core::common::protocol::commands::ControlCmd) {
        info!("[{}] 收到控制命令: {}", self.name, cmd.as_str());
    }

    async fn on_message_command(&self, message: &flare_core::common::protocol::commands::MessageCmd) {
        info!("[{}] 收到消息命令: {}", self.name, message.as_str());
    }

    async fn on_notification_command(&self, notification: &flare_core::common::protocol::commands::NotificationCmd) {
        info!("[{}] 收到通知命令: {}", self.name, notification.as_str());
    }

    async fn on_event_command(&self, event: &flare_core::common::protocol::commands::EventCmd) {
        info!("[{}] 收到事件命令: {}", self.name, event.as_str());
    }
    
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] 客户端连接已建立: {}", self.name, connection_id);
    }
    
    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] 客户端连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }
    
    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] 客户端连接错误: {} - 错误: {}", self.name, connection_id, error);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] 连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
    }
    
    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) {
        info!("[{}] 统计信息更新: {} - 收到: {} - 发送: {} - 质量: {}", 
             self.name, connection_id, stats.messages_received, stats.messages_sent, stats.quality_score);
    }
    
    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) -> bool {
        info!("[{}] 开始重连: {} - 尝试次数: {}", self.name, connection_id, attempt);
        true // 允许重连
    }
    
    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        info!("[{}] 重连成功: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }
    
    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) -> bool {
        error!("[{}] 重连失败: {} - 尝试次数: {} - 错误: {}", self.name, connection_id, attempt, error);
        attempt < 5 // 最多重连5次
    }
    
    async fn on_protocol_switched(&self, connection_id: &str, from_protocol: &str, to_protocol: &str) {
        info!("[{}] 协议切换: {} - 从 {} 切换到 {}", self.name, connection_id, from_protocol, to_protocol);
    }
    
    async fn on_heartbeat_timeout(&self, connection_id: &str) -> bool {
        warn!("[{}] 心跳超时: {}", self.name, connection_id);
        true // 允许重连
    }
    
    async fn on_heartbeat_ping(&self, connection_id: &str) {
        debug!("[{}] 收到心跳ping: {}", self.name, connection_id);
    }
    
    async fn on_heartbeat_pong(&self, connection_id: &str) {
        debug!("[{}] 收到心跳pong: {}", self.name, connection_id);
    }
}

#[async_trait::async_trait]
impl flare_core::common::connections::event::ConnectionEvent for WebSocketClientEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] WebSocket连接已建立: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] WebSocket连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] WebSocket连接错误: {} - 错误: {}", self.name, connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &flare_core::common::protocol::Frame) {
        // 获取消息内容长度
        let content_length = match &message.command {
            flare_core::common::protocol::commands::Command::Message(msg_cmd) => {
                match msg_cmd {
                    flare_core::common::protocol::commands::MessageCmd::Send(send_cmd) => send_cmd.data.len(),
                    flare_core::common::protocol::commands::MessageCmd::Data(data_cmd) => data_cmd.data.len(),
                    _ => 0,
                }
            },
            _ => 0,
        };
        
        info!("[{}] 收到WebSocket服务器消息: {} - 类型: {} - 内容长度: {}", 
              self.name, connection_id, message.get_command_type_str(), content_length);
    }

    async fn on_message_sent(&self, connection_id: &str, message: &flare_core::common::protocol::Frame) {
        info!("[{}] WebSocket数据消息已发送: {} - 类型: {}", 
              self.name, connection_id, message.get_command_type_str());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("[{}] WebSocket心跳超时: {}", self.name, connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] WebSocket连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        info!("[{}] WebSocket心跳已发送: {}", self.name, connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        info!("[{}] 收到WebSocket心跳响应: {}", self.name, connection_id);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        info!("[{}] WebSocket开始重连: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        info!("[{}] WebSocket重连成功: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        error!("[{}] WebSocket重连失败: {} - 尝试次数: {} - 错误: {}", self.name, connection_id, attempt, error);
        // 当重连失败时，可以在这里添加终止程序的逻辑
        if attempt >= 5 {
            error!("[{}] 重连尝试次数已达到上限，程序将退出", self.name);
            std::process::exit(1);
        }
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) {
        info!("[{}] WebSocket统计信息更新: {} - 收到消息: {} - 发送消息: {}", 
             self.name, connection_id, stats.messages_received, stats.messages_sent);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("启动FastClient WebSocket客户端示例");
    
    // 创建序列化配置
    let serialization_config = SerializationConfig::builder()
        .format(SerializationFormat::Protobuf)
        .build();
    
    // 设置事件处理器
    let event_handler = Arc::new(WebSocketClientEventHandler::new("Client WebSocket客户端".to_string()));
    
    // 创建客户端配置 - 简化配置，心跳和重连默认启用
    let config = ClientConfig::default()
        .with_protocol_selection(ProtocolSelection::WebSocketOnly)
        .with_server_address(Transport::WebSocket, "ws://127.0.0.1:8080".to_string())
        .with_heartbeat(15000, 60000)  // 15秒心跳，60秒超时
        .with_serialization(serialization_config)
        .with_auto_reconnect(10)       // 最多重连10次
        .with_reconnect_delay(2000);   // 2秒重连延迟
    
    // 创建客户端实例
    let client = ClientBuilder::new(config)
        .with_client_event_handler(event_handler)
        .build();
    
    // 启动客户端
    info!("正在连接WebSocket服务端...");
    let connect_start = Instant::now();
    
    // 使用更好的错误处理
    match client.connect().await {
        Ok(()) => {
            let connect_time = connect_start.elapsed();
            info!("✅ 已连接到WebSocket服务端！连接耗时: {:.2}ms", connect_time.as_secs_f64() * 1000.0);
        }
        Err(e) => {
            error!("❌ 连接WebSocket服务端失败: {}", e);
            error!("请确保服务端已启动并监听在 ws://127.0.0.1:8080");
            // 等待一段时间让事件处理器处理错误
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            return Err(e.into());
        }
    }
    
    // 客户端现在有自动心跳和重连功能，无需手动管理
    info!("客户端已启用自动心跳和重连功能");
    info!("- 自动心跳间隔: {}ms", client.get_config().heartbeat_interval_ms);
    info!("- 心跳超时: {}ms", client.get_config().heartbeat_timeout_ms);
    info!("- 自动重连: {}", if client.is_auto_reconnect_enabled() { 
        format!("启用 (最多{}次)", client.get_config().max_reconnect_attempts) 
    } else { "禁用".to_string() });
    info!("- 重连延迟: {}ms", client.get_config().reconnect_delay_ms);
    
    // 启动交互式模式
    info!("启动用户输入处理（交互式模式）...");
    let client_input = client.clone();
    let input_task = tokio::spawn(async move {
        let stdin = io::stdin();
        let mut reader = io::BufReader::new(stdin);
        let mut line = String::new();
        
        println!("\n=== WebSocket 客户端交互模式 ===");
        println!("输入消息发送到服务器，输入 'quit' 或 'exit' 退出");
        println!("输入 'status' 查看连接状态");
        println!("输入 'ping' 查看心跳状态（自动心跳已启用）");
        println!("=====================================\n");
        
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => {
                    // EOF
                    info!("输入流结束");
                    break;
                }
                Ok(_) => {
                    let input = line.trim();
                    
                    // 处理特殊命令
                    match input.to_lowercase().as_str() {
                        "quit" | "exit" => {
                            info!("用户请求退出");
                            break;
                        }
                        "status" => {
                            let is_connected = client_input.is_connected().await;
                            let state = client_input.get_state().await;
                            println!("连接状态: {:?}, 已连接: {}", state, is_connected);
                            continue;
                        }
                        "ping" => {
                            let is_connected = client_input.is_connected().await;
                            let config = client_input.get_config();
                            println!("💓 心跳状态: {}", if is_connected { "正常" } else { "断开" });
                            println!("   - 心跳间隔: {}ms", config.heartbeat_interval_ms);
                            println!("   - 心跳超时: {}ms", config.heartbeat_timeout_ms);
                            continue;
                        }
                        "" => continue, // 空输入跳过
                        _ => {} // 继续处理普通消息
                    }
                    
                    // 发送用户输入的消息
                    let send_cmd = MessageSendCommand::new(input.as_bytes().to_vec());
                    let command = Command::Message(MessageCmd::Send(send_cmd));
                    
                    match client_input.send_fire_and_forget(
                        |_| Ok(command),
                        Reliability::AtLeastOnce
                    ).await {
                        Ok(_) => {
                            println!("✅ 消息已发送: {}", input);
                        }
                        Err(e) => {
                            println!("❌ 发送消息失败: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("读取用户输入失败: {}", e);
                    break;
                }
            }
        }
    });
    
    // 等待用户输入任务完成
    info!("等待用户输入...");
    input_task.await?;
    
    // 停止客户端（自动心跳和重连任务会自动停止）
    info!("正在停止客户端...");
    if let Err(e) = client.disconnect().await {
        error!("停止客户端时发生错误: {}", e);
    } else {
        info!("客户端已停止");
    }
    
    Ok(())
}