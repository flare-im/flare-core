//! QUIC 客户端连接示例 (Client版本)
//!
//! 展示如何使用flare-core的Client类创建QUIC客户端并进行通信

use std::sync::Arc;
use std::time::Instant;
use tracing::{info, error, warn, debug};

// 修改rustls的引用
use rustls::crypto::ring;

use flare_core::{
    client::{
        Client,
        config::{ClientConfig, ProtocolSelection},
        event::ClientEvent,
        ClientBuilder,
    },
    common::{
        connections::{
            types::{Transport},
        },
        protocol::{Reliability, Frame, commands::{Command, MessageCmd, MessageSendCommand}},
        serialization::{SerializationFormat, SerializationConfig},
    },
};

/// QUIC客户端事件处理器
#[derive(Debug)]
pub struct QuicClientEventHandler {
    pub name: String,
}

impl QuicClientEventHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait::async_trait]
impl ClientEvent for QuicClientEventHandler {
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
impl flare_core::common::connections::event::ConnectionEvent for QuicClientEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] QUIC连接已建立: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] QUIC连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] QUIC连接错误: {} - 错误: {}", self.name, connection_id, error);
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
        
        info!("[{}] 收到QUIC服务器消息: {} - 类型: {} - 内容长度: {}", 
              self.name, connection_id, message.get_command_type_str(), content_length);
    }

    async fn on_message_sent(&self, connection_id: &str, message: &flare_core::common::protocol::Frame) {
        info!("[{}] QUIC数据消息已发送: {} - 类型: {}", 
              self.name, connection_id, message.get_command_type_str());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("[{}] QUIC心跳超时: {}", self.name, connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] QUIC连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        info!("[{}] QUIC心跳已发送: {}", self.name, connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        info!("[{}] 收到QUIC心跳响应: {}", self.name, connection_id);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        info!("[{}] QUIC开始重连: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        info!("[{}] QUIC重连成功: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        error!("[{}] QUIC重连失败: {} - 尝试次数: {} - 错误: {}", self.name, connection_id, attempt, error);
        // 当重连失败时，可以在这里添加终止程序的逻辑
        if attempt >= 5 {
            error!("[{}] 重连尝试次数已达到上限，程序将退出", self.name);
            std::process::exit(1);
        }
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) {
        info!("[{}] QUIC统计信息更新: {} - 收到消息: {} - 发送消息: {}", 
             self.name, connection_id, stats.messages_received, stats.messages_sent);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    // 初始化CryptoProvider
    if let Err(e) = rustls::crypto::ring::default_provider().install_default() {
        error!("设置 rustls 加密提供者失败: {:?}", e);
        // 继续执行，因为可能已经设置过了
    }
    
    info!("启动Client QUIC客户端示例");
    
    // 创建序列化配置 - 使用 Protobuf
    let serialization_config = SerializationConfig::builder()
        .format(SerializationFormat::Protobuf)
        .build();
    
    // 设置事件处理器
    let event_handler = Arc::new(QuicClientEventHandler::new("Client QUIC客户端".to_string()));
    
    // 创建客户端配置
    let config = ClientConfig::default()
        .with_protocol_selection(ProtocolSelection::QuicOnly)
        .with_server_address(Transport::Quic, "127.0.0.1:8081".to_string())
        .with_heartbeat(5000, 2000)  // 5秒心跳，2秒超时
        .with_serialization(serialization_config);
    
    // 创建客户端实例
    let mut client = ClientBuilder::new(config)
        .with_client_event_handler(event_handler)
        .build();
    
    // 启动客户端
    info!("正在连接QUIC服务端...");
    let connect_start = Instant::now();
    
    // 使用更好的错误处理
    match client.connect().await {
        Ok(()) => {
            let connect_time = connect_start.elapsed();
            info!("✅ 已连接到QUIC服务端！连接耗时: {:.2}ms", connect_time.as_secs_f64() * 1000.0);
        }
        Err(e) => {
            error!("❌ 连接QUIC服务端失败: {}", e);
            error!("请确保服务端已启动并监听在 127.0.0.1:8081");
            // 等待一段时间让事件处理器处理错误
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            return Err(e.into());
        }
    }
    
    // 发送测试消息
    info!("发送测试消息...");
    for i in 1..=5 {
        let message_id = format!("msg_{}", i);
        let send_cmd = MessageSendCommand::new(
            format!("Hello from Client QUIC client! Message #{}", i).into_bytes()
        );
        let command = Command::Message(MessageCmd::Send(send_cmd));
        
        if let Err(e) = client.send_fire_and_forget(
            |_| Ok(command.clone()),
            Reliability::AtLeastOnce
        ).await {
            error!("发送测试消息 #{} 失败: {}", i, e);
        } else {
            info!("测试消息 #{} 已发送", i);
        }
        
        // 等待一段时间
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
    
    // 启动心跳任务
    info!("启动心跳任务...");
    let client_clone = client.clone();
    let heartbeat_interval = std::time::Duration::from_secs(5); // 5秒心跳间隔
    
    let heartbeat_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(heartbeat_interval);
        loop {
            interval.tick().await;
            
            // 检查连接状态
            if client_clone.is_connected().await {
                let config = client_clone.get_config();
                info!("💓 心跳状态正常 - 间隔: {}ms, 超时: {}ms", 
                      config.heartbeat_interval_ms, config.heartbeat_timeout_ms);
            } else {
                warn!("连接已断开，停止心跳任务");
                break;
            }
        }
    });
    
    // 等待一段时间以接收响应
    info!("等待 30 秒接收服务器响应...");
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    
    // 停止心跳任务
    info!("停止心跳任务...");
    heartbeat_task.abort();
    
    // 停止客户端
    info!("正在停止客户端...");
    if let Err(e) = client.disconnect().await {
        error!("停止客户端时发生错误: {}", e);
    } else {
        info!("客户端已停止");
    }
    
    Ok(())
}