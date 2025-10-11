//! ChatClient 客户端示例（新文件）
//!
//! 使用 FastClient 协议竞速连接到 ChatServer，支持用户名输入和消息发送

use std::sync::Arc;
use tracing::{info, error};
use tokio::io::{AsyncBufReadExt, BufReader};

use flare_core::{
    client::{
        config::{ClientConfig, ProtocolSelection},
        fast::{FastClientBuilder, event::FastEvent},
    },
    common::{
        serialization::SerializationFormat,
        connections::types::Transport,
        protocol::factory::FrameFactory,
        protocol::Reliability,
        protocol::commands::{MessageCmd, DataCommand},
    },
};

/// 自定义客户端事件处理器：打印核心事件和聊天消息
pub struct CustomClientEventHandler { pub name: String }

#[async_trait::async_trait]
impl FastEvent for CustomClientEventHandler {
    async fn on_connected(&self, connection_id: &str) { info!("[{}] 已连接到服务器: {}", self.name, connection_id); }
    async fn on_disconnected(&self, connection_id: &str, reason: &str) { info!("[{}] 已断开连接: {} - 原因: {}", self.name, connection_id, reason); }
    async fn on_error(&self, connection_id: &str, error: &str) { error!("[{}] 连接错误: {} - 错误: {}", self.name, connection_id, error); }
    async fn on_heartbeat_timeout(&self, connection_id: &str) -> bool { info!("[{}] 心跳超时: {}", self.name, connection_id); true }
    async fn on_heartbeat_ping(&self, connection_id: &str) { info!("[{}] 心跳已发送: {}", self.name, connection_id); }
    async fn on_heartbeat_pong(&self, connection_id: &str) { info!("[{}] 收到心跳响应: {}", self.name, connection_id); }
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) { info!("[{}] 连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score); }
    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) -> bool { info!("[{}] 开始重连: {} - 尝试次数: {}", self.name, connection_id, attempt); true }
    async fn on_reconnected(&self, connection_id: &str, attempt: u32) { info!("[{}] 重连成功: {} - 尝试次数: {}", self.name, connection_id, attempt); }
    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) -> bool { error!("[{}] 重连失败: {} - 尝试次数: {} - 错误: {}", self.name, connection_id, attempt, error); attempt < 5 }
    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) { info!("[{}] 统计信息更新: {} - 收到消息: {} - 发送消息: {}", self.name, connection_id, stats.messages_received, stats.messages_sent); }
    async fn on_control_command(&self, _cmd: &flare_core::common::protocol::commands::ControlCmd) {}
    async fn on_message_command(&self, message: &flare_core::common::protocol::commands::MessageCmd) {
        if let MessageCmd::Data(DataCommand { data }) = message {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(data) {
                let user = v.get("user").and_then(|x| x.as_str()).unwrap_or("?");
                let text = v.get("text").and_then(|x| x.as_str()).unwrap_or("");
                if !text.is_empty() { println!("{}: {}", user, text); }
            }
        }
    }
    async fn on_notification_command(&self, _notification: &flare_core::common::protocol::commands::NotificationCmd) {}
    async fn on_event_command(&self, _event: &flare_core::common::protocol::commands::EventCmd) {}
    async fn on_authenticated(&self) { info!("[{}] 认证成功", self.name); }
    async fn on_authentication_failed(&self, error: &str) { error!("[{}] 认证失败: {}", self.name, error); }
    async fn on_heartbeat_sent(&self) { info!("[{}] 心跳发送成功", self.name); }
    async fn on_heartbeat_failed(&self, error: &str) { error!("[{}] 心跳发送失败: {}", self.name, error); }
    async fn on_auto_reconnect_started(&self, attempt: u32) { info!("[{}] 开始自动重连: 尝试次数 {}", self.name, attempt); }
    async fn on_auto_reconnect_success(&self, attempt: u32) { info!("[{}] 自动重连成功: 尝试次数 {}", self.name, attempt); }
    async fn on_auto_reconnect_failed(&self, attempt: u32, error: &str) { error!("[{}] 自动重连失败: 尝试次数 {} - 错误: {}", self.name, attempt, error); }
    async fn on_connection_quality_monitored(&self, quality_score: u8, latency_ms: u64) { info!("[{}] 连接质量监控: 评分 {} - 延迟 {}ms", self.name, quality_score, latency_ms); }
    async fn on_connection_state_changed(&self, old_state: &str, new_state: &str) { info!("[{}] 连接状态变化: {} -> {}", self.name, old_state, new_state); }
    async fn on_protocol_switched(&self, connection_id: &str, from_protocol: &str, to_protocol: &str) { info!("[{}] 协议切换: {} - 从 {} 切换到 {}", self.name, connection_id, from_protocol, to_protocol); }
}

impl CustomClientEventHandler { pub fn new(name: String) -> Self { Self { name } } }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

    // 输入用户名
    println!("请输入用户名：");
    let mut username = String::new();
    std::io::stdin().read_line(&mut username)?;
    let username = username.trim().to_string();
    let username = if username.is_empty() { "user_anon".to_string() } else { username };

    info!("启动 ChatClient 客户端示例 (用户: {})", username);

    // 客户端配置：WebSocket + QUIC，启用协议竞速
    let config = ClientConfig::new(
        "ws://127.0.0.1:4320".to_string(),
        "127.0.0.1:4321".to_string()
    )
    .with_protocol_selection(ProtocolSelection::Auto)
    .with_heartbeat(10000, 30000)
    .with_serialization(flare_core::common::serialization::SerializationConfig {
        format: SerializationFormat::Protobuf,
        ..Default::default()
    });

    if let Err(e) = config.validate() { error!("配置验证失败: {}", e); return Err(e.into()); }

    // 事件处理器
    let event_handler = Arc::new(CustomClientEventHandler::new("ChatClient".to_string()));

    // 创建 FastClient：启用认证（用户名作为 user_id），Token 简化为固定值
    let mut client = FastClientBuilder::new()
        .with_server_address(Transport::WebSocket, "ws://127.0.0.1:4320".to_string())
        .with_server_address(Transport::Quic, "127.0.0.1:4321".to_string())
        .with_protocol_selection(ProtocolSelection::Auto)
        .with_heartbeat(10000, 30000)
        .with_serialization(flare_core::common::serialization::SerializationConfig { format: SerializationFormat::Protobuf, ..Default::default() })
        .with_auth_enabled(false)
        .with_auth_user_id(username.clone())
        .with_auth_platform("desktop".to_string())
        .with_auth_token("chat_token".to_string())
        .with_event_handler(event_handler)
        .build();

    info!("ChatClient 实例创建成功");

    // 启动客户端
    info!("正在启动客户端...");
    client.start().await?;
    info!("✅ 客户端启动成功！");

    // 读取终端输入并发送聊天消息
    println!("请输入聊天内容，按 Ctrl+C 退出");
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 { break; } // EOF
        let text = line.trim().to_string();
        if text.is_empty() { continue; }

        let payload = serde_json::json!({"user": username, "text": text});
        let message_id = FrameFactory::generate_message_id();
        let frame = FrameFactory::create_data_frame(message_id, serde_json::to_vec(&payload)?, Reliability::AtLeastOnce)?;
        if let Err(e) = client.send_message(frame).await { error!("发送消息失败: {}", e); }
    }

    // 停止客户端
    info!("正在停止客户端...");
    client.stop().await?;
    info!("✅ 客户端已停止");

    Ok(())
}
