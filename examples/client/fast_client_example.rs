//! FastClient 客户端示例
//!
//! 展示如何使用 FastClient 创建高性能的客户端，支持协议竞速和自动重连

use std::sync::Arc;
use std::time::Duration;
use tracing::{info, error, warn, debug};

use flare_core::{
    client::{
        config::{ClientConfig, ProtocolSelection},
        fast::{FastClient, FastClientBuilder, event::FastEvent},
    },
    common::{
        protocol::Frame,
        serialization::SerializationFormat,
    },
};

/// 自定义客户端事件处理器
#[derive(Debug)]
pub struct CustomClientEventHandler {
    pub name: String,
}

#[async_trait::async_trait]
impl FastEvent for CustomClientEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] 已连接到服务器: {}", self.name, connection_id);
    }
    
    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] 已断开连接: {} - 原因: {}", self.name, connection_id, reason);
    }
    
    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] 连接错误: {} - 错误: {}", self.name, connection_id, error);
    }
    
    
    async fn on_heartbeat_timeout(&self, connection_id: &str) -> bool {
        info!("[{}] 心跳超时: {}", self.name, connection_id);
        true // 允许重连
    }
    
    async fn on_heartbeat_ping(&self, connection_id: &str) {
        info!("[{}] 心跳已发送: {}", self.name, connection_id);
    }
    
    async fn on_heartbeat_pong(&self, connection_id: &str) {
        info!("[{}] 收到心跳响应: {}", self.name, connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] 连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
    }
    
    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) -> bool {
        info!("[{}] 开始重连: {} - 尝试次数: {}", self.name, connection_id, attempt);
        true // 允许重连
    }
    
    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        info!("[{}] 重连成功: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }
    
    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) -> bool {
        info!("[{}] 重连失败: {} - 尝试次数: {} - 错误: {}", self.name, connection_id, attempt, error);
        attempt < 5 // 最多重连5次
    }
    
    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) {
        info!("[{}] 统计信息更新: {} - 收到消息: {} - 发送消息: {}", 
             self.name, connection_id, stats.messages_received, stats.messages_sent);
    }
    
    async fn on_control_command(&self, cmd: &flare_core::common::protocol::commands::ControlCmd) {
        info!("[{}] 收到控制命令: {:?}", self.name, cmd);
    }
    
    async fn on_message_command(&self, message: &flare_core::common::protocol::commands::MessageCmd) {
        info!("[{}] 收到消息命令: {:?}", self.name, message);
    }
    
    async fn on_notification_command(&self, notification: &flare_core::common::protocol::commands::NotificationCmd) {
        info!("[{}] 收到通知命令: {:?}", self.name, notification);
    }
    
    async fn on_event_command(&self, event: &flare_core::common::protocol::commands::EventCmd) {
        info!("[{}] 收到事件命令: {:?}", self.name, event);
    }
    
    async fn on_authenticated(&self) {
        info!("[{}] 认证成功", self.name);
    }
    
    async fn on_authentication_failed(&self, error: &str) {
        error!("[{}] 认证失败: {}", self.name, error);
    }
    
    async fn on_heartbeat_sent(&self) {
        info!("[{}] 心跳发送成功", self.name);
    }
    
    async fn on_heartbeat_failed(&self, error: &str) {
        error!("[{}] 心跳发送失败: {}", self.name, error);
    }
    
    async fn on_auto_reconnect_started(&self, attempt: u32) {
        info!("[{}] 开始自动重连: 尝试次数 {}", self.name, attempt);
    }
    
    async fn on_auto_reconnect_success(&self, attempt: u32) {
        info!("[{}] 自动重连成功: 尝试次数 {}", self.name, attempt);
    }
    
    async fn on_auto_reconnect_failed(&self, attempt: u32, error: &str) {
        error!("[{}] 自动重连失败: 尝试次数 {} - 错误: {}", self.name, attempt, error);
    }
    
    async fn on_connection_quality_monitored(&self, quality_score: u8, latency_ms: u64) {
        info!("[{}] 连接质量监控: 评分 {} - 延迟 {}ms", self.name, quality_score, latency_ms);
    }
    
    async fn on_connection_state_changed(&self, old_state: &str, new_state: &str) {
        info!("[{}] 连接状态变化: {} -> {}", self.name, old_state, new_state);
    }
    
    async fn on_protocol_switched(&self, connection_id: &str, from_protocol: &str, to_protocol: &str) {
        info!("[{}] 协议切换: {} - 从 {} 切换到 {}", self.name, connection_id, from_protocol, to_protocol);
    }
}

impl CustomClientEventHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("启动 FastClient 客户端示例");
    
    // 创建客户端配置
    let config = ClientConfig::new(
        "ws://127.0.0.1:4320".to_string(), // WebSocket 地址
        "127.0.0.1:4321".to_string()      // QUIC 地址
    )
    .with_protocol_selection(ProtocolSelection::Auto) // 自动选择协议（协议竞速）
    .with_heartbeat(10000, 30000) // 10秒心跳间隔，30秒监控超时
    .with_serialization(flare_core::common::serialization::SerializationConfig {
        format: SerializationFormat::Protobuf,
        ..Default::default()
    });
    
    // 验证配置
    if let Err(e) = config.validate() {
        error!("配置验证失败: {}", e);
        return Err(e.into());
    }
    
    info!("客户端配置:");
    info!("  - 协议选择: {:?}", config.protocol_selection);
    info!("  - WebSocket地址: {:?}", config.get_server_address(flare_core::common::connections::types::Transport::WebSocket));
    info!("  - QUIC地址: {:?}", config.get_server_address(flare_core::common::connections::types::Transport::Quic));
    info!("  - 心跳间隔: {}ms", config.heartbeat_interval_ms);
    info!("  - 自动重连: {}次", config.max_reconnect_attempts);
    info!("  - 序列化格式: {:?}", config.serialization_config.format);
    
    // 创建自定义事件处理器
    let event_handler = Arc::new(CustomClientEventHandler::new("FastClient".to_string()));
    
    // 创建 FastClient 实例
    let mut client = FastClientBuilder::new()
        .with_websocket_only()  // 仅使用WebSocket协议
        .with_server_address(flare_core::common::connections::types::Transport::WebSocket, "ws://127.0.0.1:4320".to_string())
        .with_heartbeat(10000, 30000) // 10秒心跳间隔，30秒监控超时
        .with_serialization(flare_core::common::serialization::SerializationConfig {
            format: SerializationFormat::Protobuf,
            ..Default::default()
        })
        .with_auth_enabled(true) // 启用认证
        .with_auth_user_id("user_001".to_string())
        .with_auth_platform("desktop".to_string())
        .with_auth_token("test_token_123".to_string())
        .with_event_handler(event_handler)
        .build();
    
    info!("FastClient 实例创建成功");
    
    // 启动客户端
    info!("正在启动客户端...");
    client.start().await?;
    info!("✅ 客户端启动成功！");
    
    // 等待一段时间确保连接稳定
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // 发送认证消息
    info!("发送认证消息...");
    let auth_message = Frame::new(
        flare_core::common::protocol::commands::Command::Control(
            flare_core::common::protocol::commands::ControlCmd::Connect(
                flare_core::common::protocol::commands::ConnectCommand::new(
                    "FastClient认证".to_string(),
                    "flare-core".to_string(),
                    "desktop".to_string(),
                    "1.0.0".to_string(),
                )
            )
        ),
        uuid::Uuid::new_v4().to_string(),
        flare_core::common::protocol::Reliability::AtLeastOnce,
    );
    client.send_message(auth_message).await?;
    info!("认证消息已发送");
    
    // 等待认证完成
    tokio::time::sleep(Duration::from_secs(1)).await;
    
    // 发送测试消息
    info!("发送测试消息...");
    let test_messages = vec![
        "Hello from FastClient!",
        "这是第二条测试消息",
        "FastClient 支持协议竞速",
        "自动重连功能测试",
    ];
    
    for (i, text) in test_messages.iter().enumerate() {
        let message = Frame::new(
            flare_core::common::protocol::commands::Command::Message(
                flare_core::common::protocol::commands::MessageCmd::Data(
                    flare_core::common::protocol::commands::DataCommand::new(
                        text.as_bytes().to_vec(),
                    )
                )
            ),
            format!("test_msg_{}", i),
            flare_core::common::protocol::Reliability::AtLeastOnce,
        );
        client.send_message(message).await?;
        info!("测试消息 {} 已发送: {}", i + 1, text);
        
        // 间隔发送
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    
    // 发送心跳测试
    info!("发送心跳测试...");
    let heartbeat = Frame::heartbeat("FastClient心跳".to_string());
    client.send_message(heartbeat).await?;
    info!("心跳消息已发送");
    
    // 等待接收响应
    info!("等待服务器响应...");
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // 测试请求-响应模式
    info!("测试请求-响应模式...");
    match client.send_request(
        |_| Ok(flare_core::common::protocol::commands::Command::Message(
            flare_core::common::protocol::commands::MessageCmd::Data(
                flare_core::common::protocol::commands::DataCommand::new(
                    "请求服务器信息".as_bytes().to_vec(),
                )
            )
        )),
        flare_core::common::protocol::Reliability::AtLeastOnce,
        None
    ).await {
        Ok(response) => {
            info!("✅ 收到服务器响应: {:?}", response.get_command_type_str());
        }
        Err(e) => {
            error!("❌ 请求-响应失败: {}", e);
        }
    }
    
    // 检查连接状态
    let state = client.get_state().await;
    let is_connected = client.is_connected().await;
    info!("连接状态: {:?}, 是否已连接: {}", state, is_connected);
    
    // 等待一段时间观察自动功能
    info!("等待 10 秒观察自动心跳和重连功能...");
    tokio::time::sleep(Duration::from_secs(10)).await;
    
    // 停止客户端
    info!("正在停止客户端...");
    client.stop().await?;
    info!("✅ 客户端已停止");
    
    Ok(())
}
