//! QUIC客户端示例
//!
//! 演示如何创建和使用QUIC客户端

use std::time::Duration;
use tokio::time::sleep;
use std::sync::Arc;

use flare_core::{
    common::{
        connections::{
            types::{ConnectionConfig, Transport},
            event::ConnectionEvent,
            factory::ConnectionFactory,
            traits::ConnectionStats,
        },
        protocol::{Frame, Reliability, commands::{Command, MessageCmd, MessageSendCommand}},
    },
};

/// 简单的客户端事件处理器
#[derive(Debug)]
struct SimpleClientEventHandler;

#[async_trait::async_trait]
impl ConnectionEvent for SimpleClientEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        println!("客户端: 连接已建立: {}", connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        println!("客户端: 连接已断开: {} - 原因: {}", connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        println!("客户端: 连接错误: {} - 错误: {}", connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        println!("客户端: 收到消息: {} - 类型: {}", connection_id, message.get_command_type_str());
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        println!("客户端: 消息已发送: {} - 类型: {}", connection_id, message.get_command_type_str());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        println!("客户端: 心跳超时: {}", connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        println!("客户端: 连接质量变化: {} - 评分: {}", connection_id, quality_score);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        println!("客户端: 心跳已发送: {}", connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        println!("客户端: 收到心跳响应: {}", connection_id);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        println!("客户端: 开始重连: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        println!("客户端: 重连成功: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        println!("客户端: 重连失败: {} - 尝试次数: {} - 错误: {}", connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &ConnectionStats) {
        println!("客户端: 统计信息更新: {} - 收到: {} - 发送: {} - 质量: {}", 
                 connection_id, stats.messages_received, stats.messages_sent, stats.quality_score);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化CryptoProvider
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        rustls::crypto::ring::default_provider().install_default()
            .map_err(|_| "无法安装CryptoProvider")?;
    }
    
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    // 创建客户端连接配置
    let mut config = ConnectionConfig::client(
        "quic_client".to_string(),
        "127.0.0.1:8081".to_string(),
    );
    config.transport = Transport::Quic;
    
    // 设置服务器证书路径（用于验证服务器身份）
    config.protocol_config.quic.client.server_cert_path = Some("certs/server.crt".to_string());
    config.protocol_config.quic.client.skip_server_verification = false; // 启用服务器验证
    config.protocol_config.quic.client.server_hostname = Some("localhost".to_string()); // 设置服务器主机名
    // 配置客户端证书与私钥（双向TLS）
    config.protocol_config.quic.client.client_cert_path = Some("certs/client.crt".to_string());
    config.protocol_config.quic.client.client_key_path = Some("certs/client.key".to_string());
    
    // 使用ConnectionFactory创建QUIC客户端连接
    let mut client_connection = ConnectionFactory::create_client(config).await?;
    
    // 设置事件处理器
    let event_handler = Arc::new(SimpleClientEventHandler);
    client_connection.set_event_handler(event_handler).await;
    
    // 连接到服务端
    println!("正在连接到QUIC服务端...");
    match client_connection.connect().await {
        Ok(_) => {
            println!("已连接到QUIC服务端");
            
            // 等待服务端完全准备好
            sleep(Duration::from_millis(500)).await;
            
            // 发送一些消息
            for i in 1..=5 {
                let message_id = format!("msg_{}", i);
                let send_cmd = MessageSendCommand::new(
                    format!("Hello, QUIC server! Message #{}", i).into_bytes()
                );
                let command = Command::Message(MessageCmd::Send(send_cmd));
                let message = Frame::new(command, message_id, Reliability::AtLeastOnce);
                
                match client_connection.send_message(message).await {
                    Ok(_) => println!("已发送消息 #{}", i),
                    Err(e) => println!("发送消息 #{} 失败: {}", i, e),
                }
                
                // 等待一段时间
                sleep(Duration::from_secs(1)).await;
            }
            
            // 等待一段时间以接收响应
            sleep(Duration::from_secs(5)).await;
            
            // 断开连接
            println!("正在断开连接...");
            client_connection.disconnect(Some("客户端主动断开".to_string())).await?;
            println!("已断开连接");
            
            // 等待一下确保断开完成
            sleep(Duration::from_millis(100)).await;
        }
        Err(e) => {
            println!("连接失败: {}", e);
            return Err(e.into());
        }
    }
    
    Ok(())
}