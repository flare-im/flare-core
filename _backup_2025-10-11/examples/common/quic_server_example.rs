//! QUIC服务端示例
//!
//! 演示如何创建和使用QUIC服务端

use std::sync::Arc;
use tokio::signal;
use quinn::Endpoint;
use flare_core::{
    common::{
        connections::{
            types::ConnectionConfig,
            event::ConnectionEvent,
            factory::ConnectionFactory,
            traits::ConnectionStats,
        },
        protocol::{Frame, Reliability, commands::{Command, MessageCmd, MessageSendCommand}},
    },
};

/// 简单的服务端事件处理器
#[derive(Debug)]
struct SimpleServerEventHandler;

#[async_trait::async_trait]
impl ConnectionEvent for SimpleServerEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        println!("服务端: 连接已建立: {}", connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        println!("服务端: 连接已断开: {} - 原因: {}", connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        println!("服务端: 连接错误: {} - 错误: {}", connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        println!("服务端: 收到消息: {} - 类型: {}", connection_id, message.get_command_type_str());
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        println!("服务端: 消息已发送: {} - 类型: {}", connection_id, message.get_command_type_str());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        println!("服务端: 心跳超时: {}", connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        println!("服务端: 连接质量变化: {} - 评分: {}", connection_id, quality_score);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        println!("服务端: 心跳已发送: {}", connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        println!("服务端: 收到心跳响应: {}", connection_id);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        println!("服务端: 开始重连: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        println!("服务端: 重连成功: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        println!("服务端: 重连失败: {} - 尝试次数: {} - 错误: {}", connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &ConnectionStats) {
        println!("服务端: 统计信息更新: {} - 收到: {} - 发送: {} - 质量: {}", 
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
    
    // 创建服务端连接配置
    let mut config = ConnectionConfig::server(
        "quic_server".to_string(),
        "127.0.0.1:8081".to_string(),
    );
    config.transport = flare_core::common::connections::types::Transport::Quic;
    
    // 设置证书路径
    config.protocol_config.quic.server.cert_path = "certs/server.crt".to_string();
    config.protocol_config.quic.server.key_path = "certs/server.key".to_string();
    config.protocol_config.quic.server.server_hostname = Some("localhost".to_string()); // 设置服务器主机名
    
    // 使用ConnectionFactory创建QUIC服务端端点
    let endpoint = ConnectionFactory::create_quic_server_endpoint(config).await?;
    
    println!("QUIC服务端已启动，监听地址: 127.0.0.1:8081");
    println!("按 Ctrl+C 停止服务端");

    // 运行服务端
    run_server(endpoint).await;

    Ok(())
}

async fn run_server(endpoint: Endpoint) {
    loop {
        tokio::select! {
            // 监听新的连接
            incoming = endpoint.accept() => {
                match incoming {
                    Some(conn) => {
                        tokio::spawn(async move {
                            handle_connection(conn).await;
                        });
                    }
                    None => {
                        // 服务端已关闭
                        break;
                    }
                }
            }
            // 监听关闭信号
            _ = signal::ctrl_c() => {
                println!("收到 Ctrl+C 信号，正在停止服务端...");
                break;
            }
        }
    }
    
    // 关闭端点
    endpoint.close(0u32.into(), b"Server shutting down");
    println!("服务端已停止");
}

async fn handle_connection(incoming: quinn::Incoming) {
    match incoming.await {
        Ok(connecting) => {
            let remote_addr = connecting.remote_address();
            println!("新连接来自: {}", remote_addr);
            
            // 创建服务端连接配置
            let mut connection_config = ConnectionConfig::server(
                format!("quic_server_connection_{}", remote_addr).replace(":", "_"),
                remote_addr.to_string(),
            );
            connection_config.transport = flare_core::common::connections::types::Transport::Quic;
            
            // 设置事件处理器
            let event_handler = Arc::new(SimpleServerEventHandler);
            
            // 使用ConnectionFactory创建服务端连接
            let server_connection = match ConnectionFactory::from_quic_with_handler(
                connecting,
                connection_config,
                event_handler,
            ).await {
                Ok(conn) => conn,
                Err(e) => {
                    eprintln!("创建服务端连接失败: {}", e);
                    return;
                }
            };
            
            // 接受连接
            if let Err(e) = server_connection.accept().await {
                eprintln!("接受连接失败: {}", e);
                return;
            }
            
            println!("连接已接受: {}", server_connection.id());
            
            // 等待连接完全建立
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            
            // 发送欢迎消息
            let message_id = format!("welcome_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis());
            let send_cmd = MessageSendCommand::new(
                format!("欢迎连接到QUIC服务端!").into_bytes()
            );
            let command = Command::Message(MessageCmd::Send(send_cmd));
            let message = Frame::new(command, message_id, Reliability::AtLeastOnce);
            
            if let Err(e) = server_connection.send_message(message).await {
                eprintln!("发送欢迎消息失败: {}", e);
            }
            
            // 保持连接活跃，持续监听消息
            // 这里可以添加更多的业务逻辑，例如：处理消息、发送响应等
            loop {
                tokio::select! {
                    // 监听关闭信号
                    _ = signal::ctrl_c() => {
                        println!("收到 Ctrl+C 信号，正在断开连接...");
                        break;
                    }
                    // 等待一段时间，让消息处理任务运行
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                        // 检查连接状态
                        let current_state = server_connection.state();
                        if matches!(current_state, flare_core::common::connections::types::ConnectionState::Disconnected) {
                            println!("连接已断开，停止处理");
                            break;
                        }
                    }
                }
            }
            
            // 主动关闭连接
            if let Err(e) = server_connection.close(Some("服务端主动关闭".to_string())).await {
                eprintln!("关闭连接时出错: {}", e);
            }
        }
        Err(e) => {
            eprintln!("连接错误: {}", e);
        }
    }
}