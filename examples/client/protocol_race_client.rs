//! 协议竞速客户端示例
//!
//! 展示如何使用flare-core同时尝试QUIC和WebSocket连接，选择更快的协议

use std::sync::Arc;
use std::time::Instant;
use tracing::{info, error};

// 修改rustls的引用
use rustls::crypto::ring;

use flare_core::{
    common::{
        connections::{
            factory::ConnectionFactory,
            traits::{ConnectionFactory as ConnectionFactoryTrait, ConnectionEvent},
            types::{ConnectionConfig, ConnectionType},
        },
        protocol::{Frame, MessageType, Reliability},
        serialization::SerializationFormat,
    },
};

/// 协议竞速客户端事件处理器
#[derive(Debug)]
pub struct ProtocolRaceClientEventHandler {
    pub name: String,
}

#[async_trait::async_trait]
impl ConnectionEvent for ProtocolRaceClientEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] 连接已建立: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] 连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] 连接错误: {} - 错误: {}", self.name, connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        if message.is_heartbeat() {
            info!("[{}] 收到心跳消息: {}", self.name, connection_id);
        } else {
            let payload = message.get_payload();
            if let Ok(text) = String::from_utf8(payload.to_vec()) {
                info!("[{}] 收到服务器消息: {} - 内容: {}", self.name, connection_id, text);
            } else {
                info!("[{}] 收到二进制消息: {} - 长度: {}", self.name, connection_id, payload.len());
            }
        }
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        if message.is_heartbeat() {
            info!("[{}] 心跳消息已发送: {}", self.name, connection_id);
        } else {
            info!("[{}] 数据消息已发送: {}", self.name, connection_id);
        }
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("[{}] 心跳超时: {}", self.name, connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] 连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        info!("[{}] 心跳已发送: {}", self.name, connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        info!("[{}] 收到心跳响应: {}", self.name, connection_id);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        info!("[{}] 开始重连: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        info!("[{}] 重连成功: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        info!("[{}] 重连失败: {} - 尝试次数: {} - 错误: {}", self.name, connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) {
        info!("[{}] 统计信息更新: {} - 收到消息: {} - 发送消息: {}", 
             self.name, connection_id, stats.messages_received, stats.messages_sent);
    }
}

impl ProtocolRaceClientEventHandler {
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
    
    // 初始化CryptoProvider
    rustls::crypto::CryptoProvider::install_default(ring::default_provider()).unwrap();
    
    info!("启动协议竞速客户端示例");
    
    // 创建QUIC客户端配置
    let quic_config = ConnectionConfig::client(
        "protocol_race_quic".to_string(),  // 更新为protocol_race_quic
        "127.0.0.1:8081".to_string()  // QUIC服务端地址
    ).with_type(ConnectionType::Quic)
     .with_heartbeat(5000, 2000)  // 5秒心跳，2秒超时
     .with_serialization_format(SerializationFormat::Protobuf); // 使用Protobuf序列化
    
    // 创建WebSocket客户端配置
    let websocket_config = ConnectionConfig::client(
        "protocol_race_websocket".to_string(),  // 更新为protocol_race_websocket
        "ws://127.0.0.1:8080".to_string()  // WebSocket服务端地址
    ).with_type(ConnectionType::WebSocket)
     .with_heartbeat(5000, 2000)  // 5秒心跳，2秒超时
     .with_serialization_format(SerializationFormat::Protobuf); // 使用Protobuf序列化
    
    info!("QUIC客户端配置: {:?}", quic_config);
    info!("WebSocket客户端配置: {:?}", websocket_config);
    
    // 创建连接工厂
    let factory = ConnectionFactory::new();
    
    // 同时创建两个连接
    info!("正在同时连接QUIC和WebSocket服务端...");
    let connect_start = Instant::now();
    
    // 创建QUIC连接
    let mut quic_connection = factory.create_client_connection(quic_config).await?;
    let quic_event_handler = Arc::new(ProtocolRaceClientEventHandler::new("QUIC客户端".to_string()));
    quic_connection.set_connection_event_handler(quic_event_handler as Arc<dyn ConnectionEvent>).await;
    
    // 创建WebSocket连接
    let mut websocket_connection = factory.create_client_connection(websocket_config).await?;
    let websocket_event_handler = Arc::new(ProtocolRaceClientEventHandler::new("WebSocket客户端".to_string()));
    websocket_connection.set_connection_event_handler(websocket_event_handler as Arc<dyn ConnectionEvent>).await;
    
    // 同时尝试连接
    let quic_connect = tokio::spawn(async move {
        match quic_connection.connect().await {
            Ok(_) => {
                let connect_time = connect_start.elapsed();
                info!("✅ QUIC连接成功！连接耗时: {:.2}ms", connect_time.as_secs_f64() * 1000.0);
                Ok((quic_connection, connect_time))
            },
            Err(e) => {
                error!("❌ QUIC连接失败: {}", e);
                Err(e)
            }
        }
    });
    
    let websocket_connect = tokio::spawn(async move {
        match websocket_connection.connect().await {
            Ok(_) => {
                let connect_time = connect_start.elapsed();
                info!("✅ WebSocket连接成功！连接耗时: {:.2}ms", connect_time.as_secs_f64() * 1000.0);
                Ok((websocket_connection, connect_time))
            },
            Err(e) => {
                error!("❌ WebSocket连接失败: {}", e);
                Err(e)
            }
        }
    });
    
    // 等待任一连接成功
    let quic_result = quic_connect.await?;
    let websocket_result = websocket_connect.await?;
    
    // 选择连接更快的协议
    let (client_connection, protocol_name, connect_time) = match (quic_result, websocket_result) {
        (Ok((quic_conn, quic_time)), Ok((ws_conn, ws_time))) => {
            if quic_time < ws_time {
                info!("🏁 QUIC连接更快，选择QUIC协议");
                (quic_conn, "QUIC", quic_time)
            } else {
                info!("🏁 WebSocket连接更快，选择WebSocket协议");
                (ws_conn, "WebSocket", ws_time)
            }
        },
        (Ok((quic_conn, quic_time)), Err(_)) => {
            info!("🏁 仅QUIC连接成功，选择QUIC协议");
            (quic_conn, "QUIC", quic_time)
        },
        (Err(_), Ok((ws_conn, ws_time))) => {
            info!("🏁 仅WebSocket连接成功，选择WebSocket协议");
            (ws_conn, "WebSocket", ws_time)
        },
        (Err(_), Err(_)) => {
            error!("❌ 两种协议连接都失败");
            return Err("两种协议连接都失败".into());
        }
    };
    
    info!("✅ 最终选择{}协议！总连接耗时: {:.2}ms", protocol_name, connect_time.as_secs_f64() * 1000.0);
    
    // 发送认证消息
    info!("发送认证消息...");
    let auth_message = Frame::connect(
        "protocol_race_client",
    );
    
    client_connection.send_message(auth_message).await?;
    info!("认证消息已发送");
    
    // 等待一小段时间确保认证完成
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // 发送测试消息
    info!("发送测试消息...");
    let test_message = Frame::new(
        MessageType::Data,
        1,
        Reliability::AtLeastOnce,
        format!("Hello from {} client with protocol race!", protocol_name).as_bytes().to_vec(),
    );
    
    client_connection.send_message(test_message).await?;
    info!("测试消息已发送");
    
    // 等待一段时间以接收响应
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    
    // 断开连接
    info!("正在断开连接...");
    client_connection.disconnect().await?;
    info!("连接已断开");
    
    Ok(())
}