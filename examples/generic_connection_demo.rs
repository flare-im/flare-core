//! 通用连接抽象演示
//!
//! 该示例演示了如何使用通用连接抽象层来创建和管理跨协议的连接。

use flare_core::common::connections::enhanced::{EnhancedConnection, EnhancedConnectionFactory};
use flare_core::common::connections::manager::ConnectionManager;
use flare_core::common::connections::config::ConnectionConfig;
use flare_core::common::connections::enums::{ConnectionState, Transport};
use flare_core::common::connections::traits::{BaseConnection, ConnectionEvent};
use flare_core::common::protocol::frame::Frame;
use flare_core::common::error::FlareError;
use std::sync::Arc;

/// 连接事件处理器示例
struct DemoConnectionHandler;

impl ConnectionEvent for DemoConnectionHandler {
    fn on_connected(&self) {
        println!("连接已建立");
    }

    fn on_disconnected(&self, reason: Option<String>) {
        println!("连接已断开，原因: {:?}", reason);
    }

    fn on_error(&self, err: FlareError) {
        println!("发生错误: {:?}", err);
    }

    fn on_message_received(&self, frame: Frame) {
        println!("接收到消息: {:?}", frame);
    }

    fn on_message_sent(&self, frame: Frame) {
        println!("消息已发送: {:?}", frame);
    }

    fn on_heartbeat_ping(&self) {
        println!("发送心跳Ping");
    }

    fn on_heartbeat_pong(&self, rtt_ms: u32) {
        println!("接收到心跳Pong，RTT: {}ms", rtt_ms);
    }

    fn on_heartbeat_timeout(&self) {
        println!("心跳超时");
    }

    fn on_quality_changed(&self, quality: u8) {
        println!("连接质量变化: {}", quality);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 通用连接抽象演示 ===");

    // 创建连接管理器
    let manager = ConnectionManager::new();

    // 创建QUIC连接配置
    let quic_config = ConnectionConfig {
        transport: Transport::Quic,
        remote_addr: Some("127.0.0.1:5000".to_string()),
        ..Default::default()
    };

    // 创建WebSocket连接配置
    let ws_config = ConnectionConfig {
        transport: Transport::WebSocket,
        remote_addr: Some("ws://127.0.0.1:8080".to_string()),
        ..Default::default()
    };

    // 使用连接管理器创建连接
    println!("创建QUIC连接...");
    let quic_connection = manager.create_connection(quic_config)?;
    quic_connection.set_event_handler(Arc::new(DemoConnectionHandler));

    println!("创建WebSocket连接...");
    let ws_connection = manager.create_connection(ws_config)?;
    ws_connection.set_event_handler(Arc::new(DemoConnectionHandler));

    // 显示连接信息
    println!("QUIC连接ID: {}", quic_connection.id());
    println!("WebSocket连接ID: {}", ws_connection.id());
    println!("当前连接数量: {}", manager.get_connection_count());

    // 模拟连接状态变化
    println!("设置QUIC连接为就绪状态...");
    quic_connection.ready()?;
    println!("QUIC连接状态: {:?}", quic_connection.state());

    println!("设置WebSocket连接为已建立状态...");
    ws_connection.connected()?;
    println!("WebSocket连接状态: {:?}", ws_connection.state());

    // 获取统计信息
    println!("QUIC连接统计信息: {:?}", quic_connection.stats());
    println!("WebSocket连接统计信息: {:?}", ws_connection.stats());

    // 模拟发送消息
    println!("发送测试消息...");
    let test_frame = Frame::default();
    let _ = quic_connection.send_message(test_frame.clone());
    let _ = ws_connection.send_message(test_frame);

    // 获取指定状态的连接
    let connected_connections = manager.get_connections_by_state(ConnectionState::Connected);
    println!("已建立的连接数量: {}", connected_connections.len());

    // 清理资源
    println!("关闭所有连接...");
    manager.close_all_connections()?;

    println!("演示完成");
    Ok(())
}