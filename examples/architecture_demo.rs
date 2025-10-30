//! 架构演示示例
//!
//! 展示新的标准化长连接抽象层的使用方法

use flare_core::common::connections::config::{ConnectionConfig, QuicClientConfig, QuicServerConfig};
use flare_core::common::connections::enums::{ConnectionState, Transport};
use flare_core::common::connections::traits::{ClientConnection, ServerConnection, ConnectionEvent};
use flare_core::common::protocol::frame::Frame;
use flare_core::common::protocol::factory::FrameFactory;
use flare_core::common::protocol::reliability::Reliability;
use flare_core::common::error::FlareError;
use flare_core::client::connections::websocket::WebSocketClient;
use flare_core::client::connections::quic::QuicClient;
use flare_core::server::connections::websocket::WebSocketServerConnection;
use flare_core::server::connections::quic::QuicServerConnection;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// 连接事件处理器
struct DemoEventHandler;

impl ConnectionEvent for DemoEventHandler {
    fn on_connected(&self) {
        println!("连接已建立");
    }
    
    fn on_disconnected(&self, reason: Option<String>) {
        println!("连接已断开: {:?}", reason);
    }
    
    fn on_error(&self, err: FlareError) {
        println!("发生错误: {:?}", err);
    }
    
    fn on_message_received(&self, frame: Frame) {
        println!("收到消息: {:?}", String::from_utf8_lossy(&frame.payload));
    }
    
    fn on_message_sent(&self, frame: Frame) {
        println!("消息已发送: {:?}", String::from_utf8_lossy(&frame.payload));
    }
    
    fn on_heartbeat_ping(&self) {
        println!("发送心跳Ping");
    }
    
    fn on_heartbeat_pong(&self, rtt_ms: u32) {
        println!("收到心跳Pong，RTT: {}ms", rtt_ms);
    }
    
    fn on_heartbeat_timeout(&self) {
        println!("心跳超时");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    println!("=== 标准化长连接抽象层架构演示 ===");
    
    // 1. WebSocket客户端演示
    println!("\n1. WebSocket客户端演示");
    websocket_client_demo().await?;
    
    // 2. QUIC客户端演示
    println!("\n2. QUIC客户端演示");
    quic_client_demo().await?;
    
    // 3. WebSocket服务端演示
    println!("\n3. WebSocket服务端演示");
    websocket_server_demo().await?;
    
    // 4. QUIC服务端演示
    println!("\n4. QUIC服务端演示");
    quic_server_demo().await?;
    
    println!("\n=== 演示完成 ===");
    Ok(())
}

/// WebSocket客户端演示
async fn websocket_client_demo() -> Result<(), Box<dyn std::error::Error>> {
    // 创建连接配置
    let config = ConnectionConfig {
        id: Some("ws_client_demo".to_string()),
        transport: Transport::WebSocket,
        remote_addr: Some("127.0.0.1:4320".to_string()),
        ..Default::default()
    };
    
    // 创建WebSocket客户端
    let client = WebSocketClient::new(config)?;
    
    // 设置事件处理器
    let handler = Arc::new(DemoEventHandler);
    client.set_event_handler(handler);
    
    // 连接到服务器
    match client.connect() {
        Ok(_) => println!("WebSocket客户端连接成功"),
        Err(e) => println!("WebSocket客户端连接失败: {:?}", e),
    }
    
    // 检查连接状态
    println!("连接状态: {:?}", client.state());
    
    // 断开连接
    match client.disconnect(Some("演示完成".to_string())) {
        Ok(_) => println!("WebSocket客户端断开连接成功"),
        Err(e) => println!("WebSocket客户端断开连接失败: {:?}", e),
    }
    
    Ok(())
}

/// QUIC客户端演示
async fn quic_client_demo() -> Result<(), Box<dyn std::error::Error>> {
    // 创建连接配置
    let config = ConnectionConfig {
        id: Some("quic_client_demo".to_string()),
        transport: Transport::Quic,
        remote_addr: Some("127.0.0.1:4321".to_string()),
        protocol_config: Some(flare_core::common::connections::config::ProtocolConfig::Quic(QuicClientConfig {
            server_cert_path: Some("certs/server.crt".to_string()),
            skip_server_verification: false,
            client_cert_path: None,
            client_key_path: None,
        })),
        ..Default::default()
    };
    
    // 创建QUIC客户端
    let client = QuicClient::new(config)?;
    
    // 设置事件处理器
    let handler = Arc::new(DemoEventHandler);
    client.set_event_handler(handler);
    
    // 连接到服务器
    match client.connect() {
        Ok(_) => println!("QUIC客户端连接成功"),
        Err(e) => println!("QUIC客户端连接失败: {:?}", e),
    }
    
    // 检查连接状态
    println!("连接状态: {:?}", client.state());
    
    // 断开连接
    match client.disconnect(Some("演示完成".to_string())) {
        Ok(_) => println!("QUIC客户端断开连接成功"),
        Err(e) => println!("QUIC客户端断开连接失败: {:?}", e),
    }
    
    Ok(())
}

/// WebSocket服务端演示
async fn websocket_server_demo() -> Result<(), Box<dyn std::error::Error>> {
    // 创建连接配置
    let config = ConnectionConfig {
        id: Some("ws_server_demo".to_string()),
        transport: Transport::WebSocket,
        ..Default::default()
    };
    
    // 创建WebSocket服务端连接
    let server_conn = WebSocketServerConnection::from_config(config);
    
    // 设置事件处理器
    let handler = Arc::new(DemoEventHandler);
    server_conn.set_event_handler(handler);
    
    // 接受连接
    match server_conn.accept() {
        Ok(_) => println!("WebSocket服务端连接接受成功"),
        Err(e) => println!("WebSocket服务端连接接受失败: {:?}", e),
    }
    
    // 检查连接状态
    println!("连接状态: {:?}", server_conn.state());
    
    // 关闭连接
    match server_conn.close(Some("演示完成".to_string())) {
        Ok(_) => println!("WebSocket服务端连接关闭成功"),
        Err(e) => println!("WebSocket服务端连接关闭失败: {:?}", e),
    }
    
    Ok(())
}

/// QUIC服务端演示
async fn quic_server_demo() -> Result<(), Box<dyn std::error::Error>> {
    // 创建连接配置
    let config = ConnectionConfig {
        id: Some("quic_server_demo".to_string()),
        transport: Transport::Quic,
        protocol_config: Some(flare_core::common::connections::config::ProtocolConfig::Quic(QuicServerConfig {
            cert_path: Some("certs/server.crt".to_string()),
            key_path: Some("certs/server.key".to_string()),
            require_client_auth: false,
            client_ca_cert_path: None,
        })),
        ..Default::default()
    };
    
    // 创建QUIC服务端连接
    let server_conn = QuicServerConnection::from_config(config);
    
    // 设置事件处理器
    let handler = Arc::new(DemoEventHandler);
    server_conn.set_event_handler(handler);
    
    // 接受连接
    match server_conn.accept() {
        Ok(_) => println!("QUIC服务端连接接受成功"),
        Err(e) => println!("QUIC服务端连接接受失败: {:?}", e),
    }
    
    // 检查连接状态
    println!("连接状态: {:?}", server_conn.state());
    
    // 关闭连接
    match server_conn.close(Some("演示完成".to_string())) {
        Ok(_) => println!("QUIC服务端连接关闭成功"),
        Err(e) => println!("QUIC服务端连接关闭失败: {:?}", e),
    }
    
    Ok(())
}