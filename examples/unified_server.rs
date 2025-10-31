//! 统一服务端使用示例
//! 
//! 演示如何使用 UnifiedServer 进行单个协议监听或多协议同时监听

use flare_core::common::config::{ServerConfig, TransportProtocol};
use flare_core::common::protocol::Frame;
use flare_core::common::server_trait::{Server, ConnectionHandler};
use flare_core::common::error::Result;
use flare_core::UnifiedServer;
use std::sync::Arc;
use async_trait::async_trait;

// 简单的连接处理器实现
struct MyConnectionHandler;

#[async_trait]
impl ConnectionHandler for MyConnectionHandler {
    async fn handle_frame(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        println!("收到来自连接 {} 的消息", connection_id);
        // 回显消息
        Ok(Some(frame.clone()))
    }
    
    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        println!("新连接: {}", connection_id);
        Ok(())
    }
    
    async fn on_disconnect(&self, connection_id: &str) -> Result<()> {
        println!("连接断开: {}", connection_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let handler: Arc<dyn ConnectionHandler> = Arc::new(MyConnectionHandler);
    
    // 示例 1: 仅监听 WebSocket
    println!("=== 示例 1: WebSocket 单协议服务端 ===");
    let ws_config = ServerConfig::new("0.0.0.0:8080".to_string())
        .websocket()
        .with_max_connections(1000);
    
    let mut ws_server = UnifiedServer::new(ws_config, Arc::clone(&handler))?;
    ws_server.start().await?;
    println!("WebSocket 服务器已启动在 0.0.0.0:8080");
    println!("支持的协议: {:?}", ws_server.protocols());
    
    // 示例 2: 仅监听 QUIC
    println!("\n=== 示例 2: QUIC 单协议服务端 ===");
    let quic_config = ServerConfig::new("0.0.0.0:8081".to_string())
        .quic()
        .with_max_connections(1000);
    
    let mut quic_server = UnifiedServer::new(quic_config, Arc::clone(&handler))?;
    quic_server.start().await?;
    println!("QUIC 服务器已启动在 0.0.0.0:8081");
    println!("支持的协议: {:?}", quic_server.protocols());
    
    // 示例 3: 同时监听多个协议
    println!("\n=== 示例 3: 多协议服务端（WebSocket + QUIC）===");
    let multi_config = ServerConfig::new("0.0.0.0:8082".to_string())
        .with_protocols(vec![
            TransportProtocol::WebSocket,
            TransportProtocol::QUIC,
        ])
        .with_max_connections(2000);
    
    let mut multi_server = UnifiedServer::new(multi_config, Arc::clone(&handler))?;
    multi_server.start().await?;
    println!("多协议服务器已启动在 0.0.0.0:8082");
    println!("支持的协议: {:?}", multi_server.protocols());
    println!("连接数: {}", multi_server.connection_count());
    println!("用户数: {}", multi_server.user_count());
    
    // 运行一段时间
    println!("\n服务器运行中，按 Ctrl+C 停止...");
    tokio::signal::ctrl_c().await?;
    
    println!("\n正在停止服务器...");
    multi_server.stop().await?;
    quic_server.stop().await?;
    ws_server.stop().await?;
    
    println!("服务器已停止");
    Ok(())
}

