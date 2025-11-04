//! QUIC 服务端使用示例
//! 
//! 演示如何使用 UnifiedServer 监听 QUIC 协议

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
        println!("[QUIC Server] 收到来自连接 {} 的消息: {:?}", connection_id, frame);
        // 回显消息，支持心跳测试
        if let Some(cmd) = &frame.command {
            println!("[QUIC Server] 命令类型: {:?}, 可靠性: {:?}", cmd, frame.reliability);
        }
        Ok(Some(frame.clone()))
    }
    
    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        println!("[QUIC Server] ✅ 新连接已建立: {}", connection_id);
        Ok(())
    }
    
    async fn on_disconnect(&self, connection_id: &str) -> Result<()> {
        println!("[QUIC Server] ❌ 连接已断开: {}", connection_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let handler: Arc<dyn ConnectionHandler> = Arc::new(MyConnectionHandler);
    
    // 仅监听 QUIC 协议
    println!("=== QUIC 服务端测试 ===");
    let quic_config = ServerConfig::new("0.0.0.0:8081".to_string())
        .with_protocols(vec![TransportProtocol::QUIC])
        .with_max_connections(2000);
    
    let mut quic_server = UnifiedServer::new(quic_config, Arc::clone(&handler))?;
    quic_server.start().await?;
    println!("QUIC 服务器已启动：0.0.0.0:8081");
    println!("支持的协议: {:?}", quic_server.protocols());
    
    // 运行一段时间
    println!("\n服务器运行中，按 Ctrl+C 停止...");
    tokio::signal::ctrl_c().await?;
    
    println!("\n正在停止服务器...");
    quic_server.stop().await?;
    
    println!("服务器已停止");
    Ok(())
}
