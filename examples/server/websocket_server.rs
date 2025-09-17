//! WebSocket服务端示例
//!
//! 演示如何创建和运行WebSocket服务端

use std::sync::Arc;
use tokio::time::sleep;
use std::time::Duration;

use flare_core::{
    server::{
        server::{ServerImpl, ServerConfig},
        ConnectionManager,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    // 创建服务端配置
    let config = ServerConfig::new()
        .with_local_addr("127.0.0.1:8080".to_string())
        .with_connection_timeout_ms(30000)
        .with_heartbeat_interval_ms(10000);
    
    // 创建连接管理器
    let connection_manager = Arc::new(ConnectionManager::new());
    
    // 创建服务端实例
    let server: ServerImpl<ConnectionManager> = ServerImpl::new(config, connection_manager);
    
    // 启动服务端
    server.start().await?;
    
    println!("WebSocket服务端已启动，监听地址: 127.0.0.1:8080");
    println!("按 Ctrl+C 停止服务端");
    
    // 运行一段时间
    sleep(Duration::from_secs(600)).await;
    
    // 停止服务端
    server.stop().await;
    
    Ok(())
}