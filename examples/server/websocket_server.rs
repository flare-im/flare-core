//! WebSocket服务端示例
//!
//! 演示如何创建和运行WebSocket服务端

use std::sync::Arc;
use tokio::time::sleep;
use std::time::Duration;

use flare_core::{
    server::{
        config::{ServerConfig, ProtocolConfig},
        fast::server::FastServer,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    // 创建服务端配置
    let mut config = ServerConfig::default_websocket();
    config = config.with_websocket_config(
        ProtocolConfig::new()
            .with_listen_addr("127.0.0.1:8080".to_string())
            .with_max_connections(1000)
    );
    config = config.with_connection_timeout_ms(30000);
    config = config.with_heartbeat_interval_ms(10000);
    config = config.with_auth_timeout_ms(30000);
    
    // 打印配置信息用于调试
    tracing::info!("服务器配置: {:?}", config);
    if let Some(ws_config) = &config.websocket_config {
        tracing::info!("WebSocket配置存在，监听地址: {}", ws_config.listen_addr);
    } else {
        tracing::error!("WebSocket配置不存在！");
    }
    
    // 创建FastServer实例
    let server = FastServer::new_with_config(config);
    
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