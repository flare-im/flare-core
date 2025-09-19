//! QUIC服务端示例
//!
//! 演示如何创建和运行QUIC服务端

use std::sync::Arc;
use tokio::time::sleep;
use std::time::Duration;

use flare_core::{
    server::{
        config::{ServerConfig, ProtocolConfig, TlsConfig},
        fast::server::FastServer,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    // 创建TLS配置（需要提供有效的证书和私钥路径）
    let tls_config = TlsConfig::new(
        "path/to/cert.pem".to_string(),  // 需要替换为实际的证书路径
        "path/to/key.pem".to_string(),   // 需要替换为实际的私钥路径
    );
    
    // 创建服务端配置
    let config = ServerConfig::default_quic(
        "path/to/cert.pem".to_string(),  // 需要替换为实际的证书路径
        "path/to/key.pem".to_string(),   // 需要替换为实际的私钥路径
    )
    .with_quic_config(
        ProtocolConfig::default()
            .with_listen_addr("127.0.0.1:8081".to_string())
            .with_tls_config(tls_config)
    )
    .with_connection_timeout_ms(30000)
    .with_heartbeat_interval_ms(10000)
    .with_auth_timeout_ms(30000);
    
    // 创建FastServer实例
    let server = FastServer::default();
    
    // 启动服务端
    server.start(config).await?;
    
    println!("QUIC服务端已启动，监听地址: 127.0.0.1:8081");
    println!("按 Ctrl+C 停止服务端");
    
    // 运行一段时间
    sleep(Duration::from_secs(60)).await;
    
    // 停止服务端
    server.stop().await;
    
    Ok(())
}