//! QUIC服务端示例
//!
//! 演示如何创建和运行QUIC服务端

use tokio::time::sleep;
use std::time::Duration;

// 添加rustls的引用
use rustls::crypto::ring;

use flare_core::{
    server::{
        config::{ServerConfig, ProtocolConfig, TlsConfig},
        fast::server::FastServer,
    },
    common::serialization::{SerializationConfig, SerializationFormat},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    // 初始化CryptoProvider
    rustls::crypto::CryptoProvider::install_default(ring::default_provider()).unwrap();
    
    // 创建TLS配置（需要提供有效的证书和私钥路径）
    // 注意：在实际使用中，您需要提供真实的证书和私钥文件路径
    let cert_path = "certs/server.crt";  // 请替换为实际的证书路径
    let key_path = "certs/server.key";    // 请替换为实际的私钥路径
    
    let tls_config = TlsConfig::new(
        cert_path.to_string(),
        key_path.to_string(),
    );
    
    // 创建服务端配置
    let mut config = ServerConfig::default_quic(
        cert_path.to_string(),
        key_path.to_string(),
    );
    
    // 更新QUIC配置
    config = config.with_quic_config(
        ProtocolConfig::new()
            .with_listen_addr("127.0.0.1:8081".to_string())
            .with_max_connections(1000)
            .with_tls_config(tls_config)
    );
    
    // 设置使用Protobuf序列化
    config = config.with_serialization_format(SerializationFormat::Protobuf);
    
    // 打印配置信息用于调试
    tracing::info!("服务器配置: {:?}", config);
    if let Some(quic_config) = &config.quic_config {
        tracing::info!("QUIC配置存在，监听地址: {}", quic_config.listen_addr);
    } else {
        tracing::error!("QUIC配置不存在！");
    }
    tracing::info!("序列化配置: {:?}", config.serialization_config);
    
    // 创建FastServer实例
    let server = FastServer::new_with_config(config);
    
    // 启动服务端
    server.start().await?;
    
    println!("QUIC服务端已启动，监听地址: 127.0.0.1:8081");
    println!("按 Ctrl+C 停止服务端");
    
    // 运行一段时间
    sleep(Duration::from_secs(600)).await;
    
    // 停止服务端
    server.stop().await;
    
    Ok(())
}