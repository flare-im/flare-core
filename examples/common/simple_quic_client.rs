//! 简单的QUIC客户端示例
//!
//! 使用quinn直接实现，支持TLS证书认证

use std::net::SocketAddr;
use std::fs;
use std::io::BufReader;
use std::sync::Arc;
use quinn::{Endpoint, ClientConfig};
use rustls_pemfile::certs;
use tokio::time::{sleep, Duration};

/// 创建客户端配置
fn create_client_config() -> Result<ClientConfig, Box<dyn std::error::Error>> {
    // 读取服务器证书
    let cert_file = fs::File::open("certs/server.crt")?;
    let cert_reader = &mut BufReader::new(cert_file);
    
    // 解析证书
    let cert_der = certs(cert_reader)
        .collect::<Result<Vec<_>, _>>()?;
    
    // 创建根证书存储并添加证书
    let mut root_store = rustls::RootCertStore::empty();
    for cert in cert_der {
        root_store.add(rustls::pki_types::CertificateDer::from(cert))?;
    }
    
    // 使用quinn的with_root_certificates方法
    let client_config = ClientConfig::with_root_certificates(Arc::new(root_store))?;
    
    Ok(client_config)
}

/// 发送消息到服务器
async fn send_message(connection: &quinn::Connection, message: &str) -> Result<String, Box<dyn std::error::Error>> {
    // 打开双向流
    let (mut send_stream, mut recv_stream) = connection.open_bi().await?;
    
    // 发送消息
    send_stream.write_all(message.as_bytes()).await?;
    send_stream.finish()?;
    
    println!("已发送消息: {}", message);
    
    // 读取响应
    let mut buffer = vec![0u8; 1024];
    let bytes_read = recv_stream.read(&mut buffer).await?;
    
    if let Some(bytes_read) = bytes_read {
        if bytes_read > 0 {
            let response = String::from_utf8_lossy(&buffer[..bytes_read]);
            println!("收到响应: {}", response);
            Ok(response.to_string())
        } else {
            Ok("无响应".to_string())
        }
    } else {
        Ok("无响应".to_string())
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
    
    // 检查证书文件是否存在
    if !std::path::Path::new("certs/server.crt").exists() {
        println!("证书文件不存在，请先运行证书生成工具");
        println!("运行: cargo run --example cert_generator");
        return Ok(());
    }
    
    // 创建QUIC客户端配置
    let client_config = create_client_config()?;
    
    // 创建QUIC端点
    let mut endpoint = Endpoint::client("[::]:0".parse()?)?;
    endpoint.set_default_client_config(client_config);
    
    // 连接到服务器
    let addr: SocketAddr = "127.0.0.1:8081".parse()?;
    println!("正在连接到QUIC服务器: {}", addr);
    
    let connection = endpoint.connect(addr, "localhost")?.await?;
    println!("已连接到QUIC服务器");
    
    // 发送几条测试消息
    let messages = vec![
        "Hello, QUIC Server!",
        "这是第二条消息",
        "测试消息 #3",
    ];
    
    for (i, message) in messages.iter().enumerate() {
        println!("\n--- 发送消息 {} ---", i + 1);
        match send_message(&connection, message).await {
            Ok(response) => println!("成功收到响应: {}", response),
            Err(e) => println!("发送消息失败: {}", e),
        }
        
        // 等待一段时间
        sleep(Duration::from_secs(1)).await;
    }
    
    // 关闭连接
    connection.close(0u32.into(), b"Client closing");
    println!("\n连接已关闭");
    
    Ok(())
}