//! 简单的QUIC服务端示例
//!
//! 使用quinn直接实现，支持TLS证书认证

use std::fs;
use std::io::BufReader;
use std::net::SocketAddr;
use quinn::{Endpoint, ServerConfig};
use rustls_pemfile::{certs, private_key};
use tokio::signal;

/// 创建服务器配置
fn create_server_config() -> Result<ServerConfig, Box<dyn std::error::Error>> {
    // 读取服务器证书和私钥
    let cert_file = fs::File::open("certs/server.crt")?;
    let key_file = fs::File::open("certs/server.key")?;
    
    let cert_reader = &mut BufReader::new(cert_file);
    let key_reader = &mut BufReader::new(key_file);
    
    // 解析证书链
    let cert_chain: Vec<rustls::pki_types::CertificateDer> = certs(cert_reader)
        .collect::<Result<Vec<_>, _>>()?;
    
    // 解析私钥
    let key = private_key(key_reader)?
        .ok_or("未找到私钥")?;
    
    // 使用quinn直接创建服务器配置
    let server_config = ServerConfig::with_single_cert(cert_chain, rustls::pki_types::PrivateKeyDer::from(key))?;
    
    Ok(server_config)
}

/// 处理客户端连接
async fn handle_connection(connection: quinn::Connection) {
    let addr = connection.remote_address();
    println!("新连接来自: {}", addr);
    
    // 处理双向流
    while let Ok((send_stream, recv_stream)) = connection.accept_bi().await {
        println!("接受双向流: {}", addr);
        
        // 处理接收流
        let connection_clone = connection.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_stream(send_stream, recv_stream, connection_clone).await {
                eprintln!("处理流时出错: {}", e);
            }
        });
    }
}

/// 处理单个流
async fn handle_stream(
    mut send_stream: quinn::SendStream,
    mut recv_stream: quinn::RecvStream,
    connection: quinn::Connection,
) -> Result<(), Box<dyn std::error::Error>> {
    // 读取客户端消息
    let mut buffer = vec![0u8; 1024];
    let bytes_read = recv_stream.read(&mut buffer).await?;
    
    if let Some(bytes_read) = bytes_read {
        if bytes_read > 0 {
            let message = String::from_utf8_lossy(&buffer[..bytes_read]);
            println!("收到消息: {}", message);
            
            // 发送响应
            let response = format!("服务器响应: 已收到您的消息 '{}'", message);
            send_stream.write_all(response.as_bytes()).await?;
            send_stream.finish()?;
            
            println!("已发送响应: {}", response);
        }
    }
    
    Ok(())
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
    
    // 创建QUIC服务器配置
    let server_config = create_server_config()?;
    
    // 创建QUIC端点
    let addr: SocketAddr = "127.0.0.1:8081".parse()?;
    let endpoint = Endpoint::server(server_config, addr)?;
    
    println!("QUIC服务端已启动，监听地址: {}", addr);
    println!("按 Ctrl+C 停止服务端");
    
    // 运行服务端
    run_server(endpoint).await;
    
    Ok(())
}

/// 运行服务器
async fn run_server(endpoint: Endpoint) {
    loop {
        tokio::select! {
            // 监听新的连接
            incoming = endpoint.accept() => {
                match incoming {
                    Some(conn) => {
                        tokio::spawn(async move {
                            handle_connection(conn.await.unwrap()).await;
                        });
                    }
                    None => {
                        // 服务端已关闭
                        break;
                    }
                }
            }
            // 监听关闭信号
            _ = signal::ctrl_c() => {
                println!("收到 Ctrl+C 信号，正在停止服务端...");
                break;
            }
        }
    }
    
    // 关闭端点
    endpoint.close(0u32.into(), b"Server shutting down");
    println!("服务端已停止");
}
