//! QUIC客户端示例
//!
//! 演示如何创建和使用QUIC客户端

use std::time::Duration;
use tokio::time::sleep;

use flare_core::{
    client::{Client, ClientConfig},
    common::{
        connections::{
            types::{ConnectionConfig, ConnectionType},
        },
        protocol::{Frame, MessageType, Reliability},
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    // 创建客户端配置
    let config = ClientConfig {
        connection_config: ConnectionConfig {
            connection_type: ConnectionType::Quic,
            local_addr: None,
            remote_addr: "127.0.0.1:8081".to_string(),
            enable_tls: true, // QUIC通常需要TLS
            tls_cert_path: None,
            max_message_size: 1024 * 1024,
            connection_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(10),
            reconnect_attempts: 3,
            reconnect_interval: Duration::from_secs(5),
        },
        protocol_selection: Default::default(),
    };
    
    // 创建客户端实例
    let mut client = Client::new(config);
    
    // 连接到服务端
    client.connect().await?;
    
    println!("已连接到QUIC服务端");
    
    // 发送一些消息
    for i in 1..=5 {
        let message = Frame {
            message_type: MessageType::Data,
            message_id: i,
            reliability: Reliability::AtLeastOnce,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            payload: format!("Hello, QUIC server! Message #{}", i).into_bytes(),
            session_id: None,
            priority: 0,
            compression: None,
            encrypted: false,
            metadata: None,
        };
        
        client.send_message(message).await?;
        println!("已发送消息 #{}", i);
        
        // 等待一段时间
        sleep(Duration::from_secs(1)).await;
    }
    
    // 等待一段时间以接收响应
    sleep(Duration::from_secs(5)).await;
    
    // 断开连接
    client.disconnect().await?;
    println!("已断开连接");
    
    Ok(())
}