//! QUIC 客户端使用示例
//! 
//! 演示如何使用 UnifiedClient 连接 QUIC 服务器

use flare_core::client::{Client, ClientConfig};
use flare_core::common::protocol::{ping, frame_with_system_command, Reliability};                                                                             
use flare_core::transport::events::{ConnectionEvent, ConnectionObserver};                                                          
use flare_core::client::UnifiedClient;
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 设置 rustls CryptoProvider（QUIC 需要）
    // 必须在第一次使用 rustls 之前调用
    use rustls::crypto::CryptoProvider;
    let _ = CryptoProvider::install_default(rustls::crypto::ring::default_provider());
    
    println!("=== QUIC 客户端测试 ===");
    let quic_config = ClientConfig::new("quic://127.0.0.1:8081".to_string())
        .quic()
        .with_format(flare_core::common::protocol::SerializationFormat::Protobuf);
    
    match UnifiedClient::connect_with_config(quic_config).await {
        Ok(mut quic_client) => {
            println!("QUIC 连接成功！");
            println!("使用的协议: {:?}", quic_client.active_protocol());
            
            // 添加消息观察者
            struct QUICObserver;
            impl ConnectionObserver for QUICObserver {
                fn on_event(&self, event: &ConnectionEvent) {
                    match event {
                        ConnectionEvent::Message(data) => {
                            println!("[QUIC] 收到服务器回显: {} 字节", data.len());
                        }
                        ConnectionEvent::Connected => {
                            println!("[QUIC] 连接已建立");
                        }
                        ConnectionEvent::Disconnected(reason) => {
                            println!("[QUIC] 连接已断开: {}", reason);
                        }
                        ConnectionEvent::Error(e) => {
                            eprintln!("[QUIC] 连接错误: {:?}", e);
                        }
                    }
                }
            }
            
            quic_client.add_observer(Arc::new(QUICObserver));
            
            // 发送测试消息
            let frame = frame_with_system_command(
                ping(),
                Reliability::AtLeastOnce,
            );
            quic_client.send_frame(&frame).await?;
            println!("[QUIC] 已发送 ping 消息");
            
            // 测试心跳机制 - 发送多个消息并等待响应
            println!("[QUIC] 测试心跳和消息收发...");
            for i in 1..=5 {
                let frame = frame_with_system_command(
                    ping(),
                    Reliability::AtLeastOnce,
                );
                quic_client.send_frame(&frame).await?;
                println!("[QUIC] 已发送消息 #{}", i);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            
            // 检查连接状态
            println!("[QUIC] 连接状态: is_connected = {}", quic_client.is_connected());
            
            // 等待服务器响应并保持连接
            println!("[QUIC] 等待服务器响应（心跳测试）...");
            tokio::time::sleep(Duration::from_secs(5)).await;
            
            // 断开连接
            quic_client.disconnect().await?;
            println!("[QUIC] 已断开连接");
        }
        Err(e) => {
            println!("QUIC 连接失败: {:?}", e);
        }
    }
    
    Ok(())
}


