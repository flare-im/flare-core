//! 统一客户端使用示例
//! 
//! 演示如何使用 UnifiedClient 进行单个协议连接或协议竞速

use flare_core::common::client_trait::Client;
use flare_core::common::config::{ClientConfig, TransportProtocol};
use flare_core::common::protocol::{ping, frame_with_system_command, Reliability};
use flare_core::transport::events::{ConnectionEvent, ConnectionObserver};
use flare_core::UnifiedClient;
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 示例 1: 仅使用 WebSocket 连接
    println!("=== 示例 1: WebSocket 单协议连接 ===");
    let ws_config = ClientConfig::new("ws://localhost:8080".to_string())
        .websocket()
        .with_format(flare_core::common::protocol::SerializationFormat::Protobuf);
    
    match UnifiedClient::connect_with_config(ws_config).await {
        Ok(ws_client) => {
            println!("WebSocket 连接成功！");
            println!("使用的协议: {:?}", ws_client.active_protocol());
        }
        Err(e) => {
            println!("WebSocket 连接失败: {:?}", e);
        }
    }
    
    // 示例 2: 仅使用 QUIC 连接
    println!("\n=== 示例 2: QUIC 单协议连接 ===");
    let quic_config = ClientConfig::new("quic://localhost:8081".to_string())
        .quic()
        .with_format(flare_core::common::protocol::SerializationFormat::Protobuf);
    
    match UnifiedClient::connect_with_config(quic_config).await {
        Ok(mut quic_client) => {
            println!("QUIC 连接成功！");
            println!("使用的协议: {:?}", quic_client.active_protocol());
            
            // 发送消息
            let frame = frame_with_system_command(
                ping(),
                Reliability::AtLeastOnce,
            );
            quic_client.send_frame(&frame).await?;
        }
        Err(e) => {
            println!("QUIC 连接失败: {:?}", e);
        }
    }
    
    // 示例 3: 协议竞速（同时尝试 WebSocket 和 QUIC）
    println!("\n=== 示例 3: 协议竞速（WebSocket 和 QUIC）===");
    let race_config = ClientConfig::new("ws://localhost:8080".to_string())
        .with_protocol_race(vec![
            TransportProtocol::WebSocket,
            TransportProtocol::QUIC,
        ])
        .with_race_timeout(Duration::from_secs(3))
        .with_format(flare_core::common::protocol::SerializationFormat::Protobuf);
    
    match UnifiedClient::connect_with_race(race_config).await {
        Ok(mut race_client) => {
            println!("协议竞速成功！");
            println!("最终使用的协议: {:?}", race_client.active_protocol());
            
            // 添加消息观察者
            struct MyObserver;
            impl flare_core::transport::events::ConnectionObserver for MyObserver {
                fn on_event(&self, event: &ConnectionEvent) {
                    match event {
                        ConnectionEvent::Message(data) => {
                            println!("收到消息: {} 字节", data.len());
                        }
                        ConnectionEvent::Connected => {
                            println!("连接已建立");
                        }
                        ConnectionEvent::Disconnected(reason) => {
                            println!("连接已断开: {}", reason);
                        }
                        ConnectionEvent::Error(e) => {
                            eprintln!("连接错误: {:?}", e);
                        }
                    }
                }
            }
            
            race_client.add_observer(Arc::new(MyObserver));
            
            // 发送测试消息
            let frame = frame_with_system_command(
                ping(),
                Reliability::AtLeastOnce,
            );
            race_client.send_frame(&frame).await?;
            
            // 保持连接一段时间
            tokio::time::sleep(Duration::from_secs(5)).await;
            
            race_client.disconnect().await?;
            println!("已断开连接");
        }
        Err(e) => {
            println!("协议竞速失败: {:?}", e);
        }
    }
    
    Ok(())
}

