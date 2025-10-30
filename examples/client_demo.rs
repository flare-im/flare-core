//! 客户端演示示例
//!
//! 展示如何使用增强型客户端和FastClient

use flare_core::client::enhanced_client::EnhancedClient;
use flare_core::client::fast::client::FastClient;
use flare_core::client::protocol_racer::ProtocolRacer;
use flare_core::common::connections::config::ConnectionConfig;
use flare_core::common::connections::enums::Transport;
use flare_core::common::protocol::factory::FrameFactory;
use flare_core::common::protocol::reliability::Reliability;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    println!("🚀 Flare-Core 客户端演示");
    println!("========================");
    
    // 示例1: 增强型客户端 - 协议选择
    demo_enhanced_client_protocol_selection().await?;
    
    // 示例2: 增强型客户端 - 协议竞速
    // 注意：需要运行对应的服务端
    // demo_enhanced_client_protocol_race().await?;
    
    // 示例3: FastClient
    demo_fast_client().await?;
    
    // 示例4: 协议竞速器
    // demo_protocol_racer().await?;
    
    Ok(())
}

/// 演示增强型客户端 - 协议选择
async fn demo_enhanced_client_protocol_selection() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔌 增强型客户端 - 协议选择演示");
    println!("----------------------------");
    
    // 创建客户端配置
    let mut config = ConnectionConfig::default();
    config.remote_addr = Some("127.0.0.1:9003".to_string());
    
    // 创建增强型客户端
    let mut client = EnhancedClient::new(config);
    
    // 使用WebSocket协议连接
    println!("使用WebSocket协议连接...");
    match client.connect_with_protocol(Transport::WebSocket) {
        Ok(_) => {
            println!("✅ WebSocket连接成功");
            
            // 发送消息
            let frame = FrameFactory::create_data_frame(
                FrameFactory::generate_message_id(),
                b"Hello from enhanced client".to_vec(),
                Reliability::AtLeastOnce
            )?;
            
            client.send_message(frame)?;
            println!("📤 发送消息成功");
            
            // 断开连接
            client.disconnect()?;
            println!("✅ 连接已断开");
        }
        Err(e) => {
            println!("❌ 连接失败: {}", e);
        }
    }
    
    Ok(())
}

/// 演示增强型客户端 - 协议竞速
#[allow(dead_code)]
async fn demo_enhanced_client_protocol_race() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🏎️ 增强型客户端 - 协议竞速演示");
    println!("----------------------------");
    
    // 创建客户端配置
    let mut config = ConnectionConfig::default();
    config.remote_addr = Some("127.0.0.1:9003".to_string());
    
    // 创建增强型客户端
    let mut client = EnhancedClient::new(config);
    
    // 定义要竞速的地址和协议
    let addresses = vec!["127.0.0.1:9003".to_string()];
    let protocols = vec![Transport::WebSocket, Transport::Quic];
    
    // 使用协议竞速连接
    println!("开始协议竞速...");
    match client.connect_with_race(addresses, protocols, None).await {
        Ok(_) => {
            println!("✅ 协议竞速成功");
            
            // 发送消息
            let frame = FrameFactory::create_data_frame(
                FrameFactory::generate_message_id(),
                b"Hello from enhanced client with protocol race".to_vec(),
                Reliability::AtLeastOnce
            )?;
            
            client.send_message(frame)?;
            println!("📤 发送消息成功");
            
            // 断开连接
            client.disconnect()?;
            println!("✅ 连接已断开");
        }
        Err(e) => {
            println!("❌ 协议竞速失败: {}", e);
        }
    }
    
    Ok(())
}

/// 演示FastClient
async fn demo_fast_client() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n⚡ FastClient演示");
    println!("----------------");
    
    // 创建客户端配置
    let mut config = ConnectionConfig::default();
    config.remote_addr = Some("127.0.0.1:9003".to_string());
    
    // 创建FastClient
    let client = FastClient::new(config);
    let client = Arc::new(client);
    
    // 使用WebSocket协议连接
    println!("使用WebSocket协议连接...");
    match client.connect_with_protocol(Transport::WebSocket).await {
        Ok(_) => {
            println!("✅ WebSocket连接成功");
            
            // 发送消息
            let frame = FrameFactory::create_data_frame(
                FrameFactory::generate_message_id(),
                b"Hello from FastClient".to_vec(),
                Reliability::AtLeastOnce
            )?;
            
            client.send_message(frame).await?;
            println!("📤 发送消息成功");
            
            // 断开连接
            client.disconnect().await?;
            println!("✅ 连接已断开");
        }
        Err(e) => {
            println!("❌ 连接失败: {}", e);
        }
    }
    
    Ok(())
}

/// 演示协议竞速器
#[allow(dead_code)]
async fn demo_protocol_racer() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🏎️ 协议竞速器演示");
    println!("----------------");
    
    // 创建基础配置
    let mut base_config = ConnectionConfig::default();
    base_config.remote_addr = Some("127.0.0.1:9003".to_string());
    
    // 定义要竞速的地址和协议
    let addresses = vec!["127.0.0.1:9003".to_string()];
    let protocols = vec![Transport::WebSocket, Transport::Quic];
    
    // 使用协议竞速器
    println!("开始协议竞速...");
    match ProtocolRacer::race(&base_config, &addresses, &protocols, None).await {
        Ok(connection) => {
            println!("✅ 协议竞速成功，使用协议: {:?}", base_config.transport);
            
            // 发送消息
            let frame = FrameFactory::create_data_frame(
                FrameFactory::generate_message_id(),
                b"Hello from protocol racer".to_vec(),
                Reliability::AtLeastOnce
            )?;
            
            connection.send_message(frame)?;
            println!("📤 发送消息成功");
            
            // 断开连接
            connection.disconnect(None)?;
            println!("✅ 连接已断开");
        }
        Err(e) => {
            println!("❌ 协议竞速失败: {}", e);
        }
    }
    
    Ok(())
}