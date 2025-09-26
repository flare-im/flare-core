use flare_core::common::connections::{
    factory::ConnectionFactory,
    config::ConnectionConfig,
    types::Transport,
};
use flare_core::common::protocol::{Frame, Reliability};
use flare_core::common::protocol::commands::{Command, MessageCmd, MessageSendCommand};
use std::time::Duration;
use tokio::time::sleep;

/// 自定义主机名的 QUIC 客户端示例
/// 
/// 这个示例展示了如何配置不同的主机名进行 QUIC 连接
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== QUIC 自定义主机名示例 ===");
    
    // 检查证书是否存在
    if !std::path::Path::new("certs/server.crt").exists() {
        println!("证书不存在，请先运行: cargo run --example cert_generator");
        return Ok(());
    }
    
    // 测试不同的主机名配置
    let hostnames = vec![
        "localhost",
        "127.0.0.1", 
        "flare-core-server", // 这个会失败，因为证书不匹配
    ];
    
    for hostname in hostnames {
        println!("\n--- 测试主机名: {} ---", hostname);
        
        // 创建客户端配置
        let mut config = ConnectionConfig::client(
            format!("quic_client_{}", hostname.replace(".", "_")),
            "127.0.0.1:8081".to_string(),
        );
        config.transport = Transport::Quic;
        
        // 配置 QUIC 客户端
        config.protocol_config.quic.client.server_cert_path = Some("certs/server.crt".to_string());
        config.protocol_config.quic.client.skip_server_verification = false;
        config.protocol_config.quic.client.server_hostname = Some(hostname.to_string());
        
        // 创建连接
        let mut client_connection = match ConnectionFactory::create_client(config).await {
            Ok(conn) => conn,
            Err(e) => {
                println!("创建连接失败: {}", e);
                continue;
            }
        };
        
        // 尝试连接
        match client_connection.connect().await {
            Ok(_) => {
                println!("✅ 成功连接到服务端 (主机名: {})", hostname);
                
                // 发送一条测试消息
                let message_id = format!("test_msg_{}", hostname);
                let send_cmd = MessageSendCommand::new(
                    format!("Hello from hostname: {}", hostname).into_bytes()
                );
                let command = Command::Message(MessageCmd::Send(send_cmd));
                let message = Frame::new(command, message_id, Reliability::AtLeastOnce);
                
                if let Err(e) = client_connection.send_message(message).await {
                    println!("发送消息失败: {}", e);
                } else {
                    println!("✅ 消息发送成功");
                }
                
                // 等待一下
                sleep(Duration::from_millis(100)).await;
                
                // 断开连接
                if let Err(e) = client_connection.disconnect(Some("测试完成".to_string())).await {
                    println!("断开连接失败: {}", e);
                } else {
                    println!("✅ 连接已断开");
                }
            }
            Err(e) => {
                println!("❌ 连接失败 (主机名: {}): {}", hostname, e);
            }
        }
    }
    
    println!("\n=== 测试完成 ===");
    println!("注意：只有 'localhost' 和 '127.0.0.1' 会成功，因为证书只对这些主机名有效");
    
    Ok(())
}
