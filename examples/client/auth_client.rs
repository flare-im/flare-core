//! 带认证功能的客户端示例

use flare_core::client::{
    Client, ClientConfig, AuthConfig, Transport, ProtocolSelection
};
use flare_core::common::protocol::{Frame, Reliability};
use flare_core::common::protocol::factory::FrameFactory;
use flare_core::common::connections::types::ConnectionState;
use tracing::{info, error};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    info!("启动带认证功能的客户端示例");
    
    // 创建认证配置
    let auth_config = AuthConfig {
        enabled: true,
        user_id: Some("user123".to_string()),
        platform: Some("web".to_string()),
        token: Some("token123".to_string()),
        timeout_ms: 5000,
    };
    
    // 创建客户端配置
    let config = ClientConfig::new(
        "ws://127.0.0.1:8080".to_string(),  // WebSocket地址
        "127.0.0.1:8081".to_string()       // QUIC地址
    )
    .with_protocol_selection(ProtocolSelection::Auto)  // 使用协议竞速
    .with_auth_config(auth_config)                     // 设置认证配置
    .with_heartbeat(10000, 30000)                      // 设置心跳间隔和超时
    .with_auto_reconnect(true)                         // 启用自动重连
    .with_reconnect_params(5, 1000);                   // 设置重连参数
    
    // 创建客户端
    let mut client = Client::new(config);
    
    // 连接到服务器
    match client.connect().await {
        Ok(_) => {
            info!("客户端连接成功");
            
            // 检查连接状态
            let state = client.get_state().await;
            info!("当前连接状态: {:?}", state);
            
            // 发送一条测试消息
            let message_id = FrameFactory::generate_message_id();
            let message = FrameFactory::create_message_frame(
                message_id,
                "Hello, Server!".as_bytes().to_vec(),
                Reliability::Reliable
            )?;
            
            match client.send_message(message).await {
                Ok(_) => info!("消息发送成功"),
                Err(e) => error!("消息发送失败: {}", e),
            }
            
            // 等待一段时间以观察心跳和重连功能
            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
            
            // 断开连接
            client.disconnect().await?;
            info!("客户端已断开连接");
        }
        Err(e) => {
            error!("客户端连接失败: {}", e);
            return Err(e.into());
        }
    }
    
    Ok(())
}