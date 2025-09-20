//! FastClient使用示例

use flare_core::client::{
    FastClient, FastClientBuilder, AuthConfig, Transport, ProtocolSelection
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
    
    info!("启动FastClient示例");
    
    // 使用构建器创建FastClient
    let mut client = FastClientBuilder::new()
        .with_server_address(Transport::WebSocket, "ws://127.0.0.1:8080".to_string())
        .with_server_address(Transport::Quic, "127.0.0.1:8081".to_string())
        .with_protocol_selection(ProtocolSelection::Auto)  // 使用协议竞速
        .with_auth_enabled(true)                           // 启用认证
        .with_auth_user_id("user123".to_string())          // 设置用户ID
        .with_auth_platform("web".to_string())             // 设置平台
        .with_auth_token("token123".to_string())           // 设置令牌
        .with_auth_timeout(5000)                           // 设置认证超时
        .with_heartbeat(10000, 30000)                      // 设置心跳间隔和超时
        .with_auto_reconnect(true)                         // 启用自动重连
        .with_reconnect_params(5, 1000)                    // 设置重连参数
        .build();
    
    // 启动客户端
    match client.start().await {
        Ok(_) => {
            info!("FastClient启动成功");
            
            // 检查连接状态
            let state = client.get_state().await;
            info!("当前连接状态: {:?}", state);
            
            // 发送一条测试消息
            let message_id = FrameFactory::generate_message_id();
            let message = FrameFactory::create_message_frame(
                message_id,
                "Hello, Fast Server!".as_bytes().to_vec(),
                Reliability::Reliable
            )?;
            
            match client.send_message(message).await {
                Ok(_) => info!("消息发送成功"),
                Err(e) => error!("消息发送失败: {}", e),
            }
            
            // 等待一段时间以观察心跳和重连功能
            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
            
            // 停止客户端
            client.stop().await?;
            info!("FastClient已停止");
        }
        Err(e) => {
            error!("FastClient启动失败: {}", e);
            return Err(e.into());
        }
    }
    
    Ok(())
}