//! 简化版事件处理机制使用示例

use std::sync::Arc;
use async_trait::async_trait;
use crate::server::proxy::message_handler::{MessageHandler, ConnectionEventType};
use crate::server::proxy::server_proxy::{ServerProxy, ServerConfig};
use crate::common::protocol::Frame;

/// 简单的消息处理器实现
struct SimpleMessageHandler;

#[async_trait]
impl MessageHandler for SimpleMessageHandler {
    async fn handle_user_message(&self, user_id: &str, connection_id: &str, message: &Frame) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("收到用户 {} 通过连接 {} 发送的消息: {:?}", user_id, connection_id, message);
        // 在这里处理业务逻辑
        Ok(())
    }
    
    async fn handle_authentication_request(&self, connection_id: &str, user_id: &str, platform: &str, token: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("处理连接 {} 的用户 {} 通过平台 {} 的认证请求，Token: {}", connection_id, user_id, platform, token);
        // 在这里实现认证逻辑
        Ok(())
    }
    
    async fn handle_connection_event(&self, event: ConnectionEventType, connection_id: &str, details: Option<&str>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match event {
            ConnectionEventType::Connected => {
                println!("连接已建立: {}", connection_id);
            }
            ConnectionEventType::Disconnected => {
                println!("连接已断开: {} - 原因: {:?}", connection_id, details);
            }
            ConnectionEventType::Error => {
                println!("连接错误: {} - 错误: {:?}", connection_id, details);
            }
        }
        Ok(())
    }
}

/// 启动简化版服务的示例
async fn start_simple_server() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 创建消息处理器
    let message_handler = Arc::new(SimpleMessageHandler);
    
    // 创建服务代理
    let server_proxy = ServerProxy::new(Some(message_handler));
    
    // 配置服务
    let config = ServerConfig::default();
    
    // 启动服务
    server_proxy.start(config).await?;
    
    println!("简化版服务已启动");
    
    Ok(())
}