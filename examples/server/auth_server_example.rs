//! 认证服务器示例
//!
//! 展示如何创建一个完整的认证服务器

use flare_core::{
    common::{
        connections::{
            traits::Connection,
            types::{ConnectionConfig, ConnectionRole},
        },
        protocol::Frame,
    },
    server::{
        ConnectionManager,
        UserConnectionManager,
        ServerImpl,
        ServerConfig,
        auth::SimpleAuthHandler,
        auth_handler::ServerAuthHandler,
        auth_event_handler::AuthEventHandler,
        example_auth_handler::ExampleAuthHandler,
    },
};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    // 创建基础连接管理器
    let base_manager = Arc::new(ConnectionManager::new());
    
    // 创建用户连接管理器
    let user_connection_manager = Arc::new(UserConnectionManager::new(base_manager));
    
    // 创建示例认证处理器
    let auth_handler = Arc::new(ExampleAuthHandler::new());
    
    // 添加一些测试用户
    auth_handler.add_user("token123".to_string(), "user123".to_string()).await;
    auth_handler.add_user("token456".to_string(), "user456".to_string()).await;
    
    // 创建服务端认证处理器
    let server_auth_handler = Arc::new(ServerAuthHandler::new(
        Arc::clone(&user_connection_manager),
        Arc::clone(&auth_handler),
    ));
    
    // 创建认证事件处理器
    let auth_event_handler = Arc::new(AuthEventHandler::new(Arc::clone(&user_connection_manager)));
    
    // 创建服务器配置
    let server_config = ServerConfig::new()
        .with_local_addr("127.0.0.1:8080".to_string())
        .with_connection_timeout_ms(30000)
        .with_heartbeat_interval_ms(10000)
        .with_max_connections(1000);
    
    // 创建服务器
    let server = ServerImpl::with_event_handler(
        server_config,
        Arc::clone(&user_connection_manager),
        Arc::clone(&auth_event_handler) as Arc<dyn flare_core::server::event::ServerConnectionEvent>,
    );
    
    // 启动服务器
    if let Err(e) = server.start().await {
        println!("启动服务器失败: {}", e);
        return Ok(());
    }
    
    println!("认证服务器已启动，监听地址: 127.0.0.1:8080");
    
    // 保持程序运行一段时间以观察结果
    sleep(Duration::from_secs(60)).await;
    
    // 停止服务器
    server.stop().await;
    
    println!("认证服务器已停止");
    
    Ok(())
}