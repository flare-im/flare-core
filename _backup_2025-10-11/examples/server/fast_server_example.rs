//! FastServer 服务端示例
//!
//! 展示如何使用 FastServer 创建高性能的服务端，支持 WebSocket 和 QUIC 双协议

use std::sync::Arc;
use std::time::Duration;
use tracing::{info, error};

use flare_core::{
    server::{
        config::ServerConfig,
        fast::{
            server::FastServer,
            message_handler::MessageHandler,
            auth::AuthProvider,
        },
    },
    common::{
        serialization::SerializationFormat,
        error::Result,
    },
};

/// 自定义消息处理器
#[derive(Debug)]
pub struct CustomMessageHandler {
    pub name: String,
}

#[async_trait::async_trait]
impl MessageHandler for CustomMessageHandler {
    async fn handle_error(&self, connection_id: &str, error: &flare_core::common::protocol::commands::ErrorCommand) -> Result<()> {
        info!("[{}] 处理错误: {} - {:?}", self.name, connection_id, error);
        Ok(())
    }

    async fn handle_custom_command(&self, user_id: &str, connection_id: &str, command: &flare_core::common::protocol::commands::CustomCommand) -> Result<()> {
        info!("[{}] 处理自定义命令: {} {} - {:?}", self.name, user_id, connection_id, command);
        Ok(())
    }

    async fn handle_message(&self, user_id: &str, connection_id: &str, message: &flare_core::common::protocol::commands::MessageSendCommand) -> Result<()> {
        info!("[{}] 收到来自用户 {} 的消息: {:?}", self.name, user_id, message);
        Ok(())
    }
    
    async fn handle_data_message(&self, user_id: &str, connection_id: &str, message: &flare_core::common::protocol::commands::DataCommand) -> Result<Option<Vec<u8>>> {
        info!("[{}] 收到来自用户 {} 的数据消息: {:?}", self.name, user_id, message);
        // 回显数据
        Ok(Some(format!("Echo: {}", String::from_utf8_lossy(&message.data)).as_bytes().to_vec()))
    }

    async fn handle_custom_message(&self, user_id: &str, connection_id: &str, message: &flare_core::common::protocol::commands::CustomCommand) -> Result<()> {
        info!("[{}] 收到来自用户 {} 的自定义消息: {:?}", self.name, user_id, message);
        Ok(())
    }
}

impl CustomMessageHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

/// 自定义认证提供者
#[derive(Debug)]
pub struct CustomAuthProvider {
    pub name: String,
}

#[async_trait::async_trait]
impl AuthProvider for CustomAuthProvider {
    async fn validate_token(&self, user_id: &str, platform: &str, token: &str) -> Result<bool> {
        info!("[{}] 认证用户: {} (平台: {})", self.name, user_id, platform);
        
        // 简单的认证逻辑：检查用户ID和令牌
        if user_id.starts_with("user_") && !token.is_empty() {
            info!("[{}] 用户认证成功: {}", self.name, user_id);
            Ok(true)
        } else {
            info!("[{}] 用户认证失败: {}", self.name, user_id);
            Ok(false)
        }
    }
    
    async fn get_user_info(&self, user_id: &str) -> Result<Option<Vec<u8>>> {
        info!("[{}] 获取用户信息: {}", self.name, user_id);
        Ok(Some(format!("用户信息: {}", user_id).as_bytes().to_vec()))
    }
}

impl CustomAuthProvider {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    
    info!("启动 FastServer 服务端示例");
    
    // 创建服务端配置
    let config = ServerConfig::default_dual_protocol(
        "certs/server.crt".to_string(),
        "certs/server.key".to_string()
    )
    .with_heartbeat_config(10000, 5000, 3) // 10秒心跳间隔，5秒超时，最多3次丢失
    .with_serialization_format(SerializationFormat::Protobuf)
    .with_performance_config(flare_core::server::config::ServerPerformanceConfig {
        worker_threads: 4, // 4个工作线程
        enable_cpu_affinity: true,
        enable_numa_awareness: false,
        memory_pool_size: 128 * 1024 * 1024, // 128MB内存池
        enable_zero_copy: true,
        batch_size: 100,
        enable_connection_pool: true,
        connection_pool_size: 1000,
    })
    .with_security_config(flare_core::server::config::ServerSecurityConfig {
        enable_rate_limiting: true,
        max_connections_per_ip: 10,
        rate_limit_per_second: 100,
        enable_blacklist: false,
        blacklist_file_path: None,
        enable_whitelist: false,
        whitelist_file_path: None,
        max_message_size: 10 * 1024 * 1024, // 10MB
        enable_message_encryption: false,
    })
;
    
    // 验证配置
    if let Err(e) = config.validate() {
        error!("配置验证失败: {}", e);
        return Err(e.into());
    }
    
    info!("服务端配置:");
    info!("  - 服务器类型: {:?}", config.server_type);
    info!("  - WebSocket地址: {:?}", config.websocket_config.as_ref().map(|c| c.listen_addr.clone()));
    info!("  - QUIC地址: {:?}", config.quic_config.as_ref().map(|c| c.listen_addr.clone()));
    info!("  - 最大连接数: {}", config.max_connections);
    info!("  - 心跳间隔: {}ms", config.heartbeat_interval_ms);
    info!("  - 序列化格式: {:?}", config.serialization_config.format);
    
    // 创建自定义消息处理器和认证提供者
    let message_handler: Arc<dyn MessageHandler> = Arc::new(CustomMessageHandler::new("FastServer".to_string()));
    let auth_provider: Arc<dyn AuthProvider> = Arc::new(CustomAuthProvider::new("FastServer".to_string()));
    
    // 创建 FastServer 实例
    let server = FastServer::new(
        Some(message_handler),
        Some(auth_provider),
        config,
    );
    
    info!("FastServer 实例创建成功");
    
    // 启动服务
    info!("正在启动服务...");
    server.start().await?;
    info!("✅ 服务启动成功！");
    
    // 定期打印统计信息
    let stats_task = {
        let server_ref = server.get_server().clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                
                // 获取服务统计信息
                let stats = server_ref.get_connection_manager().get_connection_stats().await;
                info!("📊 服务统计信息:");
                info!("  - 总连接数: {}", stats.total_connections);
                info!("  - 活跃连接数: {}", stats.active_connections);
                info!("  - 总消息数: {}", stats.total_messages);
                info!("  - 平均连接质量: {}", stats.average_quality);
            }
        })
    };
    
    info!("服务正在运行，按 Ctrl+C 停止...");
    
    // 等待中断信号
    tokio::signal::ctrl_c().await?;
    info!("收到停止信号，正在关闭服务...");
    
    // 取消统计任务
    stats_task.abort();
    
    // 停止服务
    server.stop().await;
    info!("✅ 服务已停止");
    
    Ok(())
}
