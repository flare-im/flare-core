//! ChatServer 服务端示例（新文件）
//! 
//! 使用 FastServer 同时支持 WebSocket 和 QUIC，提供多客户端聊天广播功能

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
        protocol::factory::FrameFactory,
        protocol::Reliability,
    },
};

/// 自定义消息处理器：解析客户端 JSON 并广播到所有连接
pub struct CustomMessageHandler {
    pub name: String,
    message_sender: tokio::sync::RwLock<Option<Arc<flare_core::server::fast::MessageSender>>>,
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

    async fn handle_message(&self, user_id: &str, _connection_id: &str, message: &flare_core::common::protocol::commands::MessageSendCommand) -> Result<()> {
        info!("[{}] 收到来自用户 {} 的消息: {:?}", self.name, user_id, message);
        Ok(())
    }
    
    async fn handle_data_message(&self, user_id: &str, _connection_id: &str, message: &flare_core::common::protocol::commands::DataCommand) -> Result<Option<Vec<u8>>> {
        // 解析客户端发来的聊天数据（JSON：{"user":"...","text":"..."}）
        let payload = &message.data;
        let parsed: serde_json::Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(e) => {
                error!("[{}] 数据解析失败: {}", self.name, e);
                return Ok(None);
            }
        };
        let sender = parsed.get("user").and_then(|v| v.as_str()).unwrap_or(user_id);
        let text = parsed.get("text").and_then(|v| v.as_str()).unwrap_or("");
        if text.is_empty() { return Ok(None); }

        // 构造广播的数据帧：沿用 Data 命令，广播相同 JSON 结构
        let broadcast_json = serde_json::json!({"user": sender, "text": text});
        let message_id = FrameFactory::generate_message_id();
        let frame = FrameFactory::create_data_frame(
            message_id,
            serde_json::to_vec(&broadcast_json).unwrap_or_default(),
            Reliability::AtLeastOnce,
        )?;

        // 广播到所有连接
        if let Some(ms) = self.message_sender.read().await.clone() {
            let _ = ms.broadcast_message(frame).await?;
        } else {
            error!("[{}] MessageSender 未初始化，无法广播", self.name);
        }
        Ok(None)
    }

    async fn handle_custom_message(&self, user_id: &str, connection_id: &str, message: &flare_core::common::protocol::commands::CustomCommand) -> Result<()> {
        info!("[{}] 收到来自用户 {} 的自定义消息: {:?}", self.name, user_id, message);
        Ok(())
    }
}

impl CustomMessageHandler {
    pub fn new(name: String) -> Self {
        Self { name, message_sender: tokio::sync::RwLock::new(None) }
    }
    pub async fn set_message_sender(&self, sender: Arc<flare_core::server::fast::MessageSender>) {
        *self.message_sender.write().await = Some(sender);
    }
}

/// 自定义认证提供者：简单规则允许非空用户名
pub struct CustomAuthProvider {
    pub name: String,
}

#[async_trait::async_trait]
impl AuthProvider for CustomAuthProvider {
    async fn validate_token(&self, user_id: &str, _platform: &str, token: &str) -> Result<bool> {
        info!("[{}] 认证用户: {}", self.name, user_id);
        Ok(!user_id.trim().is_empty() && !token.is_empty())
    }
    
    async fn get_user_info(&self, user_id: &str) -> Result<Option<Vec<u8>>> {
        info!("[{}] 获取用户信息: {}", self.name, user_id);
        Ok(Some(format!("用户信息: {}", user_id).as_bytes().to_vec()))
    }
}

impl CustomAuthProvider {
    pub fn new(name: String) -> Self { Self { name } }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();
    info!("启动 ChatServer 服务端示例");

    // 创建服务端配置（双协议）
    let mut config = ServerConfig::default_dual_protocol(
        "certs/server.crt".to_string(),
        "certs/server.key".to_string()
    )
    .with_heartbeat_config(10000, 5000, 3)
    .with_serialization_format(SerializationFormat::Protobuf)
    .with_security_config(flare_core::server::config::ServerSecurityConfig {
        enable_rate_limiting: true,
        max_connections_per_ip: 100,
        rate_limit_per_second: 200,
        enable_blacklist: false,
        blacklist_file_path: None,
        enable_whitelist: false,
        whitelist_file_path: None,
        max_message_size: 10 * 1024 * 1024,
        enable_message_encryption: false,
    })
    .with_performance_config(flare_core::server::config::ServerPerformanceConfig {
        worker_threads: 4,
        enable_cpu_affinity: false,
        enable_numa_awareness: false,
        memory_pool_size: 64 * 1024 * 1024,
        enable_zero_copy: true,
        batch_size: 100,
        enable_connection_pool: true,
        connection_pool_size: 1000,
    });

    // 根据环境变量控制是否校验客户端证书（双向TLS）
    if let Some(quic_cfg) = &mut config.quic_config {
        if let Some(tls) = &mut quic_cfg.tls_config {
            tls.require_client_auth = std::env::var("CHAT_REQUIRE_CLIENT_AUTH").ok().map(|v| v == "1").unwrap_or(false);
            if tls.require_client_auth {
                // 使用客户端证书作为信任根（本地测试）；生产环境应使用CA根证书
                tls.client_ca_cert_path = Some("certs/client.crt".to_string());
            }
        }
    }

    // 构建处理器与认证
    let handler_impl = Arc::new(CustomMessageHandler::new("ChatServer".to_string()));
    let handler: Arc<dyn MessageHandler> = handler_impl.clone();
    let auth_provider: Arc<dyn AuthProvider> = Arc::new(CustomAuthProvider::new("ChatServer".to_string()));

    // 创建 FastServer
    let server = FastServer::new(Some(handler), Some(auth_provider), config);

    // 注入 MessageSender
    handler_impl.set_message_sender(server.get_message_sender().clone()).await;

    // 启动服务
    info!("正在启动服务...");
    server.start().await?;
    info!("✅ 服务启动成功！WebSocket: 127.0.0.1:4320, QUIC: 127.0.0.1:4321");

    // 定期打印统计信息
    let stats_task = {
        let server_ref = server.get_server().clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
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
    tokio::signal::ctrl_c().await?;
    info!("收到停止信号，正在关闭服务...");

    stats_task.abort();
    server.stop().await;
    info!("✅ 服务已停止");

    Ok(())
}
