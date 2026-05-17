//! Flare 聊天室服务器示例
//!
//! 演示如何使用 FlareServerBuilder 创建完整的聊天室服务器
//! 使用所有 flare-core 的能力：
//! - ServerEventHandler（事件处理器：自动消息路由和 ACK 处理）
//! - 设备管理（设备冲突策略）
//! - 序列化协商（JSON/Protobuf）
//! - 压缩协商（Gzip/Zstd/None）
//! - 加密支持（AES-256-GCM，已注册加密器）
//! - 多协议支持（WebSocket + QUIC）
//!
//! ## 启动命令
//!
//! ```bash
//! RUST_LOG=info cargo run --example flare_chat_server
//! ```

use async_trait::async_trait;
use flare_core::common::compression::CompressionAlgorithm;
use flare_core::common::config_types::{HeartbeatConfig, TransportProtocol};
use flare_core::common::device::DeviceConflictStrategyBuilder;
use flare_core::common::encryption::{Aes256GcmEncryptor, EncryptionAlgorithm, EncryptionUtil};
use flare_core::common::message::{LogLevel, LoggingMiddleware};
use flare_core::common::protocol::{
    Frame, PayloadCommand, Reliability, SerializationFormat, frame_with_message_command,
    generate_message_id,
};
use flare_core::common::*;
use flare_core::server::connection::{ConnectionManager, ConnectionManagerTrait};
use flare_core::server::events::handler::ServerEventHandler;
use flare_core::server::handle::{DefaultServerHandle, ServerHandle};
use flare_core::server::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("🚀 启动 Flare 聊天室服务器（完整功能演示）");
    info!("");
    info!("📋 服务器功能说明：");
    info!("   - 消息管道：自动处理序列化、压缩、加密、验证、日志、性能监控");
    info!("   - 中间件：验证 → 日志 → 性能监控");
    info!("   - 设备管理：平台互斥策略（同一用户同一平台只能有一个设备在线）");
    info!("   - 协商：默认 Protobuf + Gzip 压缩（客户端可指定格式）");
    info!("   - 加密：AES-256-GCM 加密（可选，默认 None）");
    info!("   - 多协议：WebSocket + QUIC 双协议支持");
    info!("");

    // ============================================================
    // 1. 注册加密器（可选，用于加密通信）
    // ============================================================
    // ⚠️  安全警告：当前示例使用硬编码密钥，仅用于演示！
    // 生产环境必须从安全配置或密钥管理系统读取密钥，不要硬编码。
    // 详细说明请参考：doc/ENCRYPTION_SECURITY.md
    //
    // 推荐方案：
    // 1. 传输层加密（推荐）：使用 TLS/QUIC，不需要应用层加密
    // 2. 服务端密钥管理：从环境变量或密钥管理系统读取
    //    let key = std::env::var("ENCRYPTION_KEY")?;
    // 3. 客户端密钥协商：通过安全通道从服务端获取或使用密钥交换协议
    //
    // 当前实现仅用于功能演示，生产环境请替换为安全的密钥管理方案
    let encryption_key = if let Ok(key) = std::env::var("ENCRYPTION_KEY") {
        // 优先使用环境变量（如果设置）
        info!("🔐 使用环境变量 ENCRYPTION_KEY");
        key.as_bytes().to_vec()
    } else {
        // 否则使用默认示例密钥（仅用于演示）
        warn!("⚠️  使用默认示例密钥，仅用于演示！生产环境请设置 ENCRYPTION_KEY 环境变量");
        b"01234567890123456789012345678901".to_vec() // 32 bytes for AES-256
    };

    if encryption_key.len() != 32 {
        return Err(flare_core::common::error::FlareError::protocol_error(
            format!(
                "Encryption key must be exactly 32 bytes, got {} bytes",
                encryption_key.len()
            ),
        ));
    }

    let encryptor = Aes256GcmEncryptor::new(&encryption_key)?;
    EncryptionUtil::register_custom(Arc::new(encryptor));
    info!("🔐 已注册 AES-256-GCM 加密器");

    // ============================================================
    // 2. 创建连接管理器（可选，用于共享连接状态）
    // ============================================================
    let connection_manager = Arc::new(ConnectionManager::new());

    // ============================================================
    // 3. 创建聊天室事件处理器（实现 ServerEventHandler）
    // ============================================================
    let chat_handler = Arc::new(ChatRoomHandler {
        usernames: Arc::new(Mutex::new(HashMap::new())),
        server_handle: Arc::new(Mutex::new(None)),
        connection_manager: Arc::new(Mutex::new(None)),
    });

    // 注意：服务端 Flare 模式通过 ServerEventHandler 处理消息
    // 中间件和处理器是客户端特性，服务端不需要

    // ============================================================
    // 6. 创建设备管理器（用于设备冲突管理）
    // ============================================================
    let device_manager = Arc::new(DeviceManager::new(
        DeviceConflictStrategyBuilder::new()
            .platform_exclusive() // 平台互斥：同一用户同一平台只能有一个设备在线
            // 其他策略选项：
            // .mobile_exclusive()     // 移动端互斥
            // .mobile_and_pc_coexist() // 移动端和PC端共存
            // .fully_exclusive()      // 完全互斥
            // .allow_all()            // 允许所有设备同时在线
            .build(),
    ));

    // ============================================================
    // 7. 使用 FlareServerBuilder 构建服务器（使用所有能力）
    // ============================================================
    // 注意：服务端 Flare 模式通过 ServerEventHandler 处理消息
    // 所有消息处理逻辑都在 ChatRoomHandler 中实现
    let server = FlareServerBuilder::new("0.0.0.0:8080", chat_handler.clone())
        // ============================================================
        // 连接和设备管理
        // ============================================================
        .with_connection_manager(connection_manager.clone())
        .with_device_manager(device_manager)
        // ============================================================
        // 中间件配置（消息处理管道）
        // ============================================================
        .with_middleware(Arc::new(
            LoggingMiddleware::new("ChatServerLogging").with_level(LogLevel::Debug),
        ))
        // ============================================================
        // 协商配置（服务端默认格式、压缩和加密）
        // ============================================================
        .with_default_format(SerializationFormat::Protobuf) // 默认 Protobuf（客户端可指定格式）
        .with_default_compression(CompressionAlgorithm::Gzip) // 默认 Gzip 压缩
        .with_default_encryption(EncryptionAlgorithm::Aes256Gcm) // 默认 AES-256-GCM 加密
        // ============================================================
        // 协议配置（多协议支持）
        // ============================================================
        .with_protocols(vec![TransportProtocol::WebSocket, TransportProtocol::QUIC])
        .with_protocol_address(TransportProtocol::WebSocket, "0.0.0.0:8080".to_string())
        .with_protocol_address(TransportProtocol::QUIC, "0.0.0.0:8081".to_string())
        // ============================================================
        // 其他配置
        // ============================================================
        .with_max_connections(2000)
        .with_connection_timeout(std::time::Duration::from_secs(60))
        .with_heartbeat(
            HeartbeatConfig::default()
                .with_interval(std::time::Duration::from_secs(30))
                .with_timeout(std::time::Duration::from_secs(90)),
        )
        .build()?;

    // ============================================================
    // 8. 获取 ServerHandle 和 ConnectionManager 并设置到监听器
    // ============================================================
    let (server_handle, manager_trait) =
        if let Some(manager_trait) = server.get_server_handle_components() {
            let handle: Arc<dyn ServerHandle> =
                Arc::new(DefaultServerHandle::new(manager_trait.clone()));
            (handle, manager_trait)
        } else {
            return Err("无法获取连接管理器".into());
        };
    chat_handler.set_server_handle(server_handle).await;
    chat_handler.set_connection_manager(manager_trait).await;

    // ============================================================
    // 9. 启动服务器
    // ============================================================
    server.start().await?;

    info!("✅ 服务器已启动");
    info!("   WebSocket: ws://127.0.0.1:8080");
    info!("   QUIC: quic://127.0.0.1:8081");
    info!("");
    info!("📋 功能说明：");
    info!("   - 消息处理：通过 ServerEventHandler 自动处理消息路由和 ACK");
    info!("   - 中间件：LoggingMiddleware（记录所有消息的日志）");
    info!("   - 自动处理：序列化、压缩、加密、验证、日志、性能监控");
    info!("   - 设备管理：平台互斥策略（同一用户同一平台只能有一个设备在线）");
    info!("   - 协商机制：默认 Protobuf + Gzip 压缩（客户端可指定格式）");
    info!("   - 加密支持：AES-256-GCM 加密（已注册，客户端可请求使用）");
    info!("   - 多协议支持：WebSocket + QUIC 双协议");
    info!("");
    info!("📱 设备管理说明：");
    info!("   - 当前策略：平台互斥（PlatformExclusive）");
    info!("   - 规则：同一用户同一平台只能有一个设备在线");
    info!("   - 例如：同一用户的 Android 设备之间互斥，但 Android 和 iOS 可以同时在线");
    info!("   - 新设备登录时，同一平台的其他设备会被自动踢掉");
    info!("");
    info!("💬 聊天室功能：");
    info!("   - 发送消息：客户端发送的消息会广播给所有其他用户");
    info!("   - Echo 功能：发送 'echo: <text>' 会收到 Echo 响应");
    info!("   - 欢迎消息：新用户连接时会收到欢迎消息");
    info!("   - 自动 ACK：SEND 消息处理完成后，框架会自动发送 ACK");
    info!("");
    info!("服务器运行中，按 Ctrl+C 停止...");

    tokio::signal::ctrl_c().await?;
    info!("\n正在停止服务器...");
    server.stop().await?;
    info!("服务器已停止");

    Ok(())
}

/// 聊天室事件处理器（实现 ServerEventHandler）
struct ChatRoomHandler {
    usernames: Arc<Mutex<HashMap<String, String>>>, // connection_id -> username
    server_handle: Arc<Mutex<Option<Arc<dyn ServerHandle>>>>,
    connection_manager: Arc<Mutex<Option<Arc<dyn ConnectionManagerTrait>>>>, // 用于获取连接信息
}

impl ChatRoomHandler {
    async fn set_server_handle(&self, handle: Arc<dyn ServerHandle>) {
        *self.server_handle.lock().await = Some(handle);
    }

    async fn set_connection_manager(&self, manager: Arc<dyn ConnectionManagerTrait>) {
        *self.connection_manager.lock().await = Some(manager);
    }
}

#[async_trait]
impl ServerEventHandler for ChatRoomHandler {
    /// 处理 SEND 消息（聊天消息）
    async fn handle_message(
        &self,
        command: &PayloadCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        let username = self
            .usernames
            .lock()
            .await
            .get(connection_id)
            .cloned()
            .unwrap_or_else(|| "匿名".to_string());

        // 尝试解析protobuf消息内容
        let message_text = match String::from_utf8(command.payload.clone()) {
            Ok(text) => text,
            Err(_) => {
                // 如果不是有效的UTF-8，则显示十六进制调试信息
                format!("<protobuf_binary_data: {} bytes>", command.payload.len())
            }
        };

        info!("💬 [{}]: {}", username, message_text);

        // 广播消息给所有用户（排除发送者）
        let broadcast_cmd = PayloadCommand {
            r#type: 1, // MESSAGE
            message_id: generate_message_id(),
            payload: format!("[{}]: {}", username, message_text).into_bytes(),
            metadata: std::collections::HashMap::new(),
            seq: 0,
        };

        let broadcast_frame = frame_with_message_command(broadcast_cmd, Reliability::AtLeastOnce);

        // 使用 ServerHandle 广播消息
        if let Some(ref handle) = *self.server_handle.lock().await {
            if let Err(e) = handle
                .broadcast_except(&broadcast_frame, connection_id)
                .await
            {
                error!("广播消息失败: {}", e);
            }
        }

        // 返回 None，框架会自动发送 ACK
        Ok(None)
    }

    /// 处理 ACK 消息（客户端确认）
    async fn handle_ack(
        &self,
        _command: &PayloadCommand,
        _connection_id: &str,
    ) -> Result<Option<Frame>> {
        // 聊天室不需要处理 ACK
        Ok(None)
    }

    /// 处理 DATA 消息
    async fn handle_data(
        &self,
        _command: &PayloadCommand,
        _connection_id: &str,
    ) -> Result<Option<Frame>> {
        // 聊天室不需要处理 DATA
        Ok(None)
    }

    /// 处理连接建立完成事件
    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        info!("✅ 新连接: {}", connection_id);

        // 从连接管理器获取连接信息，使用客户端提供的用户ID
        // 注意：此时协商已完成，user_id 应该已经更新到连接信息中
        let username = if let Some(ref manager) = *self.connection_manager.lock().await {
            match manager.get_connection(connection_id).await {
                Some((_, conn_info)) => {
                    // 优先使用客户端提供的用户ID
                    if let Some(ref user_id) = conn_info.user_id {
                        debug!("[FlareChatServer] 从连接信息获取用户ID: {}", user_id);
                        user_id.clone()
                    } else {
                        // 如果没有用户ID，使用连接ID的前8位
                        debug!("[FlareChatServer] 连接信息中没有用户ID，使用连接ID");
                        format!("用户_{}", &connection_id[..8.min(connection_id.len())])
                    }
                }
                None => {
                    // 连接不存在，使用连接ID的前8位作为默认值
                    error!("[FlareChatServer] 连接不存在: {}", connection_id);
                    format!("用户_{}", &connection_id[..8.min(connection_id.len())])
                }
            }
        } else {
            // 连接管理器未设置，使用连接ID的前8位作为默认值
            error!("[FlareChatServer] 连接管理器未设置");
            format!("用户_{}", &connection_id[..8.min(connection_id.len())])
        };

        self.usernames
            .lock()
            .await
            .insert(connection_id.to_string(), username.clone());

        info!("📝 用户ID: {} (连接ID: {})", username, connection_id);

        // 发送欢迎消息
        let welcome_cmd = PayloadCommand {
            r#type: 1, // MESSAGE
            message_id: generate_message_id(),
            payload: format!("欢迎 {} 加入聊天室！", username).into_bytes(),
            metadata: std::collections::HashMap::new(),
            seq: 0,
        };

        let welcome_frame = frame_with_message_command(welcome_cmd, Reliability::AtLeastOnce);

        // 使用 ServerHandle 发送消息
        if let Some(ref handle) = *self.server_handle.lock().await {
            if let Err(e) = handle.send_to(connection_id, &welcome_frame).await {
                error!("发送欢迎消息失败: {}", e);
            }
        }

        Ok(())
    }

    async fn on_disconnect(&self, connection_id: &str, _reason: Option<&str>) -> Result<()> {
        let username = self
            .usernames
            .lock()
            .await
            .remove(connection_id)
            .unwrap_or_else(|| "未知用户".to_string());

        info!("❌ 用户断开: {} ({})", username, connection_id);

        Ok(())
    }
}
