//! 认证聊天室服务器示例
//!
//! 演示如何使用 token 认证功能
//!
//! ## 认证机制说明
//!
//! 1. **启用认证**：
//!    - 通过 `enable_auth()` 启用认证功能
//!    - 通过 `with_authenticator()` 设置自定义认证器
//!
//! 2. **认证流程**：
//!    - 客户端连接后，发送 CONNECT 消息，其中包含 `token` 元数据
//!    - 服务端验证 token，验证通过后标记连接为已验证
//!    - 只有已验证的连接才能收发业务消息
//!
//! 3. **认证超时**：
//!    - 通过 `with_auth_timeout()` 设置认证超时时间
//!    - 如果连接在超时时间内未完成认证，连接将被关闭
//!
//! ## 启动命令
//!
//! ```bash
//! RUST_LOG=debug cargo run --example auth_server
//! ```
//!
//! ## 配置说明
//!
//! - `enable_auth()`: 启用认证功能
//! - `with_authenticator()`: 设置认证器（自定义验证逻辑）
//! - `with_auth_timeout()`: 设置认证超时时间（默认 30 秒）

use async_trait::async_trait;
use flare_core::common::protocol::flare::core::commands::command::Type as CommandType;
use flare_core::common::protocol::{
    Frame, MessageCommand, Reliability, frame_with_message_command, generate_message_id,
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
        .with_max_level(tracing::Level::DEBUG)
        .init();

    info!("🚀 启动认证聊天室服务器");
    info!("");
    info!("📋 认证配置说明：");
    info!("   - 已启用 token 认证");
    info!("   - 只有 token='12345' 才能验证通过");
    info!("   - 认证超时时间: 30 秒");
    info!("   - 未验证的连接无法收发业务消息");
    info!("");

    // ============================================================
    // 1. 创建自定义认证器（只接受 token='12345'）
    // ============================================================
    let authenticator = Arc::new(SimpleAuthenticator);

    // ============================================================
    // 2. 创建连接管理器（可选，用于共享连接状态）
    // ============================================================
    let connection_manager = Arc::new(ConnectionManager::new());

    // ============================================================
    // 3. 创建聊天室处理器
    // ============================================================
    let handler = Arc::new(AuthChatRoomHandler {
        usernames: Arc::new(Mutex::new(HashMap::new())),
        server_handle: Arc::new(Mutex::new(None)),
        connection_manager: Arc::new(Mutex::new(None)),
    });

    // ============================================================
    // 4. 创建自定义事件处理器（用于打印收到的消息）
    // ============================================================
    let event_handler = Arc::new(DebugEventHandler);

    // ============================================================
    // 5. 使用观察者模式构建器配置服务器
    // ============================================================
    let mut server = ObserverServerBuilder::new("0.0.0.0:8080")
        // 设置连接处理器（必须）
        .with_handler(handler.clone() as Arc<dyn ConnectionHandler>)
        // 设置连接管理器（可选，用于共享连接状态）
        .with_connection_manager(connection_manager)
        // ============================================================
        // 认证配置：启用认证并设置认证器
        // ============================================================
        .enable_auth() // 启用认证
        .with_authenticator(authenticator) // 设置认证器
        .with_auth_timeout(std::time::Duration::from_secs(30)) // 设置认证超时时间
        // 设置事件处理器（用于打印收到的消息）
        .with_event_handler(event_handler)
        // ============================================================
        // 协议配置：支持多协议监听
        // ============================================================
        .with_protocols(vec![
            flare_core::common::config_types::TransportProtocol::WebSocket,
            flare_core::common::config_types::TransportProtocol::QUIC,
        ])
        .with_protocol_address(
            flare_core::common::config_types::TransportProtocol::WebSocket,
            "0.0.0.0:8080".to_string(),
        )
        .with_protocol_address(
            flare_core::common::config_types::TransportProtocol::QUIC,
            "0.0.0.0:8081".to_string(),
        )
        // ============================================================
        // 其他配置
        // ============================================================
        .with_max_connections(2000)
        .build()?;

    // ============================================================
    // 6. 获取 ServerHandle 和 ConnectionManager 并设置到处理器
    // ============================================================
    let (server_handle, manager_trait) =
        if let Some(manager_trait) = server.get_server_handle_components() {
            let handle: Arc<dyn ServerHandle> =
                Arc::new(DefaultServerHandle::new(manager_trait.clone()));
            (handle, manager_trait)
        } else {
            return Err("无法获取连接管理器".into());
        };
    handler.set_server_handle(server_handle).await;
    handler.set_connection_manager(manager_trait).await;

    // ============================================================
    // 7. 启动服务器
    // ============================================================
    server.start().await?;

    info!("✅ 服务器已启动");
    info!("   WebSocket: ws://127.0.0.1:8080");
    info!("   QUIC: quic://127.0.0.1:8081");
    info!("");
    info!("📋 认证机制说明：");
    info!("   1. 客户端连接后，发送 CONNECT 消息（包含 token）");
    info!("   2. 服务端验证 token，只有 token='12345' 才能通过");
    info!("   3. 验证通过后，连接被标记为已验证");
    info!("   4. 只有已验证的连接才能收发业务消息");
    info!("");
    info!("💡 客户端连接示例：");
    info!("   - 正确 token: with_token('12345')");
    info!("   - 错误 token: with_token('wrong') 会被拒绝");
    info!("");
    info!("📊 认证日志说明：");
    info!("   - [ServerCore] 🔐 开始验证 token：显示开始验证");
    info!("   - [ServerCore] ✅ Token 验证成功：显示验证通过");
    info!("   - [ServerCore] ❌ Token 验证失败：显示验证失败");
    info!("   - [ServerCore] ✅ 连接已标记为已验证：显示连接已验证");
    info!("");
    info!("服务器运行中，按 Ctrl+C 停止...");

    tokio::signal::ctrl_c().await?;
    info!("\n正在停止服务器...");
    server.stop().await?;
    info!("服务器已停止");

    Ok(())
}

/// 简单认证器：只接受 token='12345'
struct SimpleAuthenticator;

#[async_trait]
impl Authenticator for SimpleAuthenticator {
    async fn authenticate(
        &self,
        token: &str,
        connection_id: &str,
        device_info: Option<&DeviceInfo>,
        metadata: Option<&HashMap<String, Vec<u8>>>,
    ) -> Result<AuthResult> {
        debug!(
            "[SimpleAuthenticator] 验证 token: connection_id={}, token={}",
            connection_id, token
        );

        // 只接受 token='12345'
        if token == "12345" {
            info!(
                "[SimpleAuthenticator] ✅ Token 验证成功: connection_id={}, token={}",
                connection_id, token
            );
            // 从 token 或 metadata 中提取用户 ID（这里简化处理，直接使用 token 作为 user_id）
            Ok(AuthResult::success(Some(token.to_string())))
        } else {
            warn!(
                "[SimpleAuthenticator] ❌ Token 验证失败: connection_id={}, token={}",
                connection_id, token
            );
            Ok(AuthResult::failure(format!("Token 无效: {}", token)))
        }
    }
}

/// 自定义事件处理器：打印收到的消息
struct DebugEventHandler;

#[async_trait]
impl ServerEventHandler for DebugEventHandler {
    /// 处理消息命令：打印收到的消息
    async fn handle_message_command(
        &self,
        command: &MessageCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        let message_text = String::from_utf8_lossy(&command.payload);
        debug!(
            "[EventHandler] 📨 收到消息命令: connection_id={}, message_type={}, message_id={}, payload_len={}, content={:?}",
            connection_id,
            command.r#type,
            command.message_id,
            command.payload.len(),
            message_text
        );
        Ok(None)
    }

    /// 处理通知命令：打印收到的通知
    async fn handle_notification_command(
        &self,
        command: &NotificationCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        let notification_content = String::from_utf8_lossy(&command.content);
        debug!(
            "[EventHandler] 🔔 收到通知命令: connection_id={}, notification_type={}, title={}, content_len={}, content={:?}",
            connection_id,
            command.r#type,
            command.title,
            command.content.len(),
            notification_content
        );
        Ok(None)
    }

    /// 处理 CONNECT 系统命令：打印连接信息
    async fn handle_connect(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        debug!(
            "[EventHandler] 🔌 收到 CONNECT 命令: connection_id={}",
            connection_id
        );
        Ok(None)
    }

    /// 处理 PING 系统命令：打印心跳信息
    async fn handle_ping(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        debug!(
            "[EventHandler] 💓 收到 PING: connection_id={}",
            connection_id
        );
        Ok(None)
    }

    /// 处理 PONG 系统命令：打印心跳响应
    async fn handle_pong(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        debug!(
            "[EventHandler] 💓 收到 PONG: connection_id={}",
            connection_id
        );
        Ok(None)
    }

    /// 处理连接断开事件：打印断开信息
    async fn on_disconnect(&self, connection_id: &str, reason: Option<&str>) -> Result<()> {
        if let Some(reason) = reason {
            debug!(
                "[EventHandler] 🔌 连接断开: connection_id={}, reason={}",
                connection_id, reason
            );
        } else {
            debug!(
                "[EventHandler] 🔌 连接断开: connection_id={}",
                connection_id
            );
        }
        Ok(())
    }

    /// 处理连接错误事件：打印错误信息
    async fn on_error(&self, connection_id: &str, error: &str) -> Result<()> {
        error!(
            "[EventHandler] ❌ 连接错误: connection_id={}, error={}",
            connection_id, error
        );
        Ok(())
    }
}

/// 认证聊天室处理器
struct AuthChatRoomHandler {
    usernames: Arc<Mutex<HashMap<String, String>>>, // connection_id -> username
    server_handle: Arc<Mutex<Option<Arc<dyn ServerHandle>>>>,
    connection_manager: Arc<Mutex<Option<Arc<dyn ConnectionManagerTrait>>>>,
}

impl AuthChatRoomHandler {
    async fn set_server_handle(&self, handle: Arc<dyn ServerHandle>) {
        *self.server_handle.lock().await = Some(handle);
    }

    async fn set_connection_manager(&self, manager: Arc<dyn ConnectionManagerTrait>) {
        *self.connection_manager.lock().await = Some(manager);
    }
}

#[async_trait::async_trait]
impl ConnectionHandler for AuthChatRoomHandler {
    async fn handle_frame(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        // 处理消息命令
        if let Some(cmd) = &frame.command {
            if let Some(CommandType::Message(msg_cmd)) = &cmd.r#type {
                let message_type = msg_cmd.r#type;

                // SEND 消息：处理聊天消息
                if message_type == 0 {
                    // SEND
                    let username = self
                        .usernames
                        .lock()
                        .await
                        .get(connection_id)
                        .cloned()
                        .unwrap_or_else(|| "匿名".to_string());

                    // 解析消息内容
                    let message_text = String::from_utf8_lossy(&msg_cmd.payload);

                    info!("💬 [{}]: {}", username, message_text);

                    // 广播消息给所有用户（排除发送者）
                    let broadcast_cmd = MessageCommand {
                        r#type: 0, // SEND
                        message_id: generate_message_id(),
                        payload: format!("[{}]: {}", username, message_text).into_bytes(),
                        metadata: std::collections::HashMap::new(),
                        seq: 0,
                    };

                    let broadcast_frame =
                        frame_with_message_command(broadcast_cmd, Reliability::AtLeastOnce);

                    // 使用 ServerHandle 广播消息（自动使用每个连接的协商格式）
                    if let Some(ref handle) = *self.server_handle.lock().await {
                        if let Err(e) = handle
                            .broadcast_except(&broadcast_frame, connection_id)
                            .await
                        {
                            error!("广播消息失败: {}", e);
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        info!("✅ 新连接: {}", connection_id);

        // 从连接管理器获取连接信息，使用认证后的用户ID
        let username = if let Some(ref manager) = *self.connection_manager.lock().await {
            match manager.get_connection(connection_id).await {
                Some((_, conn_info)) => {
                    // 优先使用认证后的用户ID
                    if let Some(ref user_id) = conn_info.user_id {
                        debug!("[AuthServer] 从连接信息获取用户ID: {}", user_id);

                        // 检查连接是否已验证
                        if !conn_info.authenticated {
                            warn!(
                                "[AuthServer] ⚠️  连接未验证: connection_id={}",
                                connection_id
                            );
                            return Ok(()); // 未验证的连接不处理
                        }

                        user_id.clone()
                    } else {
                        debug!("[AuthServer] 连接信息中没有用户ID，使用连接ID");
                        format!("用户_{}", &connection_id[..8.min(connection_id.len())])
                    }
                }
                None => {
                    error!("[AuthServer] 连接不存在: {}", connection_id);
                    format!("用户_{}", &connection_id[..8.min(connection_id.len())])
                }
            }
        } else {
            error!("[AuthServer] 连接管理器未设置");
            format!("用户_{}", &connection_id[..8.min(connection_id.len())])
        };

        self.usernames
            .lock()
            .await
            .insert(connection_id.to_string(), username.clone());

        info!("📝 用户ID: {} (连接ID: {})", username, connection_id);

        // 发送欢迎消息
        let welcome_cmd = MessageCommand {
            r#type: 0, // SEND
            message_id: generate_message_id(),
            payload: format!("欢迎 {} 加入聊天室！", username).into_bytes(),
            metadata: std::collections::HashMap::new(),
            seq: 0,
        };

        let welcome_frame = frame_with_message_command(welcome_cmd, Reliability::AtLeastOnce);

        // 使用 ServerHandle 发送消息（自动使用连接的协商格式）
        if let Some(ref handle) = *self.server_handle.lock().await {
            if let Err(e) = handle.send_to(connection_id, &welcome_frame).await {
                error!("发送欢迎消息失败: {}", e);
            }
        }

        Ok(())
    }

    async fn on_disconnect(&self, connection_id: &str) -> Result<()> {
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
