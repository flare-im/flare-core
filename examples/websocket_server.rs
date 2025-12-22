//! WebSocket 聊天室服务端
//!
//! 使用基础结构（HybridServer）直接构建服务端
//! 实现一个简单的聊天室，所有连接的用户都可以发送和接收消息
//!
//! 注意：此示例使用纯 WebSocket 连接（ws://），不使用 TLS/SSL
//!
//! 此示例展示了如何：
//! 1. 实现 ServerEventHandler trait 来处理消息
//! 2. 使用 HybridServer::with_connection_manager() 创建服务器
//! 3. 使用 DefaultServerHandle 进行消息发送和连接管理

use async_trait::async_trait;
use flare_core::common::config_types::TransportProtocol;
use flare_core::common::error::Result;
use flare_core::common::protocol::{
    Frame, MessageCommand, Reliability, frame_with_message_command, generate_message_id,
    send_message,
};
use flare_core::server::HybridServer;
use flare_core::server::handle::{DefaultServerHandle, ServerHandle};
use flare_core::server::{Server, ServerConfig, ServerEventHandler};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

// 聊天室连接处理器
struct ChatRoomHandler {
    // 存储连接ID到用户名的映射
    usernames: Arc<tokio::sync::Mutex<HashMap<String, String>>>,
    // 服务器操作处理器（轻量级，用于发送消息和连接管理）
    server_handle: Arc<tokio::sync::Mutex<Option<Arc<dyn ServerHandle>>>>,
}

impl ChatRoomHandler {
    fn new() -> Self {
        Self {
            usernames: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            server_handle: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    async fn set_server_handle(&self, handle: Arc<dyn ServerHandle>) {
        *self.server_handle.lock().await = Some(handle);
    }

    // 广播消息给所有连接的客户端（排除发送者）
    async fn broadcast_message_except(&self, frame: &Frame, exclude_connection_id: &str) {
        debug!(
            "broadcast_message_except 开始: exclude={}",
            exclude_connection_id
        );
        let handle = {
            let handle_guard = self.server_handle.lock().await;
            handle_guard.clone()
        };

        if let Some(ref handle) = handle {
            debug!("broadcast_message_except: 使用 broadcast_except 排除发送者");
            if let Err(e) = handle.broadcast_except(frame, exclude_connection_id).await {
                error!("[聊天室] 广播消息失败: {}", e);
            } else {
                debug!("broadcast_message_except: 广播成功（已排除发送者）");
            }
        } else {
            warn!("[聊天室] 警告：服务器处理器未设置，无法广播消息");
        }
        debug!("broadcast_message_except 完成");
    }
}

#[async_trait]
impl ServerEventHandler for ChatRoomHandler {
    async fn handle_message(
        &self,
        command: &MessageCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        // 提取消息内容
        let message_text = String::from_utf8_lossy(&command.payload);

        // 获取或创建用户名
        let username = {
            let mut usernames = self.usernames.lock().await;
            usernames
                .entry(connection_id.to_string())
                .or_insert_with(|| {
                    // 如果消息包含用户名信息，提取用户名
                    if let Some(username_bytes) = command.metadata.get("username") {
                        String::from_utf8_lossy(username_bytes).to_string()
                    } else {
                        format!("用户_{}", &connection_id[..8.min(connection_id.len())])
                    }
                })
                .clone()
        };

        info!("[聊天室] {} 说: {}", username, message_text);

        // 构建广播消息（包含用户名）
        let mut broadcast_metadata = HashMap::new();
        broadcast_metadata.insert("username".to_string(), username.as_bytes().to_vec());
        broadcast_metadata.insert(
            "connection_id".to_string(),
            connection_id.as_bytes().to_vec(),
        );

        let broadcast_msg = send_message(
            generate_message_id(),
            format!("[{}] {}", username, message_text).into_bytes(),
            Some(broadcast_metadata),
            None,
        );

        let broadcast_frame = frame_with_message_command(broadcast_msg, Reliability::BestEffort);

        // 广播给除发送者外的所有连接
        self.broadcast_message_except(&broadcast_frame, connection_id)
            .await;

        // 不返回给单个连接，因为已经广播了
        Ok(None)
    }

    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        debug!("on_connect 开始: connection_id={}", connection_id);
        info!(
            "[聊天室] ✅ 用户 {} 加入聊天室",
            &connection_id[..8.min(connection_id.len())]
        );

        debug!("on_connect 完成: connection_id={}", connection_id);
        Ok(())
    }

    async fn on_disconnect(&self, connection_id: &str, _reason: Option<&str>) -> Result<()> {
        let username = {
            let mut usernames = self.usernames.lock().await;
            usernames.remove(connection_id)
        };

        let display_name = username
            .as_deref()
            .unwrap_or(&connection_id[..8.min(connection_id.len())]);
        info!("[聊天室] ❌ {} 离开了聊天室", display_name);

        Ok(())
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // 初始化 tracing（默认使用 debug 级别，方便调试）
    // 可以通过环境变量 RUST_LOG 覆盖：RUST_LOG=info cargo run --example websocket_server
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug")),
        )
        .init();

    info!("=== WebSocket 聊天室服务端 ===");

    // 创建 event handler
    let event_handler = Arc::new(ChatRoomHandler::new());
    let handler_for_setup = Arc::clone(&event_handler);

    // 仅监听 WebSocket 协议（无 TLS）
    let ws_config = ServerConfig::new("0.0.0.0:8080".to_string())
        .with_protocols(vec![TransportProtocol::WebSocket])
        .with_max_connections(2000);

    let mut ws_server = HybridServer::with_connection_manager(
        ws_config,
        None,
        None,
        Some(event_handler.clone() as Arc<dyn ServerEventHandler>),
        None,
    )?;

    // 从 HybridServer 获取 ServerCore，创建 DefaultServerHandle
    let server_handle: Arc<dyn ServerHandle> = if let Some(core) = ws_server.core() {
        // 使用 ServerCore 创建 DefaultServerHandle
        Arc::new(DefaultServerHandle::new(core.connection_manager_trait()))
    } else {
        return Err("无法获取 ServerCore".into());
    };

    // 设置服务器处理器到 handler
    handler_for_setup.set_server_handle(server_handle).await;

    // 启动服务器
    if let Err(e) = ws_server.start().await {
        error!("❌ 服务器启动失败: {:?}", e);
        error!("提示: 可能端口 8080 已被占用，请先关闭占用该端口的进程");
        return Err(format!("服务器启动失败: {:?}", e).into());
    }

    // 验证服务器是否真的在运行
    if !ws_server.is_running() {
        error!("❌ 服务器启动后未处于运行状态");
        return Err("服务器未正常运行".into());
    }

    info!("✅ 聊天室服务器已启动：0.0.0.0:8080");
    info!("使用 ws:// 协议连接（非 wss://）");

    // 通过 ServerHandle 获取连接数
    let conn_count = {
        let handle_guard = handler_for_setup.server_handle.lock().await;
        if let Some(ref handle) = *handle_guard {
            handle.connection_count()
        } else {
            0
        }
    };
    info!("当前在线用户: {}", conn_count);
    info!("\n服务器运行中，按 Ctrl+C 停止...");

    // 定期打印连接数
    let server_handle_clone = Arc::clone(&handler_for_setup.server_handle);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            let handle_guard = server_handle_clone.lock().await;
            if let Some(ref handle) = *handle_guard {
                let conn_count = handle.connection_count();
                if conn_count > 0 {
                    info!("当前在线用户: {}", conn_count);
                }
            }
        }
    });

    tokio::signal::ctrl_c().await?;

    info!("\n正在停止服务器...");
    ws_server.stop().await?;

    info!("服务器已停止");
    Ok(())
}
