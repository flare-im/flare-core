//! 观察者模式聊天室服务端示例
//!
//! 使用观察者模式的 Builder（ObserverServerBuilder）构建服务端
//! 实现一个功能完整的聊天室，所有连接的用户都可以发送和接收消息
//!
//! 功能特性：
//! - 多协议支持：同时监听 WebSocket 和 QUIC 协议
//! - 共享连接管理：使用 ConnectionManager 统一管理所有连接
//! - 设备管理：支持设备冲突处理策略
//! - 心跳检测：自动检测并清理超时连接
//! - 消息广播：支持单播、组播和广播
//!
//! 此示例展示了如何：
//! 1. 实现 ServerEventHandler trait 来处理消息和连接事件
//! 2. 使用 ObserverServerBuilder 创建服务器（支持多协议）
//! 3. 使用共享的 ConnectionManager 管理连接状态
//! 4. 完整的聊天室功能实现

use async_trait::async_trait;
use flare_core::common::config_types::TransportProtocol;
use flare_core::common::error::Result;
use flare_core::common::protocol::{
    Frame, MessageCommand, Reliability, SerializationFormat, frame_with_message_command,
    generate_message_id,
};
use flare_core::server::ObserverServerBuilder;
use flare_core::server::connection::{ConnectionManager, ConnectionManagerTrait};
use flare_core::server::events::handler::ServerEventHandler;
use flare_core::server::handle::{DefaultServerHandle, ServerHandle};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

/// 聊天室事件处理器（实现 ServerEventHandler）
struct ChatRoomHandler {
    usernames: Arc<Mutex<HashMap<String, String>>>, // connection_id -> username
    server_handle: Arc<Mutex<Option<Arc<dyn ServerHandle>>>>,
    #[allow(dead_code)]
    connection_manager: Arc<Mutex<Option<Arc<dyn ConnectionManagerTrait>>>>,
}

impl ChatRoomHandler {
    fn new() -> Self {
        Self {
            usernames: Arc::new(Mutex::new(HashMap::new())),
            server_handle: Arc::new(tokio::sync::Mutex::new(None)),
            connection_manager: Arc::new(Mutex::new(None)),
        }
    }

    async fn set_server_handle(&self, handle: Arc<dyn ServerHandle>) {
        *self.server_handle.lock().await = Some(handle);
    }

    #[allow(dead_code)]
    async fn set_connection_manager(&self, manager: Arc<dyn ConnectionManagerTrait>) {
        *self.connection_manager.lock().await = Some(manager);
    }

    // 广播消息给所有连接的客户端（排除发送者）
    async fn broadcast_message_except(&self, frame: &Frame, exclude_connection_id: &str) {
        let handle_guard = self.server_handle.lock().await;
        if let Some(ref handle) = *handle_guard {
            if let Err(e) = handle.broadcast_except(frame, exclude_connection_id).await {
                error!("广播消息失败: {}", e);
            }
        }
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
        let message_text = match String::from_utf8(command.payload.clone()) {
            Ok(text) => text,
            Err(_) => {
                // 如果不是有效的UTF-8，则显示十六进制调试信息
                format!("<protobuf_binary_data: {} bytes>", command.payload.len())
            }
        };

        // 获取或创建用户名
        let username = {
            let mut usernames = self.usernames.lock().await;
            usernames
                .entry(connection_id.to_string())
                .or_insert_with(|| {
                    // 如果消息包含用户名信息，提取用户名
                    if let Some(username_bytes) = command.metadata.get("username") {
                        match String::from_utf8(username_bytes.clone()) {
                            Ok(username) => username,
                            Err(_) => {
                                // 如果不是有效的UTF-8，则显示十六进制调试信息
                                format!("<invalid_username_{}>", hex::encode(username_bytes))
                            }
                        }
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

        let broadcast_msg = flare_core::common::protocol::send_message(
            generate_message_id(),
            format!("[{}] {}", username, message_text).into_bytes(),
            Some(broadcast_metadata),
            None,
        );

        let broadcast_frame = frame_with_message_command(broadcast_msg, Reliability::BestEffort);

        // 广播给除发送者外的所有连接
        self.broadcast_message_except(&broadcast_frame, connection_id)
            .await;

        // 返回 None 表示不需要发送响应（框架会自动发送 ACK）
        Ok(None)
    }

    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        info!("[聊天室] ✅ 用户 {} 加入聊天室", connection_id);

        // 初始化用户名（使用默认名称）
        {
            let mut usernames = self.usernames.lock().await;
            usernames
                .entry(connection_id.to_string())
                .or_insert_with(|| {
                    format!("用户_{}", &connection_id[..8.min(connection_id.len())])
                });
        }

        Ok(())
    }

    async fn on_disconnect(&self, connection_id: &str, reason: Option<&str>) -> Result<()> {
        let username = {
            let mut usernames = self.usernames.lock().await;
            usernames.remove(connection_id)
        };

        let display_name = username
            .as_deref()
            .unwrap_or(&connection_id[..8.min(connection_id.len())]);
        info!("[聊天室] ❌ {} 离开了聊天室", display_name);

        if let Some(reason) = reason {
            debug!("断开原因: {}", reason);
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("=== 观察者模式聊天室服务端示例 ===");
    info!("使用 ObserverServerBuilder 构建，支持多协议和共享连接管理");

    // 创建一个共享的 ConnectionManager
    let connection_manager = Arc::new(ConnectionManager::new());

    // 创建 handler
    let handler = Arc::new(ChatRoomHandler::new());
    let handler_for_setup = Arc::clone(&handler);

    // 使用 ObserverServerBuilder 创建服务器
    // 展示观察者模式的特点：多协议支持、共享连接管理
    let mut server = ObserverServerBuilder::new("0.0.0.0:8080", handler.clone())
        .with_connection_manager(connection_manager)
        // 多协议支持：同时监听 WebSocket 和 QUIC
        .with_protocols(vec![TransportProtocol::WebSocket, TransportProtocol::QUIC])
        .with_protocol_address(TransportProtocol::WebSocket, "0.0.0.0:8080".to_string())
        .with_protocol_address(TransportProtocol::QUIC, "0.0.0.0:8081".to_string())
        // 协商配置
        .with_default_format(SerializationFormat::Json)
        .with_default_compression(flare_core::common::compression::CompressionAlgorithm::None)
        // 其他配置
        .with_max_connections(2000)
        .build()?;

    // 从 ObserverServer 获取连接管理器，创建 DefaultServerHandle
    let server_handle: Arc<dyn ServerHandle> =
        if let Some(manager_trait) = server.get_server_handle_components() {
            Arc::new(DefaultServerHandle::new(manager_trait))
        } else {
            return Err(flare_core::common::error::FlareError::protocol_error(
                "无法获取连接管理器".to_string(),
            ));
        };

    // 设置服务器处理器到 handler
    handler_for_setup.set_server_handle(server_handle).await;

    // 启动服务器
    server.start().await?;

    info!("✅ 聊天室服务器已启动");
    info!("   - WebSocket: ws://0.0.0.0:8080");
    info!("   - QUIC: quic://0.0.0.0:8081");

    let protocols = server.protocols();
    info!("支持的协议: {:?}", protocols);

    // 获取连接数
    let conn_count = server.connection_count();
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
    server.stop().await?;

    info!("服务器已停止");
    Ok(())
}
