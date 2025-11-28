//! 协商和设备管理服务器示例
//! 
//! 演示如何使用序列化协商和设备管理功能
//! 
//! ## 协商机制说明
//! 
//! 1. **服务端默认配置**：
//!    - 默认序列化格式：Protobuf（可通过 `with_default_format` 配置）
//!    - 默认压缩方式：None（可通过 `with_default_compression` 配置）
//! 
//! 2. **协商逻辑**：
//!    - 如果客户端**强制指定**格式（`force_format`），服务端必须使用客户端指定的格式
//!    - 如果客户端**未强制指定**，服务端使用自己的默认配置（Protobuf）
//!    - 客户端发送 CONNECT 消息时，会包含：
//!      - `format`: 客户端首选的序列化格式
//!      - `compression`: 客户端首选的压缩算法
//!      - `force_format`: "true" 表示强制模式，服务端必须使用客户端格式
//! 
//! 3. **设备管理**：
//!    - 支持多端设备管理，配置设备冲突策略
//!    - 移动端互斥：同一用户只能有一个移动端设备在线
//! 
//! ## 启动命令
//! 
//! ```bash
//! RUST_LOG=debug cargo run --example negotiation_server
//! ```
//! 
//! ## 配置说明
//! 
//! - `with_default_format()`: 设置服务端默认序列化格式（默认 Protobuf）
//! - `with_default_compression()`: 设置服务端默认压缩算法（默认 None）
//! - `with_device_manager()`: 设置设备管理器，用于设备冲突管理
//! - `with_protocols()`: 设置支持的协议列表（WebSocket、QUIC）
//! - `with_protocol_address()`: 为每个协议设置独立的监听地址

use flare_core::server::*;
use flare_core::common::*;
use flare_core::common::device::DeviceConflictStrategyBuilder;
use flare_core::server::device::DeviceManager;
use flare_core::common::protocol::{Frame, MessageCommand, NotificationCommand, frame_with_message_command, generate_message_id, Reliability, SerializationFormat};
use flare_core::common::protocol::flare::core::commands::command::Type as CommandType;
use flare_core::common::compression::CompressionAlgorithm;
use flare_core::common::config_types::TransportProtocol;
use flare_core::server::connection::{ConnectionManager, ConnectionManagerTrait};
use flare_core::server::handle::{ServerHandle, DefaultServerHandle};
use flare_core::server::events::handler::ServerEventHandler;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::HashMap;
use tracing::{info, error, debug};
use async_trait::async_trait;

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    info!("🚀 启动协商和设备管理聊天室服务器");
    info!("");
    info!("📋 服务器配置说明：");
    info!("   - 默认序列化格式: JSON（可通过 with_default_format 修改）");
    info!("   - 默认压缩方式: None（可通过 with_default_compression 修改）");
    info!("   - 协商规则: 客户端可以指定格式，也可以不指定（使用服务端默认JSON）");
    info!("");

    // ============================================================
    // 1. 创建设备管理器（用于设备冲突管理）
    // ============================================================
    let device_manager = Arc::new(DeviceManager::new(
        DeviceConflictStrategyBuilder::new()
            .platform_exclusive() // 平台互斥：同一用户同一平台只能有一个设备在线
            // 例如：同一用户的 Android 设备之间互斥，但 Android 和 iOS 可以同时在线
            // 其他策略选项：
            // .mobile_exclusive()     // 移动端互斥：同一用户只能有一个移动端设备在线（所有移动端互斥）
            // .mobile_and_pc_coexist() // 移动端和PC端共存：移动端之间互斥，PC端之间互斥，但移动端和PC端可以同时在线
            //                          // 例如：同一用户可以有 1 个移动端 + 1 个 PC 同时在线
            // .fully_exclusive()      // 完全互斥：同一用户只能有一个设备在线（所有平台互斥）
            // .allow_all()            // 允许所有设备同时在线（无限制）
            .build()
    ));

    // ============================================================
    // 2. 创建连接管理器（可选，用于共享连接状态）
    // ============================================================
    let connection_manager = Arc::new(ConnectionManager::new());

    // ============================================================
    // 3. 创建聊天室处理器
    // ============================================================
    let handler = Arc::new(NegotiationChatRoomHandler {
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
        
        // 设置设备管理器（用于设备冲突管理）
        .with_device_manager(device_manager.clone())
        
        // 设置事件处理器（用于打印收到的消息）
        .with_event_handler(event_handler)
        
        // ============================================================
        // 协商配置：设置服务端默认格式和压缩方式
        // ============================================================
        // 默认序列化格式：JSON（如果不设置，默认就是 JSON）
        // 如果需要使用 Protobuf，可以改为：
        // .with_default_format(SerializationFormat::Protobuf)
        .with_default_format(SerializationFormat::Json)
        
        // 默认压缩方式：None（如果不设置，默认就是 None）
        // 可以改为 Gzip 或其他压缩算法：
        // .with_default_compression(CompressionAlgorithm::Gzip)
        .with_default_compression(CompressionAlgorithm::None)
        
        // ============================================================
        // 协议配置：支持多协议监听
        // ============================================================
        .with_protocols(vec![TransportProtocol::WebSocket, TransportProtocol::QUIC])
        .with_protocol_address(TransportProtocol::WebSocket, "0.0.0.0:8080".to_string())
        .with_protocol_address(TransportProtocol::QUIC, "0.0.0.0:8081".to_string())
        
        // ============================================================
        // 其他配置
        // ============================================================
        .with_max_connections(2000)
        // .with_heartbeat(HeartbeatConfig::default()) // 心跳配置
        // .with_tls(TlsConfig::none()) // TLS 配置
        
        .build()?;

    // ============================================================
    // 6. 获取 ServerHandle 和 ConnectionManager 并设置到处理器
    // ============================================================
    let (server_handle, manager_trait) = if let Some(manager_trait) = server.get_server_handle_components() {
        let handle: Arc<dyn ServerHandle> = Arc::new(DefaultServerHandle::new(manager_trait.clone()));
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
    info!("📋 协商机制说明：");
    info!("   1. 服务端默认使用 JSON + None 压缩");
    info!("   2. 客户端可以指定序列化格式，也可以不指定（使用服务端默认JSON）");
    info!("   3. 客户端强制模式时（force_format=true），服务端必须使用客户端格式");
    info!("   4. 客户端非强制模式时，优先使用客户端指定格式，否则使用服务端默认JSON");
    info!("");
    info!("📱 设备管理说明：");
    info!("   - 当前策略：平台互斥（PlatformExclusive）");
    info!("   - 规则：同一用户同一平台只能有一个设备在线");
    info!("   - 例如：同一用户的 Android 设备之间互斥，但 Android 和 iOS 可以同时在线");
    info!("   - 支持多平台同时在线：Web + PC + Android + iOS（每个平台各一个）");
    info!("   - 设备信息包含：device_id, platform, model, app_version 等");
    info!("   - 新设备登录时，同一平台的其他设备会被自动踢掉");
    info!("   - 其他可用策略：");
    info!("     * mobile_exclusive(): 移动端互斥（所有移动端互斥，但可与PC共存）");
    info!("     * mobile_and_pc_coexist(): 移动端和PC端共存（移动端之间互斥，PC端之间互斥）");
    info!("     * fully_exclusive(): 完全互斥（所有平台互斥）");
    info!("     * allow_all(): 允许所有设备同时在线");
    info!("");
    info!("💡 客户端连接示例：");
    info!("   - 不指定格式：客户端不指定 format，不设置 force_format");
    info!("     结果：服务端使用默认 JSON");
    info!("     日志：查看 [ServerCore] 📥 收到 CONNECT 消息 和 [ServerCore] ✅ 协商完成");
    info!("   - 指定格式（非强制）：客户端指定 format=Protobuf，不设置 force_format");
    info!("     结果：服务端使用客户端指定的 Protobuf（优先使用客户端格式）");
    info!("     日志：查看 [ServerCore] 📥 收到 CONNECT 消息 和 [ServerCore] ✅ 协商完成");
    info!("   - 强制模式：客户端指定 format=Json，设置 force_format=true");
    info!("     结果：服务端必须使用 JSON（因为客户端强制）");
    info!("     日志：查看 [ServerCore] 📥 收到 CONNECT 消息 和 [ServerCore] ✅ 协商完成");
    info!("");
    info!("📊 协商日志说明：");
    info!("   - [ServerCore] 📥 收到 CONNECT 消息：显示客户端连接请求");
    info!("   - [ServerCore] ✅ 协商完成：显示最终确定的序列化格式和压缩方式");
    info!("   - [ClientCore] ✅ 收到 CONNECT_ACK：显示客户端收到的协商结果");
    info!("   - [ClientCore] ✅ 解析器已更新：显示客户端解析器已更新为协商后的格式");
    info!("");
    info!("服务器运行中，按 Ctrl+C 停止...");

    tokio::signal::ctrl_c().await?;
    info!("\n正在停止服务器...");
    server.stop().await?;
    info!("服务器已停止");

    Ok(())
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
        // 打印消息信息
        let message_text = String::from_utf8_lossy(&command.payload);
        debug!(
            "[EventHandler] 📨 收到消息命令: connection_id={}, message_type={}, message_id={}, payload_len={}, content={:?}",
            connection_id,
            command.r#type,
            command.message_id,
            command.payload.len(),
            message_text
        );
        
        // 打印元数据（如果有）
        if !command.metadata.is_empty() {
            debug!(
                "[EventHandler] 📋 消息元数据: {:?}",
                command.metadata
            );
        }
        
        // 返回 None，让默认处理继续（转发给 ConnectionHandler）
        Ok(None)
    }
    
    /// 处理通知命令：打印收到的通知
    async fn handle_notification_command(
        &self,
        command: &NotificationCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        // 打印通知信息
        let notification_content = String::from_utf8_lossy(&command.content);
        debug!(
            "[EventHandler] 🔔 收到通知命令: connection_id={}, notification_type={}, title={}, content_len={}, content={:?}",
            connection_id,
            command.r#type,
            command.title,
            command.content.len(),
            notification_content
        );
        
        // 打印元数据（如果有）
        if !command.metadata.is_empty() {
            debug!(
                "[EventHandler] 📋 通知元数据: {:?}",
                command.metadata
            );
        }
        
        // 返回 None，让默认处理继续（转发给 ConnectionHandler）
        Ok(None)
    }
    
    /// 处理 CONNECT 系统命令：打印连接信息
    async fn handle_connect(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        debug!(
            "[EventHandler] 🔌 收到 CONNECT 命令: connection_id={}",
            connection_id
        );
        // 返回 None，让默认处理继续（协商、设备管理等）
        Ok(None)
    }
    
    /// 处理 PING 系统命令：打印心跳信息
    async fn handle_ping(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        debug!(
            "[EventHandler] 💓 收到 PING: connection_id={}",
            connection_id
        );
        // 返回 None，让默认处理继续（自动回复 PONG）
        Ok(None)
    }
    
    /// 处理 PONG 系统命令：打印心跳响应
    async fn handle_pong(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        debug!(
            "[EventHandler] 💓 收到 PONG: connection_id={}",
            connection_id
        );
        // 返回 None，让默认处理继续（更新连接活跃时间）
        Ok(None)
    }
    
    /// 处理连接断开事件：打印断开信息
    async fn on_disconnect(&self, connection_id: &str, reason: Option<&str>) -> Result<()> {
        if let Some(reason) = reason {
            debug!(
                "[EventHandler] 🔌 连接断开: connection_id={}, reason={}",
                connection_id,
                reason
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
            connection_id,
            error
        );
        Ok(())
    }
}

/// 协商聊天室处理器
struct NegotiationChatRoomHandler {
    usernames: Arc<Mutex<HashMap<String, String>>>, // connection_id -> username
    server_handle: Arc<Mutex<Option<Arc<dyn ServerHandle>>>>,
    connection_manager: Arc<Mutex<Option<Arc<dyn ConnectionManagerTrait>>>>, // 用于获取连接信息
}

impl NegotiationChatRoomHandler {
    async fn set_server_handle(&self, handle: Arc<dyn ServerHandle>) {
        *self.server_handle.lock().await = Some(handle);
    }
    
    async fn set_connection_manager(&self, manager: Arc<dyn ConnectionManagerTrait>) {
        *self.connection_manager.lock().await = Some(manager);
    }
}

#[async_trait::async_trait]
impl ConnectionHandler for NegotiationChatRoomHandler {
    async fn handle_frame(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        // 处理消息命令
        if let Some(cmd) = &frame.command {
            if let Some(CommandType::Message(msg_cmd)) = &cmd.r#type {
                let message_type = msg_cmd.r#type;
                
                // SEND 消息：处理聊天消息
                if message_type == 0 { // SEND
                    let username = self.usernames.lock().await
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
                    
                    let broadcast_frame = frame_with_message_command(
                        broadcast_cmd,
                        Reliability::AtLeastOnce,
                    );
                    
                    // 使用 ServerHandle 广播消息（自动使用每个连接的协商格式）
                    if let Some(ref handle) = *self.server_handle.lock().await {
                        if let Err(e) = handle.broadcast_except(&broadcast_frame, connection_id).await {
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
        
        // 从连接管理器获取连接信息，使用客户端提供的用户ID
        // 注意：此时协商已完成，user_id 应该已经更新到连接信息中
        let username = if let Some(ref manager) = *self.connection_manager.lock().await {
            match manager.get_connection(connection_id).await {
                Some((_, conn_info)) => {
                    // 优先使用客户端提供的用户ID
                    if let Some(ref user_id) = conn_info.user_id {
                        debug!("[NegotiationServer] 从连接信息获取用户ID: {}", user_id);
                        user_id.clone()
                    } else {
                        // 如果没有用户ID，使用连接ID的前8位
                        debug!("[NegotiationServer] 连接信息中没有用户ID，使用连接ID");
                        format!("用户_{}", &connection_id[..8.min(connection_id.len())])
                    }
                }
                None => {
                    // 连接不存在，使用连接ID的前8位作为默认值
                    error!("[NegotiationServer] 连接不存在: {}", connection_id);
                    format!("用户_{}", &connection_id[..8.min(connection_id.len())])
                }
            }
        } else {
            // 连接管理器未设置，使用连接ID的前8位作为默认值
            error!("[NegotiationServer] 连接管理器未设置");
            format!("用户_{}", &connection_id[..8.min(connection_id.len())])
        };
        
        self.usernames.lock().await.insert(connection_id.to_string(), username.clone());
        
        info!("📝 用户ID: {} (连接ID: {})", username, connection_id);
        
        // 发送欢迎消息
        let welcome_cmd = MessageCommand {
            r#type: 0, // SEND
            message_id: generate_message_id(),
            payload: format!("欢迎 {} 加入聊天室！", username).into_bytes(),
            metadata: std::collections::HashMap::new(),
            seq: 0,
        };
        
        let welcome_frame = frame_with_message_command(
            welcome_cmd,
            Reliability::AtLeastOnce,
        );
        
        // 使用 ServerHandle 发送消息（自动使用连接的协商格式）
        if let Some(ref handle) = *self.server_handle.lock().await {
            if let Err(e) = handle.send_to(connection_id, &welcome_frame).await {
                error!("发送欢迎消息失败: {}", e);
            }
        }
        
        Ok(())
    }

    async fn on_disconnect(&self, connection_id: &str) -> Result<()> {
        let username = self.usernames.lock().await
            .remove(connection_id)
            .unwrap_or_else(|| "未知用户".to_string());
        
        info!("❌ 用户断开: {} ({})", username, connection_id);
        
        Ok(())
    }
}
