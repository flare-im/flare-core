//! Flare 聊天室服务器示例
//!
//! 演示如何使用 FlareServerBuilder 创建完整的聊天室服务器
//! 使用所有 flare-core 的能力：
//! - MessagePipeline（消息管道）
//! - Middleware（中间件：验证、日志、性能监控）
//! - Processor（处理器）
//! - 设备管理（设备冲突策略）
//! - 序列化协商（JSON/Protobuf）
//! - 压缩协商（Gzip/Zstd/None）
//! - 加密协商（AES-256-GCM/None）
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
use flare_core::common::message::{
    ArcMessageMiddleware, ArcMessageProcessor, FunctionProcessor, LogLevel, LoggingMiddleware,
    MessageContext, MetricsMiddleware, ValidationMiddleware,
};
use flare_core::common::protocol::flare::core::commands::command::Type as CommandType;
use flare_core::common::protocol::{
    Command, Frame, FrameBuilder, MessageCommand, Reliability, SerializationFormat,
    frame_with_message_command, generate_message_id,
};
use flare_core::common::*;
use flare_core::server::connection::{ConnectionManager, ConnectionManagerTrait};
use flare_core::server::handle::{DefaultServerHandle, ServerHandle};
use flare_core::server::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

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
    info!("   - 协商：默认 JSON + None 压缩（客户端可指定格式）");
    info!("   - 多协议：WebSocket + QUIC 双协议支持");
    info!("");

    // ============================================================
    // 1. 创建连接管理器（可选，用于共享连接状态）
    // ============================================================
    let connection_manager = Arc::new(ConnectionManager::new());

    // ============================================================
    // 2. 创建聊天室监听器
    // ============================================================
    let chat_listener = Arc::new(ChatRoomListener {
        usernames: Arc::new(Mutex::new(HashMap::new())),
        server_handle: Arc::new(Mutex::new(None)),
        connection_manager: Arc::new(Mutex::new(None)),
    });

    // ============================================================
    // 3. 创建中间件
    // ============================================================
    // 验证中间件（最高优先级，最先验证）
    let validation_middleware = Arc::new(ValidationMiddleware::new(
        "ChatValidation",
        |frame: &Frame| -> Result<()> {
            // 验证消息 ID 不为空
            if frame.message_id.is_empty() {
                return Err(FlareError::message_format_error(
                    "Message ID is empty".to_string(),
                ));
            }
            Ok(())
        },
    )) as ArcMessageMiddleware;

    // 日志中间件（高优先级，最先执行）
    let logging_middleware =
        Arc::new(LoggingMiddleware::new("ChatLogging").with_level(LogLevel::Info))
            as ArcMessageMiddleware;

    // 性能监控中间件
    let metrics_middleware =
        Arc::new(MetricsMiddleware::new("ChatMetrics")) as ArcMessageMiddleware;

    // ============================================================
    // 4. 创建自定义处理器（可选，用于特殊消息处理）
    // ============================================================
    // Echo 处理器：处理以 "echo: " 开头的消息
    // 注意：由于生命周期限制，需要克隆必要的值
    let echo_processor = Arc::new(FunctionProcessor::new(
        "EchoProcessor",
        |ctx: &MessageContext| {
            let frame = ctx.frame.clone();
            async move {
                if let Some(cmd) = &frame.command {
                    if let Some(CommandType::Message(msg_cmd)) = &cmd.r#type {
                        let message_text = String::from_utf8_lossy(&msg_cmd.payload);
                        if message_text.starts_with("echo: ") {
                            let echo_text = message_text[6..].to_string();
                            debug!("[EchoProcessor] 收到 Echo 消息: {}", echo_text);

                            // 创建 Echo 响应
                            let echo_cmd = MessageCommand {
                                r#type: 0, // SEND
                                message_id: generate_message_id(),
                                payload: format!("Echo: {}", echo_text).into_bytes(),
                                metadata: std::collections::HashMap::new(),
                                seq: 0,
                            };

                            let echo_frame =
                                frame_with_message_command(echo_cmd, Reliability::AtLeastOnce);

                            return Ok(Some(echo_frame));
                        }
                    }
                }
                Ok(None)
            }
        },
    )) as ArcMessageProcessor;

    // ============================================================
    // 5. 创建设备管理器（用于设备冲突管理）
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
    // 6. 使用 FlareServerBuilder 构建服务器（使用所有能力）
    // ============================================================
    let server = FlareServerBuilder::new("0.0.0.0:8080")
        // ============================================================
        // 必须：设置消息监听器
        // ============================================================
        .with_listener(chat_listener.clone())
        // ============================================================
        // 中间件（按添加顺序执行）
        // ============================================================
        .with_middleware(validation_middleware) // 1. 验证（最高优先级）
        .with_middleware(logging_middleware) // 2. 日志
        .with_middleware(metrics_middleware) // 3. 性能监控
        // ============================================================
        // 处理器（按添加顺序执行）
        // ============================================================
        .with_processor(echo_processor) // 1. Echo 处理器
        // 监听器处理器会自动添加（最后执行）
        // ============================================================
        // 连接和设备管理
        // ============================================================
        .with_connection_manager(connection_manager.clone())
        .with_device_manager(device_manager)
        // ============================================================
        // 协商配置（服务端默认格式和压缩）
        // ============================================================
        .with_default_format(SerializationFormat::Protobuf) // 默认 JSON（客户端可指定格式）
        .with_default_compression(CompressionAlgorithm::Gzip) // 默认不压缩
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
    // 7. 获取 ServerHandle 和 ConnectionManager 并设置到监听器
    // ============================================================
    let (server_handle, manager_trait) =
        if let Some(manager_trait) = server.get_server_handle_components() {
            let handle: Arc<dyn ServerHandle> =
                Arc::new(DefaultServerHandle::new(manager_trait.clone()));
            (handle, manager_trait)
        } else {
            return Err("无法获取连接管理器".into());
        };
    chat_listener.set_server_handle(server_handle).await;
    chat_listener.set_connection_manager(manager_trait).await;

    // ============================================================
    // 8. 启动服务器
    // ============================================================
    server.start().await?;

    info!("✅ 服务器已启动");
    info!("   WebSocket: ws://127.0.0.1:8080");
    info!("   QUIC: quic://127.0.0.1:8081");
    info!("");
    info!("📋 功能说明：");
    info!("   - 消息管道：自动处理序列化、压缩、加密、验证、日志、性能监控");
    info!("   - 中间件链：验证 → 日志 → 性能监控");
    info!("   - 处理器链：Echo处理器（处理 echo: 前缀消息）→ 聊天室监听器");
    info!("   - 设备管理：平台互斥策略（同一用户同一平台只能有一个设备在线）");
    info!("   - 协商机制：默认 JSON + None 压缩（客户端可指定格式）");
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
    info!("");
    info!("服务器运行中，按 Ctrl+C 停止...");

    tokio::signal::ctrl_c().await?;
    info!("\n正在停止服务器...");
    server.stop().await?;
    info!("服务器已停止");

    Ok(())
}

/// 聊天室监听器
struct ChatRoomListener {
    usernames: Arc<Mutex<HashMap<String, String>>>, // connection_id -> username
    server_handle: Arc<Mutex<Option<Arc<dyn ServerHandle>>>>,
    connection_manager: Arc<Mutex<Option<Arc<dyn ConnectionManagerTrait>>>>, // 用于获取连接信息
}

impl ChatRoomListener {
    async fn set_server_handle(&self, handle: Arc<dyn ServerHandle>) {
        *self.server_handle.lock().await = Some(handle);
    }

    async fn set_connection_manager(&self, manager: Arc<dyn ConnectionManagerTrait>) {
        *self.connection_manager.lock().await = Some(manager);
    }
}

#[async_trait]
impl MessageListener for ChatRoomListener {
    async fn on_message(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
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

                    // 使用 ServerHandle 广播消息
                    if let Some(ref handle) = *self.server_handle.lock().await {
                        if let Err(e) = handle
                            .broadcast_except(&broadcast_frame, connection_id)
                            .await
                        {
                            error!("广播消息失败: {}", e);
                        }
                    }

                    // 返回 ACK 响应（解决客户端 Wait 模式下的超时问题）
                    // 使用相同的 message_id，以便客户端匹配响应
                    let request_message_id = frame.message_id.clone();
                    
                    debug!(
                        "[ChatRoomListener] 准备返回 ACK: 请求 frame.message_id={}, 请求 MessageCommand.message_id={:?}",
                        request_message_id,
                        frame.command.as_ref()
                            .and_then(|c| c.r#type.as_ref())
                            .and_then(|t| match t {
                                CommandType::Message(msg_cmd) => Some(&msg_cmd.message_id),
                                _ => None,
                            })
                    );
                    
                    let ack_cmd = MessageCommand {
                        r#type: 0, // SEND (复用 SEND 类型作为 ACK)
                        message_id: request_message_id.clone(), // 使用请求的 frame.message_id
                        payload: "ACK".as_bytes().to_vec(),
                        metadata: std::collections::HashMap::new(),
                        seq: 0,
                    };

                    let ack_frame = FrameBuilder::new()
                        .with_command(Command {
                            r#type: Some(CommandType::Message(ack_cmd.clone())),
                        })
                        .with_reliability(Reliability::BestEffort)
                        .with_message_id(request_message_id.clone()) // 关键：复用请求的 message_id 作为响应 Frame ID
                        .build();

                    debug!(
                        "[ChatRoomListener] ACK Frame 已创建: frame.message_id={}, MessageCommand.message_id={}",
                        ack_frame.message_id,
                        ack_cmd.message_id
                    );

                    return Ok(Some(ack_frame));
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
        let welcome_cmd = MessageCommand {
            r#type: 0, // SEND
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
