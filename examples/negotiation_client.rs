//! 协商和设备管理客户端示例
//!
//! 演示客户端如何发送协商信息（序列化格式、压缩算法、设备信息）
//!
//! ## 协商模式说明
//!
//! 1. **协商模式（默认）**：
//!    - 客户端通过 `with_format()` 和 `with_compression()` 指定首选格式
//!    - 客户端发送 CONNECT 消息时，包含 `format` 和 `compression` 元数据
//!    - **服务端决定最终格式**：如果客户端未强制，服务端使用自己的默认配置（Protobuf）
//!    - 客户端收到 CONNECT_ACK 后，更新解析器为服务端确定的格式
//!
//! 2. **强制模式**：
//!    - 客户端通过 `force_format()` 和 `force_compression()` 强制指定格式
//!    - 客户端发送 CONNECT 消息时，包含 `force_format: "true"` 元数据
//!    - **服务端必须使用客户端格式**：即使服务端默认是 Protobuf，也必须使用客户端指定的格式
//!    - 适用于某些平台不支持 Protobuf 的场景
//!
//! ## 启动命令
//!
//! ```bash
//! # 使用默认平台（PC），交互式输入用户ID
//! RUST_LOG=debug cargo run --example negotiation_client
//!
//! # 指定平台（例如 Android），交互式输入用户ID
//! DEVICE_PLATFORM=android RUST_LOG=debug cargo run --example negotiation_client
//!
//! # 通过命令行参数指定用户ID
//! RUST_LOG=debug cargo run --example negotiation_client -- user123
//!
//! # 通过环境变量指定用户ID和平台
//! USER_ID=user123 DEVICE_PLATFORM=android RUST_LOG=debug cargo run --example negotiation_client
//!
//! # 其他平台选项：web, pc, h5, android, ios, harmonyos
//! ```
//!
//! ## 测试多设备互斥
//!
//! 1. **测试同一用户同一平台互斥**：
//!    - 启动第一个客户端：`cargo run --example negotiation_client -- user1`
//!    - 启动第二个客户端（相同用户ID + 相同平台）：`cargo run --example negotiation_client -- user1`
//!    - 预期：第二个客户端登录后，第一个客户端会被踢掉
//!
//! 2. **测试同一用户不同平台共存**：
//!    - 启动第一个客户端：`DEVICE_PLATFORM=pc cargo run --example negotiation_client -- user1`
//!    - 启动第二个客户端（相同用户ID + 不同平台）：`DEVICE_PLATFORM=android cargo run --example negotiation_client -- user1`
//!    - 预期：两个客户端可以同时在线
//!
//! 3. **测试不同用户互不影响**：
//!    - 启动第一个客户端：`cargo run --example negotiation_client -- user1`
//!    - 启动第二个客户端（不同用户ID）：`cargo run --example negotiation_client -- user2`
//!    - 预期：两个客户端可以同时在线
//!
//! ## 配置说明
//!
//! - `with_format()`: 设置客户端首选的序列化格式（用于协商）
//! - `with_compression()`: 设置客户端首选的压缩算法（用于协商）
//! - `force_format()`: 强制指定序列化格式（不协商，服务端必须使用）
//! - `force_compression()`: 强制指定压缩算法（不协商，服务端必须使用）
//! - `with_device_info()`: 设置设备信息（设备ID、平台、型号等）
//! - `with_user_id()`: 设置用户ID（用于设备管理和认证）
//! - `with_protocol_race()`: 设置协议竞速（多个协议同时尝试连接）

use async_trait::async_trait;
use flare_core::client::{ClientEventHandler, ObserverClientBuilder};
use flare_core::common::config_types::{HeartbeatConfig, TransportProtocol};
use flare_core::common::device::{DeviceInfo, DevicePlatform};
use flare_core::common::error::Result;
use flare_core::common::protocol::flare::core::commands::{
    command::Type, message_command::Type as MsgType, notification_command::Type as NotifType,
    system_command::Type as SysType,
};
use flare_core::common::protocol::{
    Frame, Reliability, frame_with_message_command, generate_message_id, send_message,
};
use flare_core::transport::events::{ConnectionEvent, ConnectionObserver};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{debug, error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    info!("🚀 启动协商和设备管理客户端");
    info!("");
    info!("📋 协商模式说明：");
    info!("   1. 不指定格式：客户端不指定格式，使用服务端默认JSON");
    info!("   2. 指定格式（非强制）：客户端指定格式，服务端优先使用客户端格式");
    info!("   3. 强制模式：客户端强制指定格式，服务端必须使用客户端格式");
    info!("");

    // ============================================================
    // 1. 配置心跳
    // ============================================================
    let heartbeat_config = HeartbeatConfig::default()
        .with_interval(std::time::Duration::from_secs(30))
        .with_timeout(std::time::Duration::from_secs(90));

    // ============================================================
    // 2. 创建设备信息
    // ============================================================
    // 平台互斥策略说明：
    // - 同一用户同一平台只能有一个设备在线
    // - 例如：如果已经有一个 Android 设备在线，新的 Android 设备登录会踢掉旧的
    // - 但 Android 和 iOS 可以同时在线（因为平台不同）

    // 可以通过命令行参数或环境变量指定平台，这里使用默认 PC
    // 例如：DEVICE_PLATFORM=android cargo run --example negotiation_client
    let platform = std::env::var("DEVICE_PLATFORM")
        .ok()
        .map(|p| DevicePlatform::from_str(&p))
        .unwrap_or(DevicePlatform::PC); // 默认使用 PC

    let device_info = DeviceInfo::new(
        format!("client-device-{}-{}", platform.as_str(), std::process::id()), // 使用平台+进程ID作为设备ID
        platform.clone(),                                                      // 平台类型
    )
    // model 使用平台名称作为标识（通过 as_str() 获取）
    .with_model(platform.as_str().to_string())
    .with_app_version("1.0.0".to_string())
    // system_version 仅用于记录，不作为平台判定标准
    .with_system_version(match &platform {
        DevicePlatform::PC => "macOS 14.0".to_string(),
        DevicePlatform::Android => "Android 14".to_string(),
        DevicePlatform::IOS => "iOS 17".to_string(),
        DevicePlatform::Web => "Chrome 120".to_string(),
        DevicePlatform::H5 => "Mobile Browser".to_string(),
        DevicePlatform::HarmonyOS => "HarmonyOS 4.0".to_string(),
        DevicePlatform::Other(_) => "Unknown".to_string(),
    });

    info!("📱 设备信息：");
    info!(
        "   平台: {:?} ({})",
        device_info.platform,
        device_info.platform.as_str()
    );
    info!("   设备ID: {}", device_info.device_id);
    if let Some(ref model) = device_info.model {
        info!("   型号: {} (基于平台名称)", model);
    }
    if let Some(ref version) = device_info.app_version {
        info!("   应用版本: {}", version);
    }
    if let Some(ref sys_version) = device_info.system_version {
        info!("   系统版本: {} (仅记录，不作为平台判定标准)", sys_version);
    }
    info!("");

    // ============================================================
    // 2.5. 获取用户ID（用于测试多设备互斥）
    // ============================================================
    // 方式1：从命令行参数读取（如果提供）
    // 方式2：从环境变量读取（如果提供）
    // 方式3：从stdin读取（交互式输入）
    let user_id = if let Some(arg_user_id) = std::env::args().nth(1) {
        // 从命令行参数读取
        info!("📝 使用命令行参数指定的用户ID: {}", arg_user_id);
        arg_user_id
    } else if let Ok(env_user_id) = std::env::var("USER_ID") {
        // 从环境变量读取
        info!("📝 使用环境变量 USER_ID: {}", env_user_id);
        env_user_id
    } else {
        // 从stdin读取（交互式输入）
        info!("📝 请输入用户ID（用于测试多设备互斥，直接回车使用默认值）:");
        info!("   - 相同用户ID + 相同平台 = 新设备会踢掉旧设备");
        info!("   - 相同用户ID + 不同平台 = 可以同时在线");
        info!("   - 不同用户ID = 互不影响");
        print!("用户ID (默认: user-{}): ", std::process::id());
        use std::io::Write;
        std::io::stdout().flush().unwrap();

        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);
        let mut input_line = String::new();

        match reader.read_line(&mut input_line).await {
            Ok(_) => {
                let trimmed = input_line.trim();
                if trimmed.is_empty() {
                    format!("user-{}", std::process::id())
                } else {
                    trimmed.to_string()
                }
            }
            Err(e) => {
                error!("读取用户输入失败: {}, 使用默认用户ID", e);
                format!("user-{}", std::process::id())
            }
        }
    };

    info!("✅ 用户ID: {}", user_id);
    info!("💡 提示：使用相同用户ID + 相同平台登录第二个客户端，会看到设备互斥效果");
    info!("");

    // ============================================================
    // 3. 创建观察者（用于接收消息和连接事件）
    // ============================================================
    let observer = Arc::new(NegotiationChatObserver {
        message_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        negotiated_format: Arc::new(std::sync::atomic::AtomicU8::new(0)), // 0=Unknown, 1=JSON, 2=Protobuf
        negotiated_compression: Arc::new(std::sync::atomic::AtomicU8::new(0)), // 0=None, 1=Gzip
    });

    // ============================================================
    // 3.5. 创建事件处理器（用于打印所有收到的消息）
    // ============================================================
    let event_handler = Arc::new(DebugEventHandler {});

    // ============================================================
    // 4. 构建客户端（使用观察者模式，支持协议竞速）
    // ============================================================
    let mut client = ObserverClientBuilder::new("127.0.0.1:8080")
        .with_observer(observer.clone() as Arc<dyn ConnectionObserver>)
        .with_event_handler(event_handler.clone() as Arc<dyn ClientEventHandler>)
        // ============================================================
        // 协议配置：支持多协议竞速
        // ============================================================
        .with_protocol_race(vec![TransportProtocol::QUIC, TransportProtocol::WebSocket])
        .with_protocol_url(
            TransportProtocol::WebSocket,
            "ws://127.0.0.1:8080".to_string(),
        )
        .with_protocol_url(TransportProtocol::QUIC, "quic://127.0.0.1:8081".to_string())
        // ============================================================
        // 协商配置：客户端序列化格式（可选）
        // ============================================================
        // 场景1：不指定格式 - 使用服务端默认JSON
        // 不调用 with_format()，将使用服务端默认JSON
        // 场景2：指定格式（非强制） - 客户端指定格式，服务端优先使用
        // 取消下面的注释来指定格式：
        // .with_format(flare_core::common::protocol::SerializationFormat::Protobuf)
        // .with_compression(flare_core::common::compression::CompressionAlgorithm::None)
        // 场景3：强制模式 - 客户端强制使用指定格式（适用于不支持某些格式的平台）
        // 取消下面的注释来启用强制模式：
        // .force_format(flare_core::common::protocol::SerializationFormat::Json)
        // .force_compression(flare_core::common::compression::CompressionAlgorithm::None)
        // ============================================================
        // 设备信息配置
        // ============================================================
        .with_device_info(device_info) // 设置设备信息，将在 CONNECT 消息中发送
        .with_user_id(user_id.clone()) // 设置用户 ID（用于设备管理和多设备互斥测试）
        // ============================================================
        // 连接配置
        // ============================================================
        .with_heartbeat(heartbeat_config)
        .with_connect_timeout(std::time::Duration::from_secs(10))
        .with_reconnect_interval(std::time::Duration::from_secs(3))
        .with_max_reconnect_attempts(Some(5))
        .build_with_race()
        .await?;

    info!("✅ 连接成功");
    info!("");
    info!("📋 协商结果：");
    info!("   - 查看上面的日志，可以看到协商完成的最终格式、压缩方式和加密方式");
    info!("   - 如果不指定格式，服务端使用默认JSON");
    info!("   - 如果指定格式（非强制），服务端优先使用客户端格式");
    info!("   - 如果使用强制模式，服务端必须使用客户端指定的格式");
    info!("");
    info!("📊 协商日志说明：");
    info!("   - [ClientCore] 发送 CONNECT 消息：显示客户端发送的协商请求");
    info!("   - [ClientCore] ✅ 收到 CONNECT_ACK：显示服务端确定的最终格式");
    info!("   - [ClientCore] ✅ 解析器已更新：显示客户端解析器已更新");
    info!("   - 注意：后续消息将使用协商后的格式进行序列化和反序列化");
    info!("");
    info!("📋 使用说明：");
    info!("   - 输入消息并按回车发送");
    info!("   - 输入 'quit' 或 'exit' 退出");
    info!("   - 输入 '/userid' 查看当前用户ID");
    info!("   - 输入 '/platform' 查看当前平台");
    info!("");

    // ============================================================
    // 5. 处理用户输入和消息发送
    // ============================================================
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    loop {
        // 使用 tokio::select! 同时监听输入和连接状态
        tokio::select! {
            result = reader.read_line(&mut line) => {
                match result {
                    Ok(0) => {
                        // EOF
                        info!("输入结束，退出客户端...");
                        break;
                    }
                    Ok(_) => {
                        let message = line.trim().to_string();
                        line.clear();

                        if message.is_empty() {
                            continue;
                        }

                        if message == "quit" || message == "exit" {
                            info!("退出客户端...");
                            break;
                        }

                        // 处理特殊命令
                        if message == "/userid" {
                            info!("当前用户ID: {}", user_id);
                            continue;
                        }

                        if message == "/platform" {
                            info!("当前平台: {:?} ({})", platform, platform.as_str());
                            continue;
                        }

                        // 发送消息
                        let msg_cmd = send_message(
                            generate_message_id(),
                            message.as_bytes().to_vec(),
                            None,
                            None,
                        );

                        let frame = frame_with_message_command(
                            msg_cmd,
                            Reliability::AtLeastOnce,
                        );

                        if let Err(e) = client.send_frame(&frame).await {
                            error!("发送消息失败: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("读取输入失败: {}", e);
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                // 定期检查连接状态
                if !client.is_connected() {
                    info!("连接已断开");
                    break;
                }
            }
        }
    }

    client.disconnect().await?;
    info!("客户端已断开");

    Ok(())
}

/// 协商聊天观察者
struct NegotiationChatObserver {
    message_count: Arc<std::sync::atomic::AtomicU64>,
    // 注意：negotiated_format 和 negotiated_compression 在实际使用中应该从 ClientCore 获取
    // 这里保留用于未来扩展
    #[allow(dead_code)]
    negotiated_format: Arc<std::sync::atomic::AtomicU8>,
    #[allow(dead_code)]
    negotiated_compression: Arc<std::sync::atomic::AtomicU8>,
}

#[async_trait]
impl ConnectionObserver for NegotiationChatObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Connected => {
                info!("✅ 已连接到服务器");
                info!("   客户端已自动发送 CONNECT 消息，包含协商信息");
                info!("   - 序列化格式偏好");
                info!("   - 压缩算法偏好");
                info!("   - 设备信息");
                info!("   - 强制模式标记（如果启用）");
            }

            ConnectionEvent::Disconnected(reason) => {
                if reason.contains("设备冲突") || reason.contains("被踢") {
                    error!("❌ 连接被踢下线: {}", reason);
                    info!("💡 提示：同一用户同一平台只能有一个设备在线");
                    info!("   请关闭当前客户端，或使用不同平台登录");
                } else {
                    info!("❌ 连接断开: {}", reason);
                }
            }

            ConnectionEvent::Error(e) => {
                error!("连接错误: {:?}", e);
            }

            ConnectionEvent::Message(data) => {
                // 解析消息：MessageParser 支持自动检测格式（JSON/Protobuf）
                // 这允许我们在协商完成前也能解析消息（因为 CONNECT_ACK 可能在协商前到达）
                let parser = flare_core::common::MessageParser::json(); // 使用任意格式，parse() 会自动检测

                match parser.parse(data) {
                    Ok(frame) => {
                        if let Some(cmd) = &frame.command {
                            if let Some(Type::Message(msg_cmd)) = &cmd.r#type {
                                let message_text = String::from_utf8_lossy(&msg_cmd.payload);
                                let count = self
                                    .message_count
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                                    + 1;
                                info!("[消息 #{}] {}", count, message_text);
                            }
                        }
                    }
                    Err(e) => {
                        error!("解析消息失败: {}", e);
                    }
                }
            }
        }
    }
}

/// 调试事件处理器
///
/// 打印所有收到的系统命令、消息命令、通知命令和连接事件
struct DebugEventHandler;

#[async_trait]
impl ClientEventHandler for DebugEventHandler {
    async fn handle_system_command(
        &self,
        command_type: SysType,
        frame: &Frame,
    ) -> Result<Option<Frame>> {
        debug!("[DebugEventHandler] 📨 收到系统命令: {:?}", command_type);

        if let Some(cmd) = &frame.command {
            if let Some(Type::System(sys_cmd)) = &cmd.r#type {
                match command_type {
                    SysType::Unspecified => {
                        debug!("[DebugEventHandler] ❓ UNSPECIFIED 命令");
                    }
                    SysType::Connect => {
                        info!("[DebugEventHandler] 🔌 CONNECT 命令");
                        if let Some(format_bytes) = sys_cmd.metadata.get("format") {
                            if let Ok(format_str) = String::from_utf8(format_bytes.clone()) {
                                debug!("[DebugEventHandler]   格式: {}", format_str);
                            }
                        }
                        if let Some(compression_bytes) = sys_cmd.metadata.get("compression") {
                            if let Ok(compression_str) =
                                String::from_utf8(compression_bytes.clone())
                            {
                                debug!("[DebugEventHandler]   压缩: {}", compression_str);
                            }
                        }
                    }
                    SysType::ConnectAck => {
                        info!("[DebugEventHandler] ✅ CONNECT_ACK 命令");
                        debug!("[DebugEventHandler]   协商格式: {:?}", sys_cmd.format);
                        if let Some(compression_bytes) = sys_cmd.metadata.get("compression") {
                            if let Ok(compression_str) =
                                String::from_utf8(compression_bytes.clone())
                            {
                                debug!("[DebugEventHandler]   协商压缩: {}", compression_str);
                            }
                        }
                    }
                    SysType::Ping => {
                        debug!("[DebugEventHandler] 💓 PING 命令");
                    }
                    SysType::Pong => {
                        debug!("[DebugEventHandler] 💗 PONG 命令");
                    }
                    SysType::Kicked => {
                        warn!("[DebugEventHandler] ⚠️  KICKED 命令: {}", sys_cmd.message);
                        if let Some(reason_bytes) = sys_cmd.metadata.get("reason") {
                            if let Ok(reason_str) = String::from_utf8(reason_bytes.clone()) {
                                warn!("[DebugEventHandler]   原因: {}", reason_str);
                            }
                        }
                    }
                    SysType::Error => {
                        error!("[DebugEventHandler] ❌ ERROR 命令: {}", sys_cmd.message);
                    }
                    SysType::Close => {
                        info!("[DebugEventHandler] 🔒 CLOSE 命令");
                    }
                    SysType::Event => {
                        info!("[DebugEventHandler] 📢 EVENT 命令: {}", sys_cmd.message);
                        if !sys_cmd.data.is_empty() {
                            debug!(
                                "[DebugEventHandler]   数据大小: {} bytes",
                                sys_cmd.data.len()
                            );
                        }
                    }
                    SysType::Auth => {
                        info!("[DebugEventHandler] 🔐 AUTH 命令");
                        if !sys_cmd.data.is_empty() {
                            debug!(
                                "[DebugEventHandler]   数据大小: {} bytes",
                                sys_cmd.data.len()
                            );
                        }
                    }
                    SysType::AuthAck => {
                        info!("[DebugEventHandler] ✅ AUTH_ACK 命令: {}", sys_cmd.message);
                    }
                }
            }
        }

        Ok(None)
    }

    async fn handle_message_command(
        &self,
        command_type: MsgType,
        frame: &Frame,
    ) -> Result<Option<Frame>> {
        info!("[DebugEventHandler] 💬 收到消息命令: {:?}", command_type);

        if let Some(cmd) = &frame.command {
            if let Some(Type::Message(msg_cmd)) = &cmd.r#type {
                let payload_len = msg_cmd.payload.len();
                let message_preview = if payload_len > 50 {
                    format!("{}...", String::from_utf8_lossy(&msg_cmd.payload[..50]))
                } else {
                    String::from_utf8_lossy(&msg_cmd.payload).to_string()
                };
                debug!("[DebugEventHandler]   消息ID: {}", msg_cmd.message_id);
                debug!("[DebugEventHandler]   载荷大小: {} bytes", payload_len);
                debug!("[DebugEventHandler]   内容预览: {}", message_preview);
            }
        }

        Ok(None)
    }

    async fn handle_notification_command(
        &self,
        command_type: NotifType,
        frame: &Frame,
    ) -> Result<Option<Frame>> {
        info!("[DebugEventHandler] 🔔 收到通知命令: {:?}", command_type);

        if let Some(cmd) = &frame.command {
            if let Some(Type::Notification(notif_cmd)) = &cmd.r#type {
                debug!("[DebugEventHandler]   标题: {}", notif_cmd.title);
                debug!(
                    "[DebugEventHandler]   内容长度: {} bytes",
                    notif_cmd.content.len()
                );
            }
        }

        Ok(None)
    }

    async fn handle_connection_event(&self, event: &ConnectionEvent) -> Result<()> {
        match event {
            ConnectionEvent::Connected => {
                info!("[DebugEventHandler] 🟢 连接事件: Connected");
            }
            ConnectionEvent::Disconnected(reason) => {
                warn!("[DebugEventHandler] 🔴 连接事件: Disconnected - {}", reason);
            }
            ConnectionEvent::Error(err) => {
                error!("[DebugEventHandler] ⚠️  连接事件: Error - {:?}", err);
            }
            ConnectionEvent::Message(_) => {
                // 消息在 handle_message_command 中处理
            }
        }

        Ok(())
    }
}
