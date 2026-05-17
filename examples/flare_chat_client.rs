//! Flare 聊天室客户端示例
//!
//! 演示如何使用 FlareClientBuilder 创建完整的聊天室客户端
//! 使用所有 flare-core 的能力：
//! - MessagePipeline（消息管道）
//! - Middleware（中间件：日志、性能监控）
//! - 协议竞速（WebSocket + QUIC）
//! - 设备管理（设备信息、设备冲突处理）
//! - 序列化协商（JSON/Protobuf）
//! - 压缩协商（Gzip/Zstd/None）
//! - 加密支持（AES-256-GCM，已注册加密器，可在协商时启用）
//!
//! 所有协议选择、协议竞速、压缩、序列化和加密都由 HybridClient 和 ClientCore 自动处理
//!
//! ## 启动命令
//!
//! ```bash
//! # 使用默认平台（PC），交互式输入用户ID
//! RUST_LOG=info cargo run --example flare_chat_client
//!
//! # 指定用户ID
//! RUST_LOG=info cargo run --example flare_chat_client -- user123
//!
//! # 指定平台
//! DEVICE_PLATFORM=android RUST_LOG=info cargo run --example flare_chat_client
//!
//! # 通过环境变量指定用户ID和平台
//! USER_ID=user123 DEVICE_PLATFORM=android RUST_LOG=info cargo run --example flare_chat_client
//!
//! # 其他平台选项：web, pc, h5, android, ios, harmonyos
//! ```
//!
//! ## 测试多设备互斥
//!
//! 1. **测试同一用户同一平台互斥**：
//!    - 启动第一个客户端：`cargo run --example flare_chat_client -- user1`
//!    - 启动第二个客户端（相同用户ID + 相同平台）：`cargo run --example flare_chat_client -- user1`
//!    - 预期：第二个客户端登录后，第一个客户端会被踢掉
//!
//! 2. **测试同一用户不同平台共存**：
//!    - 启动第一个客户端：`DEVICE_PLATFORM=pc cargo run --example flare_chat_client -- user1`
//!    - 启动第二个客户端（相同用户ID + 不同平台）：`DEVICE_PLATFORM=android cargo run --example flare_chat_client -- user1`
//!    - 预期：两个客户端可以同时在线

use async_trait::async_trait;
use flare_core::client::*;
use flare_core::common::config_types::{HeartbeatConfig, TransportProtocol};
use flare_core::common::device::{DeviceInfo, DevicePlatform};
use flare_core::common::encryption::{Aes256GcmEncryptor, EncryptionUtil};
use flare_core::common::error::Result;
use flare_core::common::message::{
    ArcMessageMiddleware, LogLevel, LoggingMiddleware, MetricsMiddleware,
};
use flare_core::common::protocol::flare::core::commands::command::Type;
use flare_core::common::protocol::{
    Frame, Reliability, frame_with_message_command, generate_message_id, send_message,
};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{debug, error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("🚀 启动 Flare 聊天室客户端（完整功能演示）");
    info!("");
    info!("📋 客户端功能说明：");
    info!("   - 消息管道：自动处理序列化、压缩、加密、日志、性能监控");
    info!("   - 中间件：日志 → 性能监控");
    info!("   - 协议竞速：自动选择最快的协议（QUIC 优先，WebSocket 备选）");
    info!("   - 设备管理：支持多平台设备信息，自动处理设备冲突");
    info!("   - 协商：自动协商序列化格式、压缩方式和加密算法");
    info!("");

    // ============================================================
    // 1. 注册加密器（可选，用于加密通信）
    // ============================================================
    // ⚠️  安全警告：当前示例使用硬编码密钥，仅用于演示！
    // 生产环境必须使用安全的密钥管理方案，不要硬编码密钥。
    // 详细说明请参考：doc/ENCRYPTION_SECURITY.md
    //
    // 推荐方案：
    // 1. 传输层加密（推荐）：使用 TLS/QUIC，不需要应用层加密
    // 2. 从服务端协商密钥：通过安全通道获取会话密钥
    // 3. 从安全存储读取：iOS Keychain、Android Keystore 等
    //
    // 当前实现仅用于功能演示，生产环境请替换为安全的密钥管理方案
    let encryption_key = if let Ok(key) = std::env::var("ENCRYPTION_KEY") {
        // 优先使用环境变量（如果设置）
        info!("🔐 使用环境变量 ENCRYPTION_KEY");
        key.as_bytes().to_vec()
    } else {
        // 否则使用默认示例密钥（仅用于演示，需要与服务端一致）
        warn!(
            "⚠️  使用默认示例密钥，仅用于演示！生产环境请设置 ENCRYPTION_KEY 环境变量或使用密钥协商"
        );
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
    // 2. 获取用户ID（用于测试多设备互斥）
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
    // 3. 创建设备信息（用于设备管理和多设备互斥测试）
    // ============================================================
    // 平台互斥策略说明：
    // - 同一用户同一平台只能有一个设备在线
    // - 例如：如果已经有一个 Android 设备在线，新的 Android 设备登录会踢掉旧的
    // - 但 Android 和 iOS 可以同时在线（因为平台不同）

    // 可以通过命令行参数或环境变量指定平台，这里使用默认 PC
    // 例如：DEVICE_PLATFORM=android cargo run --example flare_chat_client
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
    // 4. 创建聊天监听器
    // ============================================================
    let chat_listener = Arc::new(ChatListener {
        message_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
    });

    // ============================================================
    // 5. 创建中间件
    // ============================================================
    // 日志中间件
    let logging_middleware =
        Arc::new(LoggingMiddleware::new("ClientLogging").with_level(LogLevel::Debug))
            as ArcMessageMiddleware;

    // 性能监控中间件
    let metrics_middleware =
        Arc::new(MetricsMiddleware::new("ClientMetrics")) as ArcMessageMiddleware;

    // ============================================================
    // 6. 使用 FlareClientBuilder 构建客户端（使用所有能力）
    // ============================================================
    // 注意：协议选择、协议竞速、压缩、序列化和加密都由 HybridClient 和 ClientCore 自动处理
    let client = FlareClientBuilder::new("127.0.0.1:8080")
        // ============================================================
        // 必须：设置消息监听器
        // ============================================================
        .with_listener(chat_listener.clone())
        // ============================================================
        // 中间件（按添加顺序执行）
        // ============================================================
        .with_middleware(logging_middleware) // 1. 日志
        .with_middleware(metrics_middleware) // 2. 性能监控
        // ============================================================
        // 协议配置：支持多协议竞速（由 HybridClient 处理）
        // ============================================================
        // 协议列表的顺序就是优先级顺序，前面的协议优先级更高
        // HybridClient 会同时尝试多个协议，选择第一个成功的
        .with_protocol_race(vec![TransportProtocol::QUIC, TransportProtocol::WebSocket])
        .with_protocol_url(
            TransportProtocol::WebSocket,
            "ws://127.0.0.1:8080".to_string(),
        )
        .with_protocol_url(TransportProtocol::QUIC, "quic://127.0.0.1:8081".to_string())
        // ============================================================
        // 设备信息配置（用于设备管理和多设备互斥测试）
        // ============================================================
        .with_device_info(device_info) // 设置设备信息，将在 CONNECT 消息中发送
        .with_user_id(user_id.clone()) // 设置用户 ID（用于设备管理和多设备互斥测试）
        // ============================================================
        // 协商配置：客户端序列化格式、压缩和加密（可选，由 ClientCore 处理）
        // ============================================================
        // 场景1：不指定格式 - 使用服务端默认格式（推荐）
        // 不调用 with_format()，将使用服务端默认格式
        // 场景2：指定格式（非强制） - 客户端指定格式，服务端优先使用
        // 取消下面的注释来指定格式：
        // .with_format(SerializationFormat::Protobuf)
        // .with_compression(CompressionAlgorithm::Gzip)
        // 场景3：强制模式 - 客户端强制使用指定格式（适用于不支持某些格式的平台）
        // 取消下面的注释来启用强制模式：
        // .force_format(SerializationFormat::Json)
        // .force_compression(CompressionAlgorithm::None)
        // 场景4：启用加密（可选）
        // 取消下面的注释来启用加密：
        // .with_encryption(EncryptionAlgorithm::Aes256Gcm)
        // ============================================================
        // 连接配置
        // ============================================================
        .with_heartbeat(
            HeartbeatConfig::default()
                .with_interval(std::time::Duration::from_secs(30))
                .with_timeout(std::time::Duration::from_secs(90)),
        )
        .with_connect_timeout(std::time::Duration::from_secs(10))
        .with_reconnect_interval(std::time::Duration::from_secs(3))
        .with_max_reconnect_attempts(Some(5))
        // 使用协议竞速连接（由 HybridClient::connect_with_race 处理）
        .build_with_race()
        .await?;

    // 获取连接成功的协议
    let active_protocol = client.active_protocol();
    let protocol_name = match active_protocol {
        TransportProtocol::WebSocket => "WebSocket",
        TransportProtocol::QUIC => "QUIC",
        TransportProtocol::TCP => "TCP",
    };

    info!("✅ 连接成功");
    info!("📡 使用的协议: {} ({:?})", protocol_name, active_protocol);
    info!("");
    info!("📋 功能说明：");
    info!("   - 消息管道：自动处理序列化、压缩、加密、日志、性能监控");
    info!("   - 中间件：日志 → 性能监控");
    info!("   - 处理器：聊天监听器");
    info!("   - 协议竞速：自动选择最快的协议（由 HybridClient 处理）");
    info!("   - 协商：自动协商序列化格式和压缩方式（由 ClientCore 处理）");
    info!("   - 设备管理：自动处理设备冲突（由服务端处理）");
    info!("");
    info!("📊 协商结果：");
    info!("   - 查看上面的日志，可以看到协商完成的最终格式、压缩方式和加密方式");
    info!("   - 如果不指定格式，服务端使用默认JSON");
    info!("   - 如果指定格式（非强制），服务端优先使用客户端格式");
    info!("   - 如果使用强制模式，服务端必须使用客户端指定的格式");
    info!("");
    info!("📋 使用说明：");
    info!("   - 输入消息并按回车发送");
    info!("   - 输入 'quit' 或 'exit' 退出");
    info!("   - 输入 '/userid' 查看当前用户ID");
    info!("   - 输入 '/platform' 查看当前平台");
    info!("   - 输入 'echo: <text>' 测试 Echo 处理器（如果服务端支持）");
    info!("");

    // ============================================================
    // 6. 处理用户输入和消息发送
    // ============================================================
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    loop {
        tokio::select! {
            result = reader.read_line(&mut line) => {
                match result {
                    Ok(0) => {
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

                        // 检查连接状态（如果断开，等待重连，不立即退出）
                        if !client.is_connected() {
                            warn!("⚠️  连接已断开，等待自动重连...");
                            // 等待一段时间，让重连机制尝试重连
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            // 如果仍然断开，提示用户，但不退出
                            if !client.is_connected() {
                                warn!("💡 提示：连接已断开，正在自动重连中，请稍候...");
                            }
                            continue; // 继续循环，不退出
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

                        // 发送消息并等待响应（按 message_id 匹配）
                        if let Err(e) = client.send_frame_and_wait(&frame, std::time::Duration::from_secs(5)).await {
                            error!("发送消息或等待响应失败: {}", e);
                            // 如果发送失败，等待重连，不立即退出
                            if !client.is_connected() {
                                warn!("⚠️  连接已断开，等待自动重连...");
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            }
                            // 继续循环，不退出
                        }
                        debug!("发送消息并等待响应成功: {}", frame.message_id);
                    }
                    Err(e) => {
                        error!("读取输入失败: {}", e);
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                // 定期检查连接状态（不退出，等待重连）
                // 连接状态检查已由自动重连机制处理，这里不需要退出
            }
        }
    }

    client.disconnect().await?;
    info!("客户端已断开");

    Ok(())
}

/// 聊天监听器
struct ChatListener {
    message_count: Arc<std::sync::atomic::AtomicU64>,
}

#[async_trait]
impl MessageListener for ChatListener {
    async fn on_message(&self, frame: &Frame) -> Result<Option<Frame>> {
        // 解析消息（消息管道已自动处理序列化、压缩等）
        if let Some(cmd) = &frame.command {
            if let Some(Type::Payload(msg_cmd)) = &cmd.r#type {
                // 尝试解析protobuf消息内容
                let message_text = match String::from_utf8(msg_cmd.payload.clone()) {
                    Ok(text) => text,
                    Err(_) => {
                        // 如果不是有效的UTF-8，则显示十六进制调试信息
                        format!("<protobuf_binary_data: {} bytes>", msg_cmd.payload.len())
                    }
                };
                let count = self
                    .message_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    + 1;
                info!("[消息 #{}] {}", count, message_text);
            }
        }
        Ok(None)
    }

    async fn on_connect(&self) -> Result<()> {
        info!("✅ 已连接到服务器");
        info!("   客户端已自动发送 CONNECT 消息，包含协商信息");
        info!("   - 序列化格式偏好（由 ClientCore 处理）");
        info!("   - 压缩算法偏好（由 ClientCore 处理）");
        info!("   - 设备信息（由 ClientCore 处理）");
        info!("   - 强制模式标记（如果启用）");
        Ok(())
    }

    async fn on_disconnect(&self, reason: Option<&str>) -> Result<()> {
        if let Some(reason) = reason {
            if reason.contains("设备冲突") || reason.contains("被踢") {
                error!("❌ 连接被踢下线: {}", reason);
                info!("💡 提示：同一用户同一平台只能有一个设备在线");
                info!("   请关闭当前客户端，或使用不同平台登录");
            } else {
                info!("❌ 连接断开: {}", reason);
            }
        } else {
            info!("❌ 连接断开");
        }
        Ok(())
    }

    async fn on_error(&self, error: &str) -> Result<()> {
        // 判断错误类型，给出更友好的提示
        if error.contains("connection lost") || error.contains("connection closed") {
            warn!("⚠️  连接丢失: {}", error);
            info!("💡 提示：这可能是网络问题或服务器关闭了连接");
            info!("   如果启用了自动重连，客户端会尝试重新连接");
        } else if error.contains("timeout") {
            warn!("⚠️  连接超时: {}", error);
            info!("💡 提示：请检查网络连接或服务器响应时间");
        } else {
            error!("❌ 连接错误: {}", error);
        }
        Ok(())
    }
}
