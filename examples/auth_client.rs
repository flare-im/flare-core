//! 认证聊天室客户端示例
//! 
//! 演示客户端如何使用 token 进行认证
//! 
//! ## 认证说明
//! 
//! 1. **设置 Token**：
//!    - 通过 `with_token()` 设置 token
//!    - Token 会在 CONNECT 消息中自动发送给服务端
//! 
//! 2. **认证流程**：
//!    - 客户端连接后，自动发送 CONNECT 消息（包含 token）
//!    - 服务端验证 token，验证通过后发送 CONNECT_ACK
//!    - 只有验证通过后，客户端才能收发业务消息
//! 
//! ## 启动命令
//! 
//! ```bash
//! # 使用正确 token（12345）
//! RUST_LOG=debug cargo run --example auth_client
//! 
//! # 使用错误 token（通过命令行参数）
//! RUST_LOG=debug cargo run --example auth_client -- wrong_token
//! 
//! # 通过环境变量指定 token
//! TOKEN=12345 RUST_LOG=debug cargo run --example auth_client
//! ```
//! 
//! ## 配置说明
//! 
//! - `with_token()`: 设置 token（用于认证）
//! - `with_user_id()`: 设置用户ID（可选，用于聊天室显示用户名）

use flare_core::client::{ObserverClientBuilder, ClientEventHandler};
use flare_core::common::config_types::{TransportProtocol, HeartbeatConfig};
use flare_core::common::protocol::{frame_with_message_command, send_message, generate_message_id, Reliability, Frame};
use flare_core::common::protocol::flare::core::commands::{
    system_command::Type as SysType,
    message_command::Type as MsgType,
    notification_command::Type as NotifType,
    command::Type,
};
use flare_core::transport::events::{ConnectionEvent, ConnectionObserver};
use flare_core::common::error::Result;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{info, error, debug, warn};
use async_trait::async_trait;

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    info!("🚀 启动认证聊天室客户端");
    info!("");
    info!("📋 认证说明：");
    info!("   - 客户端需要提供 token 进行认证");
    info!("   - 只有 token='12345' 才能验证通过");
    info!("   - 验证通过后，客户端才能收发业务消息");
    info!("");

    // ============================================================
    // 1. 获取 token（从命令行参数或环境变量或交互式输入）
    // ============================================================
    let token = if let Some(arg_token) = std::env::args().nth(1) {
        // 从命令行参数读取
        info!("📝 使用命令行参数指定的 token: {}", arg_token);
        arg_token
    } else if let Ok(env_token) = std::env::var("TOKEN") {
        // 从环境变量读取
        info!("📝 使用环境变量 TOKEN: {}", env_token);
        env_token
    } else {
        // 从stdin读取（交互式输入）
        info!("📝 请输入 token（正确 token: 12345）:");
        info!("   - 正确 token: '12345' 会验证通过");
        info!("   - 错误 token: 其他值会被拒绝");
        print!("Token (默认: 12345): ");
        use std::io::Write;
        std::io::stdout().flush().unwrap();
        
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);
        let mut input_line = String::new();
        
        match reader.read_line(&mut input_line).await {
            Ok(_) => {
                let trimmed = input_line.trim();
                if trimmed.is_empty() {
                    "12345".to_string()
                } else {
                    trimmed.to_string()
                }
            }
            Err(e) => {
                error!("读取用户输入失败: {}, 使用默认 token", e);
                "12345".to_string()
            }
        }
    };
    
    info!("✅ Token: {}", token);
    if token == "12345" {
        info!("💡 提示：使用正确 token，应该可以成功连接");
    } else {
        warn!("⚠️  警告：使用错误 token，可能会被服务端拒绝");
    }
    info!("");

    // ============================================================
    // 2. 配置心跳
    // ============================================================
    let heartbeat_config = HeartbeatConfig::default()
        .with_interval(std::time::Duration::from_secs(30))
        .with_timeout(std::time::Duration::from_secs(90));

    // ============================================================
    // 3. 创建错误标志，用于在连接错误时立即退出
    // ============================================================
    let connection_error = Arc::new(std::sync::atomic::AtomicBool::new(false));
    
    // ============================================================
    // 4. 创建观察者（用于接收消息和连接事件）
    // ============================================================
    let observer = Arc::new(AuthChatObserverWithErrorFlag {
        message_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        connection_error: Arc::clone(&connection_error),
    });
    
    // ============================================================
    // 5. 创建事件处理器（用于打印所有收到的消息）
    // ============================================================
    let event_handler = Arc::new(DebugEventHandler {});

    // ============================================================
    // 6. 构建客户端（使用观察者模式，支持协议竞速）
    // ============================================================
    let mut client = ObserverClientBuilder::new("127.0.0.1:8080")
        .with_observer(observer.clone() as Arc<dyn ConnectionObserver>)
        .with_event_handler(event_handler.clone() as Arc<dyn ClientEventHandler>)
        
        // ============================================================
        // 认证配置：设置 token
        // ============================================================
        .with_token(token.clone())  // 设置 token，将在 CONNECT 消息中发送
        
        // ============================================================
        // 协议配置：支持多协议竞速
        // ============================================================
        .with_protocol_race(vec![TransportProtocol::QUIC, TransportProtocol::WebSocket])
        .with_protocol_url(TransportProtocol::WebSocket, "ws://127.0.0.1:8080".to_string())
        .with_protocol_url(TransportProtocol::QUIC, "quic://127.0.0.1:8081".to_string())
        
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
    info!("📋 认证结果：");
    info!("   - 查看上面的日志，可以看到认证过程");
    info!("   - 如果 token 正确，会看到 '[ServerCore] ✅ Token 验证成功'");
    info!("   - 如果 token 错误，会看到 '[ServerCore] ❌ Token 验证失败'");
    info!("");
    info!("📊 认证日志说明：");
    info!("   - [ClientCore] 已添加 token 到 CONNECT 消息元数据：显示 token 已发送");
    info!("   - [ServerCore] 🔐 开始验证 token：显示服务端开始验证");
    info!("   - [ServerCore] ✅ Token 验证成功：显示验证通过");
    info!("   - [ServerCore] ❌ Token 验证失败：显示验证失败");
    info!("   - [ServerCore] ✅ 连接已标记为已验证：显示连接已验证");
    info!("");
    info!("📋 使用说明：");
    info!("   - 输入消息并按回车发送");
    info!("   - 输入 'quit' 或 'exit' 退出");
    info!("   - 输入 '/token' 查看当前 token");
    info!("");

    // ============================================================
    // 7. 处理用户输入和消息发送
    // ============================================================
    // 注意：如果发生连接错误，观察者会在 on_event 中调用 std::process::exit(1) 立即退出
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();
    
    loop {
        // 注意：如果发生连接错误，观察者会在 on_event 中调用 std::process::exit(1) 立即退出
        // 这里只需要正常处理用户输入即可
        
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
                        if message == "/token" {
                            info!("当前 token: {}", token);
                            continue;
                        }
                        
                        // 检查连接状态
                        if !client.is_connected() {
                            error!("连接已断开，无法发送消息");
                            break;
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
                            // 发送失败可能是连接问题，检查连接状态
                            if !client.is_connected() {
                                error!("连接已断开，退出程序");
                                break;
                            }
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
                    error!("连接已断开，退出程序");
                    break;
                }
            }
        }
    }

    // 尝试断开连接（如果还连接着）
    if client.is_connected() {
        if let Err(e) = client.disconnect().await {
            error!("断开连接失败: {}", e);
        }
    }
    info!("客户端已断开");

    Ok(())
}

/// 带错误标志的认证聊天观察者
/// 
/// 当发生连接错误时，立即退出程序
struct AuthChatObserverWithErrorFlag {
    message_count: Arc<std::sync::atomic::AtomicU64>,
    connection_error: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait]
impl ConnectionObserver for AuthChatObserverWithErrorFlag {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Connected => {
                info!("✅ 已连接到服务器");
                info!("   客户端已自动发送 CONNECT 消息，包含 token");
            }
            
            ConnectionEvent::Disconnected(reason) => {
                // 检查是否是认证失败导致的断开
                // 注意：协议竞速时关闭未选中的连接也会触发 Disconnected，这种情况不应该退出
                if reason.contains("认证") || reason.contains("Token") || reason.contains("验证") || 
                   reason.contains("authentication") || reason.contains("Authentication") ||
                   reason.contains("Token 无效") || reason.contains("未提供 token") {
                    error!("❌ 连接被拒绝（认证失败）: {}", reason);
                    error!("💡 提示：请检查 token 是否正确（正确 token: 12345）");
                    // 只有认证失败才退出
                    self.connection_error.store(true, std::sync::atomic::Ordering::Relaxed);
                    error!("⚠️  认证失败，立即退出程序");
                    std::process::exit(1);
                } else if reason.is_empty() || 
                          reason.contains("协议竞速") || 
                          reason.contains("未选中") ||
                          reason.contains("Closed by client") ||
                          reason.contains("Client disconnected") {
                    // 协议竞速或正常关闭导致的断开，不退出
                    debug!("ℹ️  连接断开（正常关闭）: {}", reason);
                } else {
                    // 其他断开原因，只记录日志，不退出
                    // 因为可能是网络波动等临时问题，或者协议竞速导致的正常断开
                    warn!("⚠️  连接断开: {}", reason);
                }
            }
            
            ConnectionEvent::Error(e) => {
                // 检查是否是认证相关的错误
                let error_str = format!("{:?}", e);
                if error_str.contains("认证") || error_str.contains("Token") || error_str.contains("验证") || 
                   error_str.contains("authentication") || error_str.contains("Authentication") ||
                   error_str.contains("Token 无效") || error_str.contains("未提供 token") ||
                   error_str.contains("authentication_failed") {
                    error!("❌ 连接错误（认证失败）: {:?}", e);
                    error!("💡 提示：请检查 token 是否正确（正确 token: 12345）");
                    // 只有认证失败才退出
                    self.connection_error.store(true, std::sync::atomic::Ordering::Relaxed);
                    error!("⚠️  认证失败，立即退出程序");
                    std::process::exit(1);
                } else if error_str.contains("protocol error") || 
                          error_str.contains("Connection reset") ||
                          error_str.contains("WebSocket protocol error") {
                    // WebSocket 协议错误（可能是协议竞速时关闭连接导致的），不退出
                    debug!("ℹ️  连接错误（协议竞速或协议层错误）: {:?}", e);
                } else {
                    // 其他错误，只记录日志，不退出
                    // 因为可能是网络波动等临时问题，或者协议竞速导致的正常错误
                    warn!("⚠️  连接错误: {:?}", e);
                }
            }
            
            ConnectionEvent::Message(data) => {
                // 解析消息：MessageParser 支持自动检测格式（JSON/Protobuf）
                let parser = flare_core::common::MessageParser::json();
                
                match parser.parse(data) {
                    Ok(frame) => {
                        if let Some(cmd) = &frame.command {
                            if let Some(Type::Message(msg_cmd)) = &cmd.r#type {
                                let message_text = String::from_utf8_lossy(&msg_cmd.payload);
                                let count = self.message_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
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
    async fn handle_system_command(&self, command_type: SysType, frame: &Frame) -> Result<Option<Frame>> {
        debug!("[DebugEventHandler] 📨 收到系统命令: {:?}", command_type);
        
        if let Some(cmd) = &frame.command {
            if let Some(Type::System(sys_cmd)) = &cmd.r#type {
                match command_type {
                    SysType::ConnectAck => {
                        info!("[DebugEventHandler] ✅ CONNECT_ACK 命令（认证成功）");
                    }
                    SysType::Error => {
                        error!("[DebugEventHandler] ❌ ERROR 命令: {}", sys_cmd.message);
                        if sys_cmd.message.contains("认证") || sys_cmd.message.contains("Token") {
                            warn!("[DebugEventHandler] ⚠️  认证失败，请检查 token");
                        }
                    }
                    SysType::Kicked => {
                        warn!("[DebugEventHandler] ⚠️  KICKED 命令: {}", sys_cmd.message);
                    }
                    _ => {
                        debug!("[DebugEventHandler] 其他系统命令: {:?}", command_type);
                    }
                }
            }
        }
        
        Ok(None)
    }
    
    async fn handle_message_command(&self, command_type: MsgType, frame: &Frame) -> Result<Option<Frame>> {
        debug!("[DebugEventHandler] 💬 收到消息命令: {:?}", command_type);
        Ok(None)
    }
    
    async fn handle_notification_command(&self, command_type: NotifType, _frame: &Frame) -> Result<Option<Frame>> {
        debug!("[DebugEventHandler] 🔔 收到通知命令: {:?}", command_type);
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

