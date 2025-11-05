//! 混合客户端聊天室示例
//! 
//! 使用观察者模式的 Builder（ObserverClientBuilder）构建客户端
//! 使用协议竞速连接服务器，自动选择最快的协议（WebSocket 或 QUIC）
//! 实现多用户聊天室客户端，展示所有功能特性
//! 
//! 功能特性：
//! - 协议竞速：自动选择最快的可用协议
//! - 心跳检测：自动保持连接活跃
//! - 消息路由：可选的消息路由功能（通过 enable_router() 启用）
//! - 自动重连：连接断开时自动重连
//! - 连接状态管理：完整的连接状态跟踪
//! 
//! 此示例展示了如何：
//! 1. 实现 ConnectionObserver trait 来接收消息
//! 2. 使用 ObserverClientBuilder 创建客户端（支持协议竞速）
//! 3. 配置心跳、路由等功能
//! 4. 自动选择最快的可用协议

use flare_core::client::ObserverClientBuilder;
use flare_core::common::config_types::{TransportProtocol, HeartbeatConfig};
use flare_core::common::protocol::{frame_with_message_command, send_message, generate_message_id, Reliability};
use flare_core::common::protocol::flare::core::commands::command::Type;
use flare_core::transport::events::{ConnectionEvent, ConnectionObserver};
use std::sync::Arc;
use std::io::{self, Write};
use tokio::io::{AsyncBufReadExt, BufReader};
use std::time::Duration;

// 消息观察者，用于接收和显示聊天消息
struct ChatObserver {
    username: String,
    message_count: Arc<std::sync::atomic::AtomicU64>,
}

impl ChatObserver {
    fn new(username: String) -> Self {
        Self {
            username,
            message_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
    
    fn get_message_count(&self) -> u64 {
        self.message_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl ConnectionObserver for ChatObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
                // 解析接收到的消息
                if let Ok(frame) = flare_core::common::MessageParser::new(
                    flare_core::common::protocol::SerializationFormat::Protobuf,
                    flare_core::common::compression::CompressionAlgorithm::None,
                ).parse(data) {
                    if let Some(cmd) = &frame.command {
                        if let Some(Type::Message(msg_cmd)) = &cmd.r#type {
                            let message = String::from_utf8_lossy(&msg_cmd.payload);
                            
                            // 更新消息计数
                            self.message_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            
                            // 检查是否是系统通知
                            if let Some(type_bytes) = msg_cmd.metadata.get("type") {
                                let msg_type = String::from_utf8_lossy(type_bytes);
                                if msg_type == "join" || msg_type == "leave" {
                                    println!("\n[系统] {}", message);
                                } else {
                                    println!("\n{}", message);
                                }
                            } else {
                                println!("\n{}", message);
                            }
                            
                            // 显示输入提示
                            print!("{}> ", self.username);
                            let _ = io::stdout().flush();
                        }
                    }
                }
            }
            ConnectionEvent::Connected => {
                println!("\n[系统] ✅ 已连接到聊天室服务器！");
                print!("{}> ", self.username);
                let _ = io::stdout().flush();
            }
            ConnectionEvent::Disconnected(reason) => {
                println!("\n[系统] ❌ 连接已断开: {}", reason);
            }
            ConnectionEvent::Error(e) => {
                eprintln!("\n[错误] {:?}", e);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化 tracing（默认使用 debug 级别，方便调试）
    // 可以通过环境变量 RUST_LOG 覆盖：RUST_LOG=info cargo run --example hybrid_client
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug"))
        )
        .init();
    
    // 设置 rustls CryptoProvider（QUIC 需要）
    use rustls::crypto::CryptoProvider;
    let _ = CryptoProvider::install_default(rustls::crypto::ring::default_provider());
    
    println!("=== 混合客户端聊天室（协议竞速 + 完整功能）===");
    println!();
    println!("功能特性：");
    println!("  - 协议竞速：自动选择最快的协议（WebSocket 或 QUIC）");
    println!("  - 心跳检测：自动保持连接活跃");
    println!("  - 自动重连：连接断开时自动重连");
    println!("  - 消息统计：显示接收到的消息数量");
    println!();
    
    // 获取用户名
    print!("请输入您的用户名: ");
    io::stdout().flush()?;
    let mut username = String::new();
    io::stdin().read_line(&mut username)?;
    let username = username.trim().to_string();
    
    if username.is_empty() {
        println!("用户名不能为空");
        return Err("用户名不能为空".into());
    }
    
    println!("\n正在连接到聊天室服务器（协议竞速：WebSocket 和 QUIC）...");
    println!("提示: 将自动选择最快的可用协议");
    
    // 创建观察者
    let observer = Arc::new(ChatObserver::new(username.clone()));
    let observer_clone = Arc::clone(&observer);
    
    // 配置心跳（30秒间隔，60秒超时）
    let heartbeat_config = HeartbeatConfig {
        enabled: true,
        interval: Duration::from_secs(30),
        timeout: Duration::from_secs(60),
    };
    
    // 使用 ObserverClientBuilder 创建客户端（协议竞速）
    // 展示所有可用的配置选项
    // 注意：协议列表的顺序就是优先级顺序，QUIC 在前表示 QUIC 优先级更高
    match ObserverClientBuilder::new("127.0.0.1:8080")
        .with_observer(observer as Arc<dyn ConnectionObserver>)
        .with_protocol_race(vec![TransportProtocol::QUIC, TransportProtocol::WebSocket]) // QUIC 优先级更高
        .with_protocol_url(TransportProtocol::WebSocket, "ws://127.0.0.1:8080".to_string())
        .with_protocol_url(TransportProtocol::QUIC, "quic://127.0.0.1:8081".to_string())
        .with_format(flare_core::common::protocol::SerializationFormat::Protobuf)
        .with_heartbeat(heartbeat_config)
        .with_connect_timeout(Duration::from_secs(10))
        .with_reconnect_interval(Duration::from_secs(3))
        .with_max_reconnect_attempts(Some(5))
        // 可选：启用消息路由（如果需要路由功能）
        // .enable_router()
        .build_with_race()
        .await
    {
        Ok(mut client) => {
            println!("\n✅ 连接成功！");
            println!("使用的协议: {:?}", client.active_protocol());
            println!("连接 ID: {:?}", client.connection_id());
            println!();
            
            // 发送用户名信息（首次连接时）
            let mut metadata = std::collections::HashMap::new();
            metadata.insert("username".to_string(), username.as_bytes().to_vec());
            metadata.insert("type".to_string(), "init".as_bytes().to_vec());
            
            let init_msg = send_message(
                generate_message_id(),
                format!("{} 已加入聊天室", username).into_bytes(),
                Some(metadata),
                None,
            );
            
            let init_frame = frame_with_message_command(
                init_msg,
                Reliability::BestEffort,
            );
            
            if let Err(e) = client.send_frame(&init_frame).await {
                eprintln!("⚠️  发送初始化消息失败: {}", e);
            }
            
            println!("欢迎来到聊天室！");
            println!("命令说明：");
            println!("  - 输入消息后按回车发送");
            println!("  - 输入 '/quit' 或 '/exit' 退出");
            println!("  - 输入 '/stats' 查看消息统计");
            println!("  - 输入 '/status' 查看连接状态");
            println!();
            print!("{}> ", username);
            io::stdout().flush()?;
            
            // 创建异步输入任务
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
                                println!("\n输入结束，断开连接...");
                                break;
                            }
                            Ok(_) => {
                                let input = line.trim().to_string();
                                line.clear();
                                
                                if input.is_empty() {
                                    print!("{}> ", username);
                                    let _ = io::stdout().flush();
                                    continue;
                                }
                                
                                // 处理命令
                                match input.as_str() {
                                    "/quit" | "/exit" => {
                                        println!("退出聊天室...");
                                        break;
                                    }
                                    "/stats" => {
                                        let count = observer_clone.get_message_count();
                                        println!("\n[统计] 已接收消息数: {}", count);
                                        print!("{}> ", username);
                                        let _ = io::stdout().flush();
                                        continue;
                                    }
                                    "/status" => {
                                        let is_connected = client.is_connected();
                                        let conn_id = client.connection_id();
                                        let protocol = client.active_protocol();
                                        println!("\n[状态] 连接状态: {}", if is_connected { "已连接" } else { "未连接" });
                                        println!("[状态] 连接 ID: {:?}", conn_id);
                                        println!("[状态] 使用协议: {:?}", protocol);
                                        print!("{}> ", username);
                                        let _ = io::stdout().flush();
                                        continue;
                                    }
                                    _ => {
                                        // 发送消息
                                        let mut msg_metadata = std::collections::HashMap::new();
                                        msg_metadata.insert("username".to_string(), username.as_bytes().to_vec());
                                        
                                        let chat_msg = send_message(
                                            generate_message_id(),
                                            input.as_bytes().to_vec(),
                                            Some(msg_metadata),
                                            None,
                                        );
                                        
                                        let chat_frame = frame_with_message_command(
                                            chat_msg,
                                            Reliability::BestEffort,
                                        );
                                        
                                        if let Err(e) = client.send_frame(&chat_frame).await {
                                            eprintln!("\n[错误] 发送消息失败: {}", e);
                                            println!("提示: 连接可能已断开，尝试重连...");
                                            // 客户端会自动重连（如果配置了重连）
                                            break;
                                        }
                                        
                                        print!("{}> ", username);
                                        let _ = io::stdout().flush();
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("\n[错误] 读取输入失败: {}", e);
                                break;
                            }
                        }
                    }
                }
            }
            
            // 断开连接
            println!("\n正在断开连接...");
            if let Err(e) = client.disconnect().await {
                eprintln!("断开连接时出错: {}", e);
            } else {
                println!("✅ 已断开连接");
            }
            
            // 显示最终统计
            let final_count = observer_clone.get_message_count();
            println!("本次会话接收消息总数: {}", final_count);
        }
        Err(e) => {
            eprintln!("❌ 连接失败: {:?}", e);
            eprintln!();
            eprintln!("提示:");
            eprintln!("  - 请确保服务器已启动（运行 hybrid_server 示例）");
            eprintln!("  - WebSocket 服务器应在 ws://127.0.0.1:8080");
            eprintln!("  - QUIC 服务器应在 quic://127.0.0.1:8081");
            eprintln!("  - 检查防火墙设置");
            return Err(format!("连接失败: {:?}", e).into());
        }
    }
    
    Ok(())
}
