//! WebSocket 聊天室客户端
//! 
//! 实现一个简单的聊天室客户端，支持发送和接收消息
//! 
//! 注意：此示例使用纯 WebSocket 连接（ws://），不使用 TLS/SSL

use flare_core::common::client_trait::Client;
use flare_core::common::config::ClientConfig;
use flare_core::common::protocol::{frame_with_message_command, send_message, generate_message_id, Reliability};
use flare_core::common::protocol::flare::core::commands::command::Type;
use flare_core::transport::events::{ConnectionEvent, ConnectionObserver};
use flare_core::UnifiedClient;
use std::sync::Arc;
use std::io::{self, Write};
use tokio::io::{AsyncBufReadExt, BufReader};

// 消息观察者，用于接收和显示聊天消息
struct ChatObserver {
    username: String,
}

impl ConnectionObserver for ChatObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
                // 解析接收到的消息
                if let Ok(frame) = flare_core::common::message_parser::MessageParser::new(
                    flare_core::common::protocol::SerializationFormat::Protobuf,
                    flare_core::common::compression::CompressionAlgorithm::None,
                ).parse(data) {
                    if let Some(cmd) = &frame.command {
                        if let Some(Type::Message(msg_cmd)) = &cmd.r#type {
                            let message = String::from_utf8_lossy(&msg_cmd.payload);
                            
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
                println!("\n[系统] 已连接到聊天室服务器！");
                print!("{}> ", self.username);
                let _ = io::stdout().flush();
            }
            ConnectionEvent::Disconnected(reason) => {
                println!("\n[系统] 连接已断开: {}", reason);
            }
            ConnectionEvent::Error(e) => {
                eprintln!("\n[错误] {:?}", e);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== WebSocket 聊天室客户端 ===");
    
    // 获取用户名
    print!("请输入您的用户名: ");
    io::stdout().flush()?;
    let mut username = String::new();
    io::stdin().read_line(&mut username)?;
    let username = username.trim().to_string();
    
    if username.is_empty() {
        println!("用户名不能为空，使用默认用户名: 匿名用户");
        return Err("用户名不能为空".into());
    }
    
    println!("正在连接到聊天室服务器...");
    
    // 使用 ws:// 协议（非 wss://），确保不使用 TLS
    let ws_config = ClientConfig::new("ws://127.0.0.1:8080".to_string())
        .websocket()
        .with_format(flare_core::common::protocol::SerializationFormat::Protobuf);
    
    match UnifiedClient::connect_with_config(ws_config).await {
        Ok(mut ws_client) => {
            println!("连接成功！");
            println!("使用的协议: {:?}", ws_client.active_protocol());
            
            // 创建并添加消息观察者
            let observer = Arc::new(ChatObserver {
                username: username.clone(),
            });
            ws_client.add_observer(Arc::clone(&observer) as Arc<dyn ConnectionObserver>);
            
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
            
            let _ = ws_client.send_frame(&init_frame).await;
            
            println!("\n欢迎来到聊天室！输入消息后按回车发送，输入 '/quit' 退出");
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
                                
                                // 检查退出命令
                                if input == "/quit" || input == "/exit" {
                                    println!("退出聊天室...");
                                    break;
                                }
                                
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
                                
                                if let Err(e) = ws_client.send_frame(&chat_frame).await {
                                    eprintln!("\n[错误] 发送消息失败: {}", e);
                                    break;
                                }
                                
                                print!("{}> ", username);
                                let _ = io::stdout().flush();
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
            let _ = ws_client.disconnect().await;
            println!("已断开连接");
        }
        Err(e) => {
            eprintln!("连接失败: {:?}", e);
        }
    }
    
    Ok(())
}
