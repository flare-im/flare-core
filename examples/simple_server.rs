//! 简化的 WebSocket 聊天室服务端示例
//! 
//! 使用简单模式的 Builder（ServerBuilder）构建服务端
//! 只需要简单的配置和消息处理函数即可完成长链接通信
//! 
//! 实现一个简单的聊天室，所有连接的用户都可以发送和接收消息
//! 注意：此示例使用纯 WebSocket 连接（ws://），不使用 TLS/SSL
//! 
//! 此示例展示了如何：
//! 1. 使用闭包定义消息处理逻辑（无需实现 trait）
//! 2. 使用 ServerBuilder 快速创建服务器
//! 3. 通过 MessageContext 进行广播等操作

use flare_core::server::ServerBuilder;
use flare_core::common::protocol::{frame_with_message_command, send_message, generate_message_id, Reliability};
use flare_core::common::protocol::flare::core::commands::command::Type;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, error, debug};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // 初始化 tracing（默认使用 debug 级别，方便调试）
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug"))
        )
        .init();

    info!("=== 简化的 WebSocket 聊天室服务端示例 ===");

    // 存储连接ID到用户名的映射
    let usernames: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

    // 使用 ServerBuilder 创建服务端，只需定义消息处理逻辑
    let usernames_for_message = Arc::clone(&usernames);
    let usernames_for_connect = Arc::clone(&usernames);
    let usernames_for_disconnect = Arc::clone(&usernames);
    
    let mut server = ServerBuilder::new("0.0.0.0:8080")
        // 处理接收到的消息
        .on_message(move |frame, ctx| {
            let usernames = Arc::clone(&usernames_for_message);
            Box::pin(async move {
                // 检查是否是消息命令
                if let Some(cmd) = &frame.command {
                    if let Some(Type::Message(msg_cmd)) = &cmd.r#type {
                        // 提取消息内容
                        let message_text = String::from_utf8_lossy(&msg_cmd.payload);
                        
                        // 获取或创建用户名
                        let username = {
                            let mut usernames = usernames.lock().await;
                            usernames.entry(ctx.connection_id.clone())
                                .or_insert_with(|| {
                                    // 如果消息包含用户名信息，提取用户名
                                    if let Some(username_bytes) = msg_cmd.metadata.get("username") {
                                        String::from_utf8_lossy(username_bytes).to_string()
                                    } else {
                                        format!("用户_{}", &ctx.connection_id[..8.min(ctx.connection_id.len())])
                                    }
                                })
                                .clone()
                        };
                        
                        info!("[聊天室] {} 说: {}", username, message_text);
                        
                        // 构建广播消息（包含用户名）
                        let mut broadcast_metadata = HashMap::new();
                        broadcast_metadata.insert("username".to_string(), username.as_bytes().to_vec());
                        broadcast_metadata.insert("connection_id".to_string(), ctx.connection_id.as_bytes().to_vec());
                        
                        let broadcast_msg = send_message(
                            generate_message_id(),
                            format!("[{}] {}", username, message_text).into_bytes(),
                            Some(broadcast_metadata),
                            None,
                        );
                        
                        let broadcast_frame = frame_with_message_command(
                            broadcast_msg,
                            Reliability::BestEffort,
                        );
                        
                        // 广播给除发送者外的所有连接
                        let conn_id = ctx.connection_id.clone();
                        if let Err(e) = ctx.broadcast_except(&broadcast_frame, &conn_id).await {
                            error!("广播消息失败: {}", e);
                        }
                    }
                }
                Ok(None)
            })
        })
        // 处理连接建立事件
        .on_connect(move |conn_id, _ctx| {
            let usernames = Arc::clone(&usernames_for_connect);
            Box::pin(async move {
                let conn_id = conn_id.to_string();
                debug!("on_connect 开始: connection_id={}", conn_id);
                info!("[聊天室] ✅ 用户 {} 加入聊天室", &conn_id[..8.min(conn_id.len())]);
                
                // 初始化用户名（使用默认名称）
                {
                    let mut usernames = usernames.lock().await;
                    usernames.entry(conn_id.clone())
                        .or_insert_with(|| format!("用户_{}", &conn_id[..8.min(conn_id.len())]));
                }
                
                debug!("on_connect 完成: connection_id={}", conn_id);
                Ok(())
            })
        })
        // 处理连接断开事件
        .on_disconnect(move |conn_id, _ctx| {
            let usernames = Arc::clone(&usernames_for_disconnect);
            Box::pin(async move {
                let conn_id = conn_id.to_string();
                let username = {
                    let mut usernames = usernames.lock().await;
                    usernames.remove(&conn_id)
                };

                let display_name = username.as_deref()
                    .unwrap_or(&conn_id[..8.min(conn_id.len())]);
                info!("[聊天室] ❌ {} 离开了聊天室", display_name);
                Ok(())
            })
        })
        .build()?;

    // 启动服务器
    server.start().await?;
    info!("✅ 聊天室服务器已启动：0.0.0.0:8080");
    info!("使用 ws:// 协议连接（非 wss://）");
    
    // 获取连接数
    let conn_count = server.connection_count();
    info!("当前在线用户: {}", conn_count);
    info!("\n服务器运行中，按 Ctrl+C 停止...");

    // 定期打印连接数
    let server_clone = Arc::new(tokio::sync::Mutex::new(server));
    let server_clone_for_task = Arc::clone(&server_clone);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            let server = server_clone_for_task.lock().await;
            let conn_count = server.connection_count();
            if conn_count > 0 {
                info!("当前在线用户: {}", conn_count);
            }
        }
    });

    // 等待停止信号
    tokio::signal::ctrl_c().await?;

    info!("\n正在停止服务器...");
    {
        let mut server = server_clone.lock().await;
        server.stop().await?;
    }
    info!("服务器已停止");

    Ok(())
}

