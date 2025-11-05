//! 混合服务端聊天室示例
//! 
//! 使用观察者模式的 Builder（ObserverServerBuilder）构建服务端
//! 同时监听 WebSocket 和 QUIC 协议，实现多用户聊天室
//! 
//! 注意：QUIC 协议需要 TLS 证书，WebSocket 使用纯 ws:// 协议
//! 
//! 此示例展示了如何：
//! 1. 实现 ConnectionHandler trait 来处理消息
//! 2. 使用 ObserverServerBuilder 创建服务器（支持多协议）
//! 3. 使用 DefaultServerHandle 进行消息发送和连接管理

use flare_core::server::{ConnectionHandler, ObserverServerBuilder};
use flare_core::server::handle::{ServerHandle, DefaultServerHandle};
use flare_core::common::config_types::TransportProtocol;
use flare_core::common::protocol::{Frame, frame_with_message_command, send_message, generate_message_id, Reliability};
use flare_core::common::protocol::flare::core::commands::command::Type;
use flare_core::common::error::Result;
use std::sync::Arc;
use std::collections::HashMap;
use async_trait::async_trait;
use tracing::{debug, info, error};

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
        debug!("broadcast_message_except 开始: exclude={}", exclude_connection_id);
        let handle = {
            let handle_guard = self.server_handle.lock().await;
            handle_guard.clone()
        };
        
        if let Some(ref handle) = handle {
            if let Err(e) = handle.broadcast_except(frame, exclude_connection_id).await {
                error!("[聊天室] 广播消息失败: {}", e);
            } else {
                debug!("broadcast_message_except: 广播成功（已排除发送者）");
            }
        } else {
            error!("[聊天室] 警告：服务器处理器未设置，无法广播消息");
        }
        debug!("broadcast_message_except 完成");
    }
    
    // 广播消息给所有连接的客户端
    async fn broadcast_message(&self, frame: &Frame) {
        debug!("broadcast_message 开始");
        let handle = {
            let handle_guard = self.server_handle.lock().await;
            handle_guard.clone()
        };
        
        if let Some(ref handle) = handle {
            if let Err(e) = handle.broadcast(frame).await {
                error!("[聊天室] 广播消息失败: {}", e);
            }
        } else {
            error!("[聊天室] 警告：服务器处理器未设置，无法广播消息");
        }
    }
    
    async fn broadcast_notification(&self, message: String, notification_type: &str) {
        let mut metadata = HashMap::new();
        metadata.insert("username".to_string(), "系统".as_bytes().to_vec());
        metadata.insert("type".to_string(), notification_type.as_bytes().to_vec());
        
        let notification = send_message(
            generate_message_id(),
            message.into_bytes(),
            Some(metadata),
            None,
        );
        
        let notification_frame = frame_with_message_command(
            notification,
            Reliability::BestEffort,
        );
        
        // 系统通知不需要排除任何连接，广播给所有人
        self.broadcast_message(&notification_frame).await;
    }
}

#[async_trait]
impl ConnectionHandler for ChatRoomHandler {
    async fn handle_frame(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        // 检查是否是消息命令
        if let Some(cmd) = &frame.command {
            if let Some(Type::Message(msg_cmd)) = &cmd.r#type {
                // 提取消息内容
                let message_text = String::from_utf8_lossy(&msg_cmd.payload);
                
                // 获取或创建用户名
                let username = {
                    let mut usernames = self.usernames.lock().await;
                    usernames.entry(connection_id.to_string())
                        .or_insert_with(|| {
                            // 如果消息包含用户名信息，提取用户名
                            if let Some(username_bytes) = msg_cmd.metadata.get("username") {
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
                broadcast_metadata.insert("connection_id".to_string(), connection_id.as_bytes().to_vec());
                
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
                self.broadcast_message_except(&broadcast_frame, connection_id).await;
                
                // 不返回给单个连接，因为已经广播了
                return Ok(None);
            }
        }
        
        // 其他类型的消息不处理
        Ok(None)
    }
    
    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        debug!("on_connect 开始: connection_id={}", connection_id);
        info!("[聊天室] ✅ 用户 {} 加入聊天室", &connection_id[..8.min(connection_id.len())]);
        
        debug!("on_connect 完成: connection_id={}", connection_id);
        Ok(())
    }
    
    async fn on_disconnect(&self, connection_id: &str) -> Result<()> {
        let username = {
            let mut usernames = self.usernames.lock().await;
            usernames.remove(connection_id)
        };
        
        let display_name = username.as_deref()
            .unwrap_or(&connection_id[..8.min(connection_id.len())]);
        info!("[聊天室] ❌ {} 离开了聊天室", display_name);
        
        Ok(())
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // 初始化 tracing（默认使用 debug 级别，方便调试）
    // 可以通过环境变量 RUST_LOG 覆盖：RUST_LOG=info cargo run --example hybrid_server
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug"))
        )
        .init();
    
    info!("=== 混合服务端聊天室（WebSocket + QUIC）===");
    
    // 创建一个共享的 ConnectionManager
    let connection_manager = Arc::new(flare_core::server::connection::ConnectionManager::new());
    
    // 创建 handler
    let handler = Arc::new(ChatRoomHandler::new());
    let handler_for_setup = Arc::clone(&handler);
    
    // 使用 ObserverServerBuilder 创建服务器
    // 为每个协议配置独立的地址
    let mut observer_server = ObserverServerBuilder::new("0.0.0.0:8080")
        .with_handler(handler as Arc<dyn ConnectionHandler>)
        .with_connection_manager(connection_manager)
        .with_protocols(vec![TransportProtocol::WebSocket, TransportProtocol::QUIC])
        .with_protocol_address(TransportProtocol::WebSocket, "0.0.0.0:8080".to_string())
        .with_protocol_address(TransportProtocol::QUIC, "0.0.0.0:8081".to_string())
        .with_max_connections(2000)
        .build()?;
    
    // 从 ObserverServer 获取连接管理器和解析器，创建 DefaultServerHandle
    let server_handle: Arc<dyn ServerHandle> = if let Some((manager_trait, parser)) = observer_server.get_server_handle_components() {
        Arc::new(DefaultServerHandle::new(manager_trait, parser))
    } else {
        return Err("无法获取连接管理器和解析器".into());
    };
    
    // 设置服务器处理器到 handler
    handler_for_setup.set_server_handle(server_handle).await;
    
    // 启动服务器
    observer_server.start().await?;
    
    info!("✅ 聊天室服务器已启动");
    info!("   - WebSocket: ws://0.0.0.0:8080");
    info!("   - QUIC: quic://0.0.0.0:8081");
    
    let protocols = observer_server.protocols();
    info!("支持的协议: {:?}", protocols);
    
    // 获取连接数
    let conn_count = observer_server.connection_count();
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
    observer_server.stop().await?;
    
    info!("服务器已停止");
    Ok(())
}
