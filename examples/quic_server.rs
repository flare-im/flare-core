//! QUIC 聊天室服务端
//! 
//! 实现一个简单的聊天室，所有连接的用户都可以发送和接收消息
//! 
//! 注意：此示例使用 QUIC 协议，需要 TLS 证书
//! 证书会通过 rcgen 自动生成并保存到 certs/ 目录下

use flare_core::common::config::{ServerConfig, TransportProtocol};
use flare_core::common::protocol::{Frame, frame_with_message_command, send_message, generate_message_id, Reliability};
use flare_core::common::protocol::flare::core::commands::command::Type;
use flare_core::common::server_trait::{Server, ConnectionHandler};
use flare_core::common::error::Result;
use flare_core::UnifiedServer;
use std::sync::Arc;
use std::collections::HashMap;
use async_trait::async_trait;
use tracing::{debug, info, error, warn};

// 聊天室连接处理器
struct ChatRoomHandler {
    // 存储连接ID到用户名的映射
    usernames: Arc<tokio::sync::Mutex<HashMap<String, String>>>,
    // 服务器引用，用于广播消息（延迟设置）
    // 使用 Weak 引用避免循环引用
    server: Arc<tokio::sync::Mutex<Option<std::sync::Weak<ServerWrapper>>>>,
}

impl ChatRoomHandler {
    fn new() -> Self {
        Self {
            usernames: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            server: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }
    
    async fn set_server(&self, server: std::sync::Weak<ServerWrapper>) {
        *self.server.lock().await = Some(server);
    }
    
    // 广播消息给所有连接的客户端（排除发送者）
    async fn broadcast_message_except(&self, frame: &Frame, exclude_connection_id: &str) {
        debug!("broadcast_message_except 开始: exclude={}", exclude_connection_id);
        // 使用 Weak 引用，避免循环引用
        let server_weak = {
            let server_guard = self.server.lock().await;
            server_guard.clone()
        };
        
        if let Some(server_weak) = server_weak {
            if let Some(server) = server_weak.upgrade() {
                debug!("broadcast_message_except: 使用 broadcast_except 排除发送者");
                if let Err(e) = server.broadcast_except(frame, exclude_connection_id).await {
                    error!("[聊天室] 广播消息失败: {}", e);
                } else {
                    debug!("broadcast_message_except: 广播成功（已排除发送者）");
                }
            } else {
                debug!("broadcast_message_except: Weak 引用升级失败，服务器可能已关闭");
            }
        } else {
            warn!("[聊天室] 警告：服务器引用未设置，无法广播消息");
        }
        debug!("broadcast_message_except 完成");
    }
    
    // 广播消息给所有连接的客户端（已废弃，保留用于兼容）
    async fn broadcast_message(&self, frame: &Frame) {
        debug!("broadcast_message 开始（已废弃，应使用 broadcast_message_except）");
        // 使用 Weak 引用，避免循环引用
        let server_weak = {
            let server_guard = self.server.lock().await;
            server_guard.clone()
        };
        
        if let Some(server_weak) = server_weak {
            if let Some(server) = server_weak.upgrade() {
                if let Err(e) = server.broadcast(frame).await {
                    error!("[聊天室] 广播消息失败: {}", e);
                }
            }
        } else {
            warn!("[聊天室] 警告：服务器引用未设置，无法广播消息");
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
        
        // 不在连接建立时立即广播，避免死锁
        // 广播将在用户首次发送消息时触发，或者可以完全移除广播功能
        // 这里只记录连接，不广播
        
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
        
        // 不在断开连接时立即广播，避免死锁
        // 可以在 handle_frame 中处理断开通知，或者完全移除
        
        Ok(())
    }
}

// 包装器，用于设置服务器引用并代理到内部 handler
struct ChatRoomHandlerWrapper {
    inner: Arc<ChatRoomHandler>,
}

#[async_trait]
impl ConnectionHandler for ChatRoomHandlerWrapper {
    async fn handle_frame(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        self.inner.handle_frame(frame, connection_id).await
    }
    
    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        self.inner.on_connect(connection_id).await
    }
    
    async fn on_disconnect(&self, connection_id: &str) -> Result<()> {
        self.inner.on_disconnect(connection_id).await
    }
}

// 包装 UnifiedServer 使其可以作为 Arc<dyn Server> 使用
// 关键：不要嵌套 block_in_place，因为 UnifiedServer 的方法已经处理了异步上下文
struct ServerWrapper {
    server: Arc<tokio::sync::Mutex<UnifiedServer>>,
}

#[async_trait]
impl Server for ServerWrapper {
    async fn start(&mut self) -> Result<()> {
        let mut s = self.server.lock().await;
        s.start().await
    }
    
    async fn stop(&mut self) -> Result<()> {
        let mut s = self.server.lock().await;
        s.stop().await
    }
    
    async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        let s = self.server.lock().await;
        Server::send_to(&*s, connection_id, frame).await
    }
    
    async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        let s = self.server.lock().await;
        Server::send_to_user(&*s, user_id, frame).await
    }
    
    async fn broadcast(&self, frame: &Frame) -> Result<()> {
        let s = self.server.lock().await;
        Server::broadcast(&*s, frame).await
    }
    
    async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        let s = self.server.lock().await;
        Server::broadcast_except(&*s, frame, exclude_connection_id).await
    }
    
    fn is_running(&self) -> bool {
        // 使用 block_in_place 来安全地从异步运行时调用阻塞操作
        // 这是推荐的方式，避免在异步上下文中直接调用 blocking_lock
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
            s.is_running()
        })
    }
    
    fn connection_count(&self) -> usize {
        // 关键：UnifiedServer::connection_count() 内部已经使用了 block_in_place
        // 如果我们在这里再次使用 block_in_place，会导致嵌套，可能有问题
        // 解决方案：直接使用 blocking_lock，但只在非异步上下文中调用此方法
        // 或者，我们可以使用 spawn_blocking 来避免嵌套
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
            // UnifiedServer::connection_count() 内部会再次使用 block_in_place
            // 这可能导致嵌套问题，但应该可以工作，因为 block_in_place 可以嵌套
            s.connection_count()
        })
    }
    
    fn user_count(&self) -> usize {
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
            s.user_count()
        })
    }
    
    async fn disconnect(&self, connection_id: &str) -> Result<()> {
        let s = self.server.lock().await;
        Server::disconnect(&*s, connection_id).await
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // 初始化 tracing
    // 默认日志级别可以通过环境变量 RUST_LOG 设置
    // 例如：RUST_LOG=debug cargo run --example quic_server
    // 或者：RUST_LOG=flare_core=debug,quic_server=debug
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();
    
    info!("=== QUIC 聊天室服务端 ===");
    
    // 创建 handler
    let handler = Arc::new(ChatRoomHandler::new());
    let handler_for_setup = Arc::clone(&handler);
    
    // 创建包装器
    let wrapper = Arc::new(ChatRoomHandlerWrapper {
        inner: Arc::clone(&handler),
    });
    
    // 仅监听 QUIC 协议
    let quic_config = ServerConfig::new("0.0.0.0:8081".to_string())
        .with_protocols(vec![TransportProtocol::QUIC])
        .with_max_connections(2000);
    
    let quic_server = UnifiedServer::new(quic_config, wrapper as Arc<dyn ConnectionHandler>)?;
    
    // 创建包装器（在启动前创建，确保 handler 可以立即使用）
    let server_wrapper = Arc::new(ServerWrapper {
        server: Arc::new(tokio::sync::Mutex::new(quic_server)),
    });
    
    // 先设置服务器引用到 handler，使用 Weak 引用避免循环引用
    handler_for_setup.set_server(Arc::downgrade(&server_wrapper)).await;
    
    // 现在启动服务器
    {
        let mut s = server_wrapper.server.lock().await;
        if let Err(e) = s.start().await {
            error!("❌ 服务器启动失败: {:?}", e);
            error!("提示: 可能端口 8081 已被占用，请先关闭占用该端口的进程");
            return Err(format!("服务器启动失败: {:?}", e).into());
        }
        
        // 验证服务器是否真的在运行（使用异步方式）
        // 注意：不要在持有锁的情况下调用同步方法，先释放锁
        let is_running = {
            drop(s); // 显式释放锁
            server_wrapper.is_running()
        };
        if !is_running {
            error!("❌ 服务器启动后未处于运行状态");
            return Err("服务器未正常运行".into());
        }
    }
    
    info!("✅ 聊天室服务器已启动：0.0.0.0:8081");
    info!("使用 quic:// 协议连接（需要 TLS 证书）");
    info!("证书文件位置: certs/server.crt 和 certs/server.key");
    
    // 获取连接数（直接调用 ServerWrapper 的方法，它会内部处理锁）
    let conn_count = server_wrapper.connection_count();
    info!("当前在线用户: {}", conn_count);
    info!("\n服务器运行中，按 Ctrl+C 停止...");
    
    // 定期打印连接数（在异步任务中，使用 spawn_blocking 避免问题）
    let server_wrapper_clone = Arc::clone(&server_wrapper);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            // 使用 spawn_blocking 来安全地调用同步方法
            let conn_count = tokio::task::spawn_blocking({
                let server = Arc::clone(&server_wrapper_clone);
                move || server.connection_count()
            }).await.unwrap_or(0);
            if conn_count > 0 {
                info!("当前在线用户: {}", conn_count);
            }
        }
    });
    
    tokio::signal::ctrl_c().await?;
    
    info!("\n正在停止服务器...");
    {
        let mut s = server_wrapper.server.lock().await;
        s.stop().await?;
    }
    
    info!("服务器已停止");
    Ok(())
}

