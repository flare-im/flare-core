//! WebSocket 服务端实现
//!
//! 提供基于 WebSocket 协议的服务端功能实现
//!
//! # 特性
//!
//! - 基于 [tokio-tungstenite](https://crates.io/crates/tokio-tungstenite) 库实现
//! - 支持 TLS 加密
//! - 良好的浏览器兼容性
//! - 与连接管理器无缝集成
//! - 支持两阶段认证（连接建立 -> 身份认证 -> 加入连接管理器）
//!
//! # 核心组件
//!
//! - [WebSocketServer](struct.WebSocketServer.html): WebSocket 服务端主类
//! - 使用 [ConnectionManager](../manager/traits/trait.ConnectionManager.html) 管理连接
//!
//! # 使用示例
//!
//! ```rust
//! use std::sync::Arc;
//! use flare_core::{
//!     server::{
//!         websocket::WebSocketServer,
//!         manager::ConnectionBasedManager,
//!     },
//!     common::connections::types::ConnectionConfig,
//! };
//!
//! // 创建连接管理器
//! let connection_manager = Arc::new(ConnectionBasedManager::new());
//! 
//! // 创建配置
//! let config = ConnectionConfig::server("ws_server".to_string(), "127.0.0.1:8080".to_string());
//! 
//! // 创建 WebSocket 服务端
//! let server = WebSocketServer::new(config, connection_manager);
//! ```

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, error, warn, debug};
use std::net::SocketAddr;

use crate::common::{
    error::Result,
    connections::{
        types::ConnectionConfig,
        traits::ConnectionEvent,
        factory::RawConnectionHandler,
    },
    protocol::{Frame, MessageType},
};

use super::{
    service::{ServerService, MessageHandler},
    manager::traits::ConnectionManager,
    auth::AuthManager,
};

/// WebSocket 服务端实现
///
/// 负责处理 WebSocket 协议的连接和消息
///
/// # 泛型参数
///
/// * `T` - 连接管理器类型，必须实现 [ConnectionManager](../manager/traits/trait.ConnectionManager.html) trait
pub struct WebSocketServer<T: ConnectionManager> {
    /// 配置
    config: ConnectionConfig,
    /// 连接管理器
    connection_manager: Arc<T>,
    /// 认证管理器
    auth_manager: Arc<AuthManager>,
    /// 消息处理器
    message_handler: Arc<RwLock<Option<Arc<dyn MessageHandler>>>>,
    /// 服务句柄
    server_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

impl<T: ConnectionManager + 'static> WebSocketServer<T> {
    /// 创建新的 WebSocket 服务端
    ///
    /// # 参数
    ///
    /// * `config` - 连接配置
    /// * `connection_manager` - 连接管理器
    /// * `auth_manager` - 认证管理器
    ///
    /// # 返回值
    ///
    /// 返回新的 [WebSocketServer](struct.WebSocketServer.html) 实例
    pub fn new(
        config: ConnectionConfig,
        connection_manager: Arc<T>,
        auth_manager: Arc<AuthManager>,
    ) -> Self {
        Self {
            config,
            connection_manager,
            auth_manager,
            message_handler: Arc::new(RwLock::new(None)),
            server_handle: Arc::new(RwLock::new(None)),
        }
    }
}

#[async_trait::async_trait]
impl<T: ConnectionManager + 'static> ServerService for WebSocketServer<T> {
    /// 启动 WebSocket 服务
    ///
    /// 创建 TCP 监听器并开始监听客户端连接
    ///
    /// # 返回值
    ///
    /// 返回操作结果
    async fn start(&self) -> Result<()> {
        let local_addr = self.config.local_addr.clone().unwrap_or_default();
        info!("启动 WebSocket 服务: {}", local_addr);
        
        // 解析地址
        let addr: SocketAddr = local_addr.parse().map_err(|e| {
            crate::common::error::FlareError::connection_failed(format!("地址解析失败: {}", e))
        })?;
        
        // 创建 TCP 监听器
        let listener = tokio::net::TcpListener::bind(addr).await
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("绑定端口失败: {}", e)))?;
        
        // 克隆必要的组件
        let connection_manager = Arc::clone(&self.connection_manager);
        let auth_manager = Arc::clone(&self.auth_manager);
        let message_handler = Arc::clone(&self.message_handler);
        let config = self.config.clone();
        
        // 启动服务任务
        let handle = tokio::spawn(async move {
            info!("WebSocket 服务已启动: {}", local_addr);
            
            // 监听新的客户端连接
            loop {
                match listener.accept().await {
                    Ok((tcp_stream, addr)) => {
                        info!("WebSocket客户端已连接: {}", addr);
                        
                        // 克隆组件
                        let connection_config = config.clone();
                        let connection_manager = Arc::clone(&connection_manager);
                        let auth_manager = Arc::clone(&auth_manager);
                        let message_handler = Arc::clone(&message_handler);
                        
                        // 为每个连接创建独立的任务
                        tokio::spawn(async move {
                            // 创建事件处理器
                            let connection_event_handler = Arc::new(SimpleEventHandler::new(
                                format!("WebSocket服务端-{}", addr)
                            ));
                            
                            // 创建服务端连接
                            match RawConnectionHandler::from_websocket_with_handler_arc(
                                tcp_stream, 
                                connection_config, 
                                connection_event_handler as Arc<dyn ConnectionEvent>
                            ).await {
                                Ok(connection_arc) => {
                                    let connection_id = connection_arc.get_id().to_string();
                                    info!("WebSocket 服务端连接已建立: {} (ID: {})", addr, connection_id);
                                    
                                    // 将连接添加到认证管理器
                                    auth_manager.add_pending_connection(connection_arc.clone()).await;
                                    
                                    // 启动消息处理循环
                                    let msg_handler = message_handler.read().await.clone();
                                    tokio::spawn(async move {
                                        let connection_id = connection_id.clone();
                                        loop {
                                            match connection_arc.receive_message().await {
                                                Ok(Some(frame)) => {
                                                    // 检查连接是否已认证
                                                    match auth_manager.is_authenticated(&connection_id).await {
                                                        Some(auth_info) => {
                                                            match auth_info.status {
                                                                super::auth::AuthStatus::Authenticated(_) => {
                                                                    // 已认证，正常处理消息
                                                                    if let Some(handler) = &msg_handler {
                                                                        match handler.handle_message(connection_id.clone(), frame.clone()).await {
                                                                            Ok(Some(response)) => {
                                                                                // 发送响应
                                                                                if let Err(e) = connection_arc.send_message(response).await {
                                                                                    error!("发送响应消息失败: {}", e);
                                                                                }
                                                                            }
                                                                            Ok(None) => {
                                                                                    // 不需要响应
                                                                                    debug!("消息处理完成，无需响应");
                                                                            }
                                                                            Err(e) => {
                                                                                    error!("处理消息失败: {}", e);
                                                                            }
                                                                        }
                                                                    } else {
                                                                        // 默认回显消息
                                                                        if let Err(e) = connection_arc.send_message(frame).await {
                                                                            error!("发送回显消息失败: {}", e);
                                                                        }
                                                                    }
                                                                }
                                                                super::auth::AuthStatus::Pending => {
                                                                    // 待认证，检查是否为认证消息
                                                                    if frame.message_type == MessageType::Connect {
                                                                        // 检查是否包含平台信息
                                                                        if let Some(platform_data) = frame.metadata.as_ref().and_then(|m| m.get("platform")) {
                                                                            if let Ok(platform_str) = std::str::from_utf8(platform_data) {
                                                                                let platform = super::auth::Platform::from_str(platform_str);
                                                                                let device_id = frame.metadata.as_ref().and_then(|m| m.get("device_id"))
                                                                                        .and_then(|d| std::str::from_utf8(d).ok())
                                                                                        .map(|s| s.to_string());
                                                                                let app_version = frame.metadata.as_ref().and_then(|m| m.get("app_version"))
                                                                                        .and_then(|d| std::str::from_utf8(d).ok())
                                                                                        .map(|s| s.to_string());
                                                                                    
                                                                                    // 设置平台信息
                                                                                    auth_manager.set_connection_platform(
                                                                                        &connection_id, 
                                                                                        platform, 
                                                                                        device_id, 
                                                                                        app_version
                                                                                    ).await;
                                                                                }
                                                                            }
                                                                            
                                                                            // 处理认证消息
                                                                            match auth_manager.handle_auth_message(&connection_id, frame.payload.clone()).await {
                                                                                Ok(super::auth::AuthStatus::Authenticated(user_id)) => {
                                                                                    // 认证成功，将连接添加到连接管理器
                                                                                    if let Some(_auth_info) = auth_manager.remove_authenticated(&connection_id).await {
                                                                                        // 可以在这里将用户ID与连接关联
                                                                                        info!("连接认证成功并加入管理器: {} -> 用户: {}", connection_id, user_id);
                                                                                        if let Err(e) = connection_manager.add_connection(connection_arc).await {
                                                                                            error!("添加连接到管理器失败: {}", e);
                                                                                        }
                                                                                        // 连接已移动到连接管理器，退出认证循环
                                                                                        break;
                                                                                    }
                                                                                }
                                                                                Ok(super::auth::AuthStatus::Failed) => {
                                                                                    // 认证失败，断开连接
                                                                                    warn!("认证失败，断开连接: {}", connection_id);
                                                                                    let _ = connection_arc.close().await;
                                                                                    break;
                                                                                }
                                                                                _ => {
                                                                                    // 其他状态，继续等待
                                                                                    debug!("等待认证: {}", connection_id);
                                                                                }
                                                                            }
                                                                        } else {
                                                                            // 非认证消息，发送错误响应
                                                                            let error_frame = crate::common::protocol::Frame::error(
                                                                                401, 
                                                                                "需要认证后才能发送消息"
                                                                            );
                                                                            if let Err(e) = connection_arc.send_message(error_frame).await {
                                                                                error!("发送认证错误消息失败: {}", e);
                                                                            } else {
                                                                                info!("已向未认证连接发送认证错误响应: {}", connection_id);
                                                                            }
                                                                        }
                                                                    }
                                                                    _ => {
                                                                        // 其他状态，断开连接
                                                                        warn!("认证状态异常，断开连接: {}", connection_id);
                                                                        let _ = connection_arc.close().await;
                                                                        break;
                                                                    }
                                                                }
                                                            }
                                                            None => {
                                                                // 连接不在待认证列表中，可能是已移除或超时
                                                                warn!("未知连接，断开连接: {}", connection_id);
                                                                let _ = connection_arc.close().await;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    Ok(None) => {
                                                        // 连接已关闭
                                                        info!("连接已关闭: {}", addr);
                                                        break;
                                                    }
                                                    Err(e) => {
                                                        error!("接收消息失败: {}", e);
                                                        break;
                                                    }
                                                }
                                                
                                                // 检查连接是否还活跃
                                                if !connection_arc.is_active().await {
                                                    info!("连接已断开: {}", addr);
                                                    break;
                                                }
                                            }
                                            
                                            // 从认证管理器中移除连接（如果还在的话）
                                            let _ = auth_manager.remove_authenticated(&connection_id).await;
                                        });
                                }
                                Err(e) => {
                                    error!("创建WebSocket服务端连接失败: {} - {}", addr, e);
                                }
                            }
                        });
                    }
                    Err(e) => {
                        error!("接受连接失败: {}", e);
                        // 短暂等待后继续监听
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
            }
        });
        
        // 保存服务句柄
        *self.server_handle.write().await = Some(handle);
        
        Ok(())
    }
    
    /// 停止 WebSocket 服务
    ///
    /// 停止服务任务
    async fn stop(&self) {
        info!("停止 WebSocket 服务");
        
        // 停止服务任务
        if let Some(handle) = self.server_handle.write().await.take() {
            handle.abort();
        }
    }
    
    /// 设置消息处理器
    ///
    /// 为服务设置消息处理器
    ///
    /// # 参数
    ///
    /// * `handler` - 消息处理器
    async fn set_message_handler(&self, handler: Arc<dyn MessageHandler>) {
        *self.message_handler.write().await = Some(handler);
    }
}

/// 简单事件处理器
#[derive(Debug)]
struct SimpleEventHandler {
    name: String,
}

impl SimpleEventHandler {
    fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait::async_trait]
impl ConnectionEvent for SimpleEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] 连接已建立: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] 连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] 连接错误: {} - 错误: {}", self.name, connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        if message.is_heartbeat() {
            info!("[{}] 收到心跳消息: {}", self.name, connection_id);
        } else {
            info!("[{}] 收到消息: {}", self.name, connection_id);
        }
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        if message.is_heartbeat() {
            info!("[{}] 心跳消息已发送: {}", self.name, connection_id);
        } else {
            info!("[{}] 消息已发送: {}", self.name, connection_id);
        }
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("[{}] 心跳超时: {}", self.name, connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] 连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        info!("[{}] 心跳已发送: {}", self.name, connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        info!("[{}] 收到心跳: {}", self.name, connection_id);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        info!("[{}] 开始重连: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        info!("[{}] 重连成功: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        info!("[{}] 重连失败: {} - 尝试次数: {} - 错误: {}", self.name, connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &crate::common::connections::traits::ConnectionStats) {
        info!("[{}] 统计信息更新: {} - 收到消息: {} - 发送消息: {}", 
             self.name, connection_id, stats.messages_received, stats.messages_sent);
    }
}