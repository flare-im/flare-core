//! 简单模式服务端构建器
//! 
//! 使用闭包定义消息处理逻辑

use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::server::{ServerConfig, ConnectionHandler, HybridServer, Server};
use std::sync::{Arc, Weak};
use async_trait::async_trait;
use tokio::sync::Mutex;

/// 消息处理上下文
/// 
/// 提供给消息处理函数的上下文，包含连接信息和服务器引用
pub struct MessageContext {
    /// 连接 ID
    pub connection_id: String,
    /// 服务器弱引用（用于广播等操作）
    server: Weak<ServerWrapper>,
}

impl MessageContext {
    /// 创建新的消息上下文
    fn new(connection_id: String, server: Weak<ServerWrapper>) -> Self {
        Self {
            connection_id,
            server,
        }
    }

    /// 向指定连接发送消息
    pub async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        if let Some(server) = self.server.upgrade() {
            server.send_to(connection_id, frame).await
        } else {
            Err(crate::common::error::FlareError::general_error("Server is not available"))
        }
    }

    /// 向指定用户的所有连接发送消息
    pub async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        if let Some(server) = self.server.upgrade() {
            server.send_to_user(user_id, frame).await
        } else {
            Err(crate::common::error::FlareError::general_error("Server is not available"))
        }
    }

    /// 广播消息到所有连接
    pub async fn broadcast(&self, frame: &Frame) -> Result<()> {
        if let Some(server) = self.server.upgrade() {
            server.broadcast(frame).await
        } else {
            Err(crate::common::error::FlareError::general_error("Server is not available"))
        }
    }

    /// 广播消息到所有连接，排除指定连接
    pub async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        if let Some(server) = self.server.upgrade() {
            server.broadcast_except(frame, exclude_connection_id).await
        } else {
            Err(crate::common::error::FlareError::general_error("Server is not available"))
        }
    }

    /// 断开指定连接
    pub async fn disconnect(&self, connection_id: &str) -> Result<()> {
        if let Some(server) = self.server.upgrade() {
            server.disconnect(connection_id).await
        } else {
            Err(crate::common::error::FlareError::general_error("Server is not available"))
        }
    }

    /// 获取连接数量
    pub fn connection_count(&self) -> usize {
        if let Some(server) = self.server.upgrade() {
            server.connection_count()
        } else {
            0
        }
    }

    /// 获取用户数量
    pub fn user_count(&self) -> usize {
        if let Some(server) = self.server.upgrade() {
            server.user_count()
        } else {
            0
        }
    }
}

/// 消息处理函数类型
pub type MessageHandlerFn = Box<dyn for<'a> Fn(&'a Frame, &'a MessageContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Frame>>> + Send + 'a>> + Send + Sync>;

/// 连接事件处理函数类型
pub type OnConnectFn = Box<dyn for<'a> Fn(&'a str, &'a MessageContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> + Send + Sync>;

/// 断开连接事件处理函数类型
pub type OnDisconnectFn = Box<dyn for<'a> Fn(&'a str, &'a MessageContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> + Send + Sync>;

/// 简化的连接处理器
struct SimpleConnectionHandler {
    message_handler: Option<MessageHandlerFn>,
    on_connect: Option<OnConnectFn>,
    on_disconnect: Option<OnDisconnectFn>,
    server: Arc<Mutex<Option<Weak<ServerWrapper>>>>,
}

impl SimpleConnectionHandler {
    fn new() -> Self {
        Self {
            message_handler: None,
            on_connect: None,
            on_disconnect: None,
            server: Arc::new(Mutex::new(None)),
        }
    }

    async fn set_server(&self, server: Weak<ServerWrapper>) {
        *self.server.lock().await = Some(server);
    }
}

#[async_trait]
impl ConnectionHandler for SimpleConnectionHandler {
    async fn handle_frame(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        let server_weak = {
            let server_guard = self.server.lock().await;
            server_guard.clone()
        };

        let context = if let Some(server_weak) = server_weak {
            MessageContext::new(connection_id.to_string(), server_weak)
        } else {
            MessageContext::new(connection_id.to_string(), Weak::<ServerWrapper>::new())
        };

        if let Some(ref handler) = self.message_handler {
            handler(frame, &context).await
        } else {
            Ok(None)
        }
    }

    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        let server_weak = {
            let server_guard = self.server.lock().await;
            server_guard.clone()
        };

        let context = if let Some(server_weak) = server_weak {
            MessageContext::new(connection_id.to_string(), server_weak)
        } else {
            MessageContext::new(connection_id.to_string(), Weak::<ServerWrapper>::new())
        };

        if let Some(ref handler) = self.on_connect {
            handler(connection_id, &context).await
        } else {
            Ok(())
        }
    }

    async fn on_disconnect(&self, connection_id: &str) -> Result<()> {
        let server_weak = {
            let server_guard = self.server.lock().await;
            server_guard.clone()
        };

        let context = if let Some(server_weak) = server_weak {
            MessageContext::new(connection_id.to_string(), server_weak)
        } else {
            MessageContext::new(connection_id.to_string(), Weak::<ServerWrapper>::new())
        };

        if let Some(ref handler) = self.on_disconnect {
            handler(connection_id, &context).await
        } else {
            Ok(())
        }
    }
}

/// 服务器包装器，实现 Server trait
struct ServerWrapper {
    server: Arc<Mutex<HybridServer>>,
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
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
            s.is_running()
        })
    }

    fn connection_count(&self) -> usize {
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
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

/// 简化的服务器实例
/// 
/// 提供简化的接口，自动处理服务器引用
pub struct SimpleServer {
    server: Arc<Mutex<HybridServer>>,
    handler: Arc<SimpleConnectionHandler>,
    server_wrapper: Arc<ServerWrapper>,
}

impl SimpleServer {
    /// 启动服务器
    pub async fn start(&mut self) -> Result<()> {
        // 设置服务器引用
        let server_weak = Arc::downgrade(&self.server_wrapper);
        self.handler.set_server(server_weak).await;

        let mut s = self.server.lock().await;
        s.start().await
    }

    /// 停止服务器
    pub async fn stop(&mut self) -> Result<()> {
        let mut s = self.server.lock().await;
        s.stop().await
    }

    /// 检查服务器是否运行
    pub fn is_running(&self) -> bool {
        self.server_wrapper.is_running()
    }

    /// 获取连接数量
    pub fn connection_count(&self) -> usize {
        self.server_wrapper.connection_count()
    }

    /// 获取用户数量
    pub fn user_count(&self) -> usize {
        self.server_wrapper.user_count()
    }

    /// 向指定连接发送消息
    pub async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        self.server_wrapper.send_to(connection_id, frame).await
    }

    /// 向指定用户的所有连接发送消息
    pub async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        self.server_wrapper.send_to_user(user_id, frame).await
    }

    /// 广播消息到所有连接
    pub async fn broadcast(&self, frame: &Frame) -> Result<()> {
        self.server_wrapper.broadcast(frame).await
    }

    /// 广播消息到所有连接，排除指定连接
    pub async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        self.server_wrapper.broadcast_except(frame, exclude_connection_id).await
    }

    /// 断开指定连接
    pub async fn disconnect(&self, connection_id: &str) -> Result<()> {
        self.server_wrapper.disconnect(connection_id).await
    }
}

/// 简单模式服务端构建器
/// 
/// 使用闭包定义消息处理逻辑
pub struct ServerBuilder {
    config: ServerConfig,
    message_handler: Option<MessageHandlerFn>,
    on_connect: Option<OnConnectFn>,
    on_disconnect: Option<OnDisconnectFn>,
}

impl ServerBuilder {
    /// 创建新的服务端构建器
    /// 
    /// # 参数
    /// - `bind_address`: 监听地址，例如 "0.0.0.0:8080"
    pub fn new(bind_address: impl Into<String>) -> Self {
        Self {
            config: ServerConfig::new(bind_address.into()),
            message_handler: None,
            on_connect: None,
            on_disconnect: None,
        }
    }

    /// 设置消息处理函数
    /// 
    /// # 参数
    /// - `handler`: 消息处理函数，接收 Frame 和 MessageContext，返回 Option<Frame>（可选回复）
    pub fn on_message<F>(mut self, handler: F) -> Self
    where
        F: for<'a> Fn(&'a Frame, &'a MessageContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Frame>>> + Send + 'a>> + Send + Sync + 'static,
    {
        self.message_handler = Some(Box::new(move |frame, ctx| {
            handler(frame, ctx)
        }));
        self
    }

    /// 设置连接建立事件处理函数
    /// 
    /// # 参数
    /// - `handler`: 连接建立处理函数，接收 connection_id 和 MessageContext
    pub fn on_connect<F>(mut self, handler: F) -> Self
    where
        F: for<'a> Fn(&'a str, &'a MessageContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> + Send + Sync + 'static,
    {
        self.on_connect = Some(Box::new(move |conn_id, ctx| {
            handler(conn_id, ctx)
        }));
        self
    }

    /// 设置连接断开事件处理函数
    /// 
    /// # 参数
    /// - `handler`: 连接断开处理函数，接收 connection_id 和 MessageContext
    pub fn on_disconnect<F>(mut self, handler: F) -> Self
    where
        F: for<'a> Fn(&'a str, &'a MessageContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> + Send + Sync + 'static,
    {
        self.on_disconnect = Some(Box::new(move |conn_id, ctx| {
            handler(conn_id, ctx)
        }));
        self
    }

    /// 设置传输协议
    pub fn with_protocol(mut self, protocol: crate::common::config_types::TransportProtocol) -> Self {
        self.config.transport = protocol;
        self
    }

    /// 启用多协议监听
    pub fn with_protocols(mut self, protocols: Vec<crate::common::config_types::TransportProtocol>) -> Self {
        self.config = self.config.with_protocols(protocols);
        self
    }

    /// 设置最大连接数
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.config = self.config.with_max_connections(max);
        self
    }

    /// 设置心跳配置
    pub fn with_heartbeat(mut self, heartbeat: crate::common::config_types::HeartbeatConfig) -> Self {
        self.config = self.config.with_heartbeat(heartbeat);
        self
    }

    /// 设置 TLS 配置
    pub fn with_tls(mut self, tls: crate::common::config_types::TlsConfig) -> Self {
        self.config = self.config.with_tls(tls);
        self
    }

    /// 构建服务端
    /// 
    /// # 返回
    /// 返回配置好的 SimpleServer 实例
    pub fn build(self) -> Result<SimpleServer> {
        let handler = Arc::new(SimpleConnectionHandler {
            message_handler: self.message_handler,
            on_connect: self.on_connect,
            on_disconnect: self.on_disconnect,
            server: Arc::new(Mutex::new(None)),
        });

        let server = HybridServer::new(self.config, handler.clone() as Arc<dyn ConnectionHandler>)?;
        let server_arc = Arc::new(Mutex::new(server));
        let server_wrapper = Arc::new(ServerWrapper {
            server: Arc::clone(&server_arc),
        });

        Ok(SimpleServer {
            server: server_arc,
            handler,
            server_wrapper,
        })
    }
}

