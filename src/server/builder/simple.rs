//! 简单模式服务端构建器
//! 
//! 使用闭包定义消息处理逻辑

use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::server::{ServerConfig, ConnectionHandler, HybridServer, Server};
use crate::server::handle::ServerHandle;
use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::Mutex;

/// 消息处理上下文
/// 
/// 提供给消息处理函数的上下文，包含连接信息和服务器操作处理器
pub struct MessageContext {
    /// 连接 ID
    pub connection_id: String,
    /// 服务器操作处理器（轻量级，用于发送消息和连接管理）
    handle: Arc<dyn ServerHandle>,
}

impl MessageContext {
    /// 创建新的消息上下文
    fn new(connection_id: String, handle: Arc<dyn ServerHandle>) -> Self {
        Self {
            connection_id,
            handle,
        }
    }

    /// 向指定连接发送消息
    pub async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        self.handle.send_to(connection_id, frame).await
    }

    /// 向指定用户的所有连接发送消息
    pub async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        self.handle.send_to_user(user_id, frame).await
    }

    /// 广播消息到所有连接
    pub async fn broadcast(&self, frame: &Frame) -> Result<()> {
        self.handle.broadcast(frame).await
    }

    /// 广播消息到所有连接，排除指定连接
    pub async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        self.handle.broadcast_except(frame, exclude_connection_id).await
    }

    /// 断开指定连接
    pub async fn disconnect(&self, connection_id: &str) -> Result<()> {
        self.handle.disconnect(connection_id).await
    }

    /// 获取连接数量
    pub fn connection_count(&self) -> usize {
        self.handle.connection_count()
    }

    /// 获取用户数量
    pub fn user_count(&self) -> usize {
        self.handle.user_count()
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
    handle: Arc<Mutex<Option<Arc<dyn ServerHandle>>>>,
}

impl SimpleConnectionHandler {

    async fn set_handle(&self, handle: Arc<dyn ServerHandle>) {
        *self.handle.lock().await = Some(handle);
    }
}

#[async_trait]
impl ConnectionHandler for SimpleConnectionHandler {
    async fn handle_frame(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        let handle = {
            let handle_guard = self.handle.lock().await;
            handle_guard.clone()
        };

        let context = if let Some(ref handle) = handle {
            MessageContext::new(connection_id.to_string(), Arc::clone(handle))
        } else {
            // 如果没有 handle，创建一个空的（使用 ServerCore 的默认实现）
            // 这里暂时返回错误，实际上应该在 build 时设置 handle
            return Err(crate::common::error::FlareError::general_error("Server handle is not available"));
        };

        if let Some(ref handler) = self.message_handler {
            handler(frame, &context).await
        } else {
            Ok(None)
        }
    }

    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        let handle = {
            let handle_guard = self.handle.lock().await;
            handle_guard.clone()
        };

        let context = if let Some(ref handle) = handle {
            MessageContext::new(connection_id.to_string(), Arc::clone(handle))
        } else {
            return Err(crate::common::error::FlareError::general_error("Server handle is not available"));
        };

        if let Some(ref handler) = self.on_connect {
            handler(connection_id, &context).await
        } else {
            Ok(())
        }
    }

    async fn on_disconnect(&self, connection_id: &str) -> Result<()> {
        let handle = {
            let handle_guard = self.handle.lock().await;
            handle_guard.clone()
        };

        let context = if let Some(ref handle) = handle {
            MessageContext::new(connection_id.to_string(), Arc::clone(handle))
        } else {
            return Err(crate::common::error::FlareError::general_error("Server handle is not available"));
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

/// ServerWrapper 的 ServerHandle 适配器
/// 让 ServerWrapper 可以作为 ServerHandle 使用
struct ServerWrapperHandle {
    server: Arc<Mutex<HybridServer>>,
}

#[async_trait]
impl crate::server::handle::ServerHandle for ServerWrapperHandle {
    async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        // 通过 ServerHandle 调用（HybridServer 实现了 ServerHandle）
        let s = self.server.lock().await;
        crate::server::handle::ServerHandle::send_to(&*s, connection_id, frame).await
    }
    
    async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        // 通过 ServerHandle 调用（HybridServer 实现了 ServerHandle）
        let s = self.server.lock().await;
        crate::server::handle::ServerHandle::send_to_user(&*s, user_id, frame).await
    }
    
    async fn broadcast(&self, frame: &Frame) -> Result<()> {
        // 通过 ServerHandle 调用（HybridServer 实现了 ServerHandle）
        let s = self.server.lock().await;
        crate::server::handle::ServerHandle::broadcast(&*s, frame).await
    }
    
    async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        // 通过 ServerHandle 调用（HybridServer 实现了 ServerHandle）
        let s = self.server.lock().await;
        crate::server::handle::ServerHandle::broadcast_except(&*s, frame, exclude_connection_id).await
    }
    
    async fn disconnect(&self, connection_id: &str) -> Result<()> {
        // 通过 ServerHandle 调用（HybridServer 实现了 ServerHandle）
        let s = self.server.lock().await;
        crate::server::handle::ServerHandle::disconnect(&*s, connection_id).await
    }
    
    fn connection_count(&self) -> usize {
        // 通过 ServerHandle 调用（HybridServer 实现了 ServerHandle）
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
            crate::server::handle::ServerHandle::connection_count(&*s)
        })
    }
    
    fn user_count(&self) -> usize {
        // 通过 ServerHandle 调用（HybridServer 实现了 ServerHandle）
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
            crate::server::handle::ServerHandle::user_count(&*s)
        })
    }
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

    fn is_running(&self) -> bool {
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
            s.is_running()
        })
    }
}

/// 简化的服务器实例
/// 
/// 提供简化的接口，自动处理服务器引用
pub struct SimpleServer {
    server: Arc<Mutex<HybridServer>>,
    handler: Arc<SimpleConnectionHandler>,
    server_wrapper: Arc<ServerWrapper>,
    handle: Arc<dyn ServerHandle>,
}

impl SimpleServer {
    /// 启动服务器
    pub async fn start(&mut self) -> Result<()> {
        // 设置 ServerHandle
        self.handler.set_handle(Arc::clone(&self.handle)).await;

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
        self.handle.connection_count()
    }

    /// 获取用户数量
    pub fn user_count(&self) -> usize {
        self.handle.user_count()
    }

    /// 获取 ServerHandle（用于消息发送和连接管理）
    pub fn handle(&self) -> Arc<dyn ServerHandle> {
        Arc::clone(&self.handle)
    }

    /// 向指定连接发送消息
    pub async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        ServerHandle::send_to(&*self.handle, connection_id, frame).await
    }

    /// 向指定用户的所有连接发送消息
    pub async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        ServerHandle::send_to_user(&*self.handle, user_id, frame).await
    }

    /// 广播消息到所有连接
    pub async fn broadcast(&self, frame: &Frame) -> Result<()> {
        ServerHandle::broadcast(&*self.handle, frame).await
    }

    /// 广播消息到所有连接，排除指定连接
    pub async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        ServerHandle::broadcast_except(&*self.handle, frame, exclude_connection_id).await
    }

    /// 断开指定连接
    pub async fn disconnect(&self, connection_id: &str) -> Result<()> {
        ServerHandle::disconnect(&*self.handle, connection_id).await
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
    authenticator: Option<Arc<dyn crate::server::auth::Authenticator>>,
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
            authenticator: None,
        }
    }
    
    /// 设置认证器（如果启用认证，必须提供）
    /// 
    /// 如果设置了认证器，还需要在配置中启用认证：
    /// ```rust
    /// .enable_auth()
    /// .with_authenticator(authenticator)
    /// ```
    pub fn with_authenticator(mut self, authenticator: Arc<dyn crate::server::auth::Authenticator>) -> Self {
        self.authenticator = Some(authenticator);
        self
    }
    
    /// 启用认证
    pub fn enable_auth(mut self) -> Self {
        self.config = self.config.enable_auth();
        self
    }
    
    /// 设置认证超时时间
    pub fn with_auth_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.config = self.config.with_auth_timeout(timeout);
        self
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

    /// 设置默认序列化格式（用于协商，默认 Protobuf）
    pub fn with_default_format(mut self, format: crate::common::protocol::SerializationFormat) -> Self {
        self.config = self.config.with_format(format);
        self
    }

    /// 设置默认压缩算法（用于协商，默认 None）
    pub fn with_default_compression(mut self, compression: crate::common::compression::CompressionAlgorithm) -> Self {
        self.config = self.config.with_compression(compression);
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
            handle: Arc::new(Mutex::new(None)),
        });

        // 使用 with_connection_manager 以传递 authenticator
        let server = HybridServer::with_connection_manager(
            self.config,
            handler.clone() as Arc<dyn ConnectionHandler>,
            None,
            None,
            None,
            self.authenticator,
        )?;
        let server_arc = Arc::new(Mutex::new(server));
        let server_wrapper = Arc::new(ServerWrapper {
            server: Arc::clone(&server_arc),
        });
        
        // 创建 ServerHandle 适配器（转换为 trait object）
        let handle: Arc<dyn ServerHandle> = Arc::new(ServerWrapperHandle {
            server: server_arc.clone(),
        }) as Arc<dyn ServerHandle>;

        Ok(SimpleServer {
            server: server_arc,
            handler,
            server_wrapper,
            handle,
        })
    }
}

