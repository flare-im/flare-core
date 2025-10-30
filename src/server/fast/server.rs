//! FastServer - 高性能轻量级服务端实现

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::server::config::ServerConfig;
use crate::server::server::{AggregationServer, ServerBuilder};
use crate::server::manager::traits::ConnectionManager;
use crate::server::fast::auth::DefaultAuthProvider;
use crate::server::fast::connection_manager::AuthenticatedConnectionManager;
use crate::server::fast::event_handler::FastServerEventHandlerTrait;
use crate::server::fast::message_handler::{MessageHandler, AsyncMessageHandler};
use crate::server::events::handler::EnhancedEventHandler;
use crate::common::error::FlareError;
use crate::common::protocol::frame::Frame;

/// FastServer - 高性能轻量级服务端实现
pub struct FastServer {
    /// 基础聚合服务端
    server: Arc<AggregationServer>,
    /// 是否正在运行
    is_running: Arc<RwLock<bool>>,
    /// 配置
    config: ServerConfig,
    /// 带认证功能的连接管理器
    auth_connection_manager: Option<Arc<AuthenticatedConnectionManager>>,
    /// FastServer事件处理器
    fast_event_handler: Arc<RwLock<Option<Arc<dyn FastServerEventHandlerTrait>>>>,
    /// 消息处理器
    message_handler: Arc<RwLock<Option<Arc<dyn MessageHandler>>>>,
    /// 异步消息处理器
    async_message_handler: Arc<RwLock<Option<Arc<dyn AsyncMessageHandler>>>>,
}

/// FastServer事件适配器
struct FastServerEventAdapter {
    fast_handler: Arc<dyn FastServerEventHandlerTrait>,
}

impl FastServerEventAdapter {
    fn new(fast_handler: Arc<dyn FastServerEventHandlerTrait>) -> Self {
        Self { fast_handler }
    }
}

impl crate::server::events::handler::EnhancedEventHandler for FastServerEventAdapter {
    fn on_connected(&self, connection_id: String) {
        // FastServer不直接使用连接事件，因为连接管理由连接管理器处理
        tracing::debug!("连接建立: {}", connection_id);
    }
    
    fn on_disconnected(&self, connection_id: String, reason: Option<String>) {
        tracing::debug!("连接断开: {}, 原因: {:?}", connection_id, reason);
    }
    
    fn on_error(&self, connection_id: String, err: FlareError) {
        tracing::error!("连接错误: {}, 错误: {:?}", connection_id, err);
    }
    
    fn on_message_received(&self, connection_id: String, frame: Frame) {
        tracing::debug!("收到消息: {}, 消息长度: {}", connection_id, frame.payload.len());
        // 将消息转发给FastServer处理器
        // 注意：这里需要获取用户ID，但在当前实现中我们可能需要通过其他方式获取
        // 暂时使用连接ID作为用户ID
        let user_id = connection_id.clone();
        let fast_handler = self.fast_handler.clone();
        tokio::spawn(async move {
            // 调用FastServer的消息处理方法
            if let Err(e) = fast_handler.process_incoming_message(user_id, frame).await {
                tracing::error!("处理消息失败: {:?}", e);
            }
        });
    }
    
    fn on_message_sent(&self, connection_id: String, frame: Frame) {
        tracing::debug!("消息发送成功: {}, 消息长度: {}", connection_id, frame.payload.len());
    }
    
    fn on_heartbeat_ping(&self, connection_id: String) {
        tracing::trace!("心跳Ping: {}", connection_id);
    }
    
    fn on_heartbeat_pong(&self, connection_id: String, rtt_ms: u32) {
        tracing::trace!("心跳Pong: {}, RTT: {}ms", connection_id, rtt_ms);
    }
    
    fn on_heartbeat_timeout(&self, connection_id: String) {
        tracing::warn!("心跳超时: {}", connection_id);
    }
    
    fn on_quality_changed(&self, connection_id: String, quality: u8) {
        tracing::info!("连接质量变化: {}, 质量: {}", connection_id, quality);
    }
    
    fn on_statistics_updated(&self, connection_id: String, stats: crate::common::connections::types::ConnectionStats) {
        tracing::debug!("统计信息更新: {}, 统计: {:?}", connection_id, stats);
    }
}

// 为FastServer添加一个方法来处理消息
impl FastServer {
    /// 处理接收到的消息
    pub async fn process_incoming_message(&self, user_id: String, frame: Frame) -> Result<(), FlareError> {
        // 检查是否有异步消息处理器
        {
            let async_handler = self.async_message_handler.read().await;
            if let Some(handler) = &*async_handler {
                return handler.handle_message(user_id, frame).await;
            }
        }
        
        // 检查是否有同步消息处理器
        {
            let message_handler = self.message_handler.read().await;
            if let Some(handler) = &*message_handler {
                return handler.handle_message(user_id, frame);
            }
        }
        
        // 如果没有处理器，返回错误
        Err(FlareError::general_error("未设置消息处理器".to_string()))
    }
}

impl FastServer {
    /// 创建新的FastServer实例
    pub fn new(config: ServerConfig) -> Self {
        let server = ServerBuilder::new(config.clone()).build();
        
        Self {
            server: Arc::new(server),
            is_running: Arc::new(RwLock::new(false)),
            config,
            auth_connection_manager: None,
            fast_event_handler: Arc::new(RwLock::new(None)),
            message_handler: Arc::new(RwLock::new(None)),
            async_message_handler: Arc::new(RwLock::new(None)),
        }
    }
    
    /// 创建带认证功能的FastServer实例
    pub fn new_with_auth(config: ServerConfig, auth_timeout_ms: u64) -> Self {
        // 创建认证提供者和带认证功能的连接管理器
        let auth_provider = Arc::new(DefaultAuthProvider::default());
        let auth_connection_manager = Arc::new(AuthenticatedConnectionManager::new(auth_provider, auth_timeout_ms));
        
        // 使用自定义连接管理器创建服务端
        let server = ServerBuilder::new(config.clone())
            .with_connection_manager(auth_connection_manager.clone())
            .build();
        
        Self {
            server: Arc::new(server),
            is_running: Arc::new(RwLock::new(false)),
            config,
            auth_connection_manager: Some(auth_connection_manager),
            fast_event_handler: Arc::new(RwLock::new(None)),
            message_handler: Arc::new(RwLock::new(None)),
            async_message_handler: Arc::new(RwLock::new(None)),
        }
    }
    
    /// 启动服务
    pub async fn start(&self) -> Result<(), FlareError> {
        // 检查是否已在运行
        {
            let running = self.is_running.read().await;
            if *running {
                return Err(FlareError::general_error("服务已在运行".to_string()));
            }
        }
        
        // 设置事件处理器
        if let Some(fast_handler) = &*self.fast_event_handler.read().await {
            let fast_handler_clone = fast_handler.clone();
            // 创建增强事件处理器适配器
            let enhanced_handler = Arc::new(FastServerEventAdapter::new(fast_handler_clone));
            self.server.set_event_handler(enhanced_handler).await;
        }
        
        // 标记为运行状态
        {
            let mut running = self.is_running.write().await;
            *running = true;
        }
        
        // 启动基础服务
        self.server.start().await?;
        
        tracing::info!("FastServer启动成功");
        Ok(())
    }
    
    /// 停止服务
    pub async fn stop(&self) -> Result<(), FlareError> {
        // 检查是否正在运行
        {
            let running = self.is_running.read().await;
            if !*running {
                return Ok(());
            }
        }
        
        // 停止服务
        self.server.stop().await?;
        
        // 标记为停止状态
        {
            let mut running = self.is_running.write().await;
            *running = false;
        }
        
        tracing::info!("FastServer已停止");
        Ok(())
    }
    
    /// 获取基础服务实现
    pub fn get_server(&self) -> &Arc<AggregationServer> {
        &self.server
    }
    
    /// 获取当前配置
    pub fn get_config(&self) -> &ServerConfig {
        &self.config
    }
    
    /// 检查服务是否正在运行
    pub async fn is_running(&self) -> bool {
        let running = self.is_running.read().await;
        *running
    }
    
    /// 获取连接管理器
    pub fn connection_manager(&self) -> &Arc<dyn ConnectionManager> {
        self.server.connection_manager()
    }
    
    /// 获取带认证功能的连接管理器（如果存在）
    pub fn auth_connection_manager(&self) -> Option<&Arc<AuthenticatedConnectionManager>> {
        self.auth_connection_manager.as_ref()
    }
    
    /// 设置FastServer事件处理器
    pub async fn set_event_handler(&self, handler: Arc<dyn FastServerEventHandlerTrait>) {
        let mut event_handler = self.fast_event_handler.write().await;
        *event_handler = Some(handler);
    }
    
    /// 移除FastServer事件处理器
    pub async fn remove_event_handler(&self) {
        let mut event_handler = self.fast_event_handler.write().await;
        *event_handler = None;
    }
    
    /// 设置同步消息处理器
    pub async fn set_message_handler(&self, handler: Arc<dyn MessageHandler>) {
        let mut message_handler = self.message_handler.write().await;
        *message_handler = Some(handler);
        
        // 清除异步处理器
        let mut async_handler = self.async_message_handler.write().await;
        *async_handler = None;
    }
    
    /// 设置异步消息处理器
    pub async fn set_async_message_handler(&self, handler: Arc<dyn AsyncMessageHandler>) {
        let mut async_handler = self.async_message_handler.write().await;
        *async_handler = Some(handler);
        
        // 清除同步处理器
        let mut message_handler = self.message_handler.write().await;
        *message_handler = None;
    }
    
    /// 移除消息处理器
    pub async fn remove_message_handler(&self) {
        let mut message_handler = self.message_handler.write().await;
        *message_handler = None;
        
        let mut async_handler = self.async_message_handler.write().await;
        *async_handler = None;
    }
    
    /// 处理消息
    pub async fn handle_message(&self, user_id: String, frame: Frame) -> Result<(), FlareError> {
        // 检查是否有同步消息处理器
        {
            let message_handler = self.message_handler.read().await;
            if let Some(handler) = &*message_handler {
                return handler.handle_message(user_id, frame);
            }
        }
        
        // 检查是否有异步消息处理器
        {
            let async_handler = self.async_message_handler.read().await;
            if let Some(handler) = &*async_handler {
                return handler.handle_message(user_id, frame).await;
            }
        }
        
        // 如果没有处理器，返回错误
        Err(FlareError::general_error("未设置消息处理器".to_string()))
    }
    
    /// 广播消息
    pub async fn broadcast_message(&self, frame: Frame) -> Result<(), FlareError> {
        // 检查是否有同步消息处理器
        {
            let message_handler = self.message_handler.read().await;
            if message_handler.is_some() {
                // 广播消息给所有已认证的连接
                if let Some(auth_manager) = &self.auth_connection_manager {
                    auth_manager.broadcast_message(frame)?;
                    return Ok(());
                }
            }
        }
        
        // 检查是否有异步消息处理器
        {
            let async_handler = self.async_message_handler.read().await;
            if async_handler.is_some() {
                // 广播消息给所有已认证的连接
                if let Some(auth_manager) = &self.auth_connection_manager {
                    auth_manager.broadcast_message(frame)?;
                    return Ok(());
                }
            }
        }
        
        // 如果没有处理器，返回错误
        Err(FlareError::general_error("未设置消息处理器".to_string()))
    }
}

#[async_trait::async_trait]
impl crate::server::fast::event_handler::FastServerEventHandlerTrait for FastServer {
    async fn process_incoming_message(&self, user_id: String, frame: Frame) -> Result<(), FlareError> {
        tracing::debug!("处理用户 {} 的消息，message_id: {}", user_id, frame.message_id);
        // 检查是否有异步消息处理器
        {
            let async_handler = self.async_message_handler.read().await;
            if let Some(handler) = &*async_handler {
                return handler.handle_message(user_id, frame).await;
            }
        }
        
        // 检查是否有同步消息处理器
        {
            let message_handler = self.message_handler.read().await;
            if let Some(handler) = &*message_handler {
                return handler.handle_message(user_id, frame);
            }
        }
        
        // 如果没有处理器，返回错误
        Err(FlareError::general_error("未设置消息处理器".to_string()))
    }
}
