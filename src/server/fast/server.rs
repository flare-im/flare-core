//! FastServer - 融合功能的服务端代理
//!
//! FastServer作为服务端的核心代理，整合了连接生命周期管理和服务端事件处理功能，
//! 提供统一的接口来协调所有服务端操作。

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::server::config::ServerConfig;
use crate::common::{
    error::Result,
    protocol::Frame,
};
use crate::server::fast::message_handler::{MessageHandler, DefaultMessageHandler};
use crate::server::server::AggregationServer;
use crate::server::{
    manager::{
        ConnectionManager,
        UserConnectionManager,
    },
    event::DefServerEventHandler,
    ServerEventAdapter,
};
use crate::server::fast::event_handler::FastServerEventHandler;
use crate::server::fast::message_sender::MessageSender;
use crate::server::fast::auth::{AuthProvider, DefaultAuthProvider};

/// 服务统计信息
#[derive(Debug, Clone)]
pub struct ServerStats {
    /// 总连接数
    pub total_connections: usize,
    /// 活跃连接数
    pub active_connections: usize,
    /// 已认证用户数
    pub authenticated_users: usize,
    /// 待认证连接数
    pub pending_auth_connections: usize,
}

/// FastServer - 融合功能的服务端代理
///
/// 该结构体整合了连接管理和服务端事件处理功能，提供统一的服务端接口
pub struct FastServer {
    /// 基础服务实现
    server: Arc<AggregationServer>,
    /// 用户连接管理器
    user_connection_manager: Arc<UserConnectionManager>,
    /// 用户消息处理器
    message_handler: Arc<dyn MessageHandler>,
    /// 系统事件处理器
    system_event_handler: Arc<FastServerEventHandler>,
    /// 消息发送器
    message_sender: Arc<MessageSender>,
    /// 是否正在运行
    is_running: Arc<RwLock<bool>>,
    /// 配置
    config: ServerConfig,
}

impl FastServer {
    /// 创建新的FastServer实例
    pub fn new(
        message_handler: Option<Arc<dyn MessageHandler>>,
        auth_provider: Option<Arc<dyn AuthProvider>>,
        config: ServerConfig,
    ) -> Self {
        // 创建基础连接管理器
        let base_manager = Arc::new(ConnectionManager::new());

        let auth_timeout = std::time::Duration::from_millis(config.get_auth_timeout_ms());
        let user_manager = Arc::new(UserConnectionManager::with_config(base_manager.clone(), auth_timeout));
        
        // 使用默认消息处理器（如果未提供）
        let message_handler = message_handler.unwrap_or_else(|| Arc::new(DefaultMessageHandler::default()));
        
        // 使用默认认证提供者（如果未提供）
        let auth_provider = auth_provider.unwrap_or_else(|| Arc::new(DefaultAuthProvider::default()));
        
        // 创建系统事件处理器
        let system_event_handler = Arc::new(FastServerEventHandler::new(
            user_manager.clone(),
            message_handler.clone(),
            auth_provider,
        ));
        
        // 创建消息发送器
        let message_sender = Arc::new(MessageSender::new(user_manager.clone()));
        
        // 创建默认服务端事件处理器
        let event_handler = Arc::new(DefServerEventHandler::default());
        
        // 创建服务端事件适配器
        let event_adapter = Arc::new(ServerEventAdapter::new(event_handler));
        
        // 创建服务实现
        let server = Arc::new(AggregationServer::with_event_handler(
            config.clone(),
            event_adapter,
        ));
        
        Self {
            server,
            user_connection_manager: user_manager,
            message_handler,
            system_event_handler,
            message_sender,
            is_running: Arc::new(RwLock::new(false)),
            config
        }
    }
    
    /// 启动服务
    ///
    /// # 参数
    /// * `config` - 服务配置
    ///
    /// # 返回值
    /// * `Ok(())` - 启动成功
    /// * `Err(Error)` - 启动失败
    pub async fn start(&self) -> Result<()> {
        // 检查是否已在运行
        {
            let running = self.is_running.read().await;
            if *running {
                return Err(crate::common::error::FlareError::general_error("服务已在运行".to_string()));
            }
        }
        // 标记为运行状态
        {
            let mut running = self.is_running.write().await;
            *running = true;
        }
        
        // 启动基础服务
        self.server.start().await?;
        
        tracing::info!("服务代理启动成功");
        Ok(())
    }
    
    /// 停止服务
    pub async fn stop(&self) {
        // 检查是否正在运行
        {
            let running = self.is_running.read().await;
            if !*running {
                return;
            }
        }
        
        // 停止服务
        let _ = self.server.stop().await;
        
        // 标记为停止状态
        {
            let mut running = self.is_running.write().await;
            *running = false;
        }
        
        tracing::info!("服务代理已停止");
    }
    
    /// 发送消息给用户
    ///
    /// # 参数
    /// * `user_id` - 用户ID
    /// * `message` - 消息帧
    ///
    /// # 返回值
    /// * `Ok(())` - 发送成功
    /// * `Err(Error)` - 发送失败
    pub async fn send_message_to_user(&self, user_id: &str, message: Frame) -> Result<()> {
        self.user_connection_manager.send_message_to_user(user_id, message).await?;
        Ok(())
    }
    
    /// 获取服务统计信息
    ///
    /// # 返回值
    /// 返回服务统计信息
    pub async fn get_stats(&self) -> ServerStats {
        let user_stats = self.user_connection_manager.get_stats().await;
        
        ServerStats {
            total_connections: user_stats.total_connections,
            active_connections: user_stats.active_connections,
            authenticated_users: user_stats.active_users,
            pending_auth_connections: user_stats.pending_auth_connections,
        }
    }
    
    /// 获取基础服务实现
    ///
    /// # 返回值
    /// 返回基础服务实现的引用
    pub fn get_server(&self) -> &Arc<AggregationServer> {
        &self.server
    }
    
    /// 获取用户连接管理器
    ///
    /// # 返回值
    /// 返回用户连接管理器的引用
    pub fn get_user_connection_manager(&self) -> &Arc<UserConnectionManager> {
        &self.user_connection_manager
    }
    
    /// 获取消息处理器
    ///
    /// # 返回值
    /// 返回消息处理器的引用
    pub fn get_message_handler(&self) -> &Arc<dyn MessageHandler> {
        &self.message_handler
    }
    
    /// 获取系统事件处理器
    ///
    /// # 返回值
    /// 返回系统事件处理器的引用
    pub fn get_system_event_handler(&self) -> &Arc<FastServerEventHandler> {
        &self.system_event_handler
    }
    
    /// 获取消息发送器
    ///
    /// # 返回值
    /// 返回消息发送器的引用
    pub fn get_message_sender(&self) -> &Arc<MessageSender> {
        &self.message_sender
    }
    
    /// 获取当前配置
    ///
    /// # 返回值
    /// 返回当前服务配置的引用
    pub async fn get_config(&self) -> ServerConfig {
        self.config.clone()
    }
}

impl FastServer {
    /// 创建一个自定义配置的FastServer实例
    pub fn new_with_config(
        config: ServerConfig,
    ) -> Self {
        let message_handler: Arc<dyn MessageHandler> = Arc::new(DefaultMessageHandler::default());
        let auth_provider: Arc<dyn AuthProvider> = Arc::new(DefaultAuthProvider::default());
        Self::new(
            Some(message_handler),
            Some(auth_provider),
            config,
        )
    }
}

impl Default for FastServer {
    fn default() -> Self {
        let message_handler: Arc<dyn MessageHandler> = Arc::new(DefaultMessageHandler::default());
        let auth_provider: Arc<dyn AuthProvider> = Arc::new(DefaultAuthProvider::default());
        
        Self::new(
            Some(message_handler),
            Some(auth_provider),
            ServerConfig::new(),
        )
    }
}