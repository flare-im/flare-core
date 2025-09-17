use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::info;

use crate::common::{
    error::Result,
};

use crate::server::{manager::traits::ServerConnectionManager, event::ServerEvent, ServerEventAdapter};

/// 服务器配置
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// 本地地址
    pub local_addr: Option<String>,
    /// 连接超时时间（毫秒）
    pub connection_timeout_ms: u64,
    /// 心跳间隔（毫秒）
    pub heartbeat_interval_ms: u64,
    /// 最大连接数
    pub max_connections: usize,
    /// 是否启用TLS
    pub enable_tls: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            local_addr: None,
            connection_timeout_ms: 30000,
            heartbeat_interval_ms: 10000,
            max_connections: 1000,
            enable_tls: false,
        }
    }
}

impl ServerConfig {
    /// 创建新的服务器配置
    pub fn new() -> Self {
        Self::default()
    }
    
    /// 设置本地地址
    pub fn with_local_addr(mut self, addr: String) -> Self {
        self.local_addr = Some(addr);
        self
    }
    
    /// 设置连接超时时间
    pub fn with_connection_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.connection_timeout_ms = timeout_ms;
        self
    }
    
    /// 设置心跳间隔
    pub fn with_heartbeat_interval_ms(mut self, interval_ms: u64) -> Self {
        self.heartbeat_interval_ms = interval_ms;
        self
    }
    
    /// 设置最大连接数
    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }
    
    /// 启用TLS
    pub fn enable_tls(mut self) -> Self {
        self.enable_tls = true;
        self
    }
}

/// 服务器类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerType {
    /// WebSocket服务器
    WebSocket,
    /// QUIC服务器
    Quic,
    /// 双协议服务器
    Dual,
}

/// 服务器trait
#[async_trait::async_trait]
pub trait Server: Send + Sync {
    /// 启动服务器
    async fn start(&self) -> Result<()>;
    
    /// 停止服务器
    async fn stop(&self);
}

/// 服务trait
#[async_trait::async_trait]
pub trait ServerService: Server + Send + Sync {
    /// 获取服务类型
    fn get_type(&self) -> ServerType;
    
    /// 获取本地地址
    fn get_local_addr(&self) -> Option<String>;
}

/// 服务器实现
pub struct ServerImpl<T: ServerConnectionManager> {
    /// 配置
    config: ServerConfig,
    /// 连接管理器
    connection_manager: Arc<T>,
    /// 是否正在运行
    is_running: Arc<AtomicBool>,
    /// 服务列表
    services: Arc<RwLock<std::collections::HashMap<String, Arc<dyn ServerService>>>>,
    /// 服务端事件处理器
    event_handler: Arc<dyn ServerEvent>,
}

impl<T: ServerConnectionManager + 'static> ServerImpl<T> {
    /// 创建新的服务器
    pub fn new(config: ServerConfig, connection_manager: Arc<T>) -> Self {
        Self::with_event_handler(config, connection_manager, Arc::new(crate::server::DefServerEventHandler::default()))
    }
    
    /// 创建带事件处理器的服务器
    pub fn with_event_handler(config: ServerConfig, connection_manager: Arc<T>, event_handler: Arc<dyn ServerEvent>) -> Self {
        Self {
            config,
            connection_manager,
            is_running: Arc::new(AtomicBool::new(false)),
            services: Arc::new(RwLock::new(std::collections::HashMap::new())),
            event_handler,
        }
    }
    
    /// 启动服务器
    pub async fn start(&self) -> Result<()> {
        if self.is_running.load(Ordering::Relaxed) {
            return Err(crate::common::error::FlareError::general_error("服务器已在运行".to_string()));
        }
        
        self.is_running.store(true, Ordering::Relaxed);
        info!("服务器启动中...");
        
        let event_handler = ServerEventAdapter::new(Arc::clone(&self.event_handler));    
        // 根据配置启动相应的服务
        if let Some(addr) = &self.config.local_addr {
            self.start_websocket_server(addr, Arc::from(event_handler)).await?;
        }
        
        info!("服务器启动完成");
        Ok(())
    }
    
    /// 停止服务器
    pub async fn stop(&self) {
        if !self.is_running.load(Ordering::Relaxed) {
            return;
        }
        
        self.is_running.store(false, Ordering::Relaxed);
        info!("服务器停止中...");
        
        // 停止所有服务
        let services: Vec<Arc<dyn ServerService>> = {
            let services_guard = self.services.read().await;
            services_guard.values().cloned().collect()
        };
        
        for service in services {
            service.stop().await;
        }
        
        // 清空服务列表
        {
            let mut services_guard = self.services.write().await;
            services_guard.clear();
        }
        
        info!("服务器停止完成");
    }
    
    /// 启动WebSocket服务
    async fn start_websocket_server(&self, addr: &str,event_handler:Arc<ServerEventAdapter>) -> Result<()> {
        info!("正在启动WebSocket服务: {}", addr);
        
        // 创建WebSocket服务
        let service = super::websocket::WebSocketServer::new(
            self.config.clone(),
            Arc::clone(&self.connection_manager),
            event_handler,
        );
        
        let service_arc: Arc<dyn ServerService> = Arc::new(service);
        
        // 启动服务
        service_arc.start().await?;
        
        // 注册服务
        self.services.write().await.insert("websocket".to_string(), service_arc);
        
        info!("WebSocket服务启动完成: {}", addr);
        Ok(())
    }
    
    /// 获取连接管理器
    ///
    /// # 返回值
    ///
    /// 返回连接管理器的引用
    pub fn get_connection_manager(&self) -> &Arc<T> {
        &self.connection_manager
    }
}

impl<T: ServerConnectionManager> Drop for ServerImpl<T> {
    fn drop(&mut self) {
        // 确保在析构时停止服务
        if self.is_running.load(Ordering::Relaxed) {
            // 注意：在Drop中不能使用async，这里只是记录日志
            info!("服务端正在被销毁");
        }
    }
}

/// 服务器统计信息
#[derive(Debug, Clone)]
pub struct ServerStats {
    /// 总连接数
    pub total_connections: usize,
    /// 活跃连接数
    pub active_connections: usize,
    /// 总消息数
    pub total_messages: u64,
    /// 平均连接质量
    pub average_quality: u8,
    /// 服务器运行时间
    pub uptime: Duration,
}