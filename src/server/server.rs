use crate::server::config::{ServerConfig, ServerType};
use crate::server::servers::{websocket, quic};
use crate::server::manager::traits::ConnectionManager;
use crate::server::manager::connection_manager::ConnectionManagerImpl;
use crate::server::traits::ProtocolService;
use crate::server::events::handler::{EnhancedEventHandler, EventHandlerAdapter};
use crate::common::error::FlareError;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use tracing::{info, warn};
use tokio::time::{interval, Duration};

/// 聚合型服务端
pub struct AggregationServer {
    /// 服务器配置
    config: ServerConfig,
    /// 是否正在运行
    is_running: Arc<AtomicBool>,
    /// 协议服务列表
    protocol_services: Vec<Arc<dyn ProtocolService>>,
    /// 连接管理器
    connection_manager: Arc<dyn ConnectionManager>,
    /// 心跳任务句柄
    heartbeat_task: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// 事件处理器
    event_handler: Arc<tokio::sync::RwLock<Option<Arc<dyn EnhancedEventHandler>>>>,
}

impl AggregationServer {
    /// 创建新的聚合型服务端
    pub fn new(config: ServerConfig) -> Self {
        Self::new_with_connection_manager(config, Arc::new(ConnectionManagerImpl::new()))
    }
    
    /// 创建新的聚合型服务端，使用指定的连接管理器
    pub fn new_with_connection_manager(config: ServerConfig, connection_manager: Arc<dyn ConnectionManager>) -> Self {
        // 根据配置创建协议服务
        let mut protocol_services: Vec<Arc<dyn ProtocolService>> = Vec::new();
        
        match config.server_type {
            ServerType::WebSocket => {
                if let Some(_ws_config) = &config.websocket_config {
                    let websocket_server = Arc::new(websocket::WebSocketServer::new(
                        config.clone(), 
                        connection_manager.clone()
                    ));
                    protocol_services.push(websocket_server);
                }
            },
            ServerType::Quic => {
                if let Some(_quic_config) = &config.quic_config {
                    let quic_server = Arc::new(quic::QuicServer::new(
                        config.clone(), 
                        connection_manager.clone()
                    ));
                    protocol_services.push(quic_server);
                }
            },
            ServerType::Dual => {
                // 添加WebSocket服务
                if let Some(_ws_config) = &config.websocket_config {
                    let websocket_server = Arc::new(websocket::WebSocketServer::new(
                        config.clone(), 
                        connection_manager.clone()
                    ));
                    protocol_services.push(websocket_server);
                }
                
                // 添加QUIC服务
                if let Some(_quic_config) = &config.quic_config {
                    let quic_server = Arc::new(quic::QuicServer::new(
                        config.clone(), 
                        connection_manager.clone()
                    ));
                    protocol_services.push(quic_server);
                }
            },
        }
        
        Self {
            config,
            is_running: Arc::new(AtomicBool::new(false)),
            protocol_services,
            connection_manager,
            heartbeat_task: Arc::new(tokio::sync::Mutex::new(None)),
            event_handler: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }
    
    /// 添加协议服务
    pub fn add_protocol_service(&mut self, service: Arc<dyn ProtocolService>) {
        self.protocol_services.push(service);
    }

    /// 启动服务端
    pub async fn start(&self) -> Result<(), FlareError> {
        // 设置运行状态
        self.is_running.store(true, Ordering::Relaxed);
        
        info!("开始启动聚合型服务器，包含 {} 个协议服务", self.protocol_services.len());
        
        // 启动所有协议服务
        for service in &self.protocol_services {
            info!("启动 {} 服务", service.name());
            service.start(self.connection_manager.clone()).await?;
        }
        
        // 启动心跳检测任务
        self.start_heartbeat_task().await;
        
        info!("聚合型服务器启动完成");
        Ok(())
    }

    /// 停止服务端
    pub async fn stop(&self) -> Result<(), FlareError> {
        self.is_running.store(false, Ordering::Relaxed);
        
        // 停止心跳任务
        self.stop_heartbeat_task().await;
        
        // 停止所有协议服务
        for service in &self.protocol_services {
            info!("停止 {} 服务", service.name());
            service.stop().await?;
        }
        
        info!("聚合型服务器已停止");
        Ok(())
    }

    /// 获取服务器配置
    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    /// 检查服务器是否正在运行
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }
    
    /// 获取连接管理器
    pub fn connection_manager(&self) -> &Arc<dyn ConnectionManager> {
        &self.connection_manager
    }
    
    /// 获取协议服务列表
    pub fn protocol_services(&self) -> &[Arc<dyn ProtocolService>] {
        &self.protocol_services
    }
    
    /// 设置事件处理器
    pub async fn set_event_handler(&self, handler: Arc<dyn EnhancedEventHandler>) {
        let mut event_handler = self.event_handler.write().await;
        *event_handler = Some(handler.clone());
        
        // 同时设置连接管理器的事件处理器
        self.connection_manager.set_event_handler(handler).await;
    }
    
    /// 移除事件处理器
    pub async fn remove_event_handler(&self) {
        let mut event_handler = self.event_handler.write().await;
        *event_handler = None;
    }
    
    /// 获取事件处理器适配器
    pub async fn get_event_handler_adapter(&self) -> EventHandlerAdapter {
        let event_handler = self.event_handler.read().await;
        if let Some(handler) = &*event_handler {
            EventHandlerAdapter::with_handler(handler.clone())
        } else {
            EventHandlerAdapter::new()
        }
    }
    
    /// 启动心跳检测任务
    async fn start_heartbeat_task(&self) {
        let is_running = self.is_running.clone();
        let connection_manager = self.connection_manager.clone();
        let cleanup_interval_ms = self.config.cleanup_interval_ms;
        let heartbeat_monitor_timeout_ms = self.config.heartbeat_monitor_timeout_ms;
        
        let task = tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(cleanup_interval_ms));
            
            while is_running.load(Ordering::Relaxed) {
                interval.tick().await;
                
                // 执行连接清理
                if let Err(e) = connection_manager.cleanup(heartbeat_monitor_timeout_ms) {
                    warn!("连接清理任务出错: {:?}", e);
                }
                
                // 输出统计信息
                let stats = connection_manager.stats_snapshot();
                info!("连接统计: 总数={}, 活跃={}, 平均质量={:?}", 
                      stats.total, stats.active, stats.avg_quality);
            }
            
            info!("心跳检测任务已停止");
        });
        
        let mut heartbeat_task = self.heartbeat_task.lock().await;
        *heartbeat_task = Some(task);
        
        info!("心跳检测任务已启动，清理间隔: {}ms", cleanup_interval_ms);
    }
    
    /// 停止心跳检测任务
    async fn stop_heartbeat_task(&self) {
        let mut heartbeat_task = self.heartbeat_task.lock().await;
        if let Some(task) = heartbeat_task.take() {
            task.abort();
            info!("心跳检测任务已终止");
        }
    }
}

/// 服务端构建器
pub struct ServerBuilder {
    config: ServerConfig,
    connection_manager: Option<Arc<dyn ConnectionManager>>,
    event_handler: Option<Arc<dyn EnhancedEventHandler>>,
}

impl ServerBuilder {
    /// 创建新的服务端构建器
    pub fn new(config: ServerConfig) -> Self {
        Self { 
            config, 
            connection_manager: None,
            event_handler: None,
        }
    }
    
    /// 设置连接管理器
    pub fn with_connection_manager(mut self, connection_manager: Arc<dyn ConnectionManager>) -> Self {
        self.connection_manager = Some(connection_manager);
        self
    }
    
    /// 设置事件处理器
    pub fn with_event_handler(mut self, event_handler: Arc<dyn EnhancedEventHandler>) -> Self {
        self.event_handler = Some(event_handler);
        self
    }

    /// 构建服务端实例
    pub fn build(self) -> AggregationServer {
        let server = if let Some(connection_manager) = self.connection_manager {
            AggregationServer::new_with_connection_manager(self.config, connection_manager)
        } else {
            AggregationServer::new(self.config)
        };
        
        // 如果提供了事件处理器，则设置它
        if let Some(_event_handler) = self.event_handler {
            // 注意：这里我们需要在异步上下文中设置事件处理器
            // 在实际使用中，用户需要在启动服务后手动设置事件处理器
        }
        
        server
    }
}