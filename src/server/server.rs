use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::Duration;
use tracing::info;

use crate::common::{
    error::Result,
};

use crate::server::{manager::{
    traits::ServerConnectionManager,
    ConnectionManager,
}, event::ServerEvent, ServerEventAdapter, config::{ServerConfig, ServerType}, websocket, quic, DefServerEventHandler};

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
pub struct AggregationServer {
    /// 配置
    config: ServerConfig,
    /// 是否正在运行
    is_running: Arc<AtomicBool>,
    /// 事件处理器
    event_handler: Arc<ServerEventAdapter>,
    /// 连接管理器
    connection_manager: Arc<dyn ServerConnectionManager>,
    /// WebSocket服务器实例
    websocket_server: Arc<tokio::sync::RwLock<Option<websocket::WebSocketServer>>>,
    /// QUIC服务器实例
    quic_server: Arc<tokio::sync::RwLock<Option<quic::QuicServer>>>,
}

impl AggregationServer {
    /// 创建新的服务器实例
    pub fn new(config: ServerConfig) -> Self {
        ServerBuilder::new(config).build().unwrap()
    }
    
    /// 创建带有事件处理器的服务器实例
    pub fn with_event_handler(config: ServerConfig, event_handler: Arc<dyn ServerEvent>) -> Self {
        ServerBuilder::new(config)
            .with_event_handler(event_handler)
            .build()
            .unwrap()
    }
    
    /// 创建服务器构建器
    pub fn builder(config: ServerConfig) -> ServerBuilder {
        ServerBuilder::new(config)
    }
    
    /// 启动服务器
    pub async fn start(&self) -> Result<()> {
        // 设置运行状态
        self.is_running.store(true, Ordering::Relaxed);
        
        info!("开始启动服务器，类型: {:?}", self.config.server_type);
        
        // 根据配置启动相应的服务
        match self.config.server_type {
            ServerType::WebSocket => {
                info!("启动WebSocket服务模式");
                // 仅启动WebSocket服务
                self.start_websocket().await?;
            },
            ServerType::Quic => {
                info!("启动QUIC服务模式");
                // 仅启动QUIC服务
                self.start_quic().await?;
            },
            ServerType::Dual => {
                info!("启动双协议服务模式");
                // 双协议模式：同时启动WebSocket和QUIC
                self.start_dual_protocol().await?;
            },
        }
        
        info!("服务器启动完成");
        Ok(())
    }
    
    /// 启动WebSocket服务
    async fn start_websocket(&self) -> Result<()> {
        info!("准备启动WebSocket服务");
        if let Some(config) = &self.config.websocket_config {
            info!("WebSocket配置存在，监听地址: {}", config.listen_addr);
            println!("启动WebSocket服务: {}", config.listen_addr);
            
            // 使用已有的连接管理器而不是创建新的
            let websocket_server = websocket::WebSocketServer::new(
                self.config.clone(),
                Arc::clone(&self.connection_manager),
                Arc::clone(&self.event_handler),
            );
            
            // 保存WebSocket服务器实例引用
            {
                let mut ws_server = self.websocket_server.write().await;
                *ws_server = Some(websocket_server);
            }
            
            // 启动WebSocket服务
            if let Some(ws_server) = &*self.websocket_server.read().await {
                info!("调用WebSocket服务的start方法");
                ws_server.start().await?;
                info!("WebSocket服务start方法调用完成");
            }
        } else {
            panic!("WebSocket配置不存在")
        }
        Ok(())
    }
    
    /// 启动QUIC服务
    async fn start_quic(&self) -> Result<()> {
        if let Some(config) = &self.config.quic_config {
            println!("启动QUIC服务: {}", config.listen_addr);
            
            // 创建QUIC服务器实例
            let connection_manager = Arc::new(ConnectionManager::new());
            let quic_server = quic::QuicServer::new(
                self.config.clone(),
                connection_manager,
                Arc::clone(&self.event_handler),
            );
            
            // 保存QUIC服务器实例引用
            {
                let mut q_server = self.quic_server.write().await;
                *q_server = Some(quic_server);
            }
            
            // 启动QUIC服务
            if let Some(q_server) = &*self.quic_server.read().await {
                q_server.start().await?;
            }
        }else { panic!("QUIC配置不存在") }
        Ok(())
    }
    
    /// 启动双协议服务
    async fn start_dual_protocol(&self) -> Result<()> {
        println!("启动双协议模式");
        
        // 启动WebSocket服务
        self.start_websocket().await?;
        
        // 启动QUIC服务
        self.start_quic().await?;
        
        Ok(())
    }

    /// 停止服务器
    pub async fn stop(&self) -> Result<()> {
        self.is_running.store(false, Ordering::Relaxed);
        
        // 停止WebSocket服务
        if let Some(ws_server) = &*self.websocket_server.read().await {
            ws_server.stop().await;
        }
        
        // 停止QUIC服务
        if let Some(q_server) = &*self.quic_server.read().await {
            q_server.stop().await;
        }
        
        println!("服务器已停止");
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
    pub fn get_connection_manager(&self) -> &Arc<dyn ServerConnectionManager> {
        &self.connection_manager
    }
}

impl Drop for AggregationServer {
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


/// 服务器构建器
pub struct ServerBuilder {
    /// 配置
    config: ServerConfig,
    /// 事件处理器
    event_handler: Option<Arc<dyn ServerEvent>>,
    /// 连接管理器
    connection_manager: Option<Arc<dyn ServerConnectionManager>>,
}

impl ServerBuilder {
    /// 创建新的服务器构建器
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config,
            event_handler: None,
            connection_manager: None,
        }
    }

    /// 设置事件处理器
    pub fn with_event_handler(mut self, event_handler: Arc<dyn ServerEvent>) -> Self {
        self.event_handler = Some(event_handler);
        self
    }

    /// 设置连接管理器
    pub fn with_connection_manager(mut self, connection_manager: Arc<dyn ServerConnectionManager>) -> Self {
        self.connection_manager = Some(connection_manager);
        self
    }

    /// 构建服务器实例
    pub fn build(self) -> Result<AggregationServer> {
        // 检查必要配置
        let event_handler = if let Some(handler) = self.event_handler {
            Arc::new(ServerEventAdapter::new(handler))
        } else {
            // 使用默认的事件处理器
            let default_handler = Arc::new(DefServerEventHandler::default());
            Arc::new(ServerEventAdapter::new(default_handler))
        };

        let connection_manager = if let Some(manager) = self.connection_manager {
            manager
        } else {
            // 创建默认的连接管理器
            Arc::new(ConnectionManager::new())
        };

        Ok(AggregationServer {
            config: self.config,
            is_running: Arc::new(AtomicBool::new(false)),
            event_handler,
            connection_manager,
            websocket_server: Arc::new(tokio::sync::RwLock::new(None)),
            quic_server: Arc::new(tokio::sync::RwLock::new(None)),
        })
    }
}