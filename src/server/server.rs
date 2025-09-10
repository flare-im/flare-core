//! 服务端主模块
//!
//! 提供服务端核心功能，支持启动QUIC和WebSocket服务
//!
//! # 核心功能
//!
//! - 支持单独启动QUIC或WebSocket服务
//! - 支持同时启动两种协议服务
//! - 使用泛型连接管理器，支持多种连接管理策略
//! - 提供统一的消息处理接口
//! - 支持连接心跳管理和过期处理
//!
//! # 使用示例
//!
//! ```rust
//! use std::sync::Arc;
//! use flare_core::{
//!     server::{
//!         Server, ServerConfig, ConnectionBasedManager,
//!     },
//! };
//!
//! // 创建连接管理器
//! let connection_manager = Arc::new(ConnectionBasedManager::new());
//! 
//! // 创建服务器配置
//! let config = ServerConfig::default();
//! 
//! // 创建服务器实例
//! let mut server = Server::new(config, connection_manager);
//! ```

use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::collections::HashMap;
use tokio::sync::RwLock;
use tracing::info;
use std::time::Duration;

use crate::common::{
    error::Result,
    connections::{
        types::{ConnectionConfig, ConnectionType},
    },
};

use super::{
    service::ServerService, 
    quic::QuicServer, 
    websocket::WebSocketServer,
    manager::traits::ConnectionManager,
    manager::heartbeat_manager::{HeartbeatManager, HeartbeatConfig},
    auth::{AuthManager, AuthHandler, SimpleAuthHandler},
};

/// 服务端类型
#[derive(Debug, Clone, PartialEq)]
pub enum ServerType {
    /// WebSocket服务
    WebSocket,
    /// QUIC服务
    Quic,
    /// 同时支持WebSocket和QUIC
    Both,
}

/// 服务端配置
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// WebSocket监听地址
    pub websocket_addr: Option<String>,
    /// QUIC监听地址
    pub quic_addr: Option<String>,
    /// 是否启用TLS
    pub enable_tls: bool,
    /// TLS证书路径
    pub tls_cert_path: Option<String>,
    /// TLS私钥路径
    pub tls_key_path: Option<String>,
    /// 最大连接数
    pub max_connections: usize,
    /// 连接超时时间（毫秒）
    pub connection_timeout_ms: u64,
    /// 心跳检测间隔（毫秒）
    pub heartbeat_interval_ms: u64,
    /// 是否启用自动清理过期连接
    pub enable_auto_cleanup: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            websocket_addr: Some("127.0.0.1:8080".to_string()),
            quic_addr: Some("127.0.0.1:8081".to_string()),
            enable_tls: false,
            tls_cert_path: None,
            tls_key_path: None,
            max_connections: 1000,
            connection_timeout_ms: 30000,
            heartbeat_interval_ms: 10000,
            enable_auto_cleanup: true,
        }
    }
}

/// 服务端主类
///
/// 负责协调各种服务组件，管理连接和消息处理
///
/// # 泛型参数
///
/// * `T` - 连接管理器类型，必须实现 [ConnectionManager](../manager/traits/trait.ConnectionManager.html) trait
///
/// # 使用示例
///
/// ```rust
/// use std::sync::Arc;
/// use flare_core::{
///     server::{
///         Server, ServerConfig, ConnectionBasedManager,
///     },
/// };
///
/// // 创建连接管理器
/// let connection_manager = Arc::new(ConnectionBasedManager::new());
/// 
/// // 创建服务器配置
/// let config = ServerConfig::default();
/// 
/// // 创建服务器实例
/// let mut server = Server::new(config, connection_manager);
/// ```
pub struct Server<T: ConnectionManager> {
    /// 配置
    config: ServerConfig,
    /// 连接管理器
    connection_manager: Arc<T>,
    /// 心跳管理器
    heartbeat_manager: Arc<HeartbeatManager<dyn crate::common::connections::traits::ServerConnection>>,
    /// 服务句柄
    services: Arc<RwLock<HashMap<String, Arc<dyn ServerService>>>>,
    /// 心跳任务句柄
    heartbeat_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// 是否正在运行
    is_running: AtomicBool,
}

impl<T: ConnectionManager + 'static> Server<T> {
    /// 创建新的服务端实例
    ///
    /// # 参数
    ///
    /// * `config` - 服务器配置
    /// * `connection_manager` - 连接管理器
    ///
    /// # 返回值
    ///
    /// 返回新的 [Server](struct.Server.html) 实例
    pub fn new(config: ServerConfig, connection_manager: Arc<T>) -> Self {
        // 创建心跳管理器配置
        let heartbeat_config = HeartbeatConfig {
            check_interval: Duration::from_millis(config.heartbeat_interval_ms),
            connection_timeout: Duration::from_millis(config.connection_timeout_ms),
            enable_auto_cleanup: config.enable_auto_cleanup,
        };
        
        // 创建心跳管理器
        let heartbeat_manager = Arc::new(HeartbeatManager::new(heartbeat_config));
        
        Self {
            config,
            connection_manager,
            heartbeat_manager,
            services: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_task: Arc::new(RwLock::new(None)),
            is_running: AtomicBool::new(false),
        }
    }
    
    /// 启动服务端
    ///
    /// 根据配置启动相应的服务：
    /// - 如果配置了WebSocket地址，则启动WebSocket服务
    /// - 如果配置了QUIC地址，则启动QUIC服务
    /// - 如果两个都配置了，则同时启动两个服务
    ///
    /// # 返回值
    ///
    /// 返回操作结果
    pub async fn start(&mut self) -> Result<()> {
        if self.is_running.load(Ordering::Relaxed) {
            return Ok(());
        }
        
        info!("正在启动服务端...");
        
        // 启动心跳检测任务
        if self.config.enable_auto_cleanup {
            let heartbeat_task = self.heartbeat_manager.start_heartbeat_task();
            *self.heartbeat_task.write().await = Some(heartbeat_task);
            info!("心跳检测任务已启动");
        }
        
        // 根据配置启动相应的服务
        match self.get_server_type() {
            ServerType::WebSocket => {
                if let Some(addr) = &self.config.websocket_addr {
                    self.start_websocket_server(addr).await?;
                }
            }
            ServerType::Quic => {
                if let Some(addr) = &self.config.quic_addr {
                    self.start_quic_server(addr).await?;
                }
            }
            ServerType::Both => {
                if let Some(addr) = &self.config.websocket_addr {
                    self.start_websocket_server(addr).await?;
                }
                if let Some(addr) = &self.config.quic_addr {
                    self.start_quic_server(addr).await?;
                }
            }
        }
        
        self.is_running.store(true, Ordering::Relaxed);
        info!("服务端启动完成");
        Ok(())
    }
    
    /// 停止服务端
    ///
    /// 停止所有正在运行的服务并清理资源
    ///
    /// # 返回值
    ///
    /// 返回操作结果
    pub async fn stop(&mut self) -> Result<()> {
        if !self.is_running.load(Ordering::Relaxed) {
            return Ok(());
        }
        
        info!("正在停止服务端...");
        
        // 停止心跳任务
        if let Some(handle) = self.heartbeat_task.write().await.take() {
            handle.abort();
            info!("心跳检测任务已停止");
        }
        
        // 停止所有服务
        let services = self.services.read().await;
        for (_name, service) in services.iter() {
            service.stop().await;
        }
        drop(services);
        
        // 清理所有连接
        self.connection_manager.clear_all().await;
        
        self.is_running.store(false, Ordering::Relaxed);
        info!("服务端已停止");
        Ok(())
    }
    
    /// 注册消息处理器
    ///
    /// 为所有服务注册统一的消息处理器
    ///
    /// # 参数
    ///
    /// * `handler` - 消息处理器
    pub async fn register_message_handler(&self, handler: Arc<dyn super::service::MessageHandler>) {
        let services = self.services.read().await;
        for (_name, service) in services.iter() {
            service.set_message_handler(Arc::clone(&handler)).await;
        }
    }
    
    /// 检查服务端是否正在运行
    ///
    /// # 返回值
    ///
    /// 如果服务端正在运行返回true，否则返回false
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }
    
    /// 获取服务端类型
    ///
    /// 根据配置确定服务端类型
    ///
    /// # 返回值
    ///
    /// 返回 [ServerType](enum.ServerType.html)
    pub fn get_server_type(&self) -> ServerType {
        match (&self.config.websocket_addr, &self.config.quic_addr) {
            (Some(_), Some(_)) => ServerType::Both,
            (Some(_), None) => ServerType::WebSocket,
            (None, Some(_)) => ServerType::Quic,
            (None, None) => ServerType::WebSocket, // 默认启动WebSocket
        }
    }
    
    /// 获取心跳管理器
    ///
    /// # 返回值
    ///
    /// 返回心跳管理器的引用
    pub fn get_heartbeat_manager(&self) -> &Arc<HeartbeatManager<dyn crate::common::connections::traits::ServerConnection>> {
        &self.heartbeat_manager
    }
    
    /// 获取统计信息
    ///
    /// # 返回值
    ///
    /// 返回服务器统计信息
    pub async fn get_stats(&self) -> ServerStats {
        let connection_stats = self.connection_manager.get_stats().await;
        let heartbeat_stats = self.heartbeat_manager.get_stats().await;
        
        ServerStats {
            connection_stats,
            heartbeat_stats,
            is_running: self.is_running.load(Ordering::Relaxed),
        }
    }
    
    /// 启动WebSocket服务
    async fn start_websocket_server(&self, addr: &str) -> Result<()> {
        info!("正在启动WebSocket服务: {}", addr);
        
        // 创建WebSocket服务配置
        let config = ConnectionConfig::server(
            format!("ws_server_{}", addr.replace(":", "_")),
            addr.to_string(),
        )
        .with_type(ConnectionType::WebSocket)
        .with_timeout(self.config.connection_timeout_ms)
        .with_serialization_format(crate::common::serialization::SerializationFormat::Protobuf); // 使用Protobuf序列化
        
        // 创建认证管理器
        let auth_handler: Arc<dyn AuthHandler> = Arc::new(SimpleAuthHandler::new());
        let auth_manager = Arc::new(AuthManager::new(auth_handler, Duration::from_secs(30)));
        
        // 创建WebSocket服务
        let service = WebSocketServer::new(
            config,
            Arc::clone(&self.connection_manager),
            auth_manager,
        );
        
        let service_arc: Arc<dyn ServerService> = Arc::new(service);
        
        // 启动服务
        service_arc.start().await?;
        
        // 注册服务
        self.services.write().await.insert("websocket".to_string(), service_arc);
        
        info!("WebSocket服务启动完成: {}", addr);
        Ok(())
    }
    
    /// 启动QUIC服务
    async fn start_quic_server(&self, addr: &str) -> Result<()> {
        info!("正在启动QUIC服务: {}", addr);
        
        // 创建QUIC服务配置
        let config = ConnectionConfig::server(
            format!("quic_server_{}", addr.replace(":", "_")),
            addr.to_string(),
        )
        .with_type(ConnectionType::Quic)
        .with_timeout(self.config.connection_timeout_ms)
        .with_serialization_format(crate::common::serialization::SerializationFormat::Protobuf); // 使用Protobuf序列化
        
        // 创建认证管理器
        let auth_handler: Arc<dyn AuthHandler> = Arc::new(SimpleAuthHandler::new());
        let auth_manager = Arc::new(AuthManager::new(auth_handler, Duration::from_secs(30)));
        
        // 创建QUIC服务
        let service = QuicServer::new(
            config,
            Arc::clone(&self.connection_manager),
            auth_manager,
        );
        
        let service_arc: Arc<dyn ServerService> = Arc::new(service);
        
        // 启动服务
        service_arc.start().await?;
        
        // 注册服务
        self.services.write().await.insert("quic".to_string(), service_arc);
        
        info!("QUIC服务启动完成: {}", addr);
        Ok(())
    }
    
    /// 启动WebSocket服务（带自定义认证处理器）
    pub async fn start_websocket_server_with_auth(&self, addr: &str, auth_handler: Arc<dyn AuthHandler>) -> Result<()> {
        info!("正在启动WebSocket服务（自定义认证）: {}", addr);
        
        // 创建WebSocket服务配置
        let config = ConnectionConfig::server(
            format!("ws_server_{}", addr.replace(":", "_")),
            addr.to_string(),
        )
        .with_type(ConnectionType::WebSocket)
        .with_timeout(self.config.connection_timeout_ms)
        .with_serialization_format(crate::common::serialization::SerializationFormat::Protobuf); // 使用Protobuf序列化
        
        // 创建认证管理器
        let auth_manager = Arc::new(AuthManager::new(auth_handler, Duration::from_secs(30)));
        
        // 创建WebSocket服务
        let service = WebSocketServer::new(
            config,
            Arc::clone(&self.connection_manager),
            auth_manager,
        );
        
        let service_arc: Arc<dyn ServerService> = Arc::new(service);
        
        // 启动服务
        service_arc.start().await?;
        
        // 注册服务
        self.services.write().await.insert("websocket".to_string(), service_arc);
        
        info!("WebSocket服务启动完成: {}", addr);
        Ok(())
    }
    
    /// 启动QUIC服务（带自定义认证处理器）
    pub async fn start_quic_server_with_auth(&self, addr: &str, auth_handler: Arc<dyn AuthHandler>) -> Result<()> {
        info!("正在启动QUIC服务（自定义认证）: {}", addr);
        
        // 创建QUIC服务配置
        let config = ConnectionConfig::server(
            format!("quic_server_{}", addr.replace(":", "_")),
            addr.to_string(),
        )
        .with_type(ConnectionType::Quic)
        .with_timeout(self.config.connection_timeout_ms)
        .with_serialization_format(crate::common::serialization::SerializationFormat::Protobuf); // 使用Protobuf序列化
        
        // 创建认证管理器
        let auth_manager = Arc::new(AuthManager::new(auth_handler, Duration::from_secs(30)));
        
        // 创建QUIC服务
        let service = QuicServer::new(
            config,
            Arc::clone(&self.connection_manager),
            auth_manager,
        );
        
        let service_arc: Arc<dyn ServerService> = Arc::new(service);
        
        // 启动服务
        service_arc.start().await?;
        
        // 注册服务
        self.services.write().await.insert("quic".to_string(), service_arc);
        
        info!("QUIC服务启动完成: {}", addr);
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

impl<T: ConnectionManager> Drop for Server<T> {
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
    /// 连接统计信息
    pub connection_stats: super::manager::traits::ManagerStats,
    /// 心跳统计信息
    pub heartbeat_stats: super::manager::heartbeat_manager::HeartbeatStats,
    /// 是否正在运行
    pub is_running: bool,
}