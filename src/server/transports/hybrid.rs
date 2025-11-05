//! 混合服务端接口
//! 
//! 支持单个协议或多协议同时监听
//! 统一管理连接和心跳检测，简化服务器实现

use crate::server::config::ServerConfig;
use crate::common::config_types::TransportProtocol;
use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::server::handle::ServerHandle;
use super::{Server, ConnectionHandler};
use super::server_core::ServerCore;
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;
use tracing::error;

use super::websocket::WebSocketServer;
use super::quic::QUICServer;

/// 混合服务端
/// 
/// 支持单个协议或多协议同时监听
/// 统一管理连接和心跳检测，简化服务器实现
pub struct HybridServer {
    /// 内部服务器列表
    servers: Vec<Arc<Mutex<Box<dyn Server>>>>,
    /// 使用的协议列表
    protocols: Vec<TransportProtocol>,
    /// 是否正在运行
    is_running: Arc<AtomicBool>,
    /// 服务器核心功能（统一管理连接和心跳）
    core: Option<Arc<ServerCore>>,
    /// 配置（用于启动心跳检测）
    config: ServerConfig,
}

impl HybridServer {
    /// 创建新的混合服务端
    /// 
    /// # 参数
    /// - `config`: 服务端配置
    /// - `handler`: 连接处理器
    /// 
    /// # 返回
    /// 混合服务端实例
    pub fn new(config: ServerConfig, handler: Arc<dyn ConnectionHandler>) -> Result<Self> {
        Self::with_connection_manager(config, handler, None, None, None, None)
    }
    
    /// 使用指定的连接管理器创建混合服务端
    /// 
    /// # 参数
    /// - `config`: 服务端配置
    /// - `handler`: 连接处理器
    /// - `connection_manager`: 可选的连接管理器，如果为 None，则创建新的并统一管理
    /// - `device_manager`: 可选的设备管理器，如果为 None 且配置中指定了设备冲突策略，则自动创建
    /// - `event_handler`: 可选的事件处理器
    /// - `authenticator`: 可选的认证器，如果配置中启用认证，必须提供
    /// 
    /// # 返回
    /// 混合服务端实例
    pub fn with_connection_manager(
        config: ServerConfig,
        handler: Arc<dyn ConnectionHandler>,
        connection_manager: Option<Arc<crate::server::connection::ConnectionManager>>,
        device_manager: Option<Arc<crate::server::device::DeviceManager>>,
        event_handler: Option<Arc<dyn crate::server::events::handler::ServerEventHandler>>,
        authenticator: Option<Arc<dyn crate::server::auth::Authenticator>>,
    ) -> Result<Self> {
        // 创建服务器核心，统一管理连接和心跳
        let mut core = ServerCore::new(&config, connection_manager.clone());
        
        // 确定设备管理器：优先使用传入的，否则根据配置创建
        let final_device_manager = if let Some(dm) = device_manager {
            Some(dm)
        } else if config.device_conflict_strategy != crate::common::device::DeviceConflictStrategy::AllowAll {
            Some(Arc::new(crate::server::device::DeviceManager::new(
                config.device_conflict_strategy.clone(),
            )))
        } else {
            None
        };
        
        core = core.with_device_manager(final_device_manager)
            .with_event_handler(event_handler)
            .with_authenticator(authenticator);
        
        // 将 ServerCore 包装为 Arc，以便共享给 WebSocketServer 和 QUICServer
        let shared_core = Arc::new(core);
        
        let protocols = config.get_protocols();
        let mut servers = Vec::new();
        
        for protocol in &protocols {
            let mut server_config = config.clone();
            server_config.transport = *protocol;
            server_config.transports = None;
            
            // 使用配置的协议地址，如果没有配置则使用默认地址
            let bind_address = config.get_protocol_address(protocol);
            server_config.bind_address = bind_address;
            
            // 使用共享的 ServerCore，确保设备管理器等配置正确传递
            let server: Box<dyn Server> = match protocol {
                TransportProtocol::WebSocket => {
                    Box::new(WebSocketServer::with_shared_core(
                        server_config,
                        Arc::clone(&handler),
                        shared_core.clone(),
                    ))                                                                         
                }
                TransportProtocol::QUIC => {
                    Box::new(QUICServer::with_shared_core(
                        server_config,
                        Arc::clone(&handler),
                        shared_core.clone(),
                    )?)                                                                             
                }
                TransportProtocol::TCP => {
                    return Err(crate::common::error::FlareError::protocol_error(
                        "TCP transport not yet implemented".to_string()
                    ));
                }
            };
            
            servers.push(Arc::new(Mutex::new(server)));
        }
        
        Ok(Self {
            servers,
            protocols,
            is_running: Arc::new(AtomicBool::new(false)),
            core: Some(shared_core),
            config,
        })
    }
    
    /// 获取使用的协议列表
    pub fn protocols(&self) -> &[TransportProtocol] {
        &self.protocols
    }
    
    /// 获取 ServerCore 的引用（用于创建 ServerHandle）
    pub fn core(&self) -> Option<&Arc<ServerCore>> {
        self.core.as_ref()
    }
    
    /// 获取 ServerCore 的可变引用（用于修改）
    pub fn core_mut(&mut self) -> Option<&mut Arc<ServerCore>> {
        self.core.as_mut()
    }
}

#[async_trait::async_trait]
impl Server for HybridServer {
    async fn start(&mut self) -> Result<()> {
        // 启动心跳检测（统一管理）
        if let Some(ref mut core) = self.core {
            core.start_heartbeat(&self.config);
        }
        
        let mut started_count = 0;
        let mut errors = Vec::new();
        
        // 启动所有服务器
        for server in &self.servers {
            let mut s = server.lock().await;
            match s.start().await {
                Ok(_) => {
                    started_count += 1;
                }
                Err(e) => {
                    error!("Failed to start server: {:?}", e);
                    errors.push(e);
                }
            }
        }
        
        // 如果所有服务器都启动失败，返回错误
        if started_count == 0 && !errors.is_empty() {
            self.is_running.store(false, Ordering::SeqCst);
            return Err(errors.remove(0));
        }
        
        // 如果至少有一个服务器启动成功，标记为运行状态
        if started_count > 0 {
            self.is_running.store(true, Ordering::SeqCst);
        }
        
        Ok(())
    }
    
    async fn stop(&mut self) -> Result<()> {
        self.is_running.store(false, Ordering::SeqCst);
        
        // 停止心跳检测
        if let Some(ref mut core) = self.core {
            core.stop_heartbeat();
        }
        
        // 停止所有服务器
        for server in &self.servers {
            let mut s = server.lock().await;
            if let Err(e) = s.stop().await {
                error!("Failed to stop server: {:?}", e);
            }
        }
        
        Ok(())
    }
    
    fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }
}

/// 让 HybridServer 实现 ServerHandle trait
/// 这样可以在任何需要发送消息的地方注入 HybridServer 的 ServerCore，而不需要整个 Server
#[async_trait]
impl ServerHandle for HybridServer {
    async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        // 直接通过 ServerCore（实现了 ServerHandle）发送消息
        if let Some(ref core) = self.core {
            return ServerHandle::send_to(&**core, connection_id, frame).await;
                }
        Err(crate::common::error::FlareError::protocol_error(
            "ServerCore not initialized".to_string()
        ))
    }
    
    async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        // 直接通过 ServerCore（实现了 ServerHandle）发送消息
        if let Some(ref core) = self.core {
            return ServerHandle::send_to_user(&**core, user_id, frame).await;
            }
        Err(crate::common::error::FlareError::protocol_error(
            "ServerCore not initialized".to_string()
        ))
    }
    
    async fn broadcast(&self, frame: &Frame) -> Result<()> {
        // 直接通过 ServerCore（实现了 ServerHandle）广播消息
        if let Some(ref core) = self.core {
            return ServerHandle::broadcast(&**core, frame).await;
        }
        Err(crate::common::error::FlareError::protocol_error(
            "ServerCore not initialized".to_string()
        ))
    }
    
    async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        // 直接通过 ServerCore（实现了 ServerHandle）广播消息
        if let Some(ref core) = self.core {
            return ServerHandle::broadcast_except(&**core, frame, exclude_connection_id).await;
        }
        Err(crate::common::error::FlareError::protocol_error(
            "ServerCore not initialized".to_string()
        ))
    }
    
    async fn disconnect(&self, connection_id: &str) -> Result<()> {
        // 直接通过 ServerCore（实现了 ServerHandle）断开连接
        if let Some(ref core) = self.core {
            return ServerHandle::disconnect(&**core, connection_id).await;
        }
        Err(crate::common::error::FlareError::protocol_error(
            "ServerCore not initialized".to_string()
        ))
    }
    
    fn connection_count(&self) -> usize {
        // 直接通过 ServerCore（实现了 ServerHandle）获取连接数量
        if let Some(ref core) = self.core {
            return ServerHandle::connection_count(&**core);
        }
        0
    }
    
    fn user_count(&self) -> usize {
        // 直接通过 ServerCore（实现了 ServerHandle）获取用户数量
        if let Some(ref core) = self.core {
            return ServerHandle::user_count(&**core);
        }
        0
    }
}

