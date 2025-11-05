//! 观察者模式服务端构建器
//! 
//! 使用实现了 ConnectionHandler trait 的处理器

use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::server::{ServerConfig, ConnectionHandler, HybridServer, Server};
use crate::server::connection::ConnectionManager;
use std::sync::Arc;
use tokio::sync::Mutex;

/// 观察者模式服务端构建器
/// 
/// 使用实现了 ConnectionHandler trait 的处理器
pub struct ObserverServerBuilder {
    config: ServerConfig,
    handler: Option<Arc<dyn ConnectionHandler>>,
    connection_manager: Option<Arc<ConnectionManager>>,
    device_manager: Option<Arc<crate::server::device::DeviceManager>>,
    event_handler: Option<Arc<dyn crate::server::events::handler::ServerEventHandler>>,
}

impl ObserverServerBuilder {
    /// 创建新的观察者模式构建器
    pub fn new(bind_address: impl Into<String>) -> Self {
        Self {
            config: ServerConfig::new(bind_address.into()),
            handler: None,
            connection_manager: None,
            device_manager: None,
            event_handler: None,
        }
    }
    
    /// 设置设备管理器（用于设备冲突管理）
    pub fn with_device_manager(mut self, device_manager: Arc<crate::server::device::DeviceManager>) -> Self {
        self.device_manager = Some(device_manager);
        self
    }

    /// 设置事件处理器（可选，用于细化的命令处理）
    pub fn with_event_handler(mut self, event_handler: Arc<dyn crate::server::events::handler::ServerEventHandler>) -> Self {
        self.event_handler = Some(event_handler);
        self
    }

    /// 设置连接处理器（必须）
    pub fn with_handler(mut self, handler: Arc<dyn ConnectionHandler>) -> Self {
        self.handler = Some(handler);
        self
    }

    /// 设置连接管理器（可选，用于共享连接状态）
    pub fn with_connection_manager(mut self, manager: Arc<ConnectionManager>) -> Self {
        self.connection_manager = Some(manager);
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

    /// 为特定协议设置监听地址
    pub fn with_protocol_address(mut self, protocol: crate::common::config_types::TransportProtocol, address: String) -> Self {
        self.config = self.config.with_protocol_address(protocol, address);
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
    pub fn build(self) -> Result<ObserverServer> {
        let handler = self.handler.ok_or_else(|| {
            crate::common::error::FlareError::general_error("Handler is required")
        })?;

        // 在创建 HybridServer 时就传入设备管理器和事件处理器
        // 这样确保 ServerCore 在创建时就有正确的配置，避免后续修改 Arc 的问题
        let server = if let Some(manager) = self.connection_manager {
            HybridServer::with_connection_manager(
                self.config,
                handler,
                Some(manager),
                self.device_manager,
                self.event_handler,
            )?
        } else {
            HybridServer::with_connection_manager(
                self.config,
                handler,
                None,
                self.device_manager,
                self.event_handler,
            )?
        };

        Ok(ObserverServer {
            server: Arc::new(Mutex::new(server)),
        })
    }
}

/// 观察者模式服务器实例
pub struct ObserverServer {
    server: Arc<Mutex<HybridServer>>,
}

impl ObserverServer {
    /// 启动服务器
    pub async fn start(&mut self) -> Result<()> {
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
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
            s.is_running()
        })
    }

    /// 获取连接数量
    pub fn connection_count(&self) -> usize {
        // 通过 ServerHandle 调用（HybridServer 实现了 ServerHandle）
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
            crate::server::handle::ServerHandle::connection_count(&*s)
        })
    }

    /// 获取用户数量
    pub fn user_count(&self) -> usize {
        // 通过 ServerHandle 调用（HybridServer 实现了 ServerHandle）
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
            crate::server::handle::ServerHandle::user_count(&*s)
        })
    }

    /// 向指定连接发送消息
    pub async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        // 通过 ServerHandle 调用（HybridServer 实现了 ServerHandle）
        let s = self.server.lock().await;
        crate::server::handle::ServerHandle::send_to(&*s, connection_id, frame).await
    }

    /// 向指定用户的所有连接发送消息
    pub async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        // 通过 ServerHandle 调用（HybridServer 实现了 ServerHandle）
        let s = self.server.lock().await;
        crate::server::handle::ServerHandle::send_to_user(&*s, user_id, frame).await
    }

    /// 广播消息到所有连接
    pub async fn broadcast(&self, frame: &Frame) -> Result<()> {
        // 通过 ServerHandle 调用（HybridServer 实现了 ServerHandle）
        let s = self.server.lock().await;
        crate::server::handle::ServerHandle::broadcast(&*s, frame).await
    }

    /// 广播消息到所有连接，排除指定连接
    pub async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        // 通过 ServerHandle 调用（HybridServer 实现了 ServerHandle）
        let s = self.server.lock().await;
        crate::server::handle::ServerHandle::broadcast_except(&*s, frame, exclude_connection_id).await
    }

    /// 断开指定连接
    pub async fn disconnect(&self, connection_id: &str) -> Result<()> {
        // 通过 ServerHandle 调用（HybridServer 实现了 ServerHandle）
        let s = self.server.lock().await;
        crate::server::handle::ServerHandle::disconnect(&*s, connection_id).await
    }

    /// 获取协议列表
    pub fn protocols(&self) -> Vec<crate::common::config_types::TransportProtocol> {
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
            s.protocols().to_vec()
        })
    }
    
    /// 获取连接管理器（用于创建 DefaultServerHandle）
    /// 
    /// # 返回
    /// 返回 ConnectionManagerTrait
    pub fn get_server_handle_components(&self) -> Option<Arc<dyn crate::server::connection::ConnectionManagerTrait>> {
        tokio::task::block_in_place(|| {
            let s = self.server.blocking_lock();
            if let Some(core) = s.core() {
                Some(core.connection_manager_trait())
            } else {
                None
            }
        })
    }
}

