//! 服务器核心功能
//! 
//! 提供统一的连接管理和心跳检测功能，简化服务器实现

use crate::server::connection::{ConnectionManager, ConnectionManagerTrait};
use crate::server::heartbeat::HeartbeatDetector;
use crate::server::handle::ServerHandle;
use crate::common::MessageParser;
use crate::server::config::ServerConfig;
use crate::common::protocol::Frame;
use crate::common::error::Result;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

/// 服务器核心功能
/// 
/// 统一管理连接和心跳检测，简化服务器实现
pub struct ServerCore {
    /// 连接管理器
    pub connection_manager: Arc<ConnectionManager>,
    /// 消息解析器
    pub parser: MessageParser,
    /// 心跳检测器（可选）
    heartbeat_detector: Option<HeartbeatDetector>,
}

impl ServerCore {
    /// 创建新的服务器核心
    pub fn new(
        config: &ServerConfig,
        connection_manager: Option<Arc<ConnectionManager>>,
    ) -> Self {
        let connection_manager = connection_manager.unwrap_or_else(|| {
            Arc::new(ConnectionManager::new())
        });
        
        let parser = MessageParser::new(
            config.default_serialization_format,
            config.default_compression,
        );
        
        Self {
            connection_manager,
            parser,
            heartbeat_detector: None,
        }
    }
    
    /// 启动心跳检测
    pub fn start_heartbeat(&mut self, config: &ServerConfig) {
        let manager_trait = Arc::clone(&self.connection_manager) as Arc<dyn ConnectionManagerTrait>;
        let timeout = config.connection_timeout;
        let check_interval = Duration::from_secs(timeout.as_secs() / 3).max(Duration::from_secs(10));
        
        let mut detector = HeartbeatDetector::new(
            manager_trait,
            timeout,
            check_interval,
        );
        detector.start();
        self.heartbeat_detector = Some(detector);
    }
    
    /// 停止心跳检测
    pub fn stop_heartbeat(&mut self) {
        if let Some(ref mut detector) = self.heartbeat_detector {
            detector.stop();
        }
    }
    
    /// 获取连接管理器 trait
    pub fn connection_manager_trait(&self) -> Arc<dyn ConnectionManagerTrait> {
        Arc::clone(&self.connection_manager) as Arc<dyn ConnectionManagerTrait>
    }
    
    /// 向指定连接发送消息
    pub async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        let manager_trait = self.connection_manager_trait();
        manager_trait.send_frame_to(connection_id, frame, &self.parser).await
    }
    
    /// 向指定用户的所有连接发送消息
    pub async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        let manager_trait = self.connection_manager_trait();
        manager_trait.send_frame_to_user(user_id, frame, &self.parser).await
    }
    
    /// 广播消息到所有连接
    pub async fn broadcast(&self, frame: &Frame) -> Result<()> {
        let manager_trait = self.connection_manager_trait();
        manager_trait.broadcast_frame(frame, &self.parser).await
    }
    
    /// 广播消息到所有连接，排除指定连接
    pub async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        let manager_trait = self.connection_manager_trait();
        manager_trait.broadcast_frame_except(frame, exclude_connection_id, &self.parser).await
    }
    
    /// 获取连接数量
    pub fn connection_count(&self) -> usize {
        self.connection_manager.connection_count()
    }
    
    /// 获取用户数量
    pub fn user_count(&self) -> usize {
        self.connection_manager.stats().total_users
    }
    
    /// 断开指定连接
    pub async fn disconnect(&self, connection_id: &str) -> Result<()> {
        let manager_trait = self.connection_manager_trait();
        manager_trait.remove_connection(connection_id).await
    }
    
    /// 获取所有连接 ID（异步）
    pub async fn list_connections(&self) -> Vec<String> {
        let manager_trait = self.connection_manager_trait();
        manager_trait.list_connections().await
    }
}

/// 让 ServerCore 实现 ServerHandle trait
/// 这样可以在任何需要发送消息的地方注入 ServerCore，而不需要整个 Server
#[async_trait]
impl ServerHandle for ServerCore {
    async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        self.send_to(connection_id, frame).await
    }
    
    async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        self.send_to_user(user_id, frame).await
    }
    
    async fn broadcast(&self, frame: &Frame) -> Result<()> {
        self.broadcast(frame).await
    }
    
    async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        self.broadcast_except(frame, exclude_connection_id).await
    }
    
    async fn disconnect(&self, connection_id: &str) -> Result<()> {
        self.disconnect(connection_id).await
    }
    
    fn connection_count(&self) -> usize {
        self.connection_count()
    }
    
    fn user_count(&self) -> usize {
        self.user_count()
    }
}

