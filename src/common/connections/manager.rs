//! 客户端连接管理器
//! 
//! 管理客户端连接的生命周期，包括重连、心跳等

use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Mutex};
use tracing::{info, debug, warn};

use crate::common::{
    error::Result,
    protocol::UnifiedProtocolMessage,
    connections::{
        traits::{ClientConnection, ConnectionEventHandler, ConnectionFactory as ConnectionFactoryTrait},
        types::{ConnectionConfig, ConnectionState},
        factory::ConnectionFactory,
    },
};

/// 连接管理器配置
#[derive(Debug, Clone)]
pub struct ManagerConfig {
    /// 最大连接数
    pub max_connections: usize,
    /// 连接超时
    pub connection_timeout: Duration,
    /// 重连间隔
    pub reconnect_interval: Duration,
    /// 最大重连次数
    pub max_reconnect_attempts: u32,
    /// 心跳检查间隔
    pub heartbeat_check_interval: Duration,
    /// 是否启用自动重连
    pub auto_reconnect: bool,
}

impl Default for ManagerConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            connection_timeout: Duration::from_secs(30),
            reconnect_interval: Duration::from_secs(5),
            max_reconnect_attempts: 3,
            heartbeat_check_interval: Duration::from_secs(10),
            auto_reconnect: true,
        }
    }
}

/// 连接信息
struct ConnectionInfo {
    /// 连接实例
    connection: Arc<Mutex<Box<dyn ClientConnection>>>,
    /// 连接配置
    config: ConnectionConfig,
    /// 创建时间
    created_at: Instant,
    /// 最后活跃时间
    last_activity: Instant,
    /// 重连次数
    reconnect_attempts: u32,
    /// 是否正在重连
    is_reconnecting: bool,
    /// 连接状态
    state: ConnectionState,
}

impl ConnectionInfo {
    fn new(connection: Box<dyn ClientConnection>, config: ConnectionConfig) -> Self {
        Self {
            connection: Arc::new(Mutex::new(connection)),
            config,
            created_at: Instant::now(),
            last_activity: Instant::now(),
            reconnect_attempts: 0,
            is_reconnecting: false,
            state: ConnectionState::Initializing,
        }
    }
    
    fn update_activity(&mut self) {
        self.last_activity = Instant::now();
    }
    
    fn update_state(&mut self, state: ConnectionState) {
        self.state = state;
    }
}

/// 客户端连接管理器
pub struct ConnectionManager {
    /// 连接工厂
    factory: ConnectionFactory,
    /// 所有连接
    connections: Arc<RwLock<HashMap<String, ConnectionInfo>>>,
    /// 管理器配置
    config: ManagerConfig,
    /// 事件处理器
    event_handler: Arc<RwLock<Option<Arc<dyn ConnectionEventHandler>>>>,
    /// 管理任务句柄
    management_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

impl ConnectionManager {
    /// 创建新的连接管理器
    pub fn new(config: ManagerConfig) -> Self {
        Self {
            factory: ConnectionFactory::new(),
            connections: Arc::new(RwLock::new(HashMap::new())),
            config,
            event_handler: Arc::new(RwLock::new(None)),
            management_task: Arc::new(RwLock::new(None)),
        }
    }
    
    /// 设置事件处理器
    pub async fn set_event_handler(&mut self, handler: Arc<dyn ConnectionEventHandler>) {
        *self.event_handler.write().await = Some(handler);
    }
    
    /// 创建并添加新连接
    pub async fn create_connection(&mut self, config: ConnectionConfig) -> Result<String> {
        // 检查连接数量限制
        let connections = self.connections.read().await;
        if connections.len() >= self.config.max_connections {
            return Err(crate::common::error::FlareError::connection_failed(
                "已达到最大连接数量限制"
            ));
        }
        drop(connections);
        
        // 创建连接
        let connection = self.factory.create_client_connection(config.clone()).await?;
        let connection_id = config.id.clone();
        
        // 创建连接信息
        let conn_info = ConnectionInfo::new(connection, config);
        
        // 添加到管理器
        let mut connections = self.connections.write().await;
        connections.insert(connection_id.clone(), conn_info);
        
        info!("连接已创建: {}", connection_id);
        
        Ok(connection_id)
    }
    
    /// 建立连接
    pub async fn connect(&mut self, connection_id: &str) -> Result<()> {
        // 先建立连接和启动心跳
        {
            let connections = self.connections.read().await;
            if let Some(conn_info) = connections.get(connection_id) {
                let mut connection = conn_info.connection.lock().await;
                
                // 建立连接
                connection.connect().await?;
                
                // 心跳功能已移除，由外部处理
                info!("心跳功能已移除，请使用 send_heartbeat 方法手动发送心跳");
            } else {
                return Err(crate::common::error::FlareError::connection_failed(
                    "连接不存在"
                ));
            }
        }
        
        // 更新状态和活跃时间
        let mut connections = self.connections.write().await;
        if let Some(conn_info) = connections.get_mut(connection_id) {
            conn_info.update_state(ConnectionState::Connected);
            conn_info.update_activity();
        }
        
        info!("连接已建立: {}", connection_id);
        Ok(())
    }
    
    /// 断开连接
    pub async fn disconnect(&mut self, connection_id: &str) -> Result<()> {
        // 先停止心跳和断开连接
        {
            let connections = self.connections.read().await;
            if let Some(conn_info) = connections.get(connection_id) {
                let mut connection = conn_info.connection.lock().await;
                
                // 心跳功能已移除，由外部处理
                info!("心跳功能已移除，无需停止");
                
                // 断开连接
                connection.disconnect().await?;
            } else {
                return Err(crate::common::error::FlareError::connection_failed(
                    "连接不存在"
                ));
            }
        }
        
        // 更新状态
        let mut connections = self.connections.write().await;
        if let Some(conn_info) = connections.get_mut(connection_id) {
            conn_info.update_state(ConnectionState::Disconnected);
        }
        
        info!("连接已断开: {}", connection_id);
        Ok(())
    }
    
    /// 发送消息
    pub async fn send_message(&mut self, connection_id: &str, message: UnifiedProtocolMessage) -> Result<()> {
        // 先发送消息
        {
            let connections = self.connections.read().await;
            if let Some(conn_info) = connections.get(connection_id) {
                let mut connection = conn_info.connection.lock().await;
                connection.send_message(message).await?;
            } else {
                return Err(crate::common::error::FlareError::connection_failed(
                    "连接不存在"
                ));
            }
        }
        
        // 更新活跃时间
        let mut connections = self.connections.write().await;
        if let Some(conn_info) = connections.get_mut(connection_id) {
            conn_info.update_activity();
        }
        
        debug!("消息已发送: {}", connection_id);
        Ok(())
    }
    
    /// 接收消息
    pub async fn receive_message(&mut self, connection_id: &str) -> Result<Option<UnifiedProtocolMessage>> {
        // 先接收消息
        let message = {
            let connections = self.connections.read().await;
            if let Some(conn_info) = connections.get(connection_id) {
                let mut connection = conn_info.connection.lock().await;
                connection.receive_message().await?
            } else {
                return Err(crate::common::error::FlareError::connection_failed(
                    "连接不存在"
                ));
            }
        };
        
        // 如果有消息，更新活跃时间
        if message.is_some() {
            let mut connections = self.connections.write().await;
            if let Some(conn_info) = connections.get_mut(connection_id) {
                conn_info.update_activity();
            }
        }
        
        Ok(message)
    }
    
    /// 移除连接
    pub async fn remove_connection(&mut self, connection_id: &str) -> Result<()> {
        let mut connections = self.connections.write().await;
        
        if let Some(conn_info) = connections.remove(connection_id) {
            let mut connection = conn_info.connection.lock().await;
            
            // 心跳功能已移除，由外部处理
            info!("心跳功能已移除，无需停止");
            
            // 断开连接
            let _ = connection.disconnect().await;
            
            info!("连接已移除: {}", connection_id);
            Ok(())
        } else {
            Err(crate::common::error::FlareError::connection_failed(
                "连接不存在"
            ))
        }
    }
    
    /// 获取连接状态
    pub async fn get_connection_state(&self, connection_id: &str) -> Option<ConnectionState> {
        let connections = self.connections.read().await;
        
        connections.get(connection_id).map(|conn_info| conn_info.state.clone())
    }
    
    /// 获取所有连接状态
    pub async fn get_all_connection_states(&self) -> HashMap<String, ConnectionState> {
        let connections = self.connections.read().await;
        
        connections.iter()
            .map(|(id, conn_info)| (id.clone(), conn_info.state.clone()))
            .collect()
    }
    
    /// 检查连接是否活跃
    pub async fn is_connection_active(&self, connection_id: &str) -> bool {
        let connections = self.connections.read().await;
        
        connections.get(connection_id)
            .map(|conn_info| {
                matches!(conn_info.state, ConnectionState::Connected | ConnectionState::Ready)
            })
            .unwrap_or(false)
    }
    
    /// 获取连接数量
    pub async fn get_connection_count(&self) -> usize {
        self.connections.read().await.len()
    }
    
    /// 启动管理任务
    pub async fn start_management(&mut self) -> Result<()> {
        if self.management_task.read().await.is_some() {
            return Ok(());
        }
        
        let connections = Arc::clone(&self.connections);
        let config = self.config.clone();
        let _event_handler = Arc::clone(&self.event_handler);
        
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.heartbeat_check_interval);
            
            loop {
                interval.tick().await;
                
                // 检查所有连接
                let mut connections = connections.write().await;
                let mut to_reconnect = Vec::new();
                
                for (id, conn_info) in connections.iter_mut() {
                    // 检查连接状态
                    if matches!(conn_info.state, ConnectionState::Disconnected | ConnectionState::Failed) {
                        if conn_info.reconnect_attempts < config.max_reconnect_attempts && !conn_info.is_reconnecting {
                            to_reconnect.push(id.clone());
                        }
                    }
                    
                    // 检查连接超时
                    if conn_info.last_activity.elapsed() > config.connection_timeout {
                        warn!("连接超时: {}", id);
                        conn_info.update_state(ConnectionState::Failed);
                    }
                }
                
                // 处理需要重连的连接
                for id in to_reconnect {
                    if let Some(conn_info) = connections.get_mut(&id) {
                        conn_info.is_reconnecting = true;
                        conn_info.reconnect_attempts += 1;
                        
                        info!("尝试重连: {} (第 {} 次)", id, conn_info.reconnect_attempts);
                        
                        // 这里应该实现真正的重连逻辑
                        // 目前只是标记状态
                        conn_info.update_state(ConnectionState::Reconnecting);
                    }
                }
            }
        });
        
        *self.management_task.write().await = Some(task);
        info!("连接管理任务已启动");
        Ok(())
    }
    
    /// 停止管理任务
    pub async fn stop_management(&mut self) -> Result<()> {
        if let Some(task) = self.management_task.write().await.take() {
            task.abort();
            info!("连接管理任务已停止");
        }
        Ok(())
    }
    
    /// 清理不活跃的连接
    pub async fn cleanup_inactive_connections(&mut self, timeout: Duration) -> usize {
        let mut connections = self.connections.write().await;
        let mut to_remove = Vec::new();
        
        for (id, conn_info) in connections.iter() {
            if conn_info.last_activity.elapsed() > timeout {
                to_remove.push(id.clone());
            }
        }
        
        let removed_count = to_remove.len();
        
        for id in to_remove {
            if let Some(conn_info) = connections.remove(&id) {
                let mut connection = conn_info.connection.lock().await;
                // 心跳功能已移除，由外部处理
                let _ = connection.disconnect().await;
                
                info!("清理不活跃连接: {}", id);
            }
        }
        
        removed_count
    }
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new(ManagerConfig::default())
    }
}

impl Drop for ConnectionManager {
    fn drop(&mut self) {
        // 确保在析构时停止管理任务
        if let Ok(task_guard) = self.management_task.try_write() {
            if let Some(task) = task_guard.as_ref() {
                task.abort();
            }
        }
    }
}
