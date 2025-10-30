//! FastClient - 开箱即用的高级客户端实现
//!
//! 基于基础 Client 提供丰富的功能，包括自动心跳、自动认证、断线重连等

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use tokio::time::{Duration, interval};

use crate::common::{
    error::Result,
    protocol::Frame,
    connections::types::ConnectionState,
};

use super::{
    event::FastEvent,
    auth::FastAuthManager,
    event_adapter::FastClientEventAdapter,
};



use crate::client::{
    Client,
    config::ClientConfig,
};

/// FastClient - 开箱即用的高级客户端
/// 
/// 基于 Client 提供高级功能：
/// - 自动心跳
/// - 自动重连
/// - 自动认证
/// - 连接状态监控
/// - 连接质量监控
pub struct FastClient {
    /// 基础客户端
    client: Arc<RwLock<Client>>,
    /// 是否正在运行
    is_running: Arc<RwLock<bool>>,
    /// 心跳任务句柄
    heartbeat_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// 重连任务句柄
    reconnect_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// 连接监控任务句柄
    connection_monitor_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// 认证管理器
    auth_manager: Arc<FastAuthManager>,
    /// FastClient 事件处理器
    event_handler: Arc<dyn FastEvent>,
}

impl FastClient {
    /// 创建新的FastClient实例
    pub fn new(config: ClientConfig, auth_config: super::auth::AuthConfig, event_handler: Arc<dyn FastEvent>) -> Self {
        let auth_timeout = auth_config.timeout_ms;
        let auth_manager = Arc::new(FastAuthManager::new(auth_config, auth_timeout));
        
        // 创建基础客户端，使用 ClientEvent 适配器
        let client_event_handler = Arc::new(FastClientEventAdapter::new(Arc::clone(&event_handler), Arc::clone(&auth_manager)));
        let client = Client::with_client_event_handler(config, client_event_handler);
        
        Self {
            client: Arc::new(RwLock::new(client)),
            is_running: Arc::new(RwLock::new(false)),
            heartbeat_task: Arc::new(RwLock::new(None)),
            reconnect_task: Arc::new(RwLock::new(None)),
            connection_monitor_task: Arc::new(RwLock::new(None)),
            auth_manager,
            event_handler,
        }
    }
    
    /// 创建新的FastClient实例，使用默认事件处理器
    pub fn with_default_handler(config: ClientConfig, auth_config: super::auth::AuthConfig) -> Self {
        Self::new(config, auth_config, Arc::new(super::DefFastEventHandler::default()))
    }

    /// 启动客户端
    pub async fn start(&mut self) -> Result<()> {
        // 检查是否已在运行
        {
            let running = self.is_running.read().await;
            if *running {
                return Err(crate::common::error::FlareError::general_error("客户端已在运行".to_string()));
            }
        }
        
        // 标记为运行状态
        {
            let mut running = self.is_running.write().await;
            *running = true;
        }
        
        // 重连参数已通过ClientConfig配置，无需额外设置
        
        // 连接到服务器（基础 Client 会自动处理协议选择和事件）
        {
            let client = self.client.write().await;
            client.connect().await?;
        }
        
        // 执行认证流程（如果启用）
        {
            let client = self.client.read().await;
            self.auth_manager.authenticate(&client, Some(Arc::clone(&self.event_handler))).await?;
        }
        
        // 获取配置引用
        let config = {
            let client = self.client.read().await;
            client.get_config().clone()
        };
        
        // 启动心跳任务（如果启用）
        if config.heartbeat_interval_ms > 0 {
            self.start_heartbeat_task().await?;
        }
        
        // 启动重连监控任务（如果启用）
        if config.max_reconnect_attempts > 0 {
            self.start_reconnect_task().await?;
        }
        
        // 启动连接监控任务
        self.start_connection_monitor().await?;
        
        info!("FastClient启动成功");
        Ok(())
    }
    
    /// 停止客户端
    pub async fn stop(&mut self) -> Result<()> {
        // 检查是否正在运行
        {
            let running = self.is_running.read().await;
            if !*running {
                return Ok(());
            }
        }
        
        // 停止心跳任务
        self.stop_heartbeat_task().await;
        
        // 停止重连任务
        self.stop_reconnect_task().await;
        
        // 停止连接监控任务
        self.stop_connection_monitor().await;
        
        // 断开连接
        {
            let client = self.client.write().await;
            client.disconnect().await?;
        }
        
        // 标记为停止状态
        {
            let mut running = self.is_running.write().await;
            *running = false;
        }
        
        info!("FastClient已停止");
        Ok(())
    }
    
    /// 发送消息
    pub async fn send_message(&self, message: Frame) -> Result<()> {
        let client = self.client.read().await;
        let message_handler = client.get_message_handler();
        message_handler.send_message(message).await
    }
    
    /// 发送请求并等待响应
    pub async fn send_request<F>(
        &self,
        create_command: F,
        reliability: crate::common::protocol::Reliability,
        custom_timeout: Option<std::time::Duration>,
    ) -> Result<Frame>
    where
        F: FnOnce(String) -> Result<crate::common::protocol::commands::Command>,
    {
        let client = self.client.read().await;
        client.send_request(create_command, reliability, custom_timeout).await
    }
    
    /// 发送无需等待响应的消息
    pub async fn send_fire_and_forget<F>(
        &self,
        create_command: F,
        reliability: crate::common::protocol::Reliability,
    ) -> Result<()>
    where
        F: FnOnce(String) -> Result<crate::common::protocol::commands::Command>,
    {
        let client = self.client.read().await;
        client.send_fire_and_forget(create_command, reliability).await
    }
    
    /// 发送控制消息
    pub async fn send_control(&self, control_cmd: crate::common::protocol::commands::ControlCmd) -> Result<()> {
        let client = self.client.read().await;
        client.send_control(control_cmd).await
    }
    
    /// 发送通知消息
    pub async fn send_notification(&self, notification_cmd: crate::common::protocol::commands::NotificationCmd) -> Result<()> {
        let client = self.client.read().await;
        client.send_notification(notification_cmd).await
    }
    
    /// 发送事件消息
    pub async fn send_event(&self, event_cmd: crate::common::protocol::commands::EventCmd) -> Result<()> {
        let client = self.client.read().await;
        client.send_event(event_cmd).await
    }
    
    /// 批量发送消息
    pub async fn send_batch(&self, frames: Vec<Frame>) -> Result<(usize, usize)> {
        let client = self.client.read().await;
        client.send_batch(frames).await
    }
    
    /// 获取连接状态
    pub async fn get_state(&self) -> ConnectionState {
        let client = self.client.read().await;
        client.get_state().await
    }
    
    /// 检查是否已连接
    pub async fn is_connected(&self) -> bool {
        let client = self.client.read().await;
        client.is_connected().await
    }
    
    /// 检查是否已认证
    pub async fn is_authenticated(&self) -> bool {
        self.auth_manager.is_authenticated().await
    }
    
    /// 获取当前使用的协议
    pub async fn get_current_protocol(&self) -> Option<crate::common::connections::types::Transport> {
        let client = self.client.read().await;
        client.get_current_protocol().await
    }
    
    /// 获取连接统计信息
    pub async fn get_connection_stats(&self) -> Option<crate::common::connections::traits::ConnectionStats> {
        let client = self.client.read().await;
        client.get_connection_stats().await
    }
    
    /// 获取等待中的请求数量
    pub async fn get_pending_requests_count(&self) -> usize {
        let client = self.client.read().await;
        client.get_pending_requests_count().await
    }
    
    /// 清理超时的请求
    pub async fn cleanup_timeout_requests(&self) {
        let client = self.client.read().await;
        client.cleanup_timeout_requests().await;
    }
    
    /// 获取客户端ID
    pub async fn get_client_id(&self) -> String {
        let client = self.client.read().await;
        client.get_client_id().await
    }
    
    /// 检查连接健康状态
    pub async fn is_healthy(&self) -> bool {
        let client = self.client.read().await;
        client.is_healthy().await
    }
    
    /// 获取客户端状态信息
    pub async fn get_status_info(&self) -> String {
        let client = self.client.read().await;
        client.get_status_info().await
    }
    
    /// 重连到服务器
    pub async fn reconnect(&mut self) -> Result<()> {
        let client = self.client.write().await;
        client.reconnect().await
    }
    
    /// 发送心跳消息
    pub async fn send_heartbeat(&self) -> Result<()> {
        // 检查连接状态
        if !self.is_connected().await {
            return Err(crate::common::error::FlareError::connection_failed(
                "客户端未连接，无法发送心跳".to_string()
            ));
        }
        
        // 发送心跳消息
        let client = self.client.read().await;
        
        // 通过Client的send_fire_and_forget方法发送心跳
        client.send_fire_and_forget(
            |_| Ok(crate::common::protocol::commands::Command::Control(
                crate::common::protocol::commands::ControlCmd::Ping
            )),
            crate::common::protocol::Reliability::AtLeastOnce
        ).await
    }
    
    /// 启动心跳任务
    async fn start_heartbeat_task(&self) -> Result<()> {
        let client = Arc::clone(&self.client);
        let is_running = Arc::clone(&self.is_running);
        let event_handler = Arc::clone(&self.event_handler);
        
        // 获取心跳间隔
        let heartbeat_interval = {
            let client = client.read().await;
            client.get_config().heartbeat_interval_ms
        };
        
        let heartbeat_task = tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(heartbeat_interval));
            
            loop {
                interval.tick().await;
                
                // 检查是否仍在运行
                {
                    let running = is_running.read().await;
                    if !*running {
                        break;
                    }
                }
                
                // 检查连接状态
                let state = {
                    let client = client.read().await;
                    client.get_state().await
                };
                
                if state == ConnectionState::Connected {
                    // 直接发送心跳消息
                    let heartbeat_result = {
                        let client = client.read().await;
                        client.send_fire_and_forget(
                            |_| Ok(crate::common::protocol::commands::Command::Control(
                                crate::common::protocol::commands::ControlCmd::Ping
                            )),
                            crate::common::protocol::Reliability::AtLeastOnce
                        ).await
                    };
                    
                    match heartbeat_result {
                        Ok(_) => {
                            event_handler.on_heartbeat_sent().await;
                            debug!("心跳发送成功");
                        }
                        Err(e) => {
                            event_handler.on_heartbeat_failed(&e.to_string()).await;
                            warn!("发送心跳失败: {}", e);
                        }
                    }
                }
            }
        });
        
        *self.heartbeat_task.write().await = Some(heartbeat_task);
        Ok(())
    }
    
    /// 停止心跳任务
    async fn stop_heartbeat_task(&self) {
        if let Some(task) = self.heartbeat_task.write().await.take() {
            task.abort();
        }
    }
    
    /// 启动重连任务
    async fn start_reconnect_task(&self) -> Result<()> {
        let client = Arc::clone(&self.client);
        let is_running = Arc::clone(&self.is_running);
        let event_handler = Arc::clone(&self.event_handler);
        let auth_manager = Arc::clone(&self.auth_manager);
        
        // 获取重连延迟
        let reconnect_delay = {
            let client = client.read().await;
            client.get_config().reconnect_delay_ms
        };
        
        let reconnect_task = tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(reconnect_delay));
            let mut attempt = 0;
            
            loop {
                interval.tick().await;
                
                // 检查是否仍在运行
                {
                    let running = is_running.read().await;
                    if !*running {
                        break;
                    }
                }
                
                // 检查连接状态
                let state = {
                    let client = client.read().await;
                    client.get_state().await
                };
                
                if state == ConnectionState::Disconnected || state == ConnectionState::Failed {
                    attempt += 1;
                    event_handler.on_auto_reconnect_started(attempt).await;
                    
                    // 使用基础 Client 的重连功能
                    match {
                        let client = client.write().await;
                        client.reconnect().await
                    } {
                        Ok(_) => {
                            // 重连成功，执行认证
                            match {
                                let client = client.read().await;
                                auth_manager.authenticate(&client, Some(Arc::clone(&event_handler))).await
                            } {
                                Ok(_) => {
                                    event_handler.on_auto_reconnect_success(attempt).await;
                                    info!("重连并认证成功");
                                }
                                Err(e) => {
                                    event_handler.on_auto_reconnect_failed(attempt, &e.to_string()).await;
                                    warn!("重连成功但认证失败: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            event_handler.on_auto_reconnect_failed(attempt, &e.to_string()).await;
                            warn!("重连失败: {}", e);
                        }
                    }
                }
            }
        });
        
        *self.reconnect_task.write().await = Some(reconnect_task);
        Ok(())
    }
    
    /// 停止重连任务
    async fn stop_reconnect_task(&self) {
        if let Some(task) = self.reconnect_task.write().await.take() {
            task.abort();
        }
    }
    
    /// 启动连接监控任务
    async fn start_connection_monitor(&self) -> Result<()> {
        let client = Arc::clone(&self.client);
        let is_running = Arc::clone(&self.is_running);
        let event_handler = Arc::clone(&self.event_handler);
        
        let monitor_task = tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(1000)); // 每秒检查一次
            let mut last_state = ConnectionState::Disconnected;
            
            loop {
                interval.tick().await;
                
                // 检查是否仍在运行
                {
                    let running = is_running.read().await;
                    if !*running {
                        break;
                    }
                }
                
                // 检查连接状态
                let current_state = {
                    let client = client.read().await;
                    client.get_state().await
                };
                
                // 状态变化通知
                if current_state != last_state {
                    event_handler.on_connection_state_changed(
                        &format!("{:?}", last_state),
                        &format!("{:?}", current_state)
                    ).await;
                    last_state = current_state;
                }
                
                // 如果连接断开，可以在这里添加额外的处理逻辑
                if current_state == ConnectionState::Disconnected || current_state == ConnectionState::Failed {
                    debug!("检测到连接断开，状态: {:?}", current_state);
                }
            }
        });
        
        *self.connection_monitor_task.write().await = Some(monitor_task);
        Ok(())
    }
    
    /// 停止连接监控任务
    async fn stop_connection_monitor(&self) {
        if let Some(task) = self.connection_monitor_task.write().await.take() {
            task.abort();
        }
    }
}

impl Drop for FastClient {
    fn drop(&mut self) {
        // 在运行时环境中停止任务
        let is_running = Arc::clone(&self.is_running);
        // 使用异步写入而不是阻塞写入
        let heartbeat_task = if let Ok(mut guard) = self.heartbeat_task.try_write() {
            guard.take()
        } else {
            None
        };
        let reconnect_task = if let Ok(mut guard) = self.reconnect_task.try_write() {
            guard.take()
        } else {
            None
        };
        let connection_monitor_task = if let Ok(mut guard) = self.connection_monitor_task.try_write() {
            guard.take()
        } else {
            None
        };
        
        // Spawn一个任务来处理清理
        tokio::spawn(async move {
            // 标记为停止状态
            {
                let mut running = is_running.write().await;
                *running = false;
            }
            
            // 停止心跳任务
            if let Some(task) = heartbeat_task {
                task.abort();
            }
            
            // 停止重连任务
            if let Some(task) = reconnect_task {
                task.abort();
            }
            
            // 停止连接监控任务
            if let Some(task) = connection_monitor_task {
                task.abort();
            }
        });
    }
}

// 实现 Clone trait
impl Clone for FastClient {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            is_running: Arc::new(RwLock::new(false)),
            heartbeat_task: Arc::new(RwLock::new(None)),
            reconnect_task: Arc::new(RwLock::new(None)),
            connection_monitor_task: Arc::new(RwLock::new(None)),
            auth_manager: Arc::clone(&self.auth_manager),
            event_handler: Arc::clone(&self.event_handler),
        }
    }
}


