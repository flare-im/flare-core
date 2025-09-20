//! FastClient - 开箱即用的客户端实现
//!
//! 提供高级客户端功能，包括自动心跳、自动认证、断线重连等

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use tokio::time::{Duration, interval};

use crate::common::{
    error::Result,
    protocol::Frame,
    connections::types::{ConnectionState, Transport},
};

use super::{
    client::Client,
    config::{ClientConfig, ProtocolSelection},
    auth::{AuthConfig},
    event::ClientEvent,
};

/// FastClient - 开箱即用的客户端
pub struct FastClient {
    /// 基础客户端
    client: Arc<RwLock<Client>>,
    /// 是否正在运行
    is_running: Arc<RwLock<bool>>,
    /// 心跳任务句柄
    heartbeat_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// 重连任务句柄
    reconnect_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

impl FastClient {
    /// 创建新的FastClient实例
    pub fn new(config: ClientConfig) -> Self {
        let client = Client::new(config);
        
        Self {
            client: Arc::new(RwLock::new(client)),
            is_running: Arc::new(RwLock::new(false)),
            heartbeat_task: Arc::new(RwLock::new(None)),
            reconnect_task: Arc::new(RwLock::new(None)),
        }
    }
    
    /// 创建新的FastClient实例，指定事件处理器
    pub fn with_event_handler(config: ClientConfig, event_handler: Arc<dyn ClientEvent>) -> Self {
        let client = Client::with_event_handler(config, event_handler);
        
        Self {
            client: Arc::new(RwLock::new(client)),
            is_running: Arc::new(RwLock::new(false)),
            heartbeat_task: Arc::new(RwLock::new(None)),
            reconnect_task: Arc::new(RwLock::new(None)),
        }
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
        
        // 连接到服务器
        {
            let mut client = self.client.write().await;
            client.connect().await?;
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
        if config.enable_auto_reconnect {
            self.start_reconnect_task().await?;
        }
        
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
        
        // 断开连接
        {
            let mut client = self.client.write().await;
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
        client.send_message(message).await
    }
    
    /// 发送请求并等待响应
    pub async fn send_request(&self, request: Frame) -> Result<Frame> {
        let client = self.client.read().await;
        client.send_request(request).await
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
    
    /// 启动心跳任务
    async fn start_heartbeat_task(&self) -> Result<()> {
        let client = Arc::clone(&self.client);
        let is_running = Arc::clone(&self.is_running);
        
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
                    // 发送心跳
                    if let Err(e) = {
                        let client = client.read().await;
                        client.send_heartbeat().await
                    } {
                        warn!("发送心跳失败: {}", e);
                    } else {
                        debug!("心跳发送成功");
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
        
        // 获取重连延迟
        let reconnect_delay = {
            let client = client.read().await;
            client.get_config().reconnect_delay_ms
        };
        
        let reconnect_task = tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(reconnect_delay));
            
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
                    info!("检测到连接断开，尝试重连");
                    
                    // 尝试重连
                    match {
                        let mut client = client.write().await;
                        client.connect().await
                    } {
                        Ok(_) => {
                            info!("重连成功");
                        }
                        Err(e) => {
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
}

impl Drop for FastClient {
    fn drop(&mut self) {
        // 在运行时环境中停止任务
        let is_running = Arc::clone(&self.is_running);
        let heartbeat_task = std::mem::take(&mut *self.heartbeat_task.blocking_write());
        let reconnect_task = std::mem::take(&mut *self.reconnect_task.blocking_write());
        
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
        }
    }
}

/// FastClient构建器
pub struct FastClientBuilder {
    config: ClientConfig,
    event_handler: Option<Arc<dyn ClientEvent>>,
}

impl FastClientBuilder {
    /// 创建新的构建器
    pub fn new() -> Self {
        Self {
            config: ClientConfig::default(),
            event_handler: None,
        }
    }
    
    /// 设置事件处理器
    pub fn with_event_handler(mut self, event_handler: Arc<dyn ClientEvent>) -> Self {
        self.event_handler = Some(event_handler);
        self
    }
    
    /// 设置服务器地址
    pub fn with_server_address(mut self, transport: Transport, address: String) -> Self {
        self.config = self.config.with_server_address(transport, address);
        self
    }
    
    /// 设置协议选择模式
    pub fn with_protocol_selection(mut self, selection: ProtocolSelection) -> Self {
        self.config = self.config.with_protocol_selection(selection);
        self
    }
    
    /// 设置仅使用 QUIC 协议
    pub fn with_quic_only(mut self) -> Self {
        self.config = self.config.with_quic_only();
        self
    }
    
    /// 设置仅使用 WebSocket 协议
    pub fn with_websocket_only(mut self) -> Self {
        self.config = self.config.with_websocket_only();
        self
    }
    
    /// 设置心跳间隔和超时
    pub fn with_heartbeat(mut self, interval_ms: u64, timeout_ms: u64) -> Self {
        self.config = self.config.with_heartbeat(interval_ms, timeout_ms);
        self
    }
    
    /// 启用或禁用自动重连
    pub fn with_auto_reconnect(mut self, enabled: bool) -> Self {
        self.config.enable_auto_reconnect = enabled;
        self
    }
    
    /// 设置重连参数
    pub fn with_reconnect_params(mut self, max_attempts: u32, delay_ms: u64) -> Self {
        self.config.max_reconnect_attempts = max_attempts;
        self.config.reconnect_delay_ms = delay_ms;
        self
    }
    
    /// 启用认证
    pub fn with_auth_enabled(mut self, enabled: bool) -> Self {
        self.config = self.config.with_auth_enabled(enabled);
        self
    }
    
    /// 设置认证用户ID
    pub fn with_auth_user_id(mut self, user_id: String) -> Self {
        self.config = self.config.with_auth_user_id(user_id);
        self
    }
    
    /// 设置认证平台
    pub fn with_auth_platform(mut self, platform: String) -> Self {
        self.config = self.config.with_auth_platform(platform);
        self
    }
    
    /// 设置认证令牌
    pub fn with_auth_token(mut self, token: String) -> Self {
        self.config = self.config.with_auth_token(token);
        self
    }
    
    /// 设置认证超时时间
    pub fn with_auth_timeout(mut self, timeout_ms: u64) -> Self {
        self.config = self.config.with_auth_timeout(timeout_ms);
        self
    }
    
    /// 设置完整的认证配置
    pub fn with_auth_config(mut self, auth_config: AuthConfig) -> Self {
        self.config = self.config.with_auth_config(auth_config);
        self
    }
    
    /// 设置序列化格式
    pub fn with_serialization(mut self, config: crate::common::serialization::SerializationConfig) -> Self {
        self.config = self.config.with_serialization(config);
        self
    }
    
    /// 构建FastClient实例
    pub fn build(self) -> FastClient {
        if let Some(event_handler) = self.event_handler {
            FastClient::with_event_handler(self.config, event_handler)
        } else {
            FastClient::new(self.config)
        }
    }
}

impl Default for FastClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}