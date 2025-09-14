//! 客户端主模块
//! 
//! 提供完整的客户端实现，支持WebSocket和QUIC协议竞速

use std::sync::Arc;
use tokio::sync::{RwLock, Mutex};
use tracing::{debug, info, warn, error};
use std::collections::HashMap;
use tokio::time::{timeout, Duration};

use crate::common::{
    error::Result,
    protocol::Frame,
    connections::{
        types::{ConnectionConfig, Transport, ConnectionState},
        traits::{ClientConnection, ConnectionFactory as ConnectionFactoryTrait},
        factory::ConnectionFactory,
    },
    serialization::FrameSerializer,
};

use super::{
    config::{ClientConfig, ProtocolSelection},
    protocol_racing::ProtocolRacer,
};

/// 请求回调
type RequestCallback = tokio::sync::oneshot::Sender<Result<Frame>>;

/// 客户端主类
pub struct Client {
    /// 客户端配置
    config: ClientConfig,
    /// 当前连接
    connection: Arc<RwLock<Option<Box<dyn ClientConnection>>>>,
    /// 连接状态
    state: Arc<RwLock<ConnectionState>>,
    /// 序列化器
    serializer: Arc<RwLock<Option<Arc<dyn FrameSerializer>>>>,
    /// 等待响应的请求（message_id -> 回调）
    pending_requests: Arc<Mutex<HashMap<u64, (RequestCallback, std::time::Instant)>>>,
    /// 请求超时时间（毫秒）
    request_timeout_ms: u64,
}

impl Client {
    /// 创建新的客户端实例
    /// 
    /// # 参数
    /// * `config` - 客户端配置
    /// 
    /// # 返回值
    /// 返回新的客户端实例
    pub fn new(config: ClientConfig) -> Self {
        let request_timeout_ms = config.request_timeout_ms;
        let client = Self {
            config,
            connection: Arc::new(RwLock::new(None)),
            state: Arc::new(RwLock::new(ConnectionState::Disconnected)),
            serializer: Arc::new(RwLock::new(None)),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            request_timeout_ms,
        };
        
        // 启动响应监听任务
        client.start_response_listener();
        
        client
    }
    
    /// 设置请求超时时间
    pub fn with_request_timeout(mut self, timeout_ms: u64) -> Self {
        self.request_timeout_ms = timeout_ms;
        self
    }

    /// 连接到服务器
    /// 
    /// 根据配置选择协议连接方式：
    /// - Auto: 协议竞速，选择最优协议
    /// - QuicOnly: 仅使用QUIC协议
    /// - WebSocketOnly: 仅使用WebSocket协议
    /// 
    /// # 返回值
    /// 返回操作结果
    pub async fn connect(&mut self) -> Result<()> {
        info!("开始连接到服务器");
        
        // 更新状态
        *self.state.write().await = ConnectionState::Connecting;
        
        // 创建基础连接配置
        let base_config = self.create_connection_config();
        
        // 根据协议选择模式进行连接
        let connection = match self.config.protocol_selection {
            ProtocolSelection::Auto => {
                info!("使用协议竞速模式连接");
                self.connect_with_racing(base_config).await?
            }
            ProtocolSelection::QuicOnly => {
                info!("使用QUIC协议连接");
                self.connect_single_protocol(base_config, Transport::Quic).await?
            }
            ProtocolSelection::WebSocketOnly => {
                info!("使用WebSocket协议连接");
                self.connect_single_protocol(base_config, Transport::WebSocket).await?
            }
        };
        
        // 保存连接
        *self.connection.write().await = Some(connection);
        *self.state.write().await = ConnectionState::Connected;
        
        info!("连接建立成功");
        Ok(())
    }

    /// 断开连接
    /// 
    /// # 返回值
    /// 返回操作结果
    pub async fn disconnect(&mut self) -> Result<()> {
        info!("开始断开连接");
        
        *self.state.write().await = ConnectionState::Disconnecting;
        
        // 清理所有等待的请求
        {
            let mut pending = self.pending_requests.lock().await;
            for (_, (sender, _)) in pending.drain() {
                let _ = sender.send(Err(crate::common::error::FlareError::connection_failed(
                    "连接已断开".to_string()
                )));
            }
        }
        
        if let Some(connection) = self.connection.write().await.take() {
            if let Err(e) = connection.disconnect(None).await {
                warn!("断开连接时发生错误: {}", e);
            }
        }
        
        *self.state.write().await = ConnectionState::Disconnected;
        info!("连接已断开");
        Ok(())
    }

    /// 发送消息
    /// 
    /// # 参数
    /// * `message` - 要发送的消息帧
    /// 
    /// # 返回值
    /// 返回操作结果
    pub async fn send_message(&self, message: Frame) -> Result<()> {
        debug!("发送消息: {:?}", message.get_message_type());
        
        // 检查连接状态
        let current_state = *self.state.read().await;
        if current_state != ConnectionState::Connected {
            return Err(crate::common::error::FlareError::connection_failed(
                format!("连接未建立或已断开: {:?}", current_state)
            ));
        }
        
        // 获取连接并发送消息
        if let Some(connection) = &*self.connection.read().await {
            match connection.send_message(message).await {
                Ok(()) => Ok(()),
                Err(e) => {
                    // 记录错误日志
                    error!("发送消息失败: {}", e);
                    
                    // 如果是连接错误，更新状态
                    if let Some(error_code) = e.code() {
                        match error_code {
                            crate::common::error::ErrorCode::ConnectionFailed |
                            crate::common::error::ErrorCode::ConnectionClosed |
                            crate::common::error::ErrorCode::NetworkError => {
                                // 更新连接状态为断开
                                *self.state.write().await = ConnectionState::Disconnected;
                                error!("连接已断开，状态已更新");
                            }
                            _ => {}
                        }
                    }
                    
                    Err(e)
                }
            }
        } else {
            Err(crate::common::error::FlareError::connection_failed(
                "连接不存在".to_string()
            ))
        }
    }

    /// 发送请求并等待响应（类似REST接口）
    /// 
    /// # 参数
    /// * `request` - 请求消息帧
    /// 
    /// # 返回值
    /// 返回响应消息帧或错误
    pub async fn send_request(&self, request: Frame) -> Result<Frame> {
        debug!("发送请求: {:?}", request.get_message_type());
        
        // 检查连接状态
        let current_state = *self.state.read().await;
        if current_state != ConnectionState::Connected {
            return Err(crate::common::error::FlareError::connection_failed(
                format!("连接未建立或已断开: {:?}", current_state)
            ));
        }
        
        // 创建一次性通道用于接收响应
        let (sender, receiver) = tokio::sync::oneshot::channel();
        
        // 记录请求ID和回调
        let request_id = request.get_message_id();
        {
            let mut pending = self.pending_requests.lock().await;
            pending.insert(request_id, (sender, std::time::Instant::now()));
        }
        
        // 发送请求
        let send_result = if let Some(connection) = &*self.connection.read().await {
            connection.send_message(request).await
        } else {
            Err(crate::common::error::FlareError::connection_failed(
                "连接不存在".to_string()
            ))
        };
        
        // 如果发送失败，清理等待的请求
        if let Err(e) = send_result {
            let mut pending = self.pending_requests.lock().await;
            pending.remove(&request_id);
            return Err(e);
        }
        
        // 等待响应或超时
        let timeout_duration = Duration::from_millis(self.request_timeout_ms);
        match timeout(timeout_duration, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                // 清理等待的请求
                let mut pending = self.pending_requests.lock().await;
                pending.remove(&request_id);
                Err(crate::common::error::FlareError::connection_failed(
                    "等待响应时通道关闭".to_string()
                ))
            },
            Err(_) => {
                // 超时，清理等待的请求
                let mut pending = self.pending_requests.lock().await;
                pending.remove(&request_id);
                Err(crate::common::error::FlareError::timeout(
                    "请求超时".to_string()
                ))
            }
        }
    }

    /// 发送心跳消息
    /// 
    /// # 返回值
    /// 返回操作结果
    pub async fn send_heartbeat(&self) -> Result<()> {
        debug!("发送心跳消息");
        
        // 检查连接状态
        let current_state = *self.state.read().await;
        if current_state != ConnectionState::Connected {
            return Err(crate::common::error::FlareError::connection_failed(
                format!("连接未建立或已断开: {:?}", current_state)
            ));
        }
        
        // 获取连接并发送心跳
        if let Some(connection) = &*self.connection.read().await {
            connection.send_message(Frame::heartbeat()).await
        } else {
            Err(crate::common::error::FlareError::connection_failed(
                "连接不存在".to_string()
            ))
        }
    }

    /// 获取连接状态
    /// 
    /// # 返回值
    /// 返回当前连接状态
    pub async fn get_state(&self) -> ConnectionState {
        *self.state.read().await
    }

    /// 检查是否已连接
    /// 
    /// # 返回值
    /// 如果已连接返回true，否则返回false
    pub async fn is_connected(&self) -> bool {
        *self.state.read().await == ConnectionState::Connected
    }

    /// 创建连接配置
    fn create_connection_config(&self) -> ConnectionConfig {
        // 获取默认地址或使用第一个配置的地址
        let default_addr = if !self.config.server_addresses.is_empty() {
            self.config.server_addresses.values().next().unwrap().clone()
        } else {
            "127.0.0.1:8080".to_string()
        };
        
        let mut config = ConnectionConfig::client(
            format!("client_{}", fastrand::u64(..)),
            default_addr,
        );
        
        // 设置连接参数
        config.transport = self.config.transport;
        // 这些参数现在在客户端特定配置中设置
        // config.auto_reconnect = self.config.enable_auto_reconnect;
        // config.max_reconnect_attempts = self.config.max_reconnect_attempts;
        // config.reconnect_delay_ms = self.config.reconnect_delay_ms;
        // config.heartbeat_interval_ms = self.config.heartbeat_interval_ms;
        // config.heartbeat_timeout_ms = self.config.heartbeat_monitor_timeout_ms;
        // config.auto_heartbeat_response = self.config.enable_auto_heartbeat_response;
        
        // 设置序列化配置
        config.serialization_config = Some(self.config.serialization_config.clone());
        
        config
    }

    /// 使用协议竞速连接
    async fn connect_with_racing(&self, base_config: ConnectionConfig) -> Result<Box<dyn ClientConnection>> {
        info!("使用协议竞速连接");
        
        let racer = ProtocolRacer::new(5000); // 5秒超时
        let protocols = vec![Transport::Quic, Transport::WebSocket];
        
        match racer.race(base_config, self.config.server_addresses.clone(), protocols).await {
            Ok(result) => {
                info!("协议竞速成功，选择协议: {:?}", result.protocol_type);
                Ok(result.connection)
            }
            Err(e) => {
                error!("协议竞速失败: {}", e);
                Err(e)
            }
        }
    }

    /// 使用单一协议连接
    async fn connect_single_protocol(
        &self, 
        mut base_config: ConnectionConfig, 
        protocol_type: Transport
    ) -> Result<Box<dyn ClientConnection>> {
        info!("使用单一协议连接: {:?}", protocol_type);
        
        // 获取协议对应的地址
        if let Some(address) = self.config.get_server_address(protocol_type) {
            base_config.remote_addr = address.clone();
        }
        
        base_config.transport = protocol_type;
        
        let factory = ConnectionFactory::new();
        let connection = factory.create_client_connection(base_config).await?;
        
        match connection.connect().await {
            Ok(_) => {
                info!("单一协议连接成功: {:?}", protocol_type);
                Ok(connection)
            }
            Err(e) => {
                error!("单一协议连接失败: {:?}, 错误: {}", protocol_type, e);
                Err(e)
            }
        }
    }
    
    /// 启动响应监听任务
    fn start_response_listener(&self) {
        // 客户端不直接监听响应，而是通过事件处理器处理
        // 响应会在连接的事件处理器中处理并发送到pending_requests通道
    }
    
    /// 处理接收到的响应消息
    pub async fn handle_response(&self, response: Frame) {
        let response_id = response.get_message_id();
        debug!("收到响应消息: ID={}", response_id);
        
        // 查找等待此响应的请求
        let mut pending = self.pending_requests.lock().await;
        if let Some((sender, _)) = pending.remove(&response_id) {
            // 发送响应给等待的请求
            let _ = sender.send(Ok(response));
        } else {
            debug!("收到未请求的响应消息: ID={}", response_id);
        }
    }
    
    /// 清理超时的请求
    pub async fn cleanup_timeout_requests(&self) {
        let mut pending = self.pending_requests.lock().await;
        let now = std::time::Instant::now();
        let timeout_duration = Duration::from_millis(self.request_timeout_ms);
        
        pending.retain(|_, (_, timestamp)| {
            if now.duration_since(*timestamp) > timeout_duration {
                false // 移除超时的请求
            } else {
                true // 保留未超时的请求
            }
        });
    }
}

// 实现 Clone trait
impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            connection: Arc::new(RwLock::new(None)),
            state: Arc::new(RwLock::new(ConnectionState::Disconnected)),
            serializer: self.serializer.clone(),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            request_timeout_ms: self.request_timeout_ms,
        }
    }
}