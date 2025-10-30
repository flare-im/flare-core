//! 客户端主模块
//! 
//! 提供完整的客户端实现，支持WebSocket和QUIC协议竞速

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn, error};
use async_trait::async_trait;

use crate::common::{
    error::Result,
    protocol::Frame,
    connections::{
        types::{ConnectionConfig, Transport, ConnectionState},
        traits::{ClientConnection, ConnectionStats},
        event::ConnectionEvent,
        factory::ConnectionFactory,
    },
    serialization::FrameSerializer,
};

use super::{
    config::{ClientConfig, ProtocolSelection},
    protocol_racing::ProtocolRacer,
    messaging::{MessageHandler, SendFunction},
    event::ClientEvent,
};

/// 重连配置
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// 最大重连次数
    pub max_attempts: u32,
    /// 重连间隔（毫秒）
    pub interval_ms: u64,
    /// 是否启用自动重连
    pub enabled: bool,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            interval_ms: 1000,
            enabled: true,
        }
    }
}


/// 基础客户端
/// 
/// 提供核心连接和消息处理功能，作为用户扩展的基础
/// 支持用户自定义 ClientEvent 处理业务逻辑
/// 默认启用自动心跳和重连机制，用户只需设置相关参数
pub struct Client {
    /// 客户端配置
    config: ClientConfig,
    /// 当前连接
    connection: Arc<RwLock<Option<Box<dyn ClientConnection>>>>,
    /// 连接状态
    state: Arc<RwLock<ConnectionState>>,
    /// 序列化器
    serializer: Arc<RwLock<Option<Arc<dyn FrameSerializer>>>>,
    /// 消息处理器
    message_handler: Arc<MessageHandler>,
    /// 用户自定义事件处理器
    client_event_handler: Arc<RwLock<Option<Arc<dyn ClientEvent>>>>,
    /// 当前使用的协议
    current_protocol: Arc<RwLock<Option<Transport>>>,
    /// 心跳任务句柄
    heartbeat_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// 重连任务句柄
    reconnect_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// 是否正在运行
    is_running: Arc<RwLock<bool>>,
}

impl Client {
    // ==================== 内部方法 ====================
    
    /// 设置消息处理器的发送函数
    async fn setup_message_handler_send_function(&self) {
        let connection = Arc::clone(&self.connection);
        let send_function: SendFunction = Arc::new(move |frame| {
            let connection = Arc::clone(&connection);
            Box::pin(async move {
                if let Some(conn) = &*connection.read().await {
                    conn.send_message(frame).await
        } else {
            Err(crate::common::error::FlareError::connection_failed(
                "连接不存在".to_string()
            ))
        }
            })
        });
        
        self.message_handler.set_send_function(send_function).await;
    }

    /// 创建连接配置
    fn create_connection_config(&self) -> ConnectionConfig {
        let connection_id = format!("client_{}", fastrand::u64(..));
        
        // 使用增强的配置转换方法
        self.config.to_connection_config(connection_id, None)
    }

    /// 使用协议竞速连接
    async fn connect_with_racing(&self) -> Result<super::protocol_racing::RacingResult> {
        info!("使用协议竞速连接");
        
        let racer = ProtocolRacer::new(5000); // 5秒超时
        let protocols = vec![Transport::Quic, Transport::WebSocket];
        
        // 创建基础配置用于竞速
        let base_config = self.create_connection_config();
        
        // 使用现有的 race 方法
        match racer.race(base_config, self.config.server_addresses.clone(), protocols).await {
            Ok(result) => {
                info!("协议竞速成功，选择协议: {:?}", result.protocol_type);
                Ok(result)
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
        protocol_type: Transport
    ) -> Result<Box<dyn ClientConnection>> {
        info!("使用单一协议连接: {:?}", protocol_type);
        
        // 使用增强的配置转换方法创建特定协议的连接配置
        let connection_id = format!("client_single_{}", fastrand::u64(..));
        let config = self.config.to_connection_config(connection_id, Some(protocol_type));
        
        let connection = ConnectionFactory::create_client(config).await?;
        
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
    
    /// 处理接收到的消息
    pub async fn handle_message(&self, message: Frame) {
        let message_id = message.get_message_id();
        debug!("收到消息: ID={}", message_id);
        
        // 使用统一消息处理器处理消息
        if let Err(e) = self.message_handler.handle_message(message.clone()).await {
            warn!("处理消息失败: {}", e);
        }
        
        // 如果有用户自定义事件处理器，分发到具体的事件方法
        if let Some(handler) = &*self.client_event_handler.read().await {
            self.dispatch_message_to_client_event(handler, &message).await;
        }
    }
    
    /// 将消息分发给 ClientEvent 处理器
    async fn dispatch_message_to_client_event(&self, handler: &Arc<dyn ClientEvent>, message: &Frame) {
        let command = message.get_command();
        match command {
            crate::common::protocol::commands::Command::Control(cmd) => {
                handler.on_control_command(&cmd).await;
            }
            crate::common::protocol::commands::Command::Message(cmd) => {
                handler.on_message_command(&cmd).await;
            }
            crate::common::protocol::commands::Command::Notification(cmd) => {
                handler.on_notification_command(&cmd).await;
            }
            crate::common::protocol::commands::Command::Event(cmd) => {
                handler.on_event_command(&cmd).await;
            }
        }
    }
    
    /// 触发 ClientEvent 的连接事件
    async fn trigger_client_event<F, R>(&self, event_fn: F) -> Option<R>
    where
        F: FnOnce(Arc<dyn ClientEvent>) -> std::pin::Pin<Box<dyn std::future::Future<Output = R> + Send>>,
    {
        if let Some(handler) = &*self.client_event_handler.read().await {
            Some(event_fn(Arc::clone(handler)).await)
        } else {
            None
        }
    }
    
    /// 触发 ClientEvent 的连接事件（无返回值）
    async fn trigger_client_event_void<F>(&self, event_fn: F)
    where
        F: FnOnce(Arc<dyn ClientEvent>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    {
        if let Some(handler) = &*self.client_event_handler.read().await {
            event_fn(Arc::clone(handler)).await;
        }
    }
    

    // ==================== 基础方法 ====================
    
    /// 创建新的客户端实例
    /// 
    /// # 参数
    /// * `config` - 客户端配置
    /// 
    /// # 返回值
    /// 返回新的客户端实例
    pub fn new(config: ClientConfig) -> Self {
        // 创建消息处理器
        let message_handler = Arc::new(MessageHandler::new(
            std::time::Duration::from_millis(config.request_timeout_ms)
        ));
        
        Self {
            config,
            connection: Arc::new(RwLock::new(None)),
            state: Arc::new(RwLock::new(ConnectionState::Disconnected)),
            serializer: Arc::new(RwLock::new(None)),
            message_handler,
            client_event_handler: Arc::new(RwLock::new(None)),
            current_protocol: Arc::new(RwLock::new(None)),
            heartbeat_task: Arc::new(RwLock::new(None)),
            reconnect_task: Arc::new(RwLock::new(None)),
            is_running: Arc::new(RwLock::new(false)),
        }
    }
    
    /// 创建新的客户端实例，指定客户端事件处理器
    /// 
    /// # 参数
    /// * `config` - 客户端配置
    /// * `client_event_handler` - 客户端事件处理器
    /// 
    /// # 返回值
    /// 返回新的客户端实例
    pub fn with_client_event_handler(config: ClientConfig, client_event_handler: Arc<dyn ClientEvent>) -> Self {
        // 创建消息处理器
        let message_handler = Arc::new(MessageHandler::new(
            std::time::Duration::from_millis(config.request_timeout_ms)
        ));
        
        Self {
            config,
            connection: Arc::new(RwLock::new(None)),
            state: Arc::new(RwLock::new(ConnectionState::Disconnected)),
            serializer: Arc::new(RwLock::new(None)),
            message_handler,
            client_event_handler: Arc::new(RwLock::new(Some(client_event_handler))),
            current_protocol: Arc::new(RwLock::new(None)),
            heartbeat_task: Arc::new(RwLock::new(None)),
            reconnect_task: Arc::new(RwLock::new(None)),
            is_running: Arc::new(RwLock::new(false)),
        }
    }
    
    /// 创建新的客户端实例，指定连接事件处理器
    /// 
    /// # 参数
    /// * `config` - 客户端配置
    /// * `connection_event_handler` - 连接事件处理器
    /// 
    /// # 返回值
    /// 返回新的客户端实例
    pub fn with_event_handler(config: ClientConfig, connection_event_handler: Arc<dyn ConnectionEvent>) -> Self {
        // 创建消息处理器
        let message_handler = Arc::new(MessageHandler::new(
            std::time::Duration::from_millis(config.request_timeout_ms)
        ));
        
        // 设置连接事件处理器
        let message_handler_clone = Arc::clone(&message_handler);
        tokio::spawn(async move {
            message_handler_clone.set_connection_event_handler(connection_event_handler).await;
        });
        
        Self {
            config,
            connection: Arc::new(RwLock::new(None)),
            state: Arc::new(RwLock::new(ConnectionState::Disconnected)),
            serializer: Arc::new(RwLock::new(None)),
            message_handler,
            client_event_handler: Arc::new(RwLock::new(None)),
            current_protocol: Arc::new(RwLock::new(None)),
            heartbeat_task: Arc::new(RwLock::new(None)),
            reconnect_task: Arc::new(RwLock::new(None)),
            is_running: Arc::new(RwLock::new(false)),
        }
    }
    
    /// 获取客户端配置的引用
    pub fn get_config(&self) -> &ClientConfig {
        &self.config
    }
    
    /// 设置连接事件处理器
    pub async fn set_connection_event_handler(&self, handler: Arc<dyn ConnectionEvent>) {
        self.message_handler.set_connection_event_handler(handler).await;
    }
    
    /// 设置客户端事件处理器
    /// 
    /// # 参数
    /// * `client_event_handler` - 客户端事件处理器
    pub async fn set_client_event_handler(&self, client_event_handler: Arc<dyn ClientEvent>) {
        *self.client_event_handler.write().await = Some(client_event_handler);
    }
    
    /// 检查是否启用自动重连
    pub fn is_auto_reconnect_enabled(&self) -> bool {
        self.config.max_reconnect_attempts > 0
    }
    
    /// 获取当前使用的协议
    /// 
    /// # 返回值
    /// 返回当前使用的协议，如果未连接则返回None
    pub async fn get_current_protocol(&self) -> Option<Transport> {
        *self.current_protocol.read().await
    }
    
    /// 设置当前协议
    async fn set_current_protocol(&self, protocol: Transport) {
        *self.current_protocol.write().await = Some(protocol);
    }
    
    /// 触发协议切换事件
    async fn trigger_protocol_switch(&self, connection_id: &str, from_protocol: &str, to_protocol: &str) {
        if let Some(_handler) = &*self.client_event_handler.read().await {
            let connection_id = connection_id.to_string();
            let from_protocol = from_protocol.to_string();
            let to_protocol = to_protocol.to_string();
            self.trigger_client_event(move |handler| {
                Box::pin(async move {
                    handler.on_protocol_switched(&connection_id, &from_protocol, &to_protocol).await;
                })
            }).await;
        }
    }
    
    /// 获取消息处理器
    pub fn get_message_handler(&self) -> Arc<MessageHandler> {
        Arc::clone(&self.message_handler)
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
    
    /// 获取连接统计信息
    /// 
    /// # 返回值
    /// 返回连接统计信息，如果未连接则返回None
    pub async fn get_connection_stats(&self) -> Option<ConnectionStats> {
        if let Some(connection) = &*self.connection.read().await {
            Some(connection.stats())
        } else {
            None
        }
    }
    
    /// 获取等待中的请求数量
    /// 
    /// # 返回值
    /// 返回当前等待响应的请求数量
    pub async fn get_pending_requests_count(&self) -> usize {
        self.message_handler.get_pending_count().await
    }
    
    /// 清理超时的请求
    /// 
    /// 清理所有超时的等待响应请求
    pub async fn cleanup_timeout_requests(&self) {
        self.message_handler.cleanup_timeout_requests().await;
    }
    
    /// 获取客户端ID
    /// 
    /// # 返回值
    /// 返回客户端的唯一标识符
    pub async fn get_client_id(&self) -> String {
        // 优先使用当前连接的协议地址
        if let Some(protocol) = self.get_current_protocol().await {
            if let Some(address) = self.config.get_server_address(protocol) {
                return format!("client_{}", address);
            }
        }
        
        // 如果当前协议不可用，根据协议选择模式选择地址
        match self.config.protocol_selection {
            ProtocolSelection::WebSocketOnly => {
                if let Some(address) = self.config.get_server_address(Transport::WebSocket) {
                    format!("client_{}", address)
                } else {
                    "client_unknown".to_string()
                }
            }
            ProtocolSelection::QuicOnly => {
                if let Some(address) = self.config.get_server_address(Transport::Quic) {
                    format!("client_{}", address)
                } else {
                    "client_unknown".to_string()
                }
            }
            ProtocolSelection::Auto => {
                // 对于Auto模式，优先使用WebSocket地址
                if let Some(address) = self.config.get_server_address(Transport::WebSocket) {
                    format!("client_{}", address)
                } else if let Some(address) = self.config.get_server_address(Transport::Quic) {
                    format!("client_{}", address)
                } else {
                    "client_unknown".to_string()
                }
            }
        }
    }
    
    /// 检查连接健康状态
    /// 
    /// # 返回值
    /// 返回连接是否健康
    pub async fn is_healthy(&self) -> bool {
        if !self.is_connected().await {
            return false;
        }
        
        // 检查是否有连接对象
        if self.connection.read().await.is_none() {
            return false;
        }
        
        // 可以添加更多健康检查逻辑，比如心跳检查等
        true
    }
    
    /// 获取客户端状态信息
    /// 
    /// # 返回值
    /// 返回包含客户端状态信息的字符串
    pub async fn get_status_info(&self) -> String {
        let state = self.get_state().await;
        let is_connected = self.is_connected().await;
        let is_healthy = self.is_healthy().await;
        let pending_requests = self.get_pending_requests_count().await;
        let client_id = self.get_client_id().await;
        let message_handler_status = self.message_handler.get_status_info().await;
        
        format!(
            "客户端状态: ID={}, 状态={:?}, 已连接={}, 健康={}, 等待请求={}, {}",
            client_id, state, is_connected, is_healthy, pending_requests, message_handler_status
        )
    }

    // ==================== 连接方法 ====================

    /// 连接到服务器
    /// 
    /// 根据配置选择协议连接方式：
    /// - Auto: 协议竞速，选择最优协议
    /// - QuicOnly: 仅使用QUIC协议
    /// - WebSocketOnly: 仅使用WebSocket协议
    /// 
    /// # 返回值
    /// 返回操作结果
    pub async fn connect(&self) -> Result<()> {
        // 检查是否已经连接
        if self.is_connected().await {
            return Err(crate::common::error::FlareError::connection_failed(
                "客户端已经连接，请先断开现有连接".to_string()
            ));
        }
        
        info!("开始连接到服务器: {:?}", self.config.server_addresses);
        
        // 更新状态
        *self.state.write().await = ConnectionState::Connecting;
        
        // 根据协议选择模式进行连接
        let (connection, protocol) = match self.config.protocol_selection {
            ProtocolSelection::Auto => {
                info!("使用协议竞速模式连接");
                let result = self.connect_with_racing().await?;
                (result.connection, result.protocol_type)
            }
            ProtocolSelection::QuicOnly => {
                info!("使用QUIC协议连接");
                let connection = self.connect_single_protocol(Transport::Quic).await?;
                (connection, Transport::Quic)
            }
            ProtocolSelection::WebSocketOnly => {
                info!("使用WebSocket协议连接");
                let connection = self.connect_single_protocol(Transport::WebSocket).await?;
                (connection, Transport::WebSocket)
            }
        };
        
        // 记录当前协议
        self.set_current_protocol(protocol).await;
        
        // 设置连接的事件处理器为当前 Client 实例
        let mut connection = connection;
        connection.set_event_handler(Arc::new(self.clone())).await;
        
        // 设置消息处理器的发送函数
        self.setup_message_handler_send_function().await;
        
        // 保存连接
        *self.connection.write().await = Some(connection);
        
        // 设置为已连接状态
        *self.state.write().await = ConnectionState::Connected;
        
        // 标记为运行状态
        *self.is_running.write().await = true;
        
        // 启动自动心跳任务
        if let Err(e) = self.start_auto_heartbeat().await {
            warn!("启动自动心跳失败: {}", e);
        }
        
        // 启动自动重连任务（暂时禁用，避免Send问题）
        // if let Err(e) = self.start_auto_reconnect().await {
        //     warn!("启动自动重连失败: {}", e);
        // }
        
        info!("连接建立成功，客户端ID: {}", self.get_client_id().await);
        Ok(())
    }

    /// 断开连接
    /// 
    /// # 返回值
    /// 返回操作结果
    pub async fn disconnect(&self) -> Result<()> {
        // 检查是否已经断开
        if !self.is_connected().await {
            warn!("客户端已经断开连接");
            return Ok(());
        }
        
        info!("开始断开连接，客户端ID: {}", self.get_client_id().await);
        
        *self.state.write().await = ConnectionState::Disconnecting;
        
        // 清理消息处理器中的等待请求
        let pending_count = self.message_handler.get_pending_count().await;
        if pending_count > 0 {
            info!("清理 {} 个等待中的请求", pending_count);
            self.message_handler.clear_all_requests().await;
        }
        
        // 断开底层连接
        if let Some(connection) = self.connection.write().await.take() {
            if let Err(e) = connection.disconnect(None).await {
                warn!("断开连接时发生错误: {}", e);
                // 即使断开连接出错，也要更新状态
            }
        }
        
        // 停止自动任务
        self.stop_auto_heartbeat().await;
        self.stop_auto_reconnect().await;
        
        // 标记为停止状态
        *self.is_running.write().await = false;
        
        *self.state.write().await = ConnectionState::Disconnected;
        info!("连接已断开");
        Ok(())
    }
    
    /// 重连到服务器
    /// 
    /// 先断开现有连接，然后重新连接
    /// 
    /// # 返回值
    /// 返回操作结果
    pub async fn reconnect(&self) -> Result<()> {
        info!("开始重连，客户端ID: {}", self.get_client_id().await);
        
        // 先断开现有连接
        if self.is_connected().await {
            // 停止自动任务
            self.stop_auto_heartbeat().await;
            self.stop_auto_reconnect().await;
            
            // 标记为停止状态
            *self.is_running.write().await = false;
            
            // 清理消息处理器中的等待请求
            let pending_count = self.message_handler.get_pending_count().await;
            if pending_count > 0 {
                info!("清理 {} 个等待中的请求", pending_count);
                self.message_handler.clear_all_requests().await;
            }
            
            // 断开底层连接
            if let Some(connection) = self.connection.write().await.take() {
                if let Err(e) = connection.disconnect(None).await {
                    warn!("断开连接时发生错误: {}", e);
                }
            }
            
            *self.state.write().await = ConnectionState::Disconnected;
        }
        
        // 等待一小段时间
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        
        // 重新连接
        self.connect().await
    }

    /// 发送心跳消息（内部方法）
    /// 
    /// # 返回值
    /// 返回操作结果
    async fn send_heartbeat(&self) -> Result<()> {
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
            let heartbeat_msg = Frame::heartbeat("heartbeat".to_string());
            connection.send_message(heartbeat_msg).await
        } else {
            Err(crate::common::error::FlareError::connection_failed(
                "连接不存在".to_string()
            ))
        }
    }
    
    /// 启动自动心跳任务
    /// 
    /// # 返回值
    /// 返回操作结果
    pub async fn start_auto_heartbeat(&self) -> Result<()> {
        
        // 检查是否已经启动
        if self.heartbeat_task.read().await.is_some() {
            warn!("自动心跳任务已经在运行");
            return Ok(());
        }
        
        let client = Arc::new(self.clone());
        let is_running = Arc::clone(&self.is_running);
        let heartbeat_interval = std::time::Duration::from_millis(self.config.heartbeat_interval_ms);
        
        let heartbeat_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_interval);
            let mut consecutive_failures = 0u32;
            const MAX_CONSECUTIVE_FAILURES: u32 = 3;
            
            loop {
                interval.tick().await;
                
                // 检查是否仍在运行
                if !*is_running.read().await {
                    debug!("自动心跳任务停止");
                    break;
                }
                
                // 检查连接状态
                if !client.is_connected().await {
                    debug!("连接已断开，停止自动心跳");
                    break;
                }
                
                // 发送心跳
                match client.send_heartbeat().await {
                    Ok(_) => {
                        debug!("自动心跳发送成功");
                        consecutive_failures = 0;
                    }
                    Err(e) => {
                        error!("自动心跳发送失败: {}", e);
                        consecutive_failures += 1;
                        
                        // 如果连续失败次数过多，可能需要重连
                        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                            error!("自动心跳连续失败 {} 次，可能需要重连", consecutive_failures);
                            // 触发心跳超时事件
                            if let Some(handler) = &*client.client_event_handler.read().await {
                                let handler = Arc::clone(handler);
                                let client_id = client.get_client_id().await;
                                tokio::spawn(async move {
                                    handler.on_heartbeat_timeout(&client_id).await;
                                });
                            }
                        }
                    }
                }
            }
        });
        
        // 保存任务句柄
        *self.heartbeat_task.write().await = Some(heartbeat_task);
        info!("自动心跳任务已启动，间隔: {}ms", self.config.heartbeat_interval_ms);
        Ok(())
    }
    
    /// 停止自动心跳任务
    pub async fn stop_auto_heartbeat(&self) {
        if let Some(task) = self.heartbeat_task.write().await.take() {
            task.abort();
            info!("自动心跳任务已停止");
        }
    }
    
    /// 启动自动重连任务
    /// 
    /// # 返回值
    /// 返回操作结果
    pub async fn start_auto_reconnect(&self) -> Result<()> {
        if self.config.max_reconnect_attempts == 0 {
            info!("自动重连未启用（max_reconnect_attempts=0），跳过启动");
            return Ok(());
        }
        
        // 检查是否已经启动
        if self.reconnect_task.read().await.is_some() {
            warn!("自动重连任务已经在运行");
            return Ok(());
        }
        
        let client = Arc::new(self.clone());
        let is_running = Arc::clone(&self.is_running);
        let reconnect_delay = std::time::Duration::from_millis(self.config.reconnect_delay_ms);
        let max_attempts = self.config.max_reconnect_attempts;
        
        // 启动重连监控任务
        let reconnect_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(reconnect_delay);
            let mut attempt = 0u32;
            
            loop {
                interval.tick().await;
                
                // 检查是否仍在运行
                if !*is_running.read().await {
                    debug!("自动重连任务停止");
                    break;
                }
                
                // 检查连接状态
                let state = client.get_state().await;
                if state == ConnectionState::Connected {
                    attempt = 0; // 重置重连计数
                    continue;
                }
                
                // 如果连接断开或失败，尝试重连
                if matches!(state, ConnectionState::Disconnected | ConnectionState::Failed) {
                    attempt += 1;
                    
                    // 检查重连次数限制
                    if attempt > max_attempts {
                        error!("重连尝试次数已达上限: {}", max_attempts);
                        break;
                    }
                    
                    info!("开始自动重连，尝试次数: {}/{}", attempt, max_attempts);
                    
                    // 触发重连开始事件
                    if let Some(handler) = &*client.client_event_handler.read().await {
                        let handler = Arc::clone(handler);
                        let client_id = client.get_client_id().await;
                        tokio::spawn(async move {
                            handler.on_reconnect_started(&client_id, attempt).await;
                        });
                    }
                    
                    // 尝试重连 - 使用基础的重连逻辑
                    let reconnect_result = client.reconnect().await;
                    
                    match reconnect_result {
                        Ok(_) => {
                            info!("自动重连成功，尝试次数: {}", attempt);
                            attempt = 0; // 重置计数
                            
                            // 触发重连成功事件
                            if let Some(handler) = &*client.client_event_handler.read().await {
                                let handler = Arc::clone(handler);
                                let client_id = client.get_client_id().await;
                                tokio::spawn(async move {
                                    handler.on_reconnected(&client_id, attempt).await;
                                });
                            }
                        }
                        Err(e) => {
                            error!("自动重连失败，尝试次数: {} - 错误: {}", attempt, e);
                            
                            // 触发重连失败事件
                            if let Some(handler) = &*client.client_event_handler.read().await {
                                let handler = Arc::clone(handler);
                                let client_id = client.get_client_id().await;
                                let error_msg = e.to_string();
                                tokio::spawn(async move {
                                    handler.on_reconnect_failed(&client_id, attempt, &error_msg).await;
                                });
                            }
                        }
                    }
                }
            }
        });
        
        // 保存任务句柄
        *self.reconnect_task.write().await = Some(reconnect_task);
        info!("自动重连任务已启动，延迟: {}ms", self.config.reconnect_delay_ms);
        Ok(())
    }
    
    /// 停止自动重连任务
    pub async fn stop_auto_reconnect(&self) {
        if let Some(task) = self.reconnect_task.write().await.take() {
            task.abort();
            info!("自动重连任务已停止");
        }
    }

    // ==================== 消息相关方法 ====================

    /// 发送等待响应的消息
    pub async fn send_request<F>(
        &self,
        create_command: F,
        reliability: crate::common::protocol::Reliability,
        custom_timeout: Option<std::time::Duration>,
    ) -> Result<Frame>
    where
        F: FnOnce(String) -> Result<crate::common::protocol::commands::Command>,
    {
        // 检查连接状态
        if !self.is_connected().await {
            return Err(crate::common::error::FlareError::connection_failed(
                "客户端未连接，无法发送消息".to_string()
            ));
        }
        
        self.message_handler.send_request(create_command, reliability, custom_timeout).await
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
        // 检查连接状态
        if !self.is_connected().await {
            return Err(crate::common::error::FlareError::connection_failed(
                "客户端未连接，无法发送消息".to_string()
            ));
        }
        
        self.message_handler.send_fire_and_forget(create_command, reliability).await
    }
    
    /// 发送控制消息
    pub async fn send_control(&self, control_cmd: crate::common::protocol::commands::ControlCmd) -> Result<()> {
        // 检查连接状态
        if !self.is_connected().await {
            return Err(crate::common::error::FlareError::connection_failed(
                "客户端未连接，无法发送控制消息".to_string()
            ));
        }
        
        self.message_handler.send_control(control_cmd).await
    }
    
    /// 发送通知消息
    pub async fn send_notification(&self, notification_cmd: crate::common::protocol::commands::NotificationCmd) -> Result<()> {
        // 检查连接状态
        if !self.is_connected().await {
            return Err(crate::common::error::FlareError::connection_failed(
                "客户端未连接，无法发送通知消息".to_string()
            ));
        }
        
        self.message_handler.send_notification(notification_cmd).await
    }
    
    /// 发送事件消息
    pub async fn send_event(&self, event_cmd: crate::common::protocol::commands::EventCmd) -> Result<()> {
        // 检查连接状态
        if !self.is_connected().await {
            return Err(crate::common::error::FlareError::connection_failed(
                "客户端未连接，无法发送事件消息".to_string()
            ));
        }
        
        self.message_handler.send_event(event_cmd).await
    }
    
    /// 批量发送消息
    /// 
    /// # 参数
    /// * `frames` - 要发送的消息帧列表
    /// 
    /// # 返回值
    /// 返回发送结果，包含成功和失败的消息数量
    pub async fn send_batch(&self, frames: Vec<Frame>) -> Result<(usize, usize)> {
        // 检查连接状态
        if !self.is_connected().await {
            return Err(crate::common::error::FlareError::connection_failed(
                "客户端未连接，无法批量发送消息".to_string()
            ));
        }
        
        self.message_handler.send_batch(frames).await
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
            message_handler: Arc::clone(&self.message_handler),
            client_event_handler: Arc::clone(&self.client_event_handler),
            current_protocol: Arc::new(RwLock::new(None)),
            heartbeat_task: Arc::new(RwLock::new(None)),
            reconnect_task: Arc::new(RwLock::new(None)),
            is_running: Arc::new(RwLock::new(false)),
        }
    }
}

// 实现 ConnectionEvent trait
#[async_trait]
impl ConnectionEvent for Client {
    async fn on_connected(&self, connection_id: &str) {
        info!("客户端连接已建立: {}", connection_id);
        
        // 触发 ClientEvent 回调
        let connection_id = connection_id.to_string();
        self.trigger_client_event_void(move |handler| {
            Box::pin(async move {
                handler.on_connected(&connection_id).await;
            })
        }).await;
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("客户端连接已断开: {} - 原因: {}", connection_id, reason);
        
        // 更新状态
        *self.state.write().await = ConnectionState::Disconnected;
        
        // 触发 ClientEvent 回调
        let connection_id = connection_id.to_string();
        let reason = reason.to_string();
        self.trigger_client_event_void(move |handler| {
            Box::pin(async move {
                handler.on_disconnected(&connection_id, &reason).await;
            })
        }).await;
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("客户端连接错误: {} - 错误: {}", connection_id, error);
        
        // 更新状态
        *self.state.write().await = ConnectionState::Failed;
        
        // 触发 ClientEvent 回调
        let connection_id = connection_id.to_string();
        let error = error.to_string();
        self.trigger_client_event_void(move |handler| {
            Box::pin(async move {
                handler.on_error(&connection_id, &error).await;
            })
        }).await;
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        debug!("客户端收到消息: {} - 类型: {}", connection_id, message.get_command_type_str());
        
        // 处理消息
        self.handle_message(message.clone()).await;
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        debug!("客户端发送消息: {} - 类型: {}", connection_id, message.get_command_type_str());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        warn!("客户端心跳超时: {}", connection_id);
        
        // 触发 ClientEvent 回调，让用户决定是否重连
        let _should_reconnect = self.trigger_client_event(move |handler| {
            let connection_id = connection_id.to_string();
            Box::pin(async move {
                handler.on_heartbeat_timeout(&connection_id).await
            })
        }).await.unwrap_or(true); // 默认重连
        
        if _should_reconnect && self.is_auto_reconnect_enabled() {
            info!("心跳超时，开始重连");
            // 这里可以触发重连逻辑
        }
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        debug!("客户端收到心跳ping: {}", connection_id);
        
        // 触发 ClientEvent 回调
        let connection_id = connection_id.to_string();
        self.trigger_client_event_void(move |handler| {
            Box::pin(async move {
                handler.on_heartbeat_ping(&connection_id).await;
            })
        }).await;
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        debug!("客户端收到心跳pong: {}", connection_id);
        
        // 触发 ClientEvent 回调
        let connection_id = connection_id.to_string();
        self.trigger_client_event_void(move |handler| {
            Box::pin(async move {
                handler.on_heartbeat_pong(&connection_id).await;
            })
        }).await;
    }

    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("客户端连接质量变化: {} - 评分: {}", connection_id, quality_score);
        
        // 触发 ClientEvent 回调
        let connection_id = connection_id.to_string();
        self.trigger_client_event_void(move |handler| {
            Box::pin(async move {
                handler.on_quality_changed(&connection_id, quality_score).await;
            })
        }).await;
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        info!("客户端开始重连: {} - 尝试次数: {}", connection_id, attempt);
        
        // 触发 ClientEvent 回调，让用户决定是否允许重连
        let _should_reconnect = self.trigger_client_event(move |handler| {
            let connection_id = connection_id.to_string();
            Box::pin(async move {
                handler.on_reconnect_started(&connection_id, attempt).await
            })
        }).await.unwrap_or(true); // 默认允许重连
        
        if !_should_reconnect {
            warn!("用户取消重连: {}", connection_id);
            *self.state.write().await = ConnectionState::Disconnected;
        }
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        info!("客户端重连成功: {} - 尝试次数: {}", connection_id, attempt);
        
        // 更新状态
        *self.state.write().await = ConnectionState::Connected;
        
        // 触发 ClientEvent 回调
        let connection_id = connection_id.to_string();
        self.trigger_client_event_void(move |handler| {
            Box::pin(async move {
                handler.on_reconnected(&connection_id, attempt).await;
            })
        }).await;
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        error!("客户端重连失败: {} - 尝试次数: {} - 错误: {}", connection_id, attempt, error);
        
        // 触发 ClientEvent 回调，让用户决定是否继续重连
        let should_continue = self.trigger_client_event(move |handler| {
            let connection_id = connection_id.to_string();
            let error = error.to_string();
            Box::pin(async move {
                handler.on_reconnect_failed(&connection_id, attempt, &error).await
            })
        }).await.unwrap_or(attempt < self.config.max_reconnect_attempts); // 默认检查重连次数
        
        if should_continue {
            warn!("继续重连尝试: {}", connection_id);
        } else {
            error!("重连失败，停止重连: {}", connection_id);
            *self.state.write().await = ConnectionState::Failed;
        }
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &ConnectionStats) {
        debug!("客户端统计信息更新: {} - 收到: {} - 发送: {} - 质量: {}", 
               connection_id, stats.messages_received, stats.messages_sent, stats.quality_score);
        
        // 触发 ClientEvent 回调
        let connection_id = connection_id.to_string();
        let stats = stats.clone();
        self.trigger_client_event_void(move |handler| {
            Box::pin(async move {
                handler.on_statistics_updated(&connection_id, &stats).await;
            })
        }).await;
    }
}

/// Client 构建器
pub struct ClientBuilder {
    config: ClientConfig,
    client_event_handler: Option<Arc<dyn ClientEvent>>,
}

impl ClientBuilder {
    /// 创建新的 ClientBuilder
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            client_event_handler: None,
        }
    }
    
    /// 设置客户端事件处理器
    pub fn with_client_event_handler(mut self, handler: Arc<dyn ClientEvent>) -> Self {
        self.client_event_handler = Some(handler);
        self
    }
    
    /// 构建 Client 实例
    pub fn build(self) -> Client {
        if let Some(handler) = self.client_event_handler {
            Client::with_client_event_handler(self.config, handler)
        } else {
            Client::new(self.config)
        }
    }
}