//! QUIC 连接实现
//! 
//! 提供基于 QUIC 协议的连接实现

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::{debug, info, warn, error};

use crate::common::{
    error::{Result, FlareError},
    protocol::Frame,
    connections::{
        traits::{Connection, ClientConnection, ServerConnection, ConnectionEventHandler, ConnectionStats, HeartbeatResponseHandler},
        types::{ConnectionState, ConnectionConfig},
    },
};

use quinn::{Connection as QuinnConnection, Endpoint, Connecting};

/// QUIC 连接实现
pub struct QuicConnection {
    /// 连接ID
    id: String,
    /// 连接配置
    config: ConnectionConfig,
    /// 连接状态
    state: Arc<RwLock<ConnectionState>>,
    /// 事件处理器
    event_handler: Arc<RwLock<Option<Arc<dyn ConnectionEventHandler>>>>,
    /// 最后活跃时间
    last_activity: Arc<RwLock<Instant>>,
    /// 重连次数
    reconnect_attempts: Arc<RwLock<u32>>,
    /// 连接统计
    stats: Arc<RwLock<ConnectionStats>>,
    
    /// QUIC 连接（统一字段，根据角色使用）
    connection: Arc<RwLock<Option<QuinnConnection>>>,
    /// 消息接收任务句柄
    receive_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// 心跳响应处理器
    heartbeat_response_handler: Arc<RwLock<Option<HeartbeatResponseHandler>>>,
}

impl QuicConnection {
    /// 创建新的 QUIC 连接
    pub fn new(config: ConnectionConfig) -> Self {
        let stats = ConnectionStats {
            established_at: Instant::now(),
            last_activity: Instant::now(),
            messages_received: 0,
            messages_sent: 0,
            heartbeat_responses: 0,
            quality_score: 100,
        };
        
        Self {
            id: config.id.clone(),
            config,
            state: Arc::new(RwLock::new(ConnectionState::Initializing)),
            event_handler: Arc::new(RwLock::new(None)),
            last_activity: Arc::new(RwLock::new(Instant::now())),
            reconnect_attempts: Arc::new(RwLock::new(0)),
            stats: Arc::new(RwLock::new(stats)),
            
            connection: Arc::new(RwLock::new(None)),
            receive_task: Arc::new(RwLock::new(None)),
            heartbeat_response_handler: Arc::new(RwLock::new(None)),
        }
    }
    
    /// 设置事件处理器
    pub async fn set_event_handler(&mut self, handler: Arc<dyn ConnectionEventHandler>) {
        *self.event_handler.write().await = Some(handler);
    }
    
    /// 设置 QUIC 连接（用于服务端连接）
    pub async fn set_connection(&mut self, conn: QuinnConnection) {
        *self.connection.write().await = Some(conn);
    }
    

    
    /// 启动消息接收任务
    pub async fn start_receive_task(&mut self) -> Result<()> {
        let id = self.id.clone();
        let event_handler = Arc::clone(&self.event_handler);
        let stats = Arc::clone(&self.stats);
        let last_activity = Arc::clone(&self.last_activity);
        let connection = Arc::clone(&self.connection);
        
        let task = tokio::spawn(async move {
            loop {
                // 获取可用的连接
                let conn_guard = connection.read().await;
                let conn = if let Some(conn) = &*conn_guard {
                    Some(conn)
                } else {
                    None
                };
                
                if let Some(conn) = conn {
                    // 等待双向流
                    while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                        // 更新活跃时间
                        {
                            let mut last_activity = last_activity.write().await;
                            *last_activity = Instant::now();
                        }
                        
                        // 处理接收到的数据
                        let mut buffer = vec![0u8; 1024];
                        match recv.read(&mut buffer).await {
                            Ok(Some(bytes_read)) => {
                                if bytes_read > 0 {
                                    let data = buffer[..bytes_read].to_vec();
                                    debug!("收到 QUIC 数据: {} 字节", bytes_read);
                                    
                                    // 更新统计
                                    {
                                        let mut stats = stats.write().await;
                                        stats.messages_received += 1;
                                    }
                                    
                                    // 触发事件
                                    if let Some(handler) = &*event_handler.read().await {
                                        let handler = Arc::clone(handler);
                                        let id = id.clone();
                                        tokio::spawn(async move {
                                            let frame = crate::common::protocol::Frame::new(
                                                crate::common::protocol::MessageType::Data,
                                                0,
                                                crate::common::protocol::Reliability::AtLeastOnce,
                                                data,
                                            );
                                            let message = frame;
                                            handler.on_message_received(&id, &message).await;
                                        });
                                    }
                                }
                            }
                            Ok(None) => {
                                debug!("QUIC 流已关闭");
                                break;
                            }
                            Err(e) => {
                                error!("QUIC 读取错误: {}", e);
                                break;
                            }
                        }
                    }
                } else {
                    // 没有可用的连接，等待一下
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        });
        
        *self.receive_task.write().await = Some(task);
        Ok(())
    }
    
    /// 停止消息接收任务
    async fn stop_receive_task(&mut self) -> Result<()> {
        if let Some(task) = self.receive_task.write().await.take() {
            task.abort();
        }
        Ok(())
    }
    

}

#[async_trait::async_trait]
impl Connection for QuicConnection {
    fn get_id(&self) -> &str {
        &self.id
    }
    
    async fn get_state(&self) -> ConnectionState {
        *self.state.read().await
    }
    
    async fn is_active(&self) -> bool {
        let state = *self.state.read().await;
        matches!(state, ConnectionState::Connected | ConnectionState::Ready)
    }
    
    fn get_config(&self) -> &ConnectionConfig {
        &self.config
    }
    
    async fn get_last_activity(&self) -> Instant {
        *self.last_activity.read().await
    }
    
    async fn update_last_activity(&self) {
        let mut last_activity = self.last_activity.write().await;
        *last_activity = Instant::now();
        
        let mut stats = self.stats.write().await;
        stats.last_activity = *last_activity;
    }
    
    async fn send_heartbeat(&self) -> Result<()> {
        if let Some(conn) = &*self.connection.read().await {
            // 发送心跳数据
            let heartbeat_data = b"heartbeat";
            let mut stream = conn.open_uni().await
                .map_err(|e| FlareError::message_send_failed(format!("无法打开单向流发送心跳: {}", e)))?;
            
            stream.write_all(heartbeat_data).await
                .map_err(|e| FlareError::message_send_failed(format!("心跳发送失败: {}", e)))?;
            
            // 更新统计和活跃时间
            {
                let mut stats = self.stats.write().await;
                stats.heartbeat_responses += 1;
                stats.last_activity = Instant::now();
            }
            
            let mut last_activity = self.last_activity.write().await;
            *last_activity = Instant::now();
            
            debug!("心跳发送成功: {}", self.id);
            Ok(())
        } else {
            Err(FlareError::connection_failed("QUIC 连接不可用"))
        }
    }
    
    async fn send_heartbeat_response(&self, data: Option<Vec<u8>>) -> Result<()> {
        if let Some(conn) = &*self.connection.read().await {
            let response_data = data.unwrap_or_else(|| b"heartbeat_ack".to_vec());
            let mut stream = conn.open_uni().await
                .map_err(|e| FlareError::message_send_failed(format!("无法打开单向流发送心跳响应: {}", e)))?;
            
            stream.write_all(&response_data).await
                .map_err(|e| FlareError::message_send_failed(format!("心跳响应发送失败: {}", e)))?;
            
            // 更新活跃时间
            let mut last_activity = self.last_activity.write().await;
            *last_activity = Instant::now();
            
            debug!("心跳响应发送成功: {}", self.id);
            Ok(())
        } else {
            Err(FlareError::connection_failed("QUIC 连接不可用"))
        }
    }
    
    async fn set_heartbeat_response_handler(&mut self, handler: Option<HeartbeatResponseHandler>) {
        *self.heartbeat_response_handler.write().await = handler;
    }
    
    async fn has_received_heartbeat(&self) -> bool {
        // 检查最后活跃时间是否在心跳间隔内
        let last_activity = *self.last_activity.read().await;
        let heartbeat_interval = Duration::from_millis(self.config.heartbeat_interval_ms);
        last_activity.elapsed() < heartbeat_interval
    }
    
    async fn reset_heartbeat_state(&self) {
        let mut last_activity = self.last_activity.write().await;
        *last_activity = Instant::now();
        
        let mut stats = self.stats.write().await;
        stats.last_activity = *last_activity;
    }
    
    async fn set_connection_event_handler(&mut self, handler: Arc<dyn ConnectionEventHandler>) {
        *self.event_handler.write().await = Some(handler);
    }
}

#[async_trait::async_trait]
impl ClientConnection for QuicConnection {
    async fn connect(&mut self) -> Result<()> {
        *self.state.write().await = ConnectionState::Connecting;
        
        #[cfg(feature = "quic")]
        {
            // 解析服务器地址
            let addr = self.config.remote_addr.parse::<std::net::SocketAddr>()
                .map_err(|e| FlareError::connection_failed(format!("无效的地址格式: {}", e)))?;
            
            // 创建 QUIC 端点
            let endpoint = Endpoint::client(addr)
                .map_err(|e| FlareError::connection_failed(format!("无法创建 QUIC 端点: {}", e)))?;
            
            // 连接到服务器
            let connecting = endpoint.connect(addr, "localhost")
                .map_err(|e| FlareError::connection_failed(format!("QUIC 连接失败: {}", e)))?;
            
            let new_conn = connecting.await
                .map_err(|e| FlareError::connection_failed(format!("QUIC 握手失败: {}", e)))?;
            
            // 保存连接
            *self.connection.write().await = Some(new_conn.connection);
            
            // 启动消息接收任务
            self.start_receive_task().await?;
        }
        

        
        *self.state.write().await = ConnectionState::Connected;
        *self.state.write().await = ConnectionState::Ready;
        
        // 更新最后活跃时间
        self.update_last_activity().await;
        
        // 触发连接事件
        let id = self.id.clone();
        if let Some(handler) = &*self.event_handler.read().await {
            let handler = Arc::clone(handler);
            tokio::spawn(async move {
                handler.on_connected(&id).await;
            });
        }
        
        info!("QUIC 连接已建立: {}", self.id);
        Ok(())
    }
    
    async fn disconnect(&mut self) -> Result<()> {
        *self.state.write().await = ConnectionState::Disconnecting;
        
        // 停止接收任务
        self.stop_receive_task().await?;
        
        #[cfg(feature = "quic")]
        {
            // 关闭 QUIC 连接
            if let Some(conn) = &*self.connection.read().await {
                conn.close(0u32.into(), b"client disconnect");
            }
            
            // 清理连接
            *self.connection.write().await = None;
        }
        
        *self.state.write().await = ConnectionState::Disconnected;
        
        // 触发断开事件
        let id = self.id.clone();
        if let Some(handler) = &*self.event_handler.read().await {
            let handler = Arc::clone(handler);
            tokio::spawn(async move {
                handler.on_disconnected(&id, "主动断开").await;
            });
        }
        
        info!("QUIC 连接已断开: {}", self.id);
        Ok(())
    }
    
    async fn send_message(&mut self, message: Frame) -> Result<()> {
        // 检查连接状态
        let state = *self.state.read().await;
        if !matches!(state, ConnectionState::Connected | ConnectionState::Ready) {
            return Err(FlareError::connection_failed("连接未就绪"));
        }
        
        #[cfg(feature = "quic")]
        {
            // 发送消息
            if let Some(conn) = &*self.connection.read().await {
                // 打开双向流
                let (mut send, _recv) = conn.open_bi().await
                    .map_err(|e| FlareError::message_send_failed(format!("无法打开双向流: {}", e)))?;
                
                // 序列化消息
                let message_data = serde_json::to_vec(&message)
                    .map_err(|e| FlareError::serialization_error(format!("消息序列化失败: {}", e)))?;
                
                // 发送数据
                send.write_all(&message_data).await
                    .map_err(|e| FlareError::message_send_failed(format!("消息发送失败: {}", e)))?;
                
                send.finish().await
                    .map_err(|e| FlareError::message_send_failed(format!("流关闭失败: {}", e)))?;
                
                // 更新统计和活跃时间
                {
                    let mut stats = self.stats.write().await;
                    stats.messages_sent += 1;
                }
                self.update_last_activity().await;
                
                debug!("QUIC 消息已发送: {} - 类型: {:?}", self.id, message.get_message_type());
            } else {
                return Err(FlareError::connection_failed("QUIC 连接不可用"));
            }
        }
        

        
        Ok(())
    }
    
    async fn try_reconnect(&mut self) -> Result<()> {
        let attempts = *self.reconnect_attempts.read().await;
        if attempts >= self.config.max_reconnect_attempts {
            return Err(FlareError::connection_failed("超过最大重连次数"));
        }
        
        *self.state.write().await = ConnectionState::Reconnecting;
        
        // 等待重连延迟
        tokio::time::sleep(Duration::from_millis(self.config.reconnect_delay_ms)).await;
        
        #[cfg(feature = "quic")]
        {
            // 尝试重新连接
            self.connect().await?;
        }
        

        
        // 更新重连次数
        {
            let mut attempts = self.reconnect_attempts.write().await;
            *attempts += 1;
        }
        
        // 更新最后活跃时间
        self.update_last_activity().await;
        
        info!("QUIC 重连成功: {} (第 {} 次)", self.id, attempts + 1);
        Ok(())
    }
    
    async fn needs_reconnect(&self) -> bool {
        let state = *self.state.read().await;
        matches!(state, ConnectionState::Disconnected | ConnectionState::Failed)
    }
    

    
    async fn get_reconnect_attempts(&self) -> u32 {
        *self.reconnect_attempts.read().await
    }
    
    async fn reset_reconnect_attempts(&mut self) {
        *self.reconnect_attempts.write().await = 0;
    }
}

#[async_trait::async_trait]
impl ServerConnection for QuicConnection {
    async fn accept(&mut self) -> Result<()> {
        *self.state.write().await = ConnectionState::Connecting;
        
        // 服务端连接需要从外部传入，这里只是标记状态
        // 实际的连接建立应该在 RawConnectionHandler 中处理
        
        *self.state.write().await = ConnectionState::Connected;
        *self.state.write().await = ConnectionState::Ready;
        
        // 更新最后活跃时间
        self.update_last_activity().await;
        

        
        // 触发连接事件
        let id = self.id.clone();
        if let Some(handler) = &*self.event_handler.read().await {
            let handler = Arc::clone(handler);
            tokio::spawn(async move {
                handler.on_connected(&id).await;
            });
        }
        
        info!("QUIC 服务端连接已接受: {}", self.id);
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        *self.state.write().await = ConnectionState::Disconnecting;
        
        // 停止接收任务
        self.stop_receive_task().await?;
        
        #[cfg(feature = "quic")]
        {
            // 关闭 QUIC 连接
            if let Some(conn) = &*self.connection.read().await {
                conn.close(0u32.into(), b"server close");
            }
            
            // 清理连接
            *self.connection.write().await = None;
        }
        
        *self.state.write().await = ConnectionState::Disconnected;
        
        // 触发断开事件
        let id = self.id.clone();
        if let Some(handler) = &*self.event_handler.read().await {
            let handler = Arc::clone(handler);
            tokio::spawn(async move {
                handler.on_disconnected(&id, "服务端关闭").await;
            });
        }
        
        info!("QUIC 服务端连接已关闭: {}", self.id);
        Ok(())
    }
    
    async fn send_message(&mut self, message: Frame) -> Result<()> {
        // 检查连接状态
        let state = *self.state.read().await;
        if !matches!(state, ConnectionState::Connected | ConnectionState::Ready) {
            return Err(FlareError::connection_failed("连接未就绪"));
        }
        
        #[cfg(feature = "quic")]
        {
            // 发送消息
            if let Some(conn) = &*self.connection.read().await {
                // 打开双向流
                let (mut send, _recv) = conn.open_bi().await
                    .map_err(|e| FlareError::message_send_failed(format!("无法打开双向流: {}", e)))?;
                
                // 序列化消息
                let message_data = serde_json::to_vec(&message)
                    .map_err(|e| FlareError::serialization_error(format!("消息序列化失败: {}", e)))?;
                
                // 发送数据
                send.write_all(&message_data).await
                    .map_err(|e| FlareError::message_send_failed(format!("消息发送失败: {}", e)))?;
                
                send.finish().await
                    .map_err(|e| FlareError::message_send_failed(format!("流关闭失败: {}", e)))?;
                
                // 更新统计和活跃时间
                {
                    let mut stats = self.stats.write().await;
                    stats.messages_sent += 1;
                }
                self.update_last_activity().await;
                
                debug!("QUIC 服务端消息已发送: {} - 类型: {:?}", self.id, message.get_message_type());
            } else {
                return Err(FlareError::connection_failed("QUIC 连接不可用"));
            }
        }
        

        
        Ok(())
    }
    
    async fn receive_message(&mut self) -> Result<Option<Frame>> {
        // 检查连接状态
        let state = *self.state.read().await;
        if !matches!(state, ConnectionState::Connected | ConnectionState::Ready) {
            return Err(FlareError::connection_failed("连接未就绪"));
        }
        
        // 消息接收由后台任务处理，这里返回 None
        Ok(None)
    }
    

    
    async fn is_healthy(&self) -> bool {
        let state = *self.state.read().await;
        let last_activity = *self.last_activity.read().await;
        let timeout = Duration::from_millis(self.config.heartbeat_monitor_timeout_ms);
        
        matches!(state, ConnectionState::Connected | ConnectionState::Ready) 
            && last_activity.elapsed() < timeout
    }
    
    fn get_client_info(&self) -> Option<String> {
        Some(format!("QUIC Client - {}", self.id))
    }
    
    async fn get_connection_stats(&self) -> ConnectionStats {
        self.stats.read().await.clone()
    }
}
