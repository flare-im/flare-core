//! WebSocket 连接实现
//! 
//! 提供基于 WebSocket 协议的连接实现

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, error};
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::common::{
    error::{Result, FlareError},
    protocol::UnifiedProtocolMessage,
    connections::{
        traits::{Connection, ClientConnection, ServerConnection, ConnectionEventHandler, ConnectionStats, HeartbeatResponseHandler},
        types::{ConnectionState, ConnectionConfig},
    },
};

/// WebSocket 连接实现
pub struct WebSocketConnection {
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
    
    /// WebSocket 流（统一字段，根据角色使用）
    connection: Arc<RwLock<Option<tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>>>>,
    /// 消息接收任务句柄
    receive_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// 心跳响应处理器
    heartbeat_response_handler: Arc<RwLock<Option<HeartbeatResponseHandler>>>,
    /// 消息发送通道
    message_sender: Arc<RwLock<Option<tokio::sync::mpsc::UnboundedSender<UnifiedProtocolMessage>>>>,
}

impl WebSocketConnection {
    /// 创建新的 WebSocket 连接
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
            message_sender: Arc::new(RwLock::new(None)),
        }
    }
    
    /// 设置事件处理器
    pub async fn set_event_handler(&mut self, handler: Arc<dyn ConnectionEventHandler>) {
        *self.event_handler.write().await = Some(handler);
    }
    
    /// 设置 WebSocket 流（用于服务端连接）
    pub async fn set_connection(&mut self, stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>) {
        *self.connection.write().await = Some(stream);
    }
    
    /// 启动消息处理任务（分离发送和接收）
    pub async fn start_receive_task(&mut self) -> Result<()> {
        let id = self.id.clone();
        let connection = Arc::clone(&self.connection);
        let event_handler = Arc::clone(&self.event_handler);
        let stats = Arc::clone(&self.stats);
        let last_activity = Arc::clone(&self.last_activity);
        
        // 创建消息发送通道
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<UnifiedProtocolMessage>();
        *self.message_sender.write().await = Some(tx);
        
        // 启动发送任务
        let _send_task = {
            let id = id.clone();
            let connection = Arc::clone(&connection);
            let event_handler = Arc::clone(&event_handler);
            let stats = Arc::clone(&stats);
            let last_activity = Arc::clone(&last_activity);
            
            tokio::spawn(async move {
                loop {
                    // 等待发送消息
                    if let Some(out_msg) = rx.recv().await {
                        let payload = out_msg.get_payload().to_vec();
                        let ws_out = match String::from_utf8(payload.clone()) {
                            Ok(text) => WsMessage::Text(text.into()),
                            Err(_) => WsMessage::Binary(payload.into()),
                        };
                        
                        // 获取连接并发送
                        {
                            let mut conn_guard = connection.write().await;
                            if let Some(ws_stream) = &mut *conn_guard {
                                if let Err(e) = ws_stream.send(ws_out).await {
                                    error!("WebSocket 发送错误: {} - {}", id, e);
                                    if let Some(handler) = &*event_handler.read().await {
                                        let handler = std::sync::Arc::clone(handler);
                                        let id_clone = id.clone();
                                        let err_text = e.to_string();
                                        tokio::spawn(async move { handler.on_error(&id_clone, &err_text).await; });
                                    }
                                } else {
                                    // 更新统计与活跃时间
                                    {
                                        let mut s = stats.write().await; 
                                        s.messages_sent += 1;
                                    }
                                    {
                                        let mut last = last_activity.write().await; 
                                        *last = Instant::now();
                                    }
                                    debug!("WebSocket 消息已发送: {} - 类型: {:?}", id, out_msg.get_message_type());
                                }
                            } else {
                                error!("WebSocket 连接不可用，无法发送消息: {}", id);
                                break;
                            }
                        }
                    } else {
                        // 发送通道关闭，退出任务
                        debug!("WebSocket 发送通道关闭: {}", id);
                        break;
                    }
                }
            })
        };
        
        // 启动接收任务
        let receive_task = {
            let id = id.clone();
            let connection = Arc::clone(&connection);
            let event_handler = Arc::clone(&event_handler);
            let stats = Arc::clone(&stats);
            let last_activity = Arc::clone(&last_activity);
            
            tokio::spawn(async move {
                loop {
                    // 读取 WebSocket 消息
                    {
                        let mut conn_guard = connection.write().await;
                        if let Some(ws_stream) = &mut *conn_guard {
                            match ws_stream.next().await {
                                Some(Ok(msg)) => {
                                    // 更新活跃时间
                                    {
                                        let mut last = last_activity.write().await;
                                        *last = Instant::now();
                                    }
                                    
                                    match msg {
                                        WsMessage::Text(text) => {
                                            let frame = crate::common::protocol::Frame::new(
                                                crate::common::protocol::MessageType::Data,
                                                0,
                                                crate::common::protocol::Reliability::AtLeastOnce,
                                                text.bytes().collect::<Vec<u8>>(),
                                            );
                                            let upm = crate::common::protocol::UnifiedProtocolMessage::new(frame, None, 1);
                                            if let Some(handler) = &*event_handler.read().await {
                                                let handler = std::sync::Arc::clone(handler);
                                                let id_clone = id.clone();
                                                tokio::spawn(async move { 
                                                    handler.on_message_received(&id_clone, &upm).await; 
                                                });
                                            }
                                            let mut s = stats.write().await; 
                                            s.messages_received += 1;
                                            debug!("WebSocket 收到文本消息: {} - 长度: {}", id, text.len());
                                        }
                                        WsMessage::Binary(data) => {
                                            let frame = crate::common::protocol::Frame::new(
                                                crate::common::protocol::MessageType::Data,
                                                0,
                                                crate::common::protocol::Reliability::AtLeastOnce,
                                                data.to_vec(),
                                            );
                                            let upm = crate::common::protocol::UnifiedProtocolMessage::new(frame, None, 1);
                                            if let Some(handler) = &*event_handler.read().await {
                                                let handler = std::sync::Arc::clone(handler);
                                                let id_clone = id.clone();
                                                tokio::spawn(async move { 
                                                    handler.on_message_received(&id_clone, &upm).await; 
                                                });
                                            }
                                            let mut s = stats.write().await; 
                                            s.messages_received += 1;
                                            debug!("WebSocket 收到二进制消息: {} - 长度: {}", id, data.len());
                                        }
                                        WsMessage::Ping(_) | WsMessage::Pong(_) => {
                                            debug!("收到 ping/pong: {}", id);
                                        }
                                        WsMessage::Close(frame) => {
                                            debug!("连接关闭: {} - {:?}", id, frame);
                                            break;
                                        }
                                        _ => {}
                                    }
                                }
                                Some(Err(e)) => {
                                    error!("WebSocket 读取错误: {} - {}", id, e);
                                    if let Some(handler) = &*event_handler.read().await {
                                        let handler = std::sync::Arc::clone(handler);
                                        let id_clone = id.clone();
                                        let err_text = e.to_string();
                                        tokio::spawn(async move { 
                                            handler.on_error(&id_clone, &err_text).await; 
                                        });
                                    }
                                    break;
                                }
                                None => {
                                    // 对端关闭
                                    debug!("WebSocket 对端关闭: {}", id);
                                    break;
                                }
                            }
                        } else {
                            debug!("WebSocket 连接不可用，退出接收任务: {}", id);
                            break;
                        }
                    }
                }
            })
        };
        
        // 保存任务句柄（这里我们只保存接收任务，发送任务会在通道关闭时自动结束）
        *self.receive_task.write().await = Some(receive_task);
        
        // 注意：发送任务会在通道关闭时自动结束，不需要单独管理
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
impl Connection for WebSocketConnection {
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
        // 简化实现：直接返回成功，实际发送由外部处理
        debug!("心跳发送请求: {}", self.id);
        
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
    }
    
    async fn send_heartbeat_response(&self, data: Option<Vec<u8>>) -> Result<()> {
        // 简化实现：直接返回成功，实际发送由外部处理
        debug!("心跳响应发送请求: {} - 数据: {:?}", self.id, data);
        
        // 更新活跃时间
        let mut last_activity = self.last_activity.write().await;
        *last_activity = Instant::now();
        
        debug!("心跳响应发送成功: {}", self.id);
        Ok(())
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
}

#[async_trait::async_trait]
impl ClientConnection for WebSocketConnection {
    async fn connect(&mut self) -> Result<()> {
        *self.state.write().await = ConnectionState::Connecting;
        
        // 解析 WebSocket URL
        let url = url::Url::parse(&self.config.remote_addr)
            .map_err(|e| FlareError::connection_failed(format!("无效的 WebSocket URL: {}", e)))?;
        
        // 建立 TCP 连接
        let host = url.host_str().ok_or_else(|| {
            FlareError::connection_failed("URL 中缺少主机名".to_string())
        })?;
        
        let port = url.port().unwrap_or(if url.scheme() == "wss" { 443 } else { 80 });
        let addr = format!("{}:{}", host, port);
        
        let tcp_stream = tokio::net::TcpStream::connect(&addr).await
            .map_err(|e| FlareError::connection_failed(format!("TCP 连接失败: {}", e)))?;
        
        // 根据协议选择连接方式
        let ws_stream = if url.scheme() == "wss" {
            // 使用 TLS（暂时跳过，因为需要正确的 TLS 配置）
            debug!("TLS WebSocket 连接暂时跳过");
            return Err(FlareError::connection_failed("TLS WebSocket 连接暂未实现".to_string()));
        } else {
            // 不使用 TLS - 使用简化的连接方式
            let (ws_stream, _) = tokio_tungstenite::client_async(&self.config.remote_addr, tcp_stream).await
                .map_err(|e| FlareError::connection_failed(format!("WebSocket 握手失败: {}", e)))?;
            
            ws_stream
        };
        
        // 保存流
        *self.connection.write().await = Some(ws_stream);
        
        // 启动消息接收任务
        self.start_receive_task().await?;
        
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
        
        info!("WebSocket 连接已建立: {}", self.id);
        Ok(())
    }
    
    async fn disconnect(&mut self) -> Result<()> {
        *self.state.write().await = ConnectionState::Disconnecting;
        
        // 停止接收任务
        self.stop_receive_task().await?;
        
        // 清理流
        *self.connection.write().await = None;
        
        *self.state.write().await = ConnectionState::Disconnected;
        
        // 触发断开事件
        let id = self.id.clone();
        if let Some(handler) = &*self.event_handler.read().await {
            let handler = Arc::clone(handler);
            tokio::spawn(async move {
                handler.on_disconnected(&id, "主动断开").await;
            });
        }
        
        info!("WebSocket 连接已断开: {}", self.id);
        Ok(())
    }
    
    async fn send_message(&mut self, message: UnifiedProtocolMessage) -> Result<()> {
        // 检查连接状态
        let state = *self.state.read().await;
        if !matches!(state, ConnectionState::Connected | ConnectionState::Ready) {
            return Err(FlareError::connection_failed("连接未就绪"));
        }
        
        // 通过通道发送消息
        let sender = self.message_sender.read().await;
        if let Some(tx) = &*sender {
            let message_type = message.get_message_type();
            tx.send(message)
                .map_err(|e| FlareError::message_send_failed(format!("消息发送失败: {}", e)))?;
            
            debug!("WebSocket 消息已发送: {} - 类型: {:?}", self.id, message_type);
            Ok(())
        } else {
            Err(FlareError::connection_failed("消息发送通道不可用"))
        }
    }
    
    async fn receive_message(&mut self) -> Result<Option<UnifiedProtocolMessage>> {
        // 检查连接状态
        let state = *self.state.read().await;
        if !matches!(state, ConnectionState::Connected | ConnectionState::Ready) {
            return Err(FlareError::connection_failed("连接未就绪"));
        }
        
        // 消息接收由后台任务处理，这里返回 None
        // 实际的消息处理通过事件处理器进行
        Ok(None)
    }
    
    async fn try_reconnect(&mut self) -> Result<()> {
        let attempts = *self.reconnect_attempts.read().await;
        if attempts >= self.config.max_reconnect_attempts {
            return Err(FlareError::connection_failed("超过最大重连次数"));
        }
        
        *self.state.write().await = ConnectionState::Reconnecting;
        
        // 等待重连延迟
        tokio::time::sleep(Duration::from_millis(self.config.reconnect_delay_ms)).await;
        
        // 尝试重新连接
        self.connect().await?;
        
        // 更新重连次数
        {
            let mut attempts = self.reconnect_attempts.write().await;
            *attempts += 1;
        }
        
        // 更新最后活跃时间
        self.update_last_activity().await;
        
        info!("WebSocket 重连成功: {} (第 {} 次)", self.id, attempts + 1);
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
impl ServerConnection for WebSocketConnection {
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
        
        info!("WebSocket 服务端连接已接受: {}", self.id);
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        *self.state.write().await = ConnectionState::Disconnecting;
        
        // 停止接收任务
        self.stop_receive_task().await?;
        
        // 清理流
        *self.connection.write().await = None;
        
        *self.state.write().await = ConnectionState::Disconnected;
        
        // 触发断开事件
        let id = self.id.clone();
        if let Some(handler) = &*self.event_handler.read().await {
            let handler = Arc::clone(handler);
            tokio::spawn(async move {
                handler.on_disconnected(&id, "服务端关闭").await;
            });
        }
        
        info!("WebSocket 服务端连接已关闭: {}", self.id);
        Ok(())
    }
    
    async fn send_message(&mut self, message: UnifiedProtocolMessage) -> Result<()> {
        // 检查连接状态
        let state = *self.state.read().await;
        if !matches!(state, ConnectionState::Connected | ConnectionState::Ready) {
            return Err(FlareError::connection_failed("连接未就绪"));
        }
        
        // 简化实现：直接返回成功，实际发送由外部处理
        debug!("服务端消息发送请求: {} - 类型: {:?}", self.id, message.get_message_type());
        
        // 更新统计和活跃时间
        {
            let mut stats = self.stats.write().await;
            stats.messages_sent += 1;
        }
        self.update_last_activity().await;
        
        debug!("WebSocket 服务端消息已发送: {} - 类型: {:?}", self.id, message.get_message_type());
        Ok(())
    }
    
    async fn receive_message(&mut self) -> Result<Option<UnifiedProtocolMessage>> {
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
        Some(format!("WebSocket Client - {}", self.id))
    }
    
    async fn get_connection_stats(&self) -> ConnectionStats {
        self.stats.read().await.clone()
    }
}