//! WebSocket 连接实现
//! 
//! 提供基于 WebSocket 协议的连接实现

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn, error};
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::common::{
    error::{Result, FlareError},
    protocol::Frame,
    connections::{
        traits::{Connection, ConnectionEvent, ConnectionStats, ClientConnection, ServerConnection, HeartbeatResponseHandler},
        types::{ConnectionConfig, ConnectionState},
    },
    messaging::MessageParser, // 从messaging模块导入
    serialization::FrameSerializer,
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
    event_handler: Arc<RwLock<Option<Arc<dyn ConnectionEvent>>>>,
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

    /// 消息发送通道（发送已序列化的数据）
    message_sender: Arc<RwLock<Option<tokio::sync::mpsc::UnboundedSender<Vec<u8>>>>>,
    /// 序列化器
    serializer: Arc<Box<dyn crate::common::serialization::FrameSerializer>>,
}

impl WebSocketConnection {
    /// 创建新的 WebSocket 连接（使用配置）
    pub fn new(config: ConnectionConfig) -> Self {
        let stats = ConnectionStats {
            established_at: Instant::now(),
            last_activity: Instant::now(),
            messages_received: 0,
            messages_sent: 0,
            heartbeat_responses: 0,
            quality_score: 100,
        };
        
        // 根据配置创建序列化器
        let serializer = {
            let factory = crate::common::serialization::SerializerFactory::new();
            factory.create_with_config(
                config.get_serialization_format(),
                config.get_serialization_config()
            ).unwrap_or_else(|_| {
                // 如果创建失败，使用默认JSON序列化器
                crate::common::serialization::factory::json_serializer()
            })
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
            message_sender: Arc::new(RwLock::new(None)),
            serializer: Arc::new(serializer),
        }
    }
    
    /// 创建新的 WebSocket 连接（使用自定义序列化器）
    pub fn with_serializer(config: ConnectionConfig, serializer: Arc<Box<dyn crate::common::serialization::FrameSerializer>>) -> Self {
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
            message_sender: Arc::new(RwLock::new(None)),
            serializer,
        }
    }
    
    /// 设置事件处理器（公开方法）
    pub async fn set_event_handler(&mut self, handler: Arc<dyn ConnectionEvent>) {
        *self.event_handler.write().await = Some(handler);
    }
    
    /// 获取事件处理器是否已设置
    pub async fn has_event_handler(&self) -> bool {
        self.event_handler.read().await.is_some()
    }
    
    /// 设置 WebSocket 流（用于服务端连接）
    pub async fn set_connection(&mut self, stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>) {
        *self.connection.write().await = Some(stream);
    }
    
    /// 启动消息处理任务（分离发送和接收）
    pub async fn start_receive_task(&mut self) -> Result<()> {
        let id = self.id.clone();
        let connection: Arc<RwLock<Option<tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>>>> = Arc::clone(&self.connection);
        let event_handler: Arc<RwLock<Option<Arc<dyn ConnectionEvent>>>> = Arc::clone(&self.event_handler);
        let stats: Arc<RwLock<ConnectionStats>> = Arc::clone(&self.stats);
        let last_activity: Arc<RwLock<Instant>> = Arc::clone(&self.last_activity);
        let state: Arc<RwLock<ConnectionState>> = Arc::clone(&self.state);
        let serializer = Arc::clone(&self.serializer);
        let config = self.config.clone(); // 克隆配置
        
        // 创建消息发送通道（发送已序列化的数据）
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        *self.message_sender.write().await = Some(tx);
        
        // 启动心跳监控任务
        let _heartbeat_monitor_task = {
            let id = id.clone();
            let event_handler = Arc::clone(&event_handler);
            let last_activity = Arc::clone(&last_activity);
            let stats = Arc::clone(&stats);
            let heartbeat_interval = Duration::from_millis(self.config.heartbeat_interval_ms);
            let heartbeat_timeout = Duration::from_millis(self.config.heartbeat_monitor_timeout_ms);
            
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(heartbeat_interval);
                let mut last_quality_score = 100u8;
                
                loop {
                    interval.tick().await;
                    
                    let last_act = *last_activity.read().await;
                    let elapsed = last_act.elapsed();
                    
                    // 检查心跳超时
                    if elapsed > heartbeat_timeout {
                        if let Some(handler) = &*event_handler.read().await {
                            let handler = std::sync::Arc::clone(handler);
                            let id_clone = id.clone();
                            tokio::spawn(async move {
                                handler.on_heartbeat_timeout(&id_clone).await;
                            });
                        }
                    }

                    // 计算连接质量（基于活跃度）
                    let quality_score = if elapsed > heartbeat_timeout {
                        0u8 // 超时，质量为0
                    } else if elapsed > heartbeat_interval {
                        let ratio = elapsed.as_millis() as f64 / heartbeat_timeout.as_millis() as f64;
                        ((1.0 - ratio) * 100.0).max(10.0) as u8 // 最低10分
                    } else {
                        100u8 // 正常，满分
                    };
                    
                    // 如果质量发生显著变化（差值大于10），触发质量变化事件
                    if (quality_score as i16 - last_quality_score as i16).abs() > 10 {
                        {
                            let mut stats_guard = stats.write().await;
                            stats_guard.quality_score = quality_score;
                        }
                        
                        if let Some(handler) = &*event_handler.read().await {
                            let handler = std::sync::Arc::clone(handler);
                            let id_clone = id.clone();
                            tokio::spawn(async move {
                                handler.on_quality_changed(&id_clone, quality_score).await;
                            });
                        }
                        
                        last_quality_score = quality_score;
                    }
                    
                    // 定期触发统计更新事件
                    if let Some(handler) = &*event_handler.read().await {
                        let stats_snapshot = stats.read().await.clone();
                        let handler = std::sync::Arc::clone(handler);
                        let id_clone = id.clone();
                        tokio::spawn(async move {
                            handler.on_statistics_updated(&id_clone, &stats_snapshot).await;
                        });
                    }
                }
            })
        };
        
        // 启动发送任务
        let _send_task = {
            let id = id.clone();
            let connection: Arc<RwLock<Option<tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>>>> = Arc::clone(&connection);
            let event_handler: Arc<RwLock<Option<Arc<dyn ConnectionEvent>>>> = Arc::clone(&event_handler);
            let stats: Arc<RwLock<ConnectionStats>> = Arc::clone(&stats);
            let last_activity: Arc<RwLock<Instant>> = Arc::clone(&last_activity);
            
            tokio::spawn(async move {
                loop {
                    // 等待发送消息
                    if let Some(message_data) = rx.recv().await {
                        let data_len = message_data.len();
                        info!("准备发送消息数据: {} - 长度: {}", id, data_len);
                        
                        // 直接发送二进制数据，因为我们已经序列化了消息
                        let ws_msg = WsMessage::Binary(message_data.into());
                        info!("发送二进制消息: {} - 长度: {}", id, data_len);
                        
                        // 获取连接并发送
                        {
                            let mut conn_guard = connection.write().await;
                            if let Some(ws_stream) = &mut *conn_guard {
                                if let Err(e) = ws_stream.send(ws_msg).await {
                                    error!("WebSocket 发送错误: {} - {}", id, e);
                                    if let Some(handler) = &*event_handler.read().await {
                                        let handler = std::sync::Arc::clone(handler);
                                        let id_clone = id.clone();
                                        let err_text = e.to_string();
                                        tokio::spawn(async move { handler.on_error(&id_clone, &err_text).await; });
                                    }
                                    break;
                                } else {
                                    // 更新统计与活跃时间
                                    {
                                        let mut s = stats.write().await; 
                                        s.messages_sent += 1;
                                        s.last_activity = Instant::now();
                                    }
                                    {
                                        let mut last = last_activity.write().await; 
                                        *last = Instant::now();
                                    }
                                    
                                    debug!("WebSocket 消息已发送: {}", id);
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
            let connection: Arc<RwLock<Option<tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>>>> = Arc::clone(&connection);
            let event_handler: Arc<RwLock<Option<Arc<dyn ConnectionEvent>>>> = Arc::clone(&event_handler);
            let stats: Arc<RwLock<ConnectionStats>> = Arc::clone(&stats);
            let last_activity: Arc<RwLock<Instant>> = Arc::clone(&last_activity);
            let state: Arc<RwLock<ConnectionState>> = Arc::clone(&state);
            let serializer: Arc<Box<dyn FrameSerializer>> = Arc::clone(&serializer);
            let config = config.clone(); // 使用克隆的配置
            let message_sender: Arc<RwLock<Option<tokio::sync::mpsc::UnboundedSender<Vec<u8>>>>> = Arc::clone(&self.message_sender); // 克隆发送通道
            
            // 创建统一消息解析器
            let mut parser = MessageParser::new(
                id.clone(),
                Arc::clone(&event_handler.read().await.as_ref().unwrap().clone()),
                Arc::clone(&stats),
                serializer.clone(), // 克隆serializer而不是移动
                config, // 传递配置
            );
            
            // 将消息发送通道设置到消息解析器中
            if let Some(sender) = &*message_sender.read().await {
                parser.set_message_sender(sender.clone());
            }
            
            let message_parser = Arc::new(parser);
            
            tokio::spawn(async move {
                loop {
                    // 检查连接状态
                    {
                        let current_state = *state.read().await;
                        if !matches!(current_state, ConnectionState::Connected | ConnectionState::Ready) {
                            debug!("连接状态不正确，退出接收任务: {} - 状态: {:?}", id, current_state);
                            break;
                        }
                    }
                    
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
                                            info!("WebSocket 收到文本消息: {} - 内容: {}", id, text);
                                            
                                            // 使用消息解析器处理消息
                                            if let Err(e) = message_parser.parse_and_handle(text.as_bytes().to_vec()).await {
                                                error!("消息处理失败: {}", e);
                                            }
                                        }
                                        WsMessage::Binary(data) => {
                                            debug!("WebSocket 收到二进制消息: {} - 长度: {}", id, data.len());
                                            
                                            // 使用消息解析器处理消息
                                            if let Err(e) = message_parser.parse_and_handle(data.to_vec()).await {
                                                error!("消息处理失败: {}", e);
                                            }
                                        }
                                        WsMessage::Ping(data) => {
                                            debug!("收到 ping: {}", id);
                                            // 自动回复 pong
                                            let _ = ws_stream.send(WsMessage::Pong(data)).await;
                                            // 使用统一消息解析器处理Ping消息
                                            message_parser.handle_frame(crate::common::protocol::Frame::heartbeat()).await;
                                        }
                                        WsMessage::Pong(_) => {
                                            message_parser.handle_frame(Frame::heartbeat_ack()).await;
                                        }
                                        WsMessage::Close(frame) => {
                                            debug!("连接关闭: {} - {:?}", id, frame);
                                            // 更新连接状态
                                            {
                                                let mut state_guard = state.write().await;
                                                *state_guard = ConnectionState::Disconnected;
                                            }
                                            
                                            // 触发断开事件
                                            if let Some(handler) = &*event_handler.read().await {
                                                let handler = std::sync::Arc::clone(handler);
                                                let id_clone = id.clone();
                                                tokio::spawn(async move { 
                                                    handler.on_disconnected(&id_clone, "对端关闭连接").await; 
                                                });
                                            }
                                            break;
                                        }
                                        _ => {
                                            debug!("收到其他类型消息: {}", id);
                                        }
                                    }
                                }
                                Some(Err(e)) => {
                                    error!("WebSocket 读取错误: {} - {}", id, e);
                                    
                                    // 根据错误类型决定是否继续处理或断开连接
                                    let should_disconnect = match &e {
                                        tokio_tungstenite::tungstenite::Error::ConnectionClosed 
                                        | tokio_tungstenite::tungstenite::Error::AlreadyClosed => true,
                                        tokio_tungstenite::tungstenite::Error::Protocol(_) => {
                                            // 协议错误可能是连接重置，不立即断开，给回显一个机会
                                            warn!("WebSocket 协议错误，延迟处理: {} - {}", id, e);
                                            false
                                        },
                                        _ => true,
                                    };
                                    
                                    if should_disconnect {
                                        // 更新连接状态
                                        {
                                            let mut state_guard = state.write().await;
                                            *state_guard = ConnectionState::Failed;
                                        }
                                        
                                        if let Some(handler) = &*event_handler.read().await {
                                            let handler = std::sync::Arc::clone(handler);
                                            let id_clone = id.clone();
                                            let err_text = e.to_string();
                                            tokio::spawn(async move { 
                                                handler.on_error(&id_clone, &err_text).await; 
                                            });
                                        }
                                        break;
                                    } else {
                                        // 只触发错误事件，但不断开连接
                                        if let Some(handler) = &*event_handler.read().await {
                                            let handler = std::sync::Arc::clone(handler);
                                            let id_clone = id.clone();
                                            let err_text = e.to_string();
                                            tokio::spawn(async move { 
                                                handler.on_error(&id_clone, &err_text).await; 
                                            });
                                        }
                                        // 稍微延迟后继续处理
                                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                                    }
                                }
                                None => {
                                    // 对端关闭
                                    debug!("WebSocket 对端关闭: {}", id);
                                    
                                    // 更新连接状态
                                    {
                                        let mut state_guard = state.write().await;
                                        *state_guard = ConnectionState::Disconnected;
                                    }
                                    
                                    // 触发断开事件
                                    if let Some(handler) = &*event_handler.read().await {
                                        let handler = std::sync::Arc::clone(handler);
                                        let id_clone = id.clone();
                                        tokio::spawn(async move { 
                                            handler.on_disconnected(&id_clone, "对端关闭连接").await; 
                                        });
                                    }
                                    break;
                                }
                            }
                        } else {
                            debug!("WebSocket 连接不可用，退出接收任务: {}", id);
                            break;
                        }
                    }
                }
                
                debug!("WebSocket 接收任务已结束: {}", id);
            })
        };
        
        // 保存任务句柄（这里我们只保存接收任务，发送任务会在通道关闭时自动结束）
        *self.receive_task.write().await = Some(receive_task);
        
        info!("WebSocket 消息处理任务已启动: {}", self.id);
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
        debug!("心跳发送请求: {}", self.id);
        
        // 创建心跳消息
        let heartbeat_frame = Frame::heartbeat();
        
        // 先序列化心跳消息
        let heartbeat_data = self.serializer.serialize(&heartbeat_frame).await
            .map_err(|e| FlareError::serialization_error(format!("心跳消息序列化失败: {}", e)))?;
        
        // 通过消息发送通道发送序列化后的心跳数据
        let sender = self.message_sender.read().await;
        if let Some(tx) = &*sender {
            tx.send(heartbeat_data)
                .map_err(|e| FlareError::message_send_failed(format!("心跳消息发送失败: {}", e)))?;
        }
        
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
        debug!("心跳响应发送请求: {} - 数据: {:?}", self.id, data);
        
        // 创建心跳响应消息
        let heartbeat_ack_frame = Frame::heartbeat_ack();
        
        // 先序列化心跳响应消息
        let response_data = self.serializer.serialize(&heartbeat_ack_frame).await
            .map_err(|e| FlareError::serialization_error(format!("心跳确认消息序列化失败: {}", e)))?;
        
        // 通过消息发送通道发送序列化后的心跳响应数据
        let sender = self.message_sender.read().await;
        if let Some(tx) = &*sender {
            tx.send(response_data)
                .map_err(|e| FlareError::message_send_failed(format!("心跳响应消息发送失败: {}", e)))?;
        }
        
        // 更新活跃时间
        let mut last_activity = self.last_activity.write().await;
        *last_activity = Instant::now();
        
        debug!("心跳响应发送成功: {}", self.id);
        Ok(())
    }
    
    async fn set_heartbeat_response_handler(&mut self, _handler: Option<HeartbeatResponseHandler>) {
        // 心跳响应处理器已移除，此方法为空实现
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
    
    async fn set_connection_event_handler(&mut self, handler: Arc<dyn ConnectionEvent>) {
        *self.event_handler.write().await = Some(handler);
    }
    
    async fn send_error_notification(&self, error_code: u32, error_message: &str) -> Result<()> {
        // 创建错误帧
        let error_frame = Frame::error(error_code, error_message);
        
        // 先序列化错误消息，确保序列化成功再发送
        let error_data = self.serializer.serialize(&error_frame).await
            .map_err(|e| FlareError::serialization_error(format!("错误消息序列化失败: {}", e)))?;
        
        // 通过通道发送序列化后的错误数据
        let sender = self.message_sender.read().await;
        if let Some(tx) = &*sender {
            tx.send(error_data)
                .map_err(|e| FlareError::message_send_failed(format!("错误消息发送失败: {}", e)))?;
            
            debug!("错误通知消息已提交发送: {} - 错误码: {}, 错误信息: {}", 
                   self.id, error_code, error_message);
            
            Ok(())
        } else {
            Err(FlareError::connection_failed("消息发送通道不可用"))
        }
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
            let handler: Arc<dyn ConnectionEvent> = Arc::clone(handler);
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
            let handler: Arc<dyn ConnectionEvent> = Arc::clone(handler);
            tokio::spawn(async move {
                handler.on_disconnected(&id, "主动断开").await;
            });
        }
        
        info!("WebSocket 连接已断开: {}", self.id);
        Ok(())
    }
    
    async fn send_message(&mut self, message: Frame) -> Result<()> {
        // 检查连接状态
        let state = *self.state.read().await;
        if !matches!(state, ConnectionState::Connected | ConnectionState::Ready) {
            return Err(FlareError::connection_failed("连接未就绪"));
        }
        
        // 先序列化消息，确保序列化成功再发送
        let message_data = self.serializer.serialize(&message).await
            .map_err(|e| FlareError::serialization_error(format!("消息序列化失败: {}", e)))?;
        
        // 通过通道发送序列化后的数据
        let sender = self.message_sender.read().await;
        if let Some(tx) = &*sender {
            let message_type = message.get_message_type();
            tx.send(message_data)
                .map_err(|e| FlareError::message_send_failed(format!("消息发送失败: {}", e)))?;
            
            debug!("WebSocket 消息已提交发送: {} - 类型: {:?}", self.id, message_type);
            
            // 触发消息发送事件
            if let Some(handler) = &*self.event_handler.read().await {
                let handler: Arc<dyn ConnectionEvent> = Arc::clone(handler);
                let id = self.id.clone();
                let msg_clone = message.clone();
                tokio::spawn(async move { 
                    handler.on_message_sent(&id, &msg_clone).await;
                });
            }
            
            Ok(())
        } else {
            Err(FlareError::connection_failed("消息发送通道不可用"))
        }
    }
    
    async fn try_reconnect(&mut self) -> Result<()> {
        let attempts = *self.reconnect_attempts.read().await;
        if attempts >= self.config.max_reconnect_attempts {
            // 触发重连失败事件
            if let Some(handler) = &*self.event_handler.read().await {
                let handler: Arc<dyn ConnectionEvent> = Arc::clone(handler);
                let id = self.id.clone();
                let error_msg = format!("超过最大重连次数: {}", self.config.max_reconnect_attempts);
                tokio::spawn(async move {
                    handler.on_reconnect_failed(&id, attempts + 1, &error_msg).await;
                });
            }
            return Err(FlareError::connection_failed("超过最大重连次数"));
        }
        
        *self.state.write().await = ConnectionState::Reconnecting;
        
        // 触发重连开始事件
        if let Some(handler) = &*self.event_handler.read().await {
            let handler: Arc<dyn ConnectionEvent> = Arc::clone(handler);
            let id = self.id.clone();
            tokio::spawn(async move {
                handler.on_reconnect_started(&id, attempts + 1).await;
            });
        }
        
        // 等待重连延迟
        tokio::time::sleep(Duration::from_millis(self.config.reconnect_delay_ms)).await;
        
        // 尝试重新连接
        match self.connect().await {
            Ok(_) => {
                // 更新重连次数
                {
                    let mut attempts_guard = self.reconnect_attempts.write().await;
                    *attempts_guard += 1;
                }
                
                // 更新最后活跃时间
                self.update_last_activity().await;
                
                // 触发重连成功事件
                if let Some(handler) = &*self.event_handler.read().await {
                    let handler: Arc<dyn ConnectionEvent> = Arc::clone(handler);
                    let id = self.id.clone();
                    tokio::spawn(async move {
                        handler.on_reconnected(&id, attempts + 1).await;
                    });
                }
                
                info!("WebSocket 重连成功: {} (第 {} 次)", self.id, attempts + 1);
                Ok(())
            }
            Err(e) => {
                // 触发重连失败事件
                if let Some(handler) = &*self.event_handler.read().await {
                    let handler: Arc<dyn ConnectionEvent> = Arc::clone(handler);
                    let id = self.id.clone();
                    let error_msg = e.to_string();
                    tokio::spawn(async move {
                        handler.on_reconnect_failed(&id, attempts + 1, &error_msg).await;
                    });
                }
                Err(e)
            }
        }
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
            let handler: Arc<dyn ConnectionEvent> = Arc::clone(handler);
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
            let handler: Arc<dyn ConnectionEvent> = Arc::clone(handler);
            tokio::spawn(async move {
                handler.on_disconnected(&id, "服务端关闭").await;
            });
        }
        
        info!("WebSocket 服务端连接已关闭: {}", self.id);
        Ok(())
    }
    
    async fn send_message(&mut self, message: Frame) -> Result<()> {
        // 检查连接状态
        let state = *self.state.read().await;
        if !matches!(state, ConnectionState::Connected | ConnectionState::Ready) {
            return Err(FlareError::connection_failed("连接未就绪"));
        }
        
        // 先序列化消息，确保序列化成功再发送
        let message_data = self.serializer.serialize(&message).await
            .map_err(|e| FlareError::serialization_error(format!("消息序列化失败: {}", e)))?;
        
        // 通过通道发送序列化后的数据（与客户端一样）
        let sender = self.message_sender.read().await;
        if let Some(tx) = &*sender {
            let message_type = message.get_message_type();
            tx.send(message_data)
                .map_err(|e| FlareError::message_send_failed(format!("消息发送失败: {}", e)))?;
            
            debug!("WebSocket 服务端消息已提交发送: {} - 类型: {:?}", self.id, message_type);
            
            // 触发消息发送事件
            if let Some(handler) = &*self.event_handler.read().await {
                let handler: Arc<dyn ConnectionEvent> = Arc::clone(handler);
                let id = self.id.clone();
                let msg_clone = message.clone();
                tokio::spawn(async move { 
                    handler.on_message_sent(&id, &msg_clone).await;
                });
            }
            
            Ok(())
        } else {
            Err(FlareError::connection_failed("消息发送通道不可用"))
        }
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
        Some(format!("WebSocket Client - {}", self.id))
    }
    
    async fn get_connection_stats(&self) -> ConnectionStats {
        self.stats.read().await.clone()
    }
}
