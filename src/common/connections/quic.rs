//! QUIC 连接实现
//! 
//! 提供基于 QUIC 协议的连接实现

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, error};

use crate::common::{
    error::{Result, FlareError},
    protocol::Frame,
    connections::{
        traits::{Connection, ConnectionEvent, ConnectionStats, ClientConnection, ServerConnection},
        types::{ConnectionConfig, ConnectionState},
        event::DefConnectionEventHandler,
    },
    messaging::MessageParser,
    serialization::FrameSerializer,
    serialization::SerializerFactory,
    serialization::factory::json_serializer,
};

use quinn::{Connection as QuinnConnection, Endpoint};
use crate::common::error::ErrorCode;
use crate::{Platform};

/// 跳过服务器证书验证的实现（仅用于演示）
#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer,
        _intermediates: &[rustls::pki_types::CertificateDer],
        _server_name: &rustls::pki_types::ServerName,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA1,
            rustls::SignatureScheme::ECDSA_SHA1_Legacy,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::ED448,
        ]
    }
}

/// QUIC 连接实现
pub struct QuicConnection {
    /// 连接ID
    id: String,
    /// 连接配置
    config: Arc<ConnectionConfig>,
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
    
    /// QUIC 连接（统一字段，根据角色使用）
    connection: Arc<RwLock<Option<QuinnConnection>>>,
    /// 消息接收任务句柄
    receive_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// 心跳监控任务句柄
    heartbeat_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// 消息发送任务句柄
    send_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,

    /// 消息发送通道（发送已序列化的数据）
    message_sender: Arc<RwLock<Option<tokio::sync::mpsc::UnboundedSender<Vec<u8>>>>>,
    /// 消息接收通道（接收已序列化的数据）
    message_receiver: Arc<RwLock<Option<tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>>>>,
    /// 序列化器
    serializer: Arc<Box<dyn crate::common::serialization::FrameSerializer>>,
    /// 用户ID（用于服务端连接）
    user_id: Arc<RwLock<Option<String>>>,
    /// 客户端信息（用于服务端连接）
    client_info: Arc<RwLock<Option<crate::common::connections::types::ClientInfo>>>,
}

impl QuicConnection {
    /// 创建新的 QUIC 连接（使用配置）
    pub fn new(config: ConnectionConfig) -> Self {
        // 根据配置创建序列化器
        let serializer = {
            let factory = SerializerFactory::new();
            let format = config.serialization_config.as_ref().map(|c| c.format).unwrap_or(crate::common::serialization::SerializationFormat::Json);
            factory.create_with_config(
                format,
                config.get_serialization_config()
            ).unwrap_or_else(|_| {
                // 如果创建失败，使用默认JSON序列化器
                json_serializer()
            })
        };
        
        Self::with_serializer(config, Arc::from(serializer))
    }
    
    /// 创建新的 QUIC 连接（使用自定义序列化器）
    pub fn with_serializer(config: ConnectionConfig, serializer: Arc<Box<dyn crate::common::serialization::FrameSerializer>>) -> Self {
        // 如果配置中没有ID，则生成一个UUID
        let id = if config.id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            config.id.clone()
        };

        let stats = ConnectionStats {
            established_at: Instant::now(),
            last_activity: Instant::now(),
            messages_received: 0,
            messages_sent: 0,
            heartbeat_responses: 0,
            quality_score: 100,
        };

        // 创建消息发送通道
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

        Self {
            id,
            config: Arc::new(config),
            state: Arc::new(RwLock::new(ConnectionState::Initializing)),
            event_handler: Arc::new(RwLock::new(None)),
            last_activity: Arc::new(RwLock::new(Instant::now())),
            reconnect_attempts: Arc::new(RwLock::new(0)),
            stats: Arc::new(RwLock::new(stats)),

            connection: Arc::new(RwLock::new(None)),
            receive_task: Arc::new(RwLock::new(None)),
            heartbeat_task: Arc::new(RwLock::new(None)),
            send_task: Arc::new(RwLock::new(None)),
            message_sender: Arc::new(RwLock::new(Some(tx))),
            message_receiver: Arc::new(RwLock::new(Some(rx))),
            serializer,
            user_id: Arc::new(RwLock::new(None)),
            client_info: Arc::new(RwLock::new(None)),
        }
    }
    
    /// 更新最后活跃时间
    async fn update_last_activity(&self) {
        let mut last_activity = self.last_activity.write().await;
        *last_activity = Instant::now();
        
        let mut stats = self.stats.write().await;
        stats.last_activity = *last_activity;
    }
    
    /// 设置 QUIC 连接（用于服务端连接）
    pub async fn set_connection(&mut self, conn: QuinnConnection) {
        *self.connection.write().await = Some(conn);
    }

    /// 内部版本的启动消息处理任务，可以在&self上调用
    pub async fn start_task(&self) -> Result<()> {
        // 启动心跳监控任务
        self.start_heartbeat_task_internal().await?;
        
        // 启动发送任务
        self.start_send_task_internal().await?;
        
        // 启动接收任务
        self.start_message_receive_task_internal().await?;
        
        Ok(())
    }
    
    /// 启动心跳监控任务
    async fn start_heartbeat_task_internal(&self) -> Result<()> {
        let id = self.id.clone();
        let event_handler: Arc<RwLock<Option<Arc<dyn ConnectionEvent>>>> = Arc::clone(&self.event_handler);
        let stats: Arc<RwLock<ConnectionStats>> = Arc::clone(&self.stats);
        let last_activity: Arc<RwLock<Instant>> = Arc::clone(&self.last_activity);
        let state: Arc<RwLock<ConnectionState>> = Arc::clone(&self.state);
        let config = self.config.clone();
        
        // 启动心跳监控任务
        let heartbeat_task = {
            let id = id.clone();
            let event_handler = Arc::clone(&event_handler);
            let last_activity = Arc::clone(&last_activity);
            let stats = Arc::clone(&stats);
            let heartbeat_interval = Duration::from_millis(config.heartbeat_interval_ms);
            
            // 根据角色选择心跳超时配置
            let heartbeat_timeout = if config.role == crate::common::connections::types::ConnectionRole::Client {
                Duration::from_millis(config.heartbeat_timeout_ms)
            } else {
                // 服务端使用服务端特定配置
                if let Some(server_config) = &config.server_config {
                    Duration::from_millis(server_config.heartbeat_monitor_timeout_ms)
                } else {
                    Duration::from_millis(config.heartbeat_timeout_ms) // 回退到默认值
                }
            };
            
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(heartbeat_interval);
                let mut last_quality_score = 100u8;
                let mut consecutive_timeouts = 0u32;
                
                loop {
                    interval.tick().await;
                    
                    // 检查连接状态
                    {
                        let current_state = *state.read().await;
                        if matches!(current_state, ConnectionState::Disconnected | ConnectionState::Failed) {
                            debug!("连接已断开，停止心跳监控: {}", id);
                            break;
                        }
                    }
                    
                    let last_act = *last_activity.read().await;
                    let elapsed = last_act.elapsed();
                    
                    // 检查心跳超时
                    if elapsed > heartbeat_timeout {
                        consecutive_timeouts += 1;
                        if let Some(handler) = &*event_handler.read().await {
                            let handler = std::sync::Arc::clone(handler);
                            let id_clone = id.clone();
                            tokio::spawn(async move {
                                handler.on_heartbeat_timeout(&id_clone).await;
                            });
                        }
                        
                        // 如果连续超时次数过多，可能需要断开连接
                        if consecutive_timeouts > 3 {
                            error!("连接连续心跳超时，可能需要断开: {}", id);
                        }
                    } else {
                        // 重置连续超时计数
                        consecutive_timeouts = 0;
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
        
        // 保存心跳任务句柄
        *self.heartbeat_task.write().await = Some(heartbeat_task);
        Ok(())
    }
    
    /// 启动消息发送任务
    async fn start_send_task_internal(&self) -> Result<()> {
        let id = self.id.clone();
        let connection: Arc<RwLock<Option<QuinnConnection>>> = Arc::clone(&self.connection);
        let event_handler: Arc<RwLock<Option<Arc<dyn ConnectionEvent>>>> = Arc::clone(&self.event_handler);
        let stats: Arc<RwLock<ConnectionStats>> = Arc::clone(&self.stats);
        let last_activity: Arc<RwLock<Instant>> = Arc::clone(&self.last_activity);
        let state: Arc<RwLock<ConnectionState>> = Arc::clone(&self.state);
        
        // 获取消息接收通道
        let mut message_receiver = self.message_receiver.write().await.take()
            .ok_or_else(|| FlareError::general_error("消息接收通道不可用".to_string()))?;
        
        // 启动发送任务
        let send_task = {
            let id = id.clone();
            let connection: Arc<RwLock<Option<QuinnConnection>>> = Arc::clone(&connection);
            let event_handler: Arc<RwLock<Option<Arc<dyn ConnectionEvent>>>> = Arc::clone(&event_handler);
            let stats: Arc<RwLock<ConnectionStats>> = Arc::clone(&stats);
            let last_activity: Arc<RwLock<Instant>> = Arc::clone(&last_activity);
            let state: Arc<RwLock<ConnectionState>> = Arc::clone(&state);
            
            tokio::spawn(async move {
                loop {
                    // 检查连接状态
                    {
                        let current_state = *state.read().await;
                        if matches!(current_state, ConnectionState::Disconnected | ConnectionState::Failed) {
                            debug!("连接已断开，停止发送任务: {}", id);
                            break;
                        }
                    }
                    
                    // 等待发送消息
                    if let Some(message_data) = message_receiver.recv().await {
                        let data_len = message_data.len();
                        info!("准备发送QUIC消息数据: {} - 长度: {}", id, data_len);
                        
                        // 发送消息
                        if let Some(conn) = &*connection.read().await {
                            // 打开双向流
                            match conn.open_bi().await {
                                Ok((mut send, _recv)) => {
                                    // 发送已序列化的数据
                                    if let Err(e) = send.write_all(&message_data).await {
                                        error!("QUIC 消息发送失败: {} - {}", id, e);
                                        if let Some(handler) = &*event_handler.read().await {
                                            let handler = std::sync::Arc::clone(handler);
                                            let id_clone = id.clone();
                                            let err_text = e.to_string();
                                            tokio::spawn(async move { handler.on_error(&id_clone, &err_text).await; });
                                        }
                                        break;
                                    }
                                    
                                    if let Err(e) = send.finish() {
                                        error!("QUIC 流关闭失败: {} - {}", id, e);
                                        if let Some(handler) = &*event_handler.read().await {
                                            let handler = std::sync::Arc::clone(handler);
                                            let id_clone = id.clone();
                                            let err_text = e.to_string();
                                            tokio::spawn(async move { handler.on_error(&id_clone, &err_text).await; });
                                        }
                                        break;
                                    }
                                    
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
                                    
                                    debug!("QUIC 消息已发送: {}", id);
                                }
                                Err(e) => {
                                    error!("QUIC 无法打开双向流: {} - {}", id, e);
                                    if let Some(handler) = &*event_handler.read().await {
                                        let handler = std::sync::Arc::clone(handler);
                                        let id_clone = id.clone();
                                        let err_text = e.to_string();
                                        tokio::spawn(async move { handler.on_error(&id_clone, &err_text).await; });
                                    }
                                    break;
                                }
                            }
                        } else {
                            error!("QUIC 连接不可用，无法发送消息: {}", id);
                            break;
                        }
                    } else {
                        // 发送通道关闭，退出任务
                        debug!("QUIC 发送通道关闭: {}", id);
                        break;
                    }
                }
            })
        };
        
        // 保存发送任务句柄
        *self.send_task.write().await = Some(send_task);
        Ok(())
    }
    
    /// 启动消息接收任务
    async fn start_message_receive_task_internal(&self) -> Result<()> {
        let id = self.id.clone();
        let connection: Arc<RwLock<Option<QuinnConnection>>> = Arc::clone(&self.connection);
        let event_handler: Arc<RwLock<Option<Arc<dyn ConnectionEvent>>>> = Arc::clone(&self.event_handler);
        let stats: Arc<RwLock<ConnectionStats>> = Arc::clone(&self.stats);
        let last_activity: Arc<RwLock<Instant>> = Arc::clone(&self.last_activity);
        let state: Arc<RwLock<ConnectionState>> = Arc::clone(&self.state);
        let serializer = Arc::clone(&self.serializer);
        let config = self.config.clone(); // 克隆配置
        
        // 克隆事件处理器或使用默认处理器
        let event_handler_clone = event_handler.read().await.as_ref().cloned().unwrap_or_else(|| {
            Arc::new(DefConnectionEventHandler::default()) as Arc<dyn ConnectionEvent>
        });
        
        // 启动接收任务
        let receive_task = {
            let id = id.clone();
            let connection: Arc<RwLock<Option<QuinnConnection>>> = Arc::clone(&connection);
            let _event_handler: Arc<RwLock<Option<Arc<dyn ConnectionEvent>>>> = Arc::clone(&event_handler);
            let stats: Arc<RwLock<ConnectionStats>> = Arc::clone(&stats);
            let last_activity: Arc<RwLock<Instant>> = Arc::clone(&last_activity);
            let _state: Arc<RwLock<ConnectionState>> = Arc::clone(&state);
            let serializer: Arc<Box<dyn FrameSerializer>> = Arc::clone(&serializer);
            let config = config.clone(); // 使用克隆的配置
            let message_sender: Arc<RwLock<Option<tokio::sync::mpsc::UnboundedSender<Vec<u8>>>>> = Arc::clone(&self.message_sender); // 克隆发送通道
            
            // 创建统一消息解析器
            let mut parser = MessageParser::new(
                id.clone(),
                Arc::clone(&event_handler_clone),
                Arc::clone(&stats),
                serializer.clone(), // 克隆serializer而不是移动
                (*config).clone(), // 传递配置
            );
            
            // 将消息发送通道设置到消息解析器中
            if let Some(sender) = &*message_sender.read().await {
                parser.set_message_sender(sender.clone());
            }
            
            let message_parser = Arc::new(parser);
            
            tokio::spawn(async move {
                loop {
                    // 读取 QUIC 消息
                    {
                        let mut conn_guard = connection.write().await;
                        if let Some(conn) = &mut *conn_guard {
                            // 等待双向流
                            while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                                // 更新活跃时间
                                {
                                    let mut last = last_activity.write().await;
                                    *last = Instant::now();
                                }
                                
                                // 处理接收到的数据
                                let mut buffer = vec![0u8; 1024];
                                match recv.read(&mut buffer).await {
                                    Ok(Some(bytes_read)) => {
                                        let data = buffer[..bytes_read].to_vec();
                                        // 使用序列化器解析消息
                                        match serializer.deserialize(&data).await {
                                            Ok(frame) => {
                                                debug!("成功解析消息: {:?}", frame.get_command_type_str());
                                                // 使用消息解析器处理帧
                                                message_parser.handle_frame(frame).await;
                                            },
                                            Err(e) => {
                                                error!("消息反序列化失败: {}", e);
                                                // 直接使用send流发送错误通知给发送方
                                                let error_frame = Frame::error(
                                                    format!("deserialization_error_{}", fastrand::u64(..)),
                                                    format!("消息反序列化失败: {}", e)
                                                );
                                                if let Ok(error_data) = serializer.serialize(&error_frame).await {
                                                    if let Err(send_err) = send.write_all(&error_data).await {
                                                        error!("发送错误通知失败: {}", send_err);
                                                    } else {
                                                        debug!("已发送序列化错误通知给发送方: {}", id);
                                                    }
                                                }
                                                
                                                // 解析失败，不继续处理消息
                                                continue;
                                            }
                                        };
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
                }
            })
        };
        
        *self.receive_task.write().await = Some(receive_task);
        Ok(())
    }

    
    /// 停止所有任务
    async fn stop_all_tasks(&self) -> Result<()> {
        // 停止接收任务
        if let Some(task) = self.receive_task.write().await.take() {
            task.abort();
        }
        
        // 停止发送任务
        if let Some(task) = self.send_task.write().await.take() {
            task.abort();
        }
        
        // 停止心跳任务
        if let Some(task) = self.heartbeat_task.write().await.take() {
            task.abort();
        }
        
        Ok(())
    }
}

#[async_trait::async_trait]
impl Connection for QuicConnection {
    fn id(&self) -> String {
        self.id.clone()
    }
    
    fn state(&self) -> ConnectionState {
        // 注意：这个方法需要是同步的，所以我们需要使用try_read
        if let Ok(state) = self.state.try_read() {
            *state
        } else {
            ConnectionState::Error
        }
    }

    fn config(&self) -> Arc<ConnectionConfig> {
        Arc::clone(&self.config)
    }

    fn stats(&self) -> ConnectionStats {
        // 注意：这个方法需要是同步的，所以我们需要使用try_read
        if let Ok(stats) = self.stats.try_read() {
            stats.clone()
        } else {
            ConnectionStats::default()
        }
    }
    
    fn last_activity_epoch_ms(&self) -> i64 {
        // 注意：这个方法需要是同步的，所以我们需要使用try_read
        if let Ok(last_activity) = self.last_activity.try_read() {
            last_activity.elapsed().as_millis() as i64
        } else {
            0
        }
    }
    
    fn status(&self) -> ConnectionState {
        self.state()
    }
    
    async fn send_message(&self, message: Frame) -> Result<()> {
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
            let message_type = message.get_command_type_str();
            tx.send(message_data)
                .map_err(|e| FlareError::message_send_failed(format!("消息发送失败: {}", e)))?;
            
            debug!("QUIC 服务端消息已提交发送: {} - 类型: {:?}", self.id, message_type);
            
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

    async fn close(&self, reason: Option<String>) -> Result<()> {
        *self.state.write().await = ConnectionState::Disconnecting;
        // 通过自定义消息先通知客户端，方便统一处理
        // TODO: Implement disconnect frame
        // 关闭 QUIC 连接
        if let Some(conn) = &*self.connection.read().await {
            conn.close(0u32.into(), b"connection closed");
        }
        
        // 清理连接
        *self.connection.write().await = None;
        
        *self.state.write().await = ConnectionState::Disconnected;
        
        // 触发断开事件
        let id = self.id.clone();
        let disconnect_reason = reason.clone().unwrap_or_else(|| "主动断开".to_string());
        if let Some(handler) = &*self.event_handler.read().await {
            let handler: Arc<dyn ConnectionEvent> = Arc::clone(handler);
            tokio::spawn(async move {
                handler.on_disconnected(&id, &disconnect_reason).await;
            });
        }
        // 停止所有任务
        self.stop_all_tasks().await?;
        info!("QUIC 连接已断开: {} - 原因: {}", self.id, reason.unwrap_or_else(|| "主动断开".to_string()));
        Ok(())
    }

    async fn set_event_handler(&mut self, handler: Arc<dyn ConnectionEvent>) {
        *self.event_handler.write().await = Some(handler);
    }
    
    async fn send_error_notification(&self, error_code: u32, error_message: &str) -> Result<()> {
        // 记录错误日志
        debug!("发送错误通知: 连接 {} - 错误码: {}, 错误信息: {}",
               self.id, error_code, error_message);
        
        // 创建错误帧
        let error_frame = Frame::error(
            format!("error_{}", fastrand::u64(..)),
            format!("{}: {}", error_code, error_message)
        );
        
        // 先序列化错误消息，确保序列化成功再发送
        let error_data = self.serializer.serialize(&error_frame).await
            .map_err(|e| {
                error!("错误消息序列化失败: {}", e);
                FlareError::serialization_error(format!("错误消息序列化失败: {}", e))
            })?;
        
        // 通过通道发送序列化后的错误数据
        let sender = self.message_sender.read().await;
        if let Some(tx) = &*sender {
            match tx.send(error_data) {
                Ok(()) => {
                    debug!("错误通知消息已提交发送: {} - 错误码: {}, 错误信息: {}", 
                           self.id, error_code, error_message);
                    Ok(())
                }
                Err(e) => {
                    error!("错误消息发送失败: {}", e);
                    Err(FlareError::message_send_failed(format!("错误消息发送失败: {}", e)))
                }
            }
        } else {
            error!("消息发送通道不可用");
            Err(FlareError::connection_failed("消息发送通道不可用"))
        }
    }
}

#[async_trait::async_trait]
impl ClientConnection for QuicConnection {
    async fn connect(&self) -> Result<()> {
        *self.state.write().await = ConnectionState::Connecting;
        
        // 解析服务器地址
        let addr = self.config.remote_addr.parse::<std::net::SocketAddr>()
            .map_err(|e| FlareError::connection_failed(format!("无效的地址格式: {}", e)))?;
        
        // 获取客户端特定配置
        let client_config = if let Some(config) = &self.config.client_config {
            config.clone()
        } else {
            // 如果没有客户端配置，使用默认值
            crate::common::connections::types::ClientSpecificConfig::default()
        };
        
        // 创建客户端 QUIC 配置
        let client_config_builder = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
            .with_no_client_auth();
        
        let quinn_config = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(client_config_builder)
                .map_err(|e| FlareError::connection_failed(format!("QUIC 客户端配置失败: {}", e)))?
        ));
        
        // 创建 QUIC 端点
        let mut endpoint = Endpoint::client("[::]:0".parse().unwrap())
            .map_err(|e| FlareError::connection_failed(format!("无法创建 QUIC 端点: {}", e)))?;
        
        endpoint.set_default_client_config(quinn_config);
        
        // 连接到服务器
        let connecting = endpoint.connect(addr, "localhost")
            .map_err(|e| FlareError::connection_failed(format!("QUIC 连接失败: {}", e)))?;
        
        let new_conn = connecting.await
            .map_err(|e| FlareError::connection_failed(format!("QUIC 握手失败: {}", e)))?;
        
        // 保存连接
        *self.connection.write().await = Some(new_conn);
        
        // 启动消息处理任务
        self.start_task().await?;
        
        *self.state.write().await = ConnectionState::Connected;
        
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
        
        let platform = self.config.client_config.as_ref()
            .and_then(|c| c.platform.clone())
            .unwrap_or(Platform::Web);
        // 发送链接消息
        let message_id = crate::common::protocol::factory::FrameFactory::generate_message_id();
        let frame = crate::common::protocol::factory::FrameFactory::create_ping_frame(message_id.clone()).unwrap();
        self.send_message(frame).await?;
        
        info!("QUIC 连接已建立: {}", self.id);
        Ok(())
    }
    
    async fn authenticated_complete(&self)-> Result<()> {
       *self.state.write().await = ConnectionState::Ready;    
       debug!("QUIC 认证完成: {}", self.id);
       Ok(())
    }

    async fn disconnect(&self,reason: Option<String>) -> Result<()> {
        Connection::close(self, Option::from(reason.clone().unwrap_or_else(|| "客户端主动断开".into()))).await
    }

    async fn try_reconnect(&self) -> Result<()> {
        // 获取客户端特定配置
        let client_config = if let Some(config) = &self.config.client_config {
            config.clone()
        } else {
            // 如果没有客户端配置，使用默认值
            crate::common::connections::types::ClientSpecificConfig::default()
        };
        
        let attempts = *self.reconnect_attempts.read().await;
        if attempts >= client_config.max_reconnect_attempts {
            // 触发重连失败事件
            if let Some(handler) = &*self.event_handler.read().await {
                let handler: Arc<dyn ConnectionEvent> = Arc::clone(handler);
                let id = self.id.clone();
                let error_msg = format!("超过最大重连次数: {}", client_config.max_reconnect_attempts);
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
        tokio::time::sleep(Duration::from_millis(client_config.reconnect_delay_ms)).await;
        
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

                debug!("QUIC 重连成功: {} (第 {} 次)", self.id, attempts + 1);
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
    
    fn needs_reconnect(&self) -> bool {
        // 注意：这个方法需要是同步的，所以我们需要使用try_read
        if let Ok(state) = self.state.try_read() {
            matches!(*state, ConnectionState::Disconnected | ConnectionState::Failed)
        } else {
            false
        }
    }
    
    fn get_reconnect_attempts(&self) -> u32 {
        // 注意：这个方法需要是同步的，所以我们需要使用try_read
        if let Ok(attempts) = self.reconnect_attempts.try_read() {
            *attempts
        } else {
            0
        }
    }
    
    fn reset_reconnect_attempts(&self) {
        // 注意：这个方法需要是同步的，所以我们需要使用try_write
        if let Ok(mut attempts) = self.reconnect_attempts.try_write() {
            *attempts = 0;
        }
    }
}

#[async_trait::async_trait]
impl ServerConnection for QuicConnection {
    async fn accept(&self) -> Result<()> {
        // 检查连接是否已经初始化
        {
            let state = *self.state.read().await;
            if state != ConnectionState::Initializing {
                return Err(FlareError::connection_failed("连接状态不正确，应为初始化状态"));
            }
        }
        
        *self.state.write().await = ConnectionState::Connecting;
        
        // 检查基础配置是否完成
        if self.config.id.is_empty() {
            return Err(FlareError::connection_failed("连接ID未设置"));
        }
        
        if self.config.remote_addr.is_empty() {
            return Err(FlareError::connection_failed("远程地址未设置"));
        }
        
        // 检查QUIC连接是否已设置
        {
            let connection = self.connection.read().await;
            if connection.is_none() {
                return Err(FlareError::connection_failed("QUIC连接未设置"));
            }
        }
        
        // 启动必要的任务
        self.start_task().await?;
        
        *self.state.write().await = ConnectionState::Connected;
        
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
        // 发送connect响应消息
        let message_id = crate::common::protocol::factory::FrameFactory::generate_message_id();
        let mut frame = crate::common::protocol::Frame::new(
            crate::common::protocol::commands::Command::Control(crate::common::protocol::commands::ControlCmd::Pong),
            message_id.clone(),
            crate::common::protocol::Reliability::BestEffort
        );
        self.send_message(frame).await?;

        info!("QUIC 服务端连接已接受: {}", self.id);
        Ok(())
    }

    async fn authenticate(&self,success:bool,platform: Platform, user_id: String, info: Option<Vec<u8>>,reason: Option<String>) -> Result<()> {
        let message_id = crate::common::protocol::factory::FrameFactory::generate_message_id();
        let mut frame = crate::common::protocol::factory::FrameFactory::create_auth_response_frame(
            message_id.clone(),
            false,
            0,
            None,
            Some(reason.clone().unwrap_or_else(|| "认证失败".to_string()))
        ).unwrap();
        if success {
            // 设置用户ID
            *self.user_id.write().await = Some(user_id);

            let client_info = crate::common::connections::types::ClientInfo {
                transport: crate::common::connections::types::Transport::Quic,
                address: self.config.remote_addr.clone(), // 实际应该从info中解析
                platform, // 实际应该从info中解析
            };
            *self.client_info.write().await = Some(client_info);
            let message_id = crate::common::protocol::factory::FrameFactory::generate_message_id();
            frame = crate::common::protocol::factory::FrameFactory::create_auth_response_frame(
                message_id.clone(),
                true,
                0,
                info,
                reason.clone()
            ).unwrap();
            *self.state.write().await = ConnectionState::Ready;
        }
        // 发送认证结果
        self.send_message(frame).await?;
        Ok(())
    }
    
    fn get_client_info(&self) -> Result<crate::common::connections::types::ClientInfo> {
        if let Ok(client_info) = self.client_info.try_read() {
            if let Some(info) = &*client_info {
                Ok((*info).clone())
            } else {
                Err(FlareError::general_error("客户端信息未设置".to_string()))
            }
        } else {
            Err(FlareError::general_error("无法获取客户端信息".to_string()))
        }
    }
    
    async fn get_user_id(&self) -> Option<String> {
        if let Some(user_id) = &*self.user_id.read().await {
            Some(user_id.clone())
        } else {
            None
        }
    }
    
    async fn set_user_id(&self, user_id: String) {
        let mut user_id_lock = self.user_id.write().await;
        *user_id_lock = Some(user_id);
    }
}