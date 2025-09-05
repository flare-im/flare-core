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
        traits::{Connection, ConnectionEvent, ConnectionStats, ClientConnection, ServerConnection, HeartbeatResponseHandler},
        types::{ConnectionConfig, ConnectionState, ConnectionType, ConnectionRole},
        event::DefConnectionEventHandler,
    },
    messaging::MessageParser, // 从messaging模块导入
    serialization::{FrameSerializer, factory::json_serializer},
};

use quinn::{Connection as QuinnConnection, Endpoint, Connecting};

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
    
    /// QUIC 连接（统一字段，根据角色使用）
    connection: Arc<RwLock<Option<QuinnConnection>>>,
    /// 消息接收任务句柄
    receive_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// 心跳响应处理器
    heartbeat_response_handler: Arc<RwLock<Option<HeartbeatResponseHandler>>>,
    /// 消息发送通道（发送已序列化的数据）
    message_sender: Arc<RwLock<Option<tokio::sync::mpsc::UnboundedSender<Vec<u8>>>>>,
    /// 序列化器
    serializer: Arc<Box<dyn crate::common::serialization::FrameSerializer>>,
}

impl QuicConnection {
    /// 创建新的 QUIC 连接（使用配置）
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
            heartbeat_response_handler: Arc::new(RwLock::new(None)),
            message_sender: Arc::new(RwLock::new(None)),
            serializer: Arc::new(serializer),
        }
    }
    
    /// 创建新的 QUIC 连接（使用自定义序列化器）
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
            heartbeat_response_handler: Arc::new(RwLock::new(None)),
            message_sender: Arc::new(RwLock::new(None)),
            serializer,
        }
    }
    
    /// 设置事件处理器
    pub async fn set_event_handler(&mut self, handler: Arc<dyn ConnectionEvent>) {
        *self.event_handler.write().await = Some(handler);
    }
    
    /// 设置 QUIC 连接（用于服务端连接）
    pub async fn set_connection(&mut self, conn: QuinnConnection) {
        *self.connection.write().await = Some(conn);
    }
    

    
    /// 启动消息接收任务
    pub async fn start_receive_task(&mut self) -> Result<()> {
        let id = self.id.clone();
        let event_handler: Arc<RwLock<Option<Arc<dyn ConnectionEvent>>>> = Arc::clone(&self.event_handler);
        let stats: Arc<RwLock<ConnectionStats>> = Arc::clone(&self.stats);
        let last_activity: Arc<RwLock<Instant>> = Arc::clone(&self.last_activity);
        let connection: Arc<RwLock<Option<QuinnConnection>>> = Arc::clone(&self.connection);
        let serializer = Arc::clone(&self.serializer);
        
        let event_handler = event_handler.read().await.as_ref().cloned().unwrap_or_else(|| {
            Arc::new(DefConnectionEventHandler::default()) as Arc<dyn ConnectionEvent>
        });
            
        // 创建消息发送通道（发送已序列化的数据）
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        *self.message_sender.write().await = Some(tx);
        
        // 启动发送任务
        let _send_task = {
            let id = id.clone();
            let connection: Arc<RwLock<Option<QuinnConnection>>> = Arc::clone(&connection);
            let event_handler = Arc::clone(&event_handler);
            let stats: Arc<RwLock<ConnectionStats>> = Arc::clone(&stats);
            let last_activity: Arc<RwLock<Instant>> = Arc::clone(&last_activity);
            
            tokio::spawn(async move {
                loop {
                    // 等待发送消息
                    if let Some(message_data) = rx.recv().await {
                        info!("准备发送QUIC消息数据: {} - 长度: {}", id, message_data.len());
                        
                        // 发送消息
                        if let Some(conn) = &*connection.read().await {
                            // 打开双向流
                            match conn.open_bi().await {
                                Ok((mut send, _recv)) => {
                                    // 发送已序列化的数据
                                    if let Err(e) = send.write_all(&message_data).await {
                                        error!("QUIC 消息发送失败: {} - {}", id, e);
                                        let handler_clone: Arc<dyn ConnectionEvent> = Arc::clone(&event_handler);
                                        let id_clone = id.clone();
                                        let err_text = e.to_string();
                                        tokio::spawn(async move { handler_clone.on_error(&id_clone, &err_text).await; });
                                        break;
                                    }
                                    
                                    if let Err(e) = send.finish() {
                                        error!("QUIC 流关闭失败: {} - {}", id, e);
                                        let handler_clone: Arc<dyn ConnectionEvent> = Arc::clone(&event_handler);
                                        let id_clone = id.clone();
                                        let err_text = e.to_string();
                                        tokio::spawn(async move { handler_clone.on_error(&id_clone, &err_text).await; });
                                        break;
                                    }
                                    
                                    // 更新统计和活跃时间
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
                                    let handler_clone: Arc<dyn ConnectionEvent> = Arc::clone(&event_handler);
                                    let id_clone = id.clone();
                                    let err_text = e.to_string();
                                    tokio::spawn(async move { handler_clone.on_error(&id_clone, &err_text).await; });
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
        
        // 启动接收任务
        let task = {
            let id = id.clone();
            let event_handler: Arc<dyn ConnectionEvent> = event_handler;
            let stats: Arc<RwLock<ConnectionStats>> = Arc::clone(&stats);
            let last_activity: Arc<RwLock<Instant>> = Arc::clone(&last_activity);
            let connection: Arc<RwLock<Option<QuinnConnection>>> = Arc::clone(&connection);
            let serializer: Arc<Box<dyn FrameSerializer>> = Arc::clone(&serializer);
            
            // 创建统一消息解析器
            let message_parser = Arc::new(MessageParser::new(
                id.clone(),
                Arc::clone(&event_handler),
                Arc::clone(&stats),
                serializer,
            ));

            tokio::spawn(async move {
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
                                        
                                        // 使用统一消息解析器处理数据
                                        message_parser.parse_and_handle(data).await;
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
            })
        };
        
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
        // 创建心跳帧
        let heartbeat_frame = Frame::heartbeat();
        
        // 先序列化心跳消息，确保序列化成功再发送
        let heartbeat_data = self.serializer.serialize(&heartbeat_frame).await
            .map_err(|e| FlareError::serialization_error(format!("心跳消息序列化失败: {}", e)))?;
        
        // 通过通道发送序列化后的心跳数据
        let sender = self.message_sender.read().await;
        if let Some(tx) = &*sender {
            tx.send(heartbeat_data)
                .map_err(|e| FlareError::message_send_failed(format!("心跳消息发送失败: {}", e)))?;
            
            debug!("心跳消息已提交发送: {}", self.id);
            
            // 触发心跳发送事件
            if let Some(handler) = &*self.event_handler.read().await {
                let handler = Arc::clone(handler);
                let id = self.id.clone();
                tokio::spawn(async move {
                    handler.on_heartbeat_sent(&id).await;
                });
            }
            
            Ok(())
        } else {
            Err(FlareError::connection_failed("消息发送通道不可用"))
        }
    }
    
    async fn send_heartbeat_response(&self, data: Option<Vec<u8>>) -> Result<()> {
        // 创建心跳确认帧
        let heartbeat_ack_frame = Frame::heartbeat_ack();
        
        // 先序列化心跳确认消息，确保序列化成功再发送
        let response_data = self.serializer.serialize(&heartbeat_ack_frame).await
            .map_err(|e| FlareError::serialization_error(format!("心跳确认消息序列化失败: {}", e)))?;
        
        // 通过通道发送序列化后的心跳响应数据
        let sender = self.message_sender.read().await;
        if let Some(tx) = &*sender {
            tx.send(response_data)
                .map_err(|e| FlareError::message_send_failed(format!("心跳响应消息发送失败: {}", e)))?;
            
            // 更新活跃时间
            let mut last_activity = self.last_activity.write().await;
            *last_activity = Instant::now();
            
            debug!("心跳响应消息已提交发送: {}", self.id);
            Ok(())
        } else {
            Err(FlareError::connection_failed("消息发送通道不可用"))
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
    
    async fn set_connection_event_handler(&mut self, handler: Arc<dyn ConnectionEvent>) {
        *self.event_handler.write().await = Some(handler);
    }
}

#[async_trait::async_trait]
impl ClientConnection for QuicConnection {
    async fn connect(&mut self) -> Result<()> {
        *self.state.write().await = ConnectionState::Connecting;
        
        // 解析服务器地址
        let addr = self.config.remote_addr.parse::<std::net::SocketAddr>()
            .map_err(|e| FlareError::connection_failed(format!("无效的地址格式: {}", e)))?;
        
        // 创建客户端 QUIC 配置
        let client_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
            .with_no_client_auth();
        
        let quinn_config = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(client_config)
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
        
        info!("QUIC 连接已建立: {}", self.id);
        Ok(())
    }
    
    async fn disconnect(&mut self) -> Result<()> {
        *self.state.write().await = ConnectionState::Disconnecting;
        
        // 停止接收任务
        self.stop_receive_task().await?;
        
        // 停止接收任务
        self.stop_receive_task().await?;
        
        // 关闭 QUIC 连接
        if let Some(conn) = &*self.connection.read().await {
            conn.close(0u32.into(), b"client disconnect");
        }
        
        // 清理连接
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
        
        info!("QUIC 连接已断开: {}", self.id);
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
            tx.send(message_data)
                .map_err(|e| FlareError::message_send_failed(format!("消息发送失败: {}", e)))?;
            
            debug!("QUIC 消息已提交发送: {} - 类型: {:?}", self.id, message.get_message_type());
            
            // 触发消息发送事件
            if let Some(handler) = &*self.event_handler.read().await {
                let handler = Arc::clone(handler);
                let id = self.id.clone();
                let msg_clone = message.clone();
                tokio::spawn(async move { 
                    handler.on_message_sent(&id, &msg_clone).await;
                    
                    // 如果是心跳消息，触发心跳发送事件
                    if msg_clone.is_heartbeat() {
                        handler.on_heartbeat_sent(&id).await;
                    }
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
            return Err(FlareError::connection_failed("超过最大重连次数"));
        }
        
        *self.state.write().await = ConnectionState::Reconnecting;
        
        // 等待重连延迟
        tokio::time::sleep(Duration::from_millis(self.config.reconnect_delay_ms)).await;
        
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
        
        // 停止接收任务
        self.stop_receive_task().await?;
        
        // 关闭 QUIC 连接
        if let Some(conn) = &*self.connection.read().await {
            conn.close(0u32.into(), b"server close");
        }
        
        // 清理连接
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
        
        info!("QUIC 服务端连接已关闭: {}", self.id);
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
            tx.send(message_data)
                .map_err(|e| FlareError::message_send_failed(format!("消息发送失败: {}", e)))?;
            
            debug!("QUIC 服务端消息已提交发送: {} - 类型: {:?}", self.id, message.get_message_type());
            
            // 触发消息发送事件
            if let Some(handler) = &*self.event_handler.read().await {
                let handler = Arc::clone(handler);
                let id = self.id.clone();
                let msg_clone = message.clone();
                tokio::spawn(async move { 
                    handler.on_message_sent(&id, &msg_clone).await;
                    
                    // 如果是心跳消息，触发心跳发送事件
                    if msg_clone.is_heartbeat() {
                        handler.on_heartbeat_sent(&id).await;
                    }
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
        Some(format!("QUIC Client - {}", self.id))
    }
    
    async fn get_connection_stats(&self) -> ConnectionStats {
        self.stats.read().await.clone()
    }
}
