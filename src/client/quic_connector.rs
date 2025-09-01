//! QUIC客户端连接器
//!
//! 提供QUIC协议的客户端连接实现

use std::sync::Arc;
use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::{info, debug, error};

use crate::common::{
    connections::{Connection, ConnectionConfig, ConnectionState, ConnectionMetrics, ConnectionType, ConnectionRole, ConnectionSummary},
    error::{Result, FlareError},
    protocol::{UnifiedProtocolMessage, Frame, MessageType, Reliability},
};

use rustls::{ClientConfig as RustlsClientConfig, RootCertStore};
use rustls_pemfile::certs;
use quinn::{Endpoint, Connection as QuinnConnection};
use quinn::crypto::rustls::QuicClientConfig;
use rustls_native_certs;

use super::config::ClientConfig;

const MAX_MESSAGE_SIZE: usize = 1024 * 1024; // 1MB

/// QUIC客户端连接器
pub struct QuicConnector {
    config: ClientConfig,
    endpoint: Option<Endpoint>,
    connection: Arc<Mutex<Option<QuinnConnection>>>,
    connection_state: Arc<tokio::sync::RwLock<ConnectionState>>,
    connection_metrics: Arc<tokio::sync::RwLock<ConnectionMetrics>>,
    last_heartbeat: Arc<Mutex<Instant>>,
    heartbeat_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl QuicConnector {
    /// 创建新的QUIC连接器
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            endpoint: None,
            connection: Arc::new(Mutex::new(None)),
            connection_state: Arc::new(tokio::sync::RwLock::new(ConnectionState::Disconnected)),
            connection_metrics: Arc::new(tokio::sync::RwLock::new(ConnectionMetrics::default())),
            last_heartbeat: Arc::new(Mutex::new(Instant::now())),
            heartbeat_task: Arc::new(Mutex::new(None)),
        }
    }
    
    /// 连接到QUIC服务器
    pub async fn connect(&mut self) -> Result<()> {
        info!("开始连接QUIC服务器: {}", self.config.connection.remote_addr);
        
        // 更新连接状态
        {
            let mut state = self.connection_state.write().await;
            *state = ConnectionState::Connecting;
        }
        
        // 创建客户端端点
        let endpoint = self.create_client_endpoint()?;
        self.endpoint = Some(endpoint);
        
        // 建立连接
        let connection = self.establish_connection().await?;
        
        // 保存连接
        {
            let mut conn = self.connection.lock().await;
            *conn = Some(connection);
        }
        
        // 更新连接状态
        {
            let mut state = self.connection_state.write().await;
            *state = ConnectionState::Connected;
        }
        
        // 启动心跳任务
        self.start_heartbeat_task().await;
        
        info!("QUIC连接建立成功");
        Ok(())
    }
    
    /// 断开连接
    pub async fn disconnect(&mut self) -> Result<()> {
        info!("断开QUIC连接");
        
        // 停止心跳任务
        if let Some(task) = self.heartbeat_task.lock().await.take() {
            task.abort();
        }
        
        // 断开连接
        if let Some(conn) = self.connection.lock().await.take() {
            conn.close(0u32.into(), b"client_disconnect");
        }
        
        // 更新连接状态
        {
            let mut state = self.connection_state.write().await;
            *state = ConnectionState::Disconnected;
        }
        
        info!("QUIC连接已断开");
        Ok(())
    }
    
    /// 发送消息
    pub async fn send_message(&self, message: UnifiedProtocolMessage) -> Result<()> {
        let conn = self.connection.lock().await;
        
        if let Some(connection) = conn.as_ref() {
            // 序列化消息
            let message_bytes = bincode::serialize(&message)
                .map_err(|e| FlareError::serialization_error(e.to_string()))?;
            
            // 发送消息
            let mut send = connection.open_uni().await
                .map_err(|e| FlareError::message_send_failed(format!("发送消息失败: {}", e)))?;
            
            send.write_all(&message_bytes).await
                .map_err(|e| FlareError::message_send_failed(format!("写入消息失败: {}", e)))?;
            
            send.finish()
                .map_err(|e| FlareError::message_send_failed(format!("完成发送失败: {}", e)))?;
            
            // 更新连接指标
            self.update_connection_metrics().await;
            
            debug!("QUIC消息发送成功: {:?}", message.frame.message_type);
            Ok(())
        } else {
            Err(FlareError::ConnectionFailed("没有可用的连接".to_string()))
        }
    }
    
    /// 接收消息
    pub async fn receive_message(&self) -> Result<Option<UnifiedProtocolMessage>> {
        let conn = self.connection.lock().await;
        
        if let Some(connection) = conn.as_ref() {
            // 等待接收流
            let (_, mut recv) = connection.accept_bi().await
                .map_err(|e| FlareError::ConnectionFailed(format!("无法接受双向流: {}", e)))?;
            
            // 读取消息
            let message_bytes = recv.read_to_end(MAX_MESSAGE_SIZE).await
                .map_err(|e| FlareError::DecodeError(format!("读取消息失败: {}", e)))?;
            
            if message_bytes.is_empty() {
                return Ok(None);
            }
            
            // 反序列化消息
            let frame = Frame::from_bytes(&message_bytes)
                .map_err(|e| FlareError::DecodeError(format!("反序列化消息失败: {}", e)))?;
            
            let message = UnifiedProtocolMessage::new(frame, None, 0);
            
            // 更新连接指标
            self.update_connection_metrics().await;
            
            debug!("QUIC消息接收成功: {:?}", message.frame.message_type);
            Ok(Some(message))
        } else {
            Err(FlareError::ConnectionFailed("没有可用的连接".to_string()))
        }
    }
    
    /// 检查连接是否活跃
    pub async fn is_active(&self) -> bool {
        let state = self.connection_state.read().await;
        *state == ConnectionState::Connected
    }
    
    /// 获取连接状态
    pub async fn get_connection_state(&self) -> ConnectionState {
        let state = self.connection_state.read().await;
        *state
    }
    
    /// 获取连接质量指标
    pub async fn get_connection_metrics(&self) -> ConnectionMetrics {
        let metrics = self.connection_metrics.read().await;
        metrics.clone()
    }
    
    /// 创建客户端端点
    fn create_client_endpoint(&self) -> Result<Endpoint> {
        // 加载系统证书
        let mut root_store = RootCertStore::empty();
        let sys = rustls_native_certs::load_native_certs();
        root_store.add_parsable_certificates(sys.certs);

        // 加载自定义CA证书（如果提供）
        if let Some(ca_cert_path) = &self.config.connection.custom_ca_cert {
            let cert_file = std::fs::File::open(ca_cert_path)
                .map_err(|e| FlareError::InvalidConfiguration(format!("无法打开CA证书文件: {}", e)))?;
            
            let certs_vec = certs(&mut std::io::BufReader::new(cert_file))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| FlareError::InvalidConfiguration(format!("解析自定义CA证书失败: {}", e)))?;
            root_store.add_parsable_certificates(certs_vec);
        }
        
        let mut client_config = RustlsClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        
        // 设置ALPN协议
        client_config.alpn_protocols = vec![b"flare-core".to_vec()];
        
        // 创建QUIC客户端配置
        let client_crypto = Arc::new(QuicClientConfig::try_from(Arc::new(client_config))
            .map_err(|e| FlareError::InvalidConfiguration(format!("构建QUIC TLS失败: {}", e)))?);
        let quic_config = quinn::ClientConfig::new(client_crypto);
        
        let mut endpoint = Endpoint::client("127.0.0.1:0".parse().unwrap())
            .map_err(|e| FlareError::ConnectionFailed(format!("创建客户端端点失败: {}", e)))?;
        // 设置默认客户端配置
        endpoint.set_default_client_config(quic_config);

        Ok(endpoint)
    }
    
    /// 建立连接
    async fn establish_connection(&self) -> Result<QuinnConnection> {
        let endpoint = self.endpoint.as_ref()
            .ok_or_else(|| FlareError::InvalidConfiguration("端点未初始化".to_string()))?;
        
        let remote_addr = self.config.connection.remote_addr.parse::<std::net::SocketAddr>()
            .map_err(|e| FlareError::InvalidConfiguration(format!("无效的远程地址 {}: {}", self.config.connection.remote_addr, e)))?;
        
        // 建立连接
        let connection = endpoint.connect(remote_addr, "localhost")
            .map_err(|e| FlareError::ConnectionFailed(format!("连接失败: {}", e)))?
            .await
            .map_err(|e| FlareError::ConnectionFailed(format!("等待连接失败: {}", e)))?;
        
        info!("QUIC连接已建立");
        Ok(connection)
    }
    
    /// 启动心跳任务
    async fn start_heartbeat_task(&self) {
        let heartbeat_interval = Duration::from_millis(self.config.connection.heartbeat_interval_ms as u64);
        let last_heartbeat = Arc::clone(&self.last_heartbeat);
        let connection = Arc::clone(&self.connection);
        let connection_state = Arc::clone(&self.connection_state);
        
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_interval);
            
            loop {
                interval.tick().await;
                
                // 检查连接状态
                let state = *connection_state.read().await;
                if state != ConnectionState::Connected {
                    break;
                }
                
                // 发送心跳（基于连接的辅助函数）
                if let Err(e) = Self::send_heartbeat_on(&connection).await {
                    error!("发送心跳失败: {}", e);
                    break;
                }
                
                // 更新最后心跳时间
                {
                    let mut last = last_heartbeat.lock().await;
                    *last = Instant::now();
                }
            }
        });
        
        {
            let mut heartbeat_task = self.heartbeat_task.lock().await;
            *heartbeat_task = Some(task);
        }
    }
    
    /// 发送心跳
    async fn send_heartbeat(&self) -> Result<()> {
        Self::send_heartbeat_on(&self.connection).await
    }

    /// 基于连接对象发送心跳（用于任务中）
    async fn send_heartbeat_on(connection: &Arc<Mutex<Option<QuinnConnection>>>) -> Result<()> {
        let heartbeat_frame = Frame::heartbeat();
        let heartbeat_message = UnifiedProtocolMessage::new(heartbeat_frame, None, 0);

        // 取出连接
        let guard = connection.lock().await;
        let conn = guard.as_ref().ok_or_else(|| FlareError::connection_failed("QUIC连接不存在"))?;

        // 获取发送流
        let mut send = conn
            .open_uni()
            .await
            .map_err(|e| FlareError::message_send_failed(format!("打开QUIC流失败: {}", e)))?;

        // 序列化并发送心跳消息
        let data = bincode::serialize(&heartbeat_message)
            .map_err(|e| FlareError::message_send_failed(format!("序列化心跳失败: {}", e)))?;

        send.write_all(&data)
            .await
            .map_err(|e| FlareError::message_send_failed(format!("发送心跳失败: {}", e)))?;

        // 完成流
        send.finish()
            .map_err(|e| FlareError::message_send_failed(format!("完成心跳发送失败: {}", e)))?;

        Ok(())
    }
    
    /// 更新连接指标
    async fn update_connection_metrics(&self) {
        // 更新连接指标
        let mut metrics = self.connection_metrics.write().await;
        
        if let Some(connection) = self.connection.lock().await.as_ref() {
            let stats = connection.stats();
            
            // 计算丢包率（使用可用的统计信息）
            let packet_loss_percent = if stats.path.lost_packets > 0 {
                (stats.path.lost_packets as f32 / (stats.path.sent_packets + stats.path.lost_packets) as f32) * 100.0
            } else {
                0.0
            };
            
            // 计算带宽（使用可用的统计信息）
            let bandwidth_bps = stats.path.sent_packets * 8; // 简化计算
            
            metrics.packet_loss_percent = packet_loss_percent;
            metrics.bandwidth_bps = bandwidth_bps;
            metrics.last_updated = chrono::Utc::now().timestamp_millis() as u64;
        }
    }
}

impl Drop for QuicConnector {
    fn drop(&mut self) {
        // 确保在析构时断开连接
        if let Ok(mut guard) = self.heartbeat_task.try_lock() {
            if let Some(task) = guard.take() {
                task.abort();
            }
        }
    }
}

#[async_trait::async_trait]
impl Connection for QuicConnector {
    fn get_id(&self) -> &str {
        &self.config.server_url
    }
    
    fn get_connection_type(&self) -> ConnectionType {
        ConnectionType::Quic
    }
    
    fn get_role(&self) -> ConnectionRole {
        ConnectionRole::Client
    }
    
    fn get_state(&self) -> ConnectionState {
        let state = self.connection_state.blocking_read();
        *state
    }
    
    fn get_config(&self) -> &crate::common::connections::types::ConnectionConfig {
        // 这里需要转换配置类型，暂时返回一个默认值
        static DEFAULT_CONFIG: LazyLock<crate::common::connections::types::ConnectionConfig> = LazyLock::new(|| crate::common::connections::types::ConnectionConfig {
            id: String::new(),
            connection_type: crate::common::connections::types::ConnectionType::Quic,
            role: crate::common::connections::types::ConnectionRole::Client,
            platform: crate::common::connections::types::ConnectionPlatform::Desktop,
            remote_addr: String::new(),
            local_addr: None,
            timeout_ms: 5000,
            heartbeat_interval_ms: 15000,
            heartbeat_timeout_ms: 5000,
            max_missed_heartbeats: 2,
            auto_reconnect: true,
            max_reconnect_attempts: 3,
            reconnect_delay_ms: 500,
            enable_tls: true,
            enable_compression: true,
            enable_0rtt: true,
            enable_connection_migration: true,
            enable_multipath: true,
            custom_config: HashMap::new(),
        });
        &DEFAULT_CONFIG
    }
    
    async fn get_metrics(&self) -> ConnectionMetrics {
        self.connection_metrics.read().await.clone()
    }
    
    async fn get_stats(&self) -> crate::common::connections::types::ConnectionStats {
        crate::common::connections::types::ConnectionStats::default() // TODO: 实现实际的统计信息
    }
    
    async fn send_message(&self, message: UnifiedProtocolMessage) -> Result<()> {
        QuicConnector::send_message(self, message).await
    }
    
    async fn receive_message(&self) -> Result<Option<UnifiedProtocolMessage>> {
        QuicConnector::receive_message(self).await
    }
    
    async fn send_raw(&self, _data: Vec<u8>) -> Result<()> {
        // TODO: 实现原始数据发送
        Err(FlareError::ProtocolError("原始数据发送尚未实现".to_string()))
    }
    
    async fn receive_raw(&self) -> Result<Option<Vec<u8>>> {
        // TODO: 实现原始数据接收
        Err(FlareError::ProtocolError("原始数据接收尚未实现".to_string()))
    }
    
    async fn connect(&mut self) -> Result<()> {
        self.connect().await
    }
    
    async fn disconnect(&mut self) -> Result<()> {
        self.disconnect().await
    }
    
    async fn is_active(&self) -> bool {
        QuicConnector::is_active(self).await
    }
    
    async fn send_heartbeat(&self) -> Result<()> {
        QuicConnector::send_heartbeat(self).await
    }
    
    async fn handle_heartbeat_response(&self) -> Result<()> {
        // TODO: 实现心跳响应处理
        Ok(())
    }
    
    async fn update_metrics(&mut self) {
        // TODO: 实现指标更新
    }
    
    fn get_summary(&self) -> ConnectionSummary {
        let state = self.get_state();
        let metrics = self.connection_metrics.blocking_read();
        
        ConnectionSummary {
            id: self.config.server_url.clone(),
            connection_type: ConnectionType::Quic,
            role: ConnectionRole::Client,
            state,
            remote_addr: self.config.server_url.clone(),
            local_addr: None,
            is_active: state == ConnectionState::Connected,
            latency_ms: metrics.latency_ms,
            stability_score: metrics.stability_score,
            last_activity: metrics.last_updated,
        }
    }
    
    fn clone_box(&self) -> Box<dyn Connection> {
        Box::new(QuicConnector {
            config: self.config.clone(),
            endpoint: self.endpoint.clone(),
            connection: Arc::clone(&self.connection),
            connection_state: Arc::clone(&self.connection_state),
            connection_metrics: Arc::clone(&self.connection_metrics),
            last_heartbeat: Arc::clone(&self.last_heartbeat),
            heartbeat_task: Arc::clone(&self.heartbeat_task),
        })
    }
}
