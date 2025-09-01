//! Flare QUIC服务器模块
//!
//! 提供QUIC协议支持的服务端实现

use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::{RwLock, Mutex};
use tracing::{info, warn, debug, error};
use quinn::{Endpoint, ServerConfig as QuinnServerConfig, Connection as QuinnConnection, Incoming};
use rustls::{ServerConfig as RustlsServerConfig};
use quinn::crypto::rustls::QuicServerConfig as QuinnRustlsServerCrypto;
use rustls_pemfile::{certs, pkcs8_private_keys};

use crate::common::{
    error::{Result, FlareError},
    protocol::UnifiedProtocolMessage,
    connections::{
        ConnectionState, ConnectionConfig, ConnectionType, ConnectionRole,
        QuicConnection, ServerConnection,
    },
};

use super::config::{ServerConfig, QuicServerConfig};

/// QUIC连接包装器
pub struct QuicConnection {
    /// 连接ID
    id: String,
    /// QUIC连接
    connection: QuinnConnection,
    /// 连接状态
    state: ConnectionState,
    /// 连接指标
    metrics: ConnectionMetrics,
    /// 最后活动时间
    last_activity: std::time::Instant,
}

impl QuicConnection {
    /// 创建新的QUIC连接
    pub fn new(id: String, connection: QuinnConnection) -> Self {
        Self {
            id,
            connection,
            state: ConnectionState::Connected,
            metrics: ConnectionMetrics::default(),
            last_activity: std::time::Instant::now(),
        }
    }
    
    /// 获取连接ID
    pub fn get_id(&self) -> &str {
        &self.id
    }
    
    /// 获取连接状态
    pub fn get_state(&self) -> ConnectionState {
        self.state
    }
    
    /// 更新连接状态
    pub fn set_state(&mut self, state: ConnectionState) {
        self.state = state;
    }
    
    /// 更新最后活动时间
    pub fn update_activity(&mut self) {
        self.last_activity = std::time::Instant::now();
    }
    
    /// 检查连接是否活跃
    pub fn is_active(&self, timeout: std::time::Duration) -> bool {
        self.last_activity.elapsed() < timeout
    }
}

/// QUIC服务器
pub struct QuicServer {
    config: QuicServerConfig,
    running: Arc<RwLock<bool>>,
    server_task: Option<tokio::task::JoinHandle<()>>,
    connections: Arc<Mutex<HashMap<String, QuicConnection>>>,
    connection_counter: Arc<Mutex<u64>>,
}

impl QuicServer {
    pub fn new(config: ServerConfig) -> Self {
        let quic_config = config.protocol.quic.clone();
        Self {
            config: quic_config,
            running: Arc::new(RwLock::new(false)),
            server_task: None,
            connections: Arc::new(Mutex::new(HashMap::new())),
            connection_counter: Arc::new(Mutex::new(0)),
        }
    }
    
    /// 启动QUIC服务器
    pub async fn start(&mut self) -> Result<()> {
        info!("启动QUIC服务器: {}", self.config.bind_addr);
        
        // 解析绑定地址
        let bind_addr = self.config.bind_addr.parse::<std::net::SocketAddr>()
            .map_err(|e| FlareError::InvalidConfiguration(format!("无效的绑定地址 {}: {}", self.config.bind_addr, e)))?;
        
        // 创建QUIC服务器配置
        let server_config = self.create_server_config()?;
        
        // 创建QUIC端点
        let endpoint = Endpoint::server(server_config, bind_addr)
            .map_err(|e| FlareError::NetworkError(format!("无法创建QUIC端点: {}", e)))?;
        
        info!("QUIC服务器已绑定到: {}", self.config.bind_addr);
        
        // 启动服务器任务
        let running = self.running.clone();
        let connections = Arc::clone(&self.connections);
        let connection_counter = Arc::clone(&self.connection_counter);
        let config = self.config.clone();
        
        let server_task = tokio::spawn(async move {
            Self::server_loop(
                endpoint,
                running,
                connections,
                connection_counter,
                config,
            ).await;
        });
        
        self.server_task = Some(server_task);
        {
            let mut running = self.running.write().await;
            *running = true;
        }
        
        info!("QUIC服务器启动成功");
        Ok(())
    }
    
    /// 服务器主循环
    async fn server_loop(
        endpoint: Endpoint,
        running: Arc<RwLock<bool>>,
        connections: Arc<Mutex<HashMap<String, QuicConnection>>>,
        connection_counter: Arc<Mutex<u64>>,
        config: QuicServerConfig,
    ) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
        
        loop {
            // 检查是否应该停止
            if !*running.read().await {
                break;
            }
            
            // 等待新连接或超时
            tokio::select! {
                incoming = endpoint.accept() => {
                    if let Some(incoming) = incoming {
                        let connections = Arc::clone(&connections);
                        let connection_counter = Arc::clone(&connection_counter);
                        let config = config.clone();
                        
                        tokio::spawn(async move {
                            if let Err(e) = Self::handle_incoming(
                                incoming,
                                connections,
                                connection_counter,
                                config,
                            ).await {
                                error!("处理QUIC连接失败: {}", e);
                            }
                        });
                    }
                }
                _ = interval.tick() => {
                    // 定期清理不活跃的连接
                    Self::cleanup_inactive_connections(&connections, &config).await;
                }
            }
        }
        
        info!("QUIC服务器主循环已停止");
    }
    
    /// 停止QUIC服务器
    pub async fn stop(&mut self) -> Result<()> {
        {
            let mut running = self.running.write().await;
            *running = false;
        }
        
        if let Some(task) = self.server_task.take() {
            task.abort();
        }
        
        // 断开所有连接
        let mut connections = self.connections.lock().await;
        for (_, conn) in connections.iter_mut() {
            conn.set_state(ConnectionState::Disconnected);
        }
        connections.clear();
        
        info!("QUIC服务器已停止");
        Ok(())
    }
    
    /// 检查服务器是否正在运行
    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }
    
    /// 获取当前连接数
    pub async fn get_connection_count(&self) -> usize {
        let connections = self.connections.lock().await;
        connections.len()
    }
    
    /// 处理QUIC连接
    async fn handle_incoming(
        incoming: Incoming,
        connections: Arc<Mutex<HashMap<String, QuicConnection>>>,
        connection_counter: Arc<Mutex<u64>>,
        config: QuicServerConfig,
    ) -> Result<()> {
        info!("收到QUIC连接请求");
        
        // 等待连接建立
        let connection = incoming.await
            .map_err(|e| FlareError::ConnectionFailed(format!("QUIC连接建立失败: {}", e)))?;
        
        // 生成连接ID
        let connection_id = {
            let mut counter = connection_counter.lock().await;
            *counter += 1;
            format!("quic_{}", *counter)
        };
        
        info!("QUIC连接已建立: {}", connection_id);
        
        // 创建连接包装器
        let quic_conn = QuicConnection::new(connection_id.clone(), connection);
        
        // 添加到连接池
        {
            let mut conns = connections.lock().await;
            if conns.len() >= config.max_connections {
                warn!("连接数已达上限，拒绝新连接: {}", connection_id);
                return Err(FlareError::ConnectionFailed("连接数已达上限".to_string()));
            }
            conns.insert(connection_id.clone(), quic_conn);
        }
        
        // 启动连接处理任务
        let connections = Arc::clone(&connections);
        let connection_id_clone = connection_id.clone();
        
        tokio::spawn(async move {
            if let Err(e) = Self::handle_connection(
                connection_id_clone.clone(),
                connections,
            ).await {
                error!("处理QUIC连接 {} 失败: {}", connection_id_clone, e);
            }
        });
        
        Ok(())
    }
    
    /// 处理单个连接
    async fn handle_connection(
        connection_id: String,
        connections: Arc<Mutex<HashMap<String, QuicConnection>>>,
    ) -> Result<()> {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        
        loop {
            interval.tick().await;
            
            // 检查连接是否还存在
            let connection_exists = {
                let conns = connections.lock().await;
                conns.contains_key(&connection_id)
            };
            
            if !connection_exists {
                debug!("QUIC连接 {} 已断开", connection_id);
                break;
            }
            
            // 更新连接活动时间
            {
                let mut conns = connections.lock().await;
                if let Some(conn) = conns.get_mut(&connection_id) {
                    conn.update_activity();
                }
            }
            
            // 这里可以添加心跳检查、消息处理等逻辑
        }
        
        Ok(())
    }
    
    /// 清理不活跃的连接
    async fn cleanup_inactive_connections(
        connections: &Arc<Mutex<HashMap<String, QuicConnection>>>,
        _config: &QuicServerConfig,
    ) {
        let mut conns = connections.lock().await;
        let timeout = std::time::Duration::from_secs(300); // 5分钟超时
        
        let expired_keys: Vec<String> = conns
            .iter()
            .filter(|(_, conn)| !conn.is_active(timeout))
            .map(|(id, _)| id.clone())
            .collect();
        
        for key in &expired_keys {
            if let Some(mut conn) = conns.remove(key) {
                info!("清理不活跃的QUIC连接: {}", conn.get_id());
                conn.set_state(ConnectionState::Disconnected);
            }
        }
        
        if !expired_keys.is_empty() {
            debug!("清理了 {} 个不活跃的QUIC连接", expired_keys.len());
        }
    }
    
    /// 创建QUIC服务器配置
    fn create_server_config(&self) -> Result<QuinnServerConfig> {
        // 读取TLS证书和私钥
        let cert_file = std::fs::File::open(&self.config.cert_path)
            .map_err(|e| FlareError::InvalidConfiguration(format!("无法读取证书文件 {}: {}", self.config.cert_path, e)))?;
        
        let key_file = std::fs::File::open(&self.config.key_path)
            .map_err(|e| FlareError::InvalidConfiguration(format!("无法读取私钥文件 {}: {}", self.config.key_path, e)))?;
        
        // 解析证书
        let certs: Vec<_> = certs(&mut std::io::BufReader::new(cert_file))
            .filter_map(|r| r.ok())
            .collect();
        
        if certs.is_empty() {
            return Err(FlareError::InvalidConfiguration("证书文件为空".to_string()));
        }
        
        // 解析私钥
        let keys: Vec<_> = pkcs8_private_keys(&mut std::io::BufReader::new(key_file))
            .filter_map(|r| r.ok())
            .collect();
        
        if keys.is_empty() {
            return Err(FlareError::InvalidConfiguration("私钥文件为空".to_string()));
        }
        
        // 构建 rustls ServerConfig（rustls 0.23）
        let key = keys.into_iter().next().ok_or_else(|| FlareError::InvalidConfiguration("未找到有效私钥".to_string()))?;
        let mut rustls_config = RustlsServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key.into())
            .map_err(|e| FlareError::InvalidConfiguration(format!("装配证书失败: {}", e)))?;
        // 设置ALPN协议
        rustls_config.alpn_protocols = self.config.alpn_protocols.clone();
        // 转换为 Quinn 的加密配置
        let server_crypto = Arc::new(QuinnRustlsServerCrypto::try_from(rustls_config)
            .map_err(|e| FlareError::InvalidConfiguration(format!("构建QUIC Server TLS失败: {}", e)))?);
        let server_config = QuinnServerConfig::with_crypto(server_crypto);
        
        Ok(server_config)
    }
    
    /// 发送消息到指定连接
    pub async fn send_message(&self, connection_id: &str, message: UnifiedProtocolMessage) -> Result<()> {
        let mut conns = self.connections.lock().await;
        
        if let Some(conn) = conns.get_mut(connection_id) {
            // 序列化消息
            let _message_bytes = bincode::serialize(&message)
                .map_err(|e| FlareError::serialization_error(e.to_string()))?;
            
            // 发送消息（这里需要实现具体的发送逻辑）
            debug!("向QUIC连接 {} 发送消息: {:?}", connection_id, message.frame.message_type);
            
            // 更新连接活动时间
            conn.update_activity();
            
            Ok(())
        } else {
            Err(FlareError::ConnectionFailed(format!("连接 {} 不存在", connection_id)))
        }
    }
    
    /// 广播消息到所有连接
    pub async fn broadcast_message(&self, message: UnifiedProtocolMessage) -> Result<()> {
        let mut conns = self.connections.lock().await;
        let mut success_count = 0;
        let mut error_count = 0;
        
        for (connection_id, _conn) in conns.iter_mut() {
            match self.send_message(connection_id, message.clone()).await {
                Ok(_) => success_count += 1,
                Err(e) => {
                    error!("向QUIC连接 {} 广播消息失败: {}", connection_id, e);
                    error_count += 1;
                }
            }
        }
        
        info!("QUIC广播完成: 成功 {} 个连接, 失败 {} 个连接", success_count, error_count);
        Ok(())
    }
} 