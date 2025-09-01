//! Flare 服务端
//! 
//! 提供基本的连接接受和消息处理功能

use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::{Mutex, RwLock};
use tracing::{info, debug, warn};

use crate::common::{
    error::{Result, FlareError},
    connections::{
        traits::{ServerConnection, ConnectionFactory},
        types::{ConnectionConfig},
        factory::ConnectionFactory as FlareConnectionFactory,
    },
    protocol::UnifiedProtocolMessage,
};

/// Flare 服务端
pub struct FlareServer {
    /// 服务端配置
    config: ServerConfig,
    /// 连接工厂
    factory: Arc<dyn ConnectionFactory>,
    /// 活跃连接
    connections: Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn ServerConnection>>>>>>,
    /// 是否正在运行
    running: Arc<RwLock<bool>>,
}

/// 服务端配置
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// 监听地址
    pub listen_addr: String,
    /// 连接类型
    pub connection_type: crate::common::connections::types::ConnectionType,
    /// 最大连接数
    pub max_connections: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:8080".to_string(),
            connection_type: crate::common::connections::types::ConnectionType::WebSocket,
            max_connections: 1000,
        }
    }
}

impl FlareServer {
    /// 创建新的服务端
    pub fn new(config: ServerConfig) -> Self {
        let factory = Arc::new(FlareConnectionFactory::new());
        
        Self {
            config,
            factory,
            connections: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// 启动服务端
    pub async fn start(&mut self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            return Ok(());
        }
        
        *running = true;
        info!("启动服务端，监听地址: {}", self.config.listen_addr);
        
        // 这里应该启动实际的监听逻辑
        // 暂时只是标记为运行状态
        
        Ok(())
    }

    /// 停止服务器
    pub async fn stop(&self) -> Result<()> {
        info!("停止 Flare 服务器");
        
        // 关闭所有连接
        let mut connections = self.connections.write().await;
        for connection in connections.iter() {
            if let Ok(mut conn) = connection.try_write() {
                // 尝试关闭连接，根据连接类型调用相应的方法
                if let Err(e) = conn.close().await {
                    warn!("关闭连接失败: {}", e);
                }
            }
        }
        connections.clear();
        
        info!("Flare 服务器已停止");
        Ok(())
    }
    
    /// 创建 WebSocket 连接
    pub async fn create_websocket_connection(&self, id: String, addr: String) -> Result<Arc<RwLock<WebSocketConnection>>> {
        let config = ConnectionConfig::server(id.clone(), addr)
            .with_type(ConnectionType::WebSocket)
            .with_heartbeat(self.config.heartbeat_interval * 1000, 10000)
            .with_reconnect(5, 1000);
        
        let connection = WebSocketConnection::new(config);
        let connection = Arc::new(RwLock::new(connection));
        
        // 添加到连接列表
        {
            let mut connections = self.connections.write().await;
            connections.push(Arc::clone(&connection) as Arc<RwLock<dyn crate::common::connections::traits::Connection>>);
        }
        
        info!("创建 WebSocket 连接: {}", id);
        Ok(connection)
    }
    
    /// 创建 QUIC 连接
    pub async fn create_quic_connection(&self, id: String, addr: String) -> Result<Arc<RwLock<QuicConnection>>> {
        let config = ConnectionConfig::server(id.clone(), addr)
            .with_type(ConnectionType::Quic)
            .with_heartbeat(self.config.heartbeat_interval * 1000, 10000)
            .with_reconnect(5, 1000);
        
        let connection = QuicConnection::new(config);
        let connection = Arc::new(RwLock::new(connection));
        
        // 添加到连接列表
        {
            let mut connections = self.connections.write().await;
            connections.push(Arc::clone(&connection) as Arc<RwLock<dyn crate::common::connections::traits::Connection>>);
        }
        
        info!("创建 QUIC 连接: {}", id);
        Ok(connection)
    }
    
    /// 获取连接数量
    pub async fn get_connection_count(&self) -> usize {
        self.connections.read().await.len()
    }
    
    /// 广播消息
    pub async fn broadcast(&self, message: UnifiedProtocolMessage) -> Result<()> {
        let connections = self.connections.read().await;
        
        for connection in connections.iter() {
            if let Ok(mut conn) = connection.try_write() {
                // 尝试发送消息，根据连接类型调用相应的方法
                if let Err(e) = conn.send_message(message.clone()).await {
                    warn!("广播消息失败: {}", e);
                }
            }
        }
        
        info!("广播消息完成，目标连接数: {}", connections.len());
        Ok(())
    }

    /// 检查服务端是否正在运行
    pub async fn is_running(&self) -> bool {
        let running = self.running.read().await;
        *running
    }

    /// 接受新连接
    pub async fn accept_connection(&mut self, client_id: String) -> Result<()> {
        if !self.is_running().await {
            return Err(FlareError::connection_failed("服务端未启动"));
        }

        let current_connections = self.connections.read().await;
        if current_connections.len() >= self.config.max_connections {
            return Err(FlareError::connection_failed("连接数量已达上限"));
        }
        drop(current_connections);

        // 创建连接配置
        let config = ConnectionConfig {
            id: client_id.clone(),
            connection_type: self.config.connection_type,
            role: crate::common::connections::types::ConnectionRole::Server,
            platform: crate::common::connections::types::ConnectionPlatform::Server,
            remote_addr: "client".to_string(),
            local_addr: Some(self.config.listen_addr.clone()),
            timeout_ms: 30000,
            heartbeat_interval_ms: 30000,
            heartbeat_timeout_ms: 10000,
            max_missed_heartbeats: 3,
            auto_reconnect: true,
            max_reconnect_attempts: 5,
            reconnect_delay_ms: 1000,
            reconnect_backoff_factor: 2.0,
            max_reconnect_delay_ms: 60000,
            enable_tls: false,
            enable_compression: true,
            enable_0rtt: false,
            enable_connection_migration: false,
            enable_multipath: false,
            enable_flow_control: true,
            enable_congestion_control: true,
            buffer_size: 65536,
            max_message_size: 1048576,
            enable_message_fragmentation: true,
            fragment_size: 16384,
            enable_message_ack: true,
            message_ack_timeout_ms: 5000,
            enable_message_retransmission: true,
            max_retransmission_attempts: 3,
            enable_connection_pooling: false,
            connection_pool_size: 10,
            enable_load_balancing: false,
            load_balancing_strategy: crate::common::connections::types::LoadBalancingStrategy::RoundRobin,
            enable_monitoring: false,
            monitoring_interval_ms: 5000,
            enable_logging: true,
            log_level: crate::common::connections::types::LogLevel::Info,
            custom_config: std::collections::HashMap::new(),
        };

        // 创建服务端连接
        let mut connection = self.factory
            .create_server_connection(config)
            .await
            .map_err(|e| FlareError::connection_failed(&format!("创建连接失败: {}", e)))?;

        // 接受连接
        connection.accept().await
            .map_err(|e| FlareError::connection_failed(&format!("接受连接失败: {}", e)))?;

        // 保存连接
        {
            let mut connections = self.connections.write().await;
            connections.insert(client_id.clone(), Arc::new(Mutex::new(connection)));
        }

        info!("接受新连接: {}", client_id);
        Ok(())
    }

    /// 关闭连接
    pub async fn close_connection(&mut self, client_id: &str) -> Result<()> {
        let mut connections = self.connections.write().await;
        
        if let Some(connection) = connections.remove(client_id) {
            let mut conn_guard = connection.lock().await;
            conn_guard.close().await?;
            info!("连接已关闭: {}", client_id);
        }
        
        Ok(())
    }

    /// 关闭所有连接
    async fn close_all_connections(&mut self) {
        let mut connections = self.connections.write().await;
        
        for (client_id, connection) in connections.iter() {
            if let Ok(mut conn_guard) = connection.try_lock() {
                if let Err(e) = conn_guard.close().await {
                    warn!("关闭连接 {} 失败: {}", client_id, e);
                }
            }
        }
        
        connections.clear();
        info!("所有连接已关闭");
    }

    /// 发送消息到指定连接
    pub async fn send_message_to(&self, client_id: &str, message: UnifiedProtocolMessage) -> Result<()> {
        let connections = self.connections.read().await;
        
        if let Some(connection) = connections.get(client_id) {
            let mut conn_guard = connection.lock().await;
            conn_guard.send_message(message).await?;
            Ok(())
        } else {
            Err(FlareError::connection_failed("连接不存在"))
        }
    }

    /// 广播消息到所有连接
    pub async fn broadcast_message(&self, message: UnifiedProtocolMessage) -> Result<usize> {
        let connections = self.connections.read().await;
        let mut success_count = 0;
        let mut failed_count = 0;

        for (client_id, connection) in connections.iter() {
            let mut conn_guard = connection.lock().await;
            match conn_guard.send_message(message.clone()).await {
                Ok(_) => {
                    success_count += 1;
                    debug!("消息发送成功到连接: {}", client_id);
                }
                Err(e) => {
                    failed_count += 1;
                    warn!("消息发送失败到连接 {}: {}", client_id, e);
                }
            }
        }

        if failed_count > 0 {
            warn!("广播消息完成，成功: {}, 失败: {}", success_count, failed_count);
        } else {
            info!("消息广播完成，成功发送到 {} 个连接", success_count);
        }

        Ok(success_count)
    }

    /// 获取连接数量
    pub async fn get_connection_count(&self) -> usize {
        let connections = self.connections.read().await;
        connections.len()
    }

    /// 获取所有连接ID
    pub async fn get_connection_ids(&self) -> Vec<String> {
        let connections = self.connections.read().await;
        connections.keys().cloned().collect()
    }
}

/// 服务端构建器
pub struct ServerBuilder {
    config: ServerConfig,
}

impl ServerBuilder {
    /// 创建新的服务端构建器
    pub fn new() -> Self {
        let config = ServerConfig::default();
        Self { config }
    }

    /// 设置监听地址
    pub fn with_listen_addr(mut self, listen_addr: String) -> Self {
        self.config.listen_addr = listen_addr;
        self
    }

    /// 设置连接类型
    pub fn with_connection_type(mut self, connection_type: crate::common::connections::types::ConnectionType) -> Self {
        self.config.connection_type = connection_type;
        self
    }

    /// 设置最大连接数
    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.config.max_connections = max_connections;
        self
    }

    /// 构建服务端
    pub fn build(self) -> FlareServer {
        FlareServer::new(self.config)
    }
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}
