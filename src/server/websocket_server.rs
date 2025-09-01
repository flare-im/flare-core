//! WebSocket 服务器实现
//! 
//! 提供 WebSocket 服务端功能，接受客户端连接

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, debug, warn, error};

use crate::common::{
    error::{Result, FlareError},
    protocol::UnifiedProtocolMessage,
    connections::{
        ConnectionState, ConnectionConfig, ConnectionType, ConnectionRole,
        WebSocketConnection, ServerConnection,
    },
};

/// WebSocket 服务器配置
#[derive(Debug, Clone)]
pub struct WebSocketServerConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
    pub enable_tls: bool,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

impl Default for WebSocketServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
            max_connections: 1000,
            enable_tls: false,
            cert_path: None,
            key_path: None,
        }
    }
}

/// WebSocket 服务器
pub struct WebSocketServer {
    config: WebSocketServerConfig,
    connections: Arc<RwLock<Vec<Arc<RwLock<WebSocketConnection>>>>>,
}

impl WebSocketServer {
    /// 创建新的 WebSocket 服务器
    pub fn new(config: WebSocketServerConfig) -> Self {
        Self {
            config,
            connections: Arc::new(RwLock::new(Vec::new())),
        }
    }
    
    /// 启动服务器
    pub async fn start(&self) -> Result<()> {
        info!("启动 WebSocket 服务器: {}:{}", self.config.host, self.config.port);
        
        // 模拟服务器启动
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        info!("WebSocket 服务器启动成功");
        Ok(())
    }
    
    /// 停止服务器
    pub async fn stop(&self) -> Result<()> {
        info!("停止 WebSocket 服务器");
        
        // 关闭所有连接
        let mut connections = self.connections.write().await;
        for connection in connections.iter() {
            if let Ok(mut conn) = connection.try_write() {
                if let Err(e) = conn.close().await {
                    warn!("关闭连接失败: {}", e);
                }
            }
        }
        connections.clear();
        
        info!("WebSocket 服务器已停止");
        Ok(())
    }
    
    /// 创建新连接
    pub async fn create_connection(&self, id: String) -> Result<Arc<RwLock<WebSocketConnection>>> {
        let config = ConnectionConfig::server(
            id.clone(),
            format!("{}:{}", self.config.host, self.config.port),
        )
        .with_type(ConnectionType::WebSocket)
        .with_heartbeat(30000, 10000)
        .with_reconnect(5, 1000);
        
        let connection = WebSocketConnection::new(config);
        let connection = Arc::new(RwLock::new(connection));
        
        // 添加到连接列表
        {
            let mut connections = self.connections.write().await;
            connections.push(Arc::clone(&connection));
        }
        
        info!("创建 WebSocket 连接: {}", id);
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
                if let Err(e) = conn.send_message(message.clone()).await {
                    warn!("广播消息失败: {}", e);
                }
            }
        }
        
        info!("广播消息完成，目标连接数: {}", connections.len());
        Ok(())
    }
} 