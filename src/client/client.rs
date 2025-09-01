//! Flare 客户端
//! 
//! 提供基本的连接和消息传输功能

use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, debug};

use crate::common::{
    error::{Result, FlareError},
    connections::{
        factory::ConnectionFactory,
        manager::ConnectionManager,
        types::ConnectionConfig,
    },
    protocol::UnifiedProtocolMessage,
};

/// Flare 客户端
pub struct FlareClient {
    /// 连接配置
    config: ConnectionConfig,
    /// 连接管理器
    conn_manager: Arc<Mutex<ConnectionManager>>,
}

impl FlareClient {
    /// 创建新的客户端
    pub fn new(config: ConnectionConfig) -> Self {
        let factory = Arc::new(ConnectionFactory::new());
        let conn_manager = Arc::new(Mutex::new(ConnectionManager::new(factory)));
        
        Self {
            config,
            conn_manager,
        }
    }

    /// 连接到服务器
    pub async fn connect(&self) -> Result<()> {
        let mut conn_manager = self.conn_manager.lock().await;
        conn_manager.connect(self.config.clone()).await
    }

    /// 断开连接
    pub async fn disconnect(&self) -> Result<()> {
        let mut conn_manager = self.conn_manager.lock().await;
        conn_manager.disconnect().await
    }

    /// 检查连接状态
    pub async fn is_connected(&self) -> bool {
        let conn_manager = self.conn_manager.lock().await;
        conn_manager.is_connected().await
    }

    /// 发送消息
    pub async fn send_message(&self, message: UnifiedProtocolMessage) -> Result<()> {
        let mut conn_manager = self.conn_manager.lock().await;
        conn_manager.send_message(message).await
    }

    /// 接收消息
    pub async fn receive_message(&self) -> Result<Option<UnifiedProtocolMessage>> {
        let mut conn_manager = self.conn_manager.lock().await;
        conn_manager.receive_message().await
    }

    /// 尝试重连
    pub async fn try_reconnect(&self) -> Result<()> {
        let mut conn_manager = self.conn_manager.lock().await;
        conn_manager.try_reconnect(self.config.clone()).await
    }

    /// 获取连接状态
    pub async fn get_connection_state(&self) -> crate::common::connections::types::ConnectionState {
        let conn_manager = self.conn_manager.lock().await;
        conn_manager.get_connection_state().await
    }
}

/// 客户端构建器
pub struct ClientBuilder {
    config: ConnectionConfig,
}

impl ClientBuilder {
    /// 创建新的客户端构建器
    pub fn new() -> Self {
        let config = ConnectionConfig::default();
        Self { config }
    }

    /// 设置连接ID
    pub fn with_id(mut self, id: String) -> Self {
        self.config.id = id;
        self
    }

    /// 设置远程地址
    pub fn with_remote_addr(mut self, remote_addr: String) -> Self {
        self.config.remote_addr = remote_addr;
        self
    }

    /// 设置连接类型
    pub fn with_connection_type(mut self, connection_type: crate::common::connections::types::ConnectionType) -> Self {
        self.config.connection_type = connection_type;
        self
    }

    /// 设置平台
    pub fn with_platform(mut self, platform: crate::common::connections::types::ConnectionPlatform) -> Self {
        self.config.platform = platform;
        self
    }

    /// 构建客户端
    pub fn build(self) -> FlareClient {
        FlareClient::new(self.config)
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}
