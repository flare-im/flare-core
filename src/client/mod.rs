//! 客户端模块 - 长连接可靠性和协议竞速

pub mod config;
pub mod connection_manager;
pub mod protocol_racing;
pub mod quic_connector;
pub mod websocket_connector;
pub mod client;

use std::sync::Arc;
use tokio::sync::Mutex;
use crate::common::{FlareError, ConnectionState, UnifiedProtocolMessage};
use crate::client::connection_manager::ConnectionManager;
use crate::client::config::ClientConfig;

// 重新导出简化客户端
pub use client::{FlareClient, ClientBuilder};

/// Flare 客户端
/// 
/// 专注于两个核心功能：
/// 1. 长连接可靠性：自动重连、心跳、ACK机制
/// 2. 协议竞速：QUIC vs WebSocket 智能选择
pub struct FlareClientLegacy {
    config: ClientConfig,
    conn_manager: Arc<Mutex<ConnectionManager>>,
}

impl FlareClientLegacy {
    /// 创建新的客户端
    pub fn new(config: ClientConfig) -> Self {
        let conn_manager = Arc::new(Mutex::new(ConnectionManager::new(config.clone())));
        Self {
            config,
            conn_manager,
        }
    }

    /// 连接到服务器
    pub async fn connect(&self) -> Result<(), FlareError> {
        let mut conn_manager = self.conn_manager.lock().await;
        conn_manager.force_reconnect().await
    }

    /// 断开连接
    pub async fn disconnect(&self) -> Result<(), FlareError> {
        let mut conn_manager = self.conn_manager.lock().await;
        conn_manager.disconnect().await
    }

    /// 检查连接状态
    pub async fn is_connected(&self) -> bool {
        let conn_manager = self.conn_manager.lock().await;
        conn_manager.get_connection_state().await == ConnectionState::Connected
    }

    /// 发送消息
    pub async fn send_message(&self, message: UnifiedProtocolMessage) -> Result<(), FlareError> {
        let conn_manager = self.conn_manager.lock().await;
        conn_manager.send_message(message).await
    }

    /// 接收消息
    pub async fn receive_message(&self) -> Result<Option<UnifiedProtocolMessage>, FlareError> {
        let conn_manager = self.conn_manager.lock().await;
        conn_manager.receive_message().await
    }
}

 
 