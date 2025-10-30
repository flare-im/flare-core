//! 增强型客户端
//!
//! 支持用户协议选择和协议竞速功能

use std::sync::Arc;

use crate::common::connections::config::ConnectionConfig;
use crate::common::connections::enums::Transport;
use crate::common::connections::traits::{ClientConnection, ConnectionEvent};
use crate::common::connections::factory::ConnectionFactory;
use crate::client::connections::websocket::WebSocketClient;
use crate::client::connections::quic::QuicClient;
use crate::common::error::FlareError;
use crate::client::protocol_racer::ProtocolRacer;
use crate::client::reconnect::ReconnectManager;

/// 增强型客户端
pub struct EnhancedClient {
    /// 客户端连接
    connection: Option<Arc<dyn ClientConnection>>,
    /// 配置
    config: ConnectionConfig,
    /// 重连管理器
    reconnect_manager: Option<ReconnectManager>,
}

impl EnhancedClient {
    /// 创建新的增强型客户端
    pub fn new(config: ConnectionConfig) -> Self {
        Self {
            connection: None,
            config,
            reconnect_manager: None,
        }
    }
    
    /// 使用指定协议连接
    pub fn connect_with_protocol(&mut self, transport: Transport) -> Result<(), FlareError> {
        // 创建配置
        let config = self.config.clone();
        
        // 创建客户端连接
        let client: Arc<dyn ClientConnection> = match transport {
            Transport::WebSocket => {
                let ws_client = WebSocketClient::new(config)?;
                Arc::new(ws_client)
            },
            Transport::Quic => {
                let quic_client = QuicClient::new(config)?;
                Arc::new(quic_client)
            },
        };
        
        // 连接
        client.connect()?;
        
        // 保存连接
        self.connection = Some(client);
        
        tracing::info!("增强型客户端连接成功，使用协议: {:?}", transport);
        Ok(())
    }
    
    /// 使用协议竞速连接
    pub async fn connect_with_race(
        &mut self,
        addresses: Vec<String>,
        protocols: Vec<Transport>,
        handler: Option<Arc<dyn ConnectionEvent>>,
    ) -> Result<(), FlareError> {
        // 使用协议竞速器
        let client = ProtocolRacer::race(
            &self.config,
            &addresses,
            &protocols,
            handler,
        ).await?;
        
        // 保存连接
        self.connection = Some(client);
        
        tracing::info!("增强型客户端协议竞速连接成功");
        Ok(())
    }
    
    /// 启用自动重连
    pub fn enable_auto_reconnect(&mut self, max_retries: u32, retry_interval_ms: u64) {
        self.reconnect_manager = Some(ReconnectManager::new(max_retries, retry_interval_ms));
    }
    
    /// 断开连接
    pub fn disconnect(&mut self) -> Result<(), FlareError> {
        if let Some(client) = &self.connection {
            client.disconnect(None)?;
        }
        self.connection = None;
        tracing::info!("增强型客户端已断开连接");
        Ok(())
    }
    
    /// 发送消息
    pub fn send_message(&self, frame: crate::common::protocol::frame::Frame) -> Result<(), FlareError> {
        if let Some(client) = &self.connection {
            client.send_message(frame)
        } else {
            Err(FlareError::general_error("未建立连接".to_string()))
        }
    }
    
    /// 设置事件处理器
    pub fn set_event_handler(&self, handler: Arc<dyn ConnectionEvent>) -> Result<(), FlareError> {
        if let Some(client) = &self.connection {
            client.set_event_handler(handler);
            Ok(())
        } else {
            Err(FlareError::general_error("未建立连接".to_string()))
        }
    }
    
    /// 获取当前配置
    pub fn get_config(&self) -> &ConnectionConfig {
        &self.config
    }
    
    /// 获取连接状态
    pub fn is_connected(&self) -> bool {
        if let Some(client) = &self.connection {
            matches!(client.state(), crate::common::connections::enums::ConnectionState::Connected)
        } else {
            false
        }
    }
    
    /// 获取连接统计信息
    pub fn get_stats(&self) -> Option<crate::common::connections::types::ConnectionStats> {
        if let Some(client) = &self.connection {
            Some(client.stats())
        } else {
            None
        }
    }
}

impl Default for EnhancedClient {
    fn default() -> Self {
        Self::new(ConnectionConfig::default())
    }
}