//! FastClient - 高性能客户端实现

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::common::connections::config::ConnectionConfig;
use crate::common::connections::enums::Transport;
use crate::common::connections::traits::{ClientConnection, ConnectionEvent};
use crate::common::connections::factory::ConnectionFactory;
use crate::common::error::FlareError;
use crate::client::protocol_racer::ProtocolRacer;

/// FastClient - 高性能客户端实现
pub struct FastClient {
    /// 客户端连接
    connection: Arc<RwLock<Option<Arc<dyn ClientConnection>>>>,
    /// 配置
    config: ConnectionConfig,
    /// 是否正在运行
    is_running: Arc<RwLock<bool>>,
}

impl FastClient {
    /// 创建新的FastClient实例
    pub fn new(config: ConnectionConfig) -> Self {
        Self {
            connection: Arc::new(RwLock::new(None)),
            config,
            is_running: Arc::new(RwLock::new(false)),
        }
    }
    
    /// 使用指定协议连接
    pub async fn connect_with_protocol(&self, transport: Transport) -> Result<(), FlareError> {
        // 检查是否已在运行
        {
            let running = self.is_running.read().await;
            if *running {
                return Err(FlareError::general_error("客户端已在运行".to_string()));
            }
        }
        
        // 标记为运行状态
        {
            let mut running = self.is_running.write().await;
            *running = true;
        }
        
        // 创建配置
        let mut config = self.config.clone();
        config.transport = transport.clone();
        
        // 创建客户端连接
        let client = ConnectionFactory::create_client(config)?;
        let client: Arc<dyn ClientConnection> = Arc::from(client);
        
        // 连接
        client.connect()?;
        
        // 保存连接
        {
            let mut conn = self.connection.write().await;
            *conn = Some(client);
        }
        
        tracing::info!("FastClient连接成功，使用协议: {:?}", transport);
        Ok(())
    }
    
    /// 使用协议竞速连接
    pub async fn connect_with_race(
        &self, 
        addresses: Vec<String>, 
        protocols: Vec<Transport>
    ) -> Result<(), FlareError> {
        // 检查是否已在运行
        {
            let running = self.is_running.read().await;
            if *running {
                return Err(FlareError::general_error("客户端已在运行".to_string()));
            }
        }
        
        // 标记为运行状态
        {
            let mut running = self.is_running.write().await;
            *running = true;
        }
        
        // 使用协议竞速器
        let client = ProtocolRacer::race(
            &self.config,
            &addresses,
            &protocols,
            None
        ).await?;
        
        // 保存连接
        {
            let mut conn = self.connection.write().await;
            *conn = Some(client);
        }
        
        tracing::info!("FastClient协议竞速连接成功");
        Ok(())
    }
    
    /// 断开连接
    pub async fn disconnect(&self) -> Result<(), FlareError> {
        // 检查是否正在运行
        {
            let running = self.is_running.read().await;
            if !*running {
                return Ok(());
            }
        }
        
        // 获取连接并断开
        {
            let conn = self.connection.read().await;
            if let Some(client) = &*conn {
                client.disconnect(None)?;
            }
        }
        
        // 清除连接
        {
            let mut conn = self.connection.write().await;
            *conn = None;
        }
        
        // 标记为停止状态
        {
            let mut running = self.is_running.write().await;
            *running = false;
        }
        
        tracing::info!("FastClient已断开连接");
        Ok(())
    }
    
    /// 发送消息
    pub async fn send_message(&self, frame: crate::common::protocol::frame::Frame) -> Result<(), FlareError> {
        let conn = self.connection.read().await;
        if let Some(client) = &*conn {
            client.send_message(frame)
        } else {
            Err(FlareError::general_error("未建立连接".to_string()))
        }
    }
    
    /// 设置事件处理器
    pub async fn set_event_handler(&self, handler: Arc<dyn ConnectionEvent>) -> Result<(), FlareError> {
        let conn = self.connection.read().await;
        if let Some(client) = &*conn {
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
    
    /// 检查是否已连接
    pub async fn is_connected(&self) -> bool {
        let conn = self.connection.read().await;
        if let Some(client) = &*conn {
            matches!(client.state(), crate::common::connections::enums::ConnectionState::Connected)
        } else {
            false
        }
    }
    
    /// 获取连接状态
    pub async fn connection_state(&self) -> Option<crate::common::connections::enums::ConnectionState> {
        let conn = self.connection.read().await;
        if let Some(client) = &*conn {
            Some(client.state())
        } else {
            None
        }
    }
    
    /// 获取当前连接
    pub async fn get_connection(&self) -> Option<Arc<dyn ClientConnection>> {
        let conn = self.connection.read().await;
        conn.clone()
    }
}

impl Default for FastClient {
    fn default() -> Self {
        Self::new(ConnectionConfig::default())
    }
}