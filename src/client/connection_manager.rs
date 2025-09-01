//! 客户端连接管理器 - 专注于长连接可靠性

use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, error, warn, debug};
use tokio::time::{sleep, Duration};

use crate::common::{
    error::{Result, FlareError},
    connections::{Connection, ConnectionConfig, ConnectionState, ConnectionMetrics},
    managers::common::ConnectionPool,
    protocol::{Frame, MessageType, Reliability, UnifiedProtocolMessage, ProtocolSelection},
};
use crate::client::config::ClientConfig;
use crate::client::quic_connector::QuicConnector;
use crate::client::websocket_connector::WebSocketConnector;

/// 客户端连接管理器
pub struct ConnectionManager {
    config: ClientConfig,
    connection_pool: Arc<ConnectionPool>,
    current_connection: Arc<Mutex<Option<Box<dyn Connection>>>>,
    connection_state: Arc<Mutex<ConnectionState>>,
    reconnect_attempts: Arc<Mutex<u32>>,
    current_protocol: Arc<Mutex<ProtocolSelection>>,
    quic_connector: Arc<Mutex<Option<QuicConnector>>>,
    websocket_connector: Arc<Mutex<Option<WebSocketConnector>>>,
}

impl ConnectionManager {
    /// 创建新的连接管理器
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            connection_pool: Arc::new(ConnectionPool::new()),
            current_connection: Arc::new(Mutex::new(None)),
            connection_state: Arc::new(Mutex::new(ConnectionState::Disconnected)),
            reconnect_attempts: Arc::new(Mutex::new(0)),
            current_protocol: Arc::new(Mutex::new(ProtocolSelection::Auto)),
            quic_connector: Arc::new(Mutex::new(None)),
            websocket_connector: Arc::new(Mutex::new(None)),
        }
    }

    /// 强制重连
    pub async fn force_reconnect(&mut self) -> Result<()> {
        info!("开始强制重连");
        
        // 断开当前连接
        self.disconnect().await?;
        
        // 等待一段时间后重连
        sleep(Duration::from_millis(100)).await;
        
        // 尝试连接
        self.connect_internal().await
    }

    /// 断开连接
    pub async fn disconnect(&mut self) -> Result<()> {
        info!("断开连接");
        
        // 断开QUIC连接
        if let Some(mut quic_conn) = self.quic_connector.lock().await.take() {
            if let Err(e) = quic_conn.disconnect().await {
                warn!("断开QUIC连接时出错: {}", e);
            }
        }
        
        // 断开WebSocket连接
        if let Some(mut ws_conn) = self.websocket_connector.lock().await.take() {
            if let Err(e) = ws_conn.disconnect().await {
                warn!("断开WebSocket连接时出错: {}", e);
            }
        }
        
        {
            let mut current_conn = self.current_connection.lock().await;
            if let Some(mut conn) = current_conn.take() {
                if let Err(e) = conn.disconnect().await {
                    warn!("断开连接时出错: {}", e);
                }
            }
        }
        
        {
            let mut state = self.connection_state.lock().await;
            *state = ConnectionState::Disconnected;
        }
        
        {
            let mut attempts = self.reconnect_attempts.lock().await;
            *attempts = 0;
        }
        
        Ok(())
    }

    /// 获取连接状态
    pub async fn get_connection_state(&self) -> ConnectionState {
        let state = self.connection_state.lock().await;
        *state
    }

    /// 发送消息
    pub async fn send_message(&self, message: UnifiedProtocolMessage) -> Result<()> {
        let protocol = *self.current_protocol.lock().await;
        
        match protocol {
            ProtocolSelection::QuicOnly => {
                if let Some(quic_conn) = self.quic_connector.lock().await.as_ref() {
                    quic_conn.send_message(message).await
                } else {
                    Err(FlareError::connection_failed("QUIC连接器未初始化".to_string()))
                }
            }
            ProtocolSelection::WebSocketOnly => {
                if let Some(ws_conn) = self.websocket_connector.lock().await.as_ref() {
                    ws_conn.send_message(message).await
                } else {
                    Err(FlareError::connection_failed("WebSocket连接器未初始化".to_string()))
                }
            }
            ProtocolSelection::Auto => {
                // 自动选择最佳协议
                if let Some(quic_conn) = self.quic_connector.lock().await.as_ref() {
                    if quic_conn.is_active().await {
                        return quic_conn.send_message(message).await;
                    }
                }
                
                if let Some(ws_conn) = self.websocket_connector.lock().await.as_ref() {
                    if ws_conn.is_active().await {
                        return ws_conn.send_message(message).await;
                    }
                }
                
                Err(FlareError::connection_failed("没有可用的连接".to_string()))
            }
        }
    }

    /// 接收消息
    pub async fn receive_message(&self) -> Result<Option<UnifiedProtocolMessage>> {
        let protocol = *self.current_protocol.lock().await;
        
        match protocol {
            ProtocolSelection::QuicOnly => {
                if let Some(quic_conn) = self.quic_connector.lock().await.as_ref() {
                    quic_conn.receive_message().await
                } else {
                    Err(FlareError::connection_failed("QUIC连接器未初始化".to_string()))
                }
            }
            ProtocolSelection::WebSocketOnly => {
                if let Some(ws_conn) = self.websocket_connector.lock().await.as_ref() {
                    ws_conn.receive_message().await
                } else {
                    Err(FlareError::connection_failed("WebSocket连接器未初始化".to_string()))
                }
            }
            ProtocolSelection::Auto => {
                // 自动选择最佳协议
                if let Some(quic_conn) = self.quic_connector.lock().await.as_ref() {
                    if quic_conn.is_active().await {
                        return quic_conn.receive_message().await;
                    }
                }
                
                if let Some(ws_conn) = self.websocket_connector.lock().await.as_ref() {
                    if ws_conn.is_active().await {
                        return ws_conn.receive_message().await;
                    }
                }
                
                Err(FlareError::connection_failed("没有可用的连接".to_string()))
            }
        }
    }

    /// 获取连接质量指标
    pub async fn get_connection_metrics(&self) -> Option<ConnectionMetrics> {
        let protocol = *self.current_protocol.lock().await;
        
        match protocol {
            ProtocolSelection::QuicOnly => {
                if let Some(quic_conn) = self.quic_connector.lock().await.as_ref() {
                    Some(quic_conn.get_connection_metrics().await)
                } else {
                    None
                }
            }
            ProtocolSelection::WebSocketOnly => {
                if let Some(ws_conn) = self.websocket_connector.lock().await.as_ref() {
                    Some(ws_conn.get_connection_metrics().await)
                } else {
                    None
                }
            }
            ProtocolSelection::Auto => {
                // 返回最佳协议的指标
                let quic_metrics = if let Some(quic_conn) = self.quic_connector.lock().await.as_ref() {
                    Some(quic_conn.get_connection_metrics().await)
                } else {
                    None
                };
                
                let ws_metrics = if let Some(ws_conn) = self.websocket_connector.lock().await.as_ref() {
                    Some(ws_conn.get_connection_metrics().await)
                } else {
                    None
                };
                
                // 比较指标，返回更好的
                match (quic_metrics, ws_metrics) {
                    (Some(qm), Some(wm)) => {
                        if qm.stability_score > wm.stability_score {
                            Some(qm)
                        } else {
                            Some(wm)
                        }
                    }
                    (Some(qm), None) => Some(qm),
                    (None, Some(wm)) => Some(wm),
                    (None, None) => None,
                }
            }
        }
    }

    /// 获取当前使用的协议
    pub async fn get_current_protocol(&self) -> ProtocolSelection {
        *self.current_protocol.lock().await
    }

    /// 切换协议
    pub async fn switch_protocol(&self, protocol: ProtocolSelection) -> Result<()> {
        info!("切换到协议: {:?}", protocol);
        
        // 更新当前协议
        {
            let mut current = self.current_protocol.lock().await;
            *current = protocol;
        }
        
        // 如果当前没有连接，尝试建立连接
        let state = self.get_connection_state().await;
        if state == ConnectionState::Disconnected {
            // 这里应该调用连接逻辑，但为了避免循环调用，我们只更新状态
            debug!("协议已切换到 {:?}，等待下次连接时使用", protocol);
        }
        
        Ok(())
    }

    /// 强制使用QUIC协议
    pub async fn force_quic(&self) -> Result<()> {
        self.switch_protocol(ProtocolSelection::QuicOnly).await
    }

    /// 强制使用WebSocket协议
    pub async fn force_websocket(&self) -> Result<()> {
        self.switch_protocol(ProtocolSelection::WebSocketOnly).await
    }

    /// 内部连接方法
    async fn connect_internal(&mut self) -> Result<()> {
        info!("尝试建立连接");
        
        {
            let mut state = self.connection_state.lock().await;
            *state = ConnectionState::Connecting;
        }
        
        let protocol = *self.current_protocol.lock().await;
        
        match protocol {
            ProtocolSelection::QuicOnly => {
                self.connect_quic().await?;
            }
            ProtocolSelection::WebSocketOnly => {
                self.connect_websocket().await?;
            }
            ProtocolSelection::Auto => {
                // 尝试QUIC，如果失败则回退到WebSocket
                if let Err(e) = self.connect_quic().await {
                    warn!("QUIC连接失败，回退到WebSocket: {}", e);
                    self.connect_websocket().await?;
                }
            }
        }
        
        {
            let mut state = self.connection_state.lock().await;
            *state = ConnectionState::Connected;
        }
        
        info!("连接建立成功");
        Ok(())
    }

    /// 连接QUIC
    async fn connect_quic(&mut self) -> Result<()> {
        info!("尝试建立QUIC连接");
        
        let mut quic_connector = QuicConnector::new(self.config.clone());
        quic_connector.connect().await?;
        
        {
            let mut quic = self.quic_connector.lock().await;
            *quic = Some(quic_connector);
        }
        
        info!("QUIC连接建立成功");
        Ok(())
    }

    /// 连接WebSocket
    async fn connect_websocket(&mut self) -> Result<()> {
        info!("尝试建立WebSocket连接");
        
        let mut ws_connector = WebSocketConnector::new(self.config.clone());
        ws_connector.connect().await?;
        
        {
            let mut ws = self.websocket_connector.lock().await;
            *ws = Some(ws_connector);
        }
        
        info!("WebSocket连接建立成功");
        Ok(())
    }

    /// 启动重连任务
    pub async fn start_reconnect_task(&self) {
        let config = self.config.clone();
        let connection_state = Arc::clone(&self.connection_state);
        let reconnect_attempts = Arc::clone(&self.reconnect_attempts);
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(1000));
            
            loop {
                interval.tick().await;
                
                let state = *connection_state.lock().await;
                if state == ConnectionState::Connected {
                    continue;
                }
                
                let mut attempts = reconnect_attempts.lock().await;
                if *attempts >= config.connection.max_reconnect_attempts {
                    error!("重连次数已达上限，停止重连");
                    break;
                }
                
                *attempts += 1;
                let delay = config.connection.reconnect_delay_ms * *attempts;
                info!("第{}次重连尝试，延迟{}ms", *attempts, delay);
                
                sleep(Duration::from_millis(delay as u64)).await;
                
                // 这里应该尝试重新连接
                // 简化实现，直接设置为重连状态
                {
                    let mut state = connection_state.lock().await;
                    *state = ConnectionState::Reconnecting;
                }
            }
        });
    }
}
