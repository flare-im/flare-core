//! WebSocket客户端连接器
//!
//! 提供WebSocket协议的客户端连接实现

use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn, debug, error};
use tokio_tungstenite::{connect_async, WebSocketStream, MaybeTlsStream};
use tokio::net::TcpStream;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use crate::common::{
    connections::{Connection, ConnectionConfig, ConnectionState, ConnectionMetrics, ConnectionType, ConnectionRole, ConnectionSummary},
    error::{Result, FlareError},
    protocol::{UnifiedProtocolMessage, Frame, MessageType, Reliability},
};

use super::config::ClientConfig;

/// WebSocket客户端连接器
pub struct WebSocketConnector {
    config: ClientConfig,
    websocket: Arc<Mutex<Option<WebSocketStream<MaybeTlsStream<TcpStream>>>>>,
    connection_state: Arc<RwLock<ConnectionState>>,
    connection_metrics: Arc<RwLock<ConnectionMetrics>>,
    last_heartbeat: Arc<Mutex<Instant>>,
    heartbeat_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    message_receiver: Arc<Mutex<Option<tokio::sync::mpsc::Receiver<UnifiedProtocolMessage>>>>,
    message_sender: Arc<Mutex<Option<tokio::sync::mpsc::Sender<UnifiedProtocolMessage>>>>,
}

impl WebSocketConnector {
    /// 创建新的WebSocket连接器
    pub fn new(config: ClientConfig) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        
        Self {
            config,
            websocket: Arc::new(Mutex::new(None)),
            connection_state: Arc::new(RwLock::new(ConnectionState::Disconnected)),
            connection_metrics: Arc::new(RwLock::new(ConnectionMetrics::default())),
            last_heartbeat: Arc::new(Mutex::new(Instant::now())),
            heartbeat_task: Arc::new(Mutex::new(None)),
            message_receiver: Arc::new(Mutex::new(Some(rx))),
            message_sender: Arc::new(Mutex::new(Some(tx))),
        }
    }
    
    /// 连接到WebSocket服务器
    pub async fn connect(&mut self) -> Result<()> {
        info!("开始连接WebSocket服务器: {}", self.config.connection.remote_addr);
        
        // 更新连接状态
        {
            let mut state = self.connection_state.write().await;
            *state = ConnectionState::Connecting;
        }
        
        // 构建WebSocket URL
        let ws_url = if self.config.connection.remote_addr.starts_with("ws://") || 
                        self.config.connection.remote_addr.starts_with("wss://") {
            self.config.connection.remote_addr.clone()
        } else {
            format!("ws://{}", self.config.connection.remote_addr)
        };
        
        // 建立WebSocket连接
        let (ws_stream, _) = connect_async(&ws_url).await
            .map_err(|e| FlareError::ConnectionFailed(format!("WebSocket连接失败: {}", e)))?;
        
        // 保存WebSocket流
        {
            let mut ws = self.websocket.lock().await;
            *ws = Some(ws_stream);
        }
        
        // 更新连接状态
        {
            let mut state = self.connection_state.write().await;
            *state = ConnectionState::Connected;
        }
        
        // 启动消息处理任务
        self.start_message_handling_task().await;
        
        // 启动心跳任务
        self.start_heartbeat_task().await;
        
        info!("WebSocket连接建立成功");
        Ok(())
    }
    
    /// 断开连接
    pub async fn disconnect(&mut self) -> Result<()> {
        info!("断开WebSocket连接");
        
        // 停止心跳任务
        if let Some(task) = self.heartbeat_task.lock().await.take() {
            task.abort();
        }
        
        // 关闭WebSocket连接
        if let Some(mut ws) = self.websocket.lock().await.take() {
            if let Err(e) = ws.close(None).await {
                warn!("关闭WebSocket连接时出错: {}", e);
            }
        }
        
        // 更新连接状态
        {
            let mut state = self.connection_state.write().await;
            *state = ConnectionState::Disconnected;
        }
        
        info!("WebSocket连接已断开");
        Ok(())
    }
    
    /// 发送消息
    pub async fn send_message(&self, message: UnifiedProtocolMessage) -> Result<()> {
        let data = bincode::serialize(&message)
            .map_err(|e| FlareError::serialization_error(e.to_string()))?;
        
        let mut websocket = self.websocket.lock().await;
        if let Some(ws) = websocket.as_mut() {
            ws.send(tokio_tungstenite::tungstenite::Message::Binary(data.into()))
                .await
                .map_err(|e| FlareError::message_send_failed(format!("发送WebSocket消息失败: {}", e)))?;
        }
        
        Ok(())
    }
    
    /// 接收消息
    pub async fn receive_message(&self) -> Result<Option<UnifiedProtocolMessage>> {
        let mut receiver = self.message_receiver.lock().await;
        
        if let Some(receiver) = receiver.as_mut() {
            // 从消息通道接收消息
            match receiver.try_recv() {
                Ok(message) => {
                    // 更新连接指标
                    self.update_connection_metrics().await;
                    Ok(Some(message))
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    Ok(None)
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    Err(FlareError::ConnectionFailed("消息通道已断开".to_string()))
                }
            }
        } else {
            Err(FlareError::ConnectionFailed("消息接收器未初始化".to_string()))
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
    
    /// 启动消息处理任务
    async fn start_message_handling_task(&self) {
        let websocket = Arc::clone(&self.websocket);
        let message_sender = Arc::clone(&self.message_sender);
        let connection_state = Arc::clone(&self.connection_state);
        
        tokio::spawn(async move {
            let mut ws = websocket.lock().await;
            
            if let Some(mut websocket) = ws.as_mut() {
                let sender = message_sender.lock().await;
                
                if let Some(sender) = sender.as_ref() {
                    while let Some(msg) = websocket.next().await {
                        match msg {
                            Ok(tokio_tungstenite::tungstenite::Message::Binary(data)) => {
                                // 解析消息（先在本地变量完成，避免跨await持有非Send错误类型）
                                let parsed = Frame::from_bytes(&data).map_err(|e| e.to_string());
                                match parsed {
                                    Ok(frame) => {
                                        let message = UnifiedProtocolMessage::new(frame, None, 0);
                                        if let Err(e) = sender.send(message).await {
                                            error!("发送消息到通道失败: {}", e);
                                            break;
                                        }
                                    }
                                    Err(err_str) => {
                                        warn!("解析WebSocket消息失败: {}", err_str);
                                    }
                                }
                            }
                            Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                                info!("WebSocket连接关闭");
                                break;
                            }
                            Ok(tokio_tungstenite::tungstenite::Message::Ping(data)) => {
                                // 自动响应Ping
                                if let Err(e) = websocket.send(tokio_tungstenite::tungstenite::Message::Pong(data)).await {
                                    error!("发送Pong失败: {}", e);
                                    break;
                                }
                            }
                            Ok(tokio_tungstenite::tungstenite::Message::Pong(_)) => {
                                // 处理Pong消息
                                debug!("收到WebSocket Pong");
                            }
                            Ok(tokio_tungstenite::tungstenite::Message::Text(_)) => {
                                // 忽略文本消息
                            }
                            Ok(tokio_tungstenite::tungstenite::Message::Frame(_)) => {
                                // 忽略原始帧
                            }
                            Err(e) => {
                                error!("WebSocket错误: {}", e);
                                break;
                            }
                        }
                    }
                }
            }
            
            // 更新连接状态
            {
                let mut state = connection_state.write().await;
                *state = ConnectionState::Disconnected;
            }
        });
    }
    
    /// 启动心跳任务
    async fn start_heartbeat_task(&self) {
        let heartbeat_interval = Duration::from_millis(self.config.connection.heartbeat_interval_ms as u64);
        let last_heartbeat = Arc::clone(&self.last_heartbeat);
        let websocket = Arc::clone(&self.websocket);
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
                
                // 发送心跳（基于websocket的辅助函数）
                if let Err(e) = Self::send_heartbeat_on(&websocket).await {
                    error!("发送WebSocket心跳失败: {}", e);
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
        Self::send_heartbeat_on(&self.websocket).await
    }

    /// 基于websocket对象发送心跳（用于任务中）
    async fn send_heartbeat_on(websocket: &Arc<Mutex<Option<WebSocketStream<MaybeTlsStream<TcpStream>>>>>) -> Result<()> {
        let heartbeat_frame = Frame::heartbeat();
        let heartbeat_message = UnifiedProtocolMessage::new(heartbeat_frame, None, 0);

        let data = bincode::serialize(&heartbeat_message)
            .map_err(|e| FlareError::serialization_error(e.to_string()))?;

        let mut ws_guard = websocket.lock().await;
        if let Some(ws) = ws_guard.as_mut() {
            ws.send(tokio_tungstenite::tungstenite::Message::Binary(data.into()))
                .await
                .map_err(|e| FlareError::message_send_failed(format!("发送WebSocket心跳失败: {}", e)))?;
        }

        Ok(())
    }
    
    /// 更新连接指标
    async fn update_connection_metrics(&self) {
        // 这里简化实现，实际应该通过ping-pong测量延迟
        let latency_ms = 30; // 模拟延迟
        let jitter_ms = 5;   // 模拟抖动
        let packet_loss_percent = 0.0; // WebSocket通常没有丢包
        let bandwidth_bps = 1_000_000; // 模拟1Mbps带宽
        let stability_score = 95; // WebSocket通常比较稳定
        
        // 更新指标
        let mut metrics = self.connection_metrics.write().await;
        metrics.latency_ms = latency_ms;
        metrics.jitter_ms = jitter_ms;
        metrics.packet_loss_percent = packet_loss_percent;
        metrics.bandwidth_bps = bandwidth_bps;
        metrics.stability_score = stability_score;
        metrics.last_updated = chrono::Utc::now().timestamp_millis() as u64;
    }
}

impl Drop for WebSocketConnector {
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
impl Connection for WebSocketConnector {
    fn get_id(&self) -> &str {
        &self.config.server_url
    }
    
    fn get_connection_type(&self) -> ConnectionType {
        ConnectionType::WebSocket
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
            connection_type: crate::common::connections::types::ConnectionType::WebSocket,
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
            enable_0rtt: false,
            enable_connection_migration: false,
            enable_multipath: false,
            custom_config: std::collections::HashMap::new(),
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
        WebSocketConnector::send_message(self, message).await
    }
    
    async fn receive_message(&self) -> Result<Option<UnifiedProtocolMessage>> {
        WebSocketConnector::receive_message(self).await
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
        WebSocketConnector::is_active(self).await
    }
    
    async fn send_heartbeat(&self) -> Result<()> {
        WebSocketConnector::send_heartbeat(self).await
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
            connection_type: ConnectionType::WebSocket,
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
        Box::new(WebSocketConnector {
            config: self.config.clone(),
            websocket: Arc::clone(&self.websocket),
            connection_state: Arc::clone(&self.connection_state),
            connection_metrics: Arc::clone(&self.connection_metrics),
            message_sender: Arc::clone(&self.message_sender),
            message_receiver: Arc::clone(&self.message_receiver),
            last_heartbeat: Arc::clone(&self.last_heartbeat),
            heartbeat_task: Arc::clone(&self.heartbeat_task),
        })
    }
}
