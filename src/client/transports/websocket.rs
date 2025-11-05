//! WebSocket 客户端实现
//! 
//! 只处理协议层，连接状态管理、心跳、消息路由等功能委托给 ClientCore

use crate::client::transports::{Client, ClientCore};
use crate::client::config::ClientConfig;
use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::common::{generate_id};
use crate::transport::connection::Connection;
use crate::transport::events::{ConnectionEvent, ConnectionObserver, ArcObserver};
use crate::transport::websocket::WebSocketTransport;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;

/// WebSocket 客户端
/// 
/// 只处理协议层，其他功能委托给 ClientCore
pub struct WebSocketClient {
    config: ClientConfig,
    connection: Option<Arc<Mutex<Box<dyn Connection>>>>,
    connection_id: String,
    core: ClientCore,
    reconnect_attempts: u32,
}

impl WebSocketClient {
    /// 创建新的 WebSocket 客户端
    pub fn new(config: ClientConfig) -> Self {
        let connection_id = config.connection_id.clone().unwrap_or_else(generate_id);
        let core = ClientCore::new(&config);
        
        Self {
            config,
            connection: None,
            connection_id,
            core,
            reconnect_attempts: 0,
        }
    }

    /// 创建并连接
    pub async fn connect_with_config(config: ClientConfig) -> Result<Self> {
        let mut client = Self::new(config);
        client.connect().await?;
        Ok(client)
    }
    
    /// 使用 ClientCore 创建（用于 HybridClient）
    pub fn with_core(config: ClientConfig, core: ClientCore) -> Self {
        let connection_id = config.connection_id.clone().unwrap_or_else(generate_id);
        Self {
            config,
            connection: None,
            connection_id,
            core,
            reconnect_attempts: 0,
        }
    }

    async fn internal_connect(&mut self) -> Result<()> {
        let url_str = &self.config.server_url;
        
        let ws_stream_result = timeout(
            self.config.connect_timeout,
            connect_async(url_str),
        ).await;

        let (ws_stream, _) = ws_stream_result
            .map_err(|_| crate::common::error::FlareError::connection_timeout("Connection timeout".to_string()))?
            .map_err(|e| crate::common::error::FlareError::connection_failed(e.to_string()))?;

        let transport = WebSocketTransport::new(ws_stream);
        let connection: Box<dyn Connection> = Box::new(transport);
        let connection_arc = Arc::new(Mutex::new(connection));

        // 创建消息观察者，委托给 ClientCore
        let core_state_mgr = Arc::clone(&self.core.state_manager);
        let core_parser = self.core.parser.clone();
        let core_observers = Arc::clone(&self.core.observers);
        let core_clone = Arc::new(self.core.clone()); // 用于记录 PONG
        
        let message_observer = Arc::new(ClientMessageObserver {
            state_manager: core_state_mgr,
            parser: core_parser,
            observers: core_observers,
            core: core_clone,
        });
        
        {
            let mut conn = connection_arc.lock().await;
            conn.add_observer(message_observer);
        }

        self.connection = Some(connection_arc.clone());
        
        // 发送 CONNECT 消息
        let mut metadata = std::collections::HashMap::new();
        for (k, v) in &self.config.metadata {
            metadata.insert(k.clone(), v.as_bytes().to_vec());
        }
        let connect_cmd = crate::common::protocol::connect(
            self.config.serialization_format,
            metadata,
        );
        let connect_frame = crate::common::protocol::frame_with_system_command(
            connect_cmd,
            crate::common::protocol::Reliability::AtLeastOnce,
        );
        
        self.send_frame_internal(&connect_frame).await?;

        // 启动心跳（通过 ClientCore）
        self.core.start_heartbeat(connection_arc);

        self.core.handle_connection_event(&ConnectionEvent::Connected);
        self.reconnect_attempts = 0;
        
        Ok(())
    }

    async fn send_frame_internal(&self, frame: &Frame) -> Result<()> {
        if !self.core.can_send() {
            return Err(crate::common::error::FlareError::connection_failed(
                "Cannot send: connection state is not ready".to_string()
            ));
        }

        let data = self.core.parser.serialize(frame)?;
        if let Some(ref conn) = self.connection {
            let mut c = conn.lock().await;
            c.send(&data).await?;
        } else {
            return Err(crate::common::error::FlareError::connection_failed("Not connected".to_string()));
        }
        Ok(())
    }

    async fn try_reconnect(&mut self) -> Result<()> {
        if let Some(max_attempts) = self.config.max_reconnect_attempts {
            if self.reconnect_attempts >= max_attempts {
                return Err(crate::common::error::FlareError::connection_failed(
                    format!("Max reconnect attempts ({}) exceeded", max_attempts)
                ));
            }
        }

        self.core.state_manager.start_connecting();
        self.reconnect_attempts += 1;

        sleep(self.config.reconnect_interval).await;

        if let Some(ref conn) = self.connection.take() {
            let mut c = conn.lock().await;
            let _ = c.close().await;
        }

        self.internal_connect().await
    }
    
    /// 获取 ClientCore（用于外部访问）
    pub fn core(&self) -> &ClientCore {
        &self.core
    }
}

// 消息观察者，委托给 ClientCore
struct ClientMessageObserver {
    state_manager: Arc<crate::client::connection::ConnectionStateManager>,
    parser: crate::common::MessageParser,
    observers: Arc<std::sync::Mutex<Vec<ArcObserver>>>,
    core: Arc<ClientCore>,
}

impl ConnectionObserver for ClientMessageObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
                // 解析消息
                if let Ok(frame) = self.parser.parse(data) {
                    // 检查是否是 PONG（心跳响应）
                    if let Some(cmd) = &frame.command {
                        if let Some(crate::common::protocol::flare::core::commands::command::Type::System(sys_cmd)) = &cmd.r#type {
                            if sys_cmd.r#type == crate::common::protocol::flare::core::commands::system_command::Type::Pong as i32 {
                                // 记录 PONG，更新心跳（通过 ClientCore）
                                self.core.record_pong();
                            }
                        }
                    }
                    
                    // 通知所有观察者
                    if let Ok(observers) = self.observers.lock() {
                        for observer in observers.iter() {
                            observer.on_event(&ConnectionEvent::Message(data.clone()));
                        }
                    }
                }
            }
            ConnectionEvent::Connected => {
                self.state_manager.set_connected();
                if let Ok(observers) = self.observers.lock() {
                    for observer in observers.iter() {
                        observer.on_event(event);
                    }
                }
            }
            ConnectionEvent::Disconnected(_) => {
                self.state_manager.set_disconnected();
                if let Ok(observers) = self.observers.lock() {
                    for observer in observers.iter() {
                        observer.on_event(event);
                    }
                }
            }
            ConnectionEvent::Error(_) => {
                self.state_manager.set_failed();
                if let Ok(observers) = self.observers.lock() {
                    for observer in observers.iter() {
                        observer.on_event(event);
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Client for WebSocketClient {
    async fn connect(&mut self) -> Result<()> {
        if !self.core.can_connect() {
            return Err(crate::common::error::FlareError::protocol_error(
                format!("Cannot connect: state is unavailable")
            ));
        }

        self.core.state_manager.start_connecting();
        
        match self.internal_connect().await {
            Ok(()) => {
                Ok(())
            }
            Err(e) => {
                self.core.state_manager.set_failed();
                // 如果允许重连，尝试重连
                if self.config.max_reconnect_attempts.map(|n| n > 0).unwrap_or(true) {
                    self.try_reconnect().await
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.core.state_manager.set_state(crate::client::connection::ConnectionState::Disconnecting);

        // 停止心跳（通过 ClientCore）
        self.core.stop_heartbeat();

        if let Some(ref conn) = self.connection.take() {
            let mut c = conn.lock().await;
            c.close().await?;
        }
        
        self.core.handle_connection_event(&ConnectionEvent::Disconnected("Client disconnected".to_string()));
        Ok(())
    }

    async fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        // 如果未连接，尝试重连
        if !self.is_connected() && self.config.max_reconnect_attempts.map(|n| n > 0).unwrap_or(true) {
            if let Err(e) = self.try_reconnect().await {
                return Err(e);
            }
        }
        
        self.send_frame_internal(frame).await
    }

    fn is_connected(&self) -> bool {
        matches!(self.core.state(), crate::client::connection::ConnectionState::Connected)
            && self.connection.is_some()
    }

    fn add_observer(&mut self, observer: ArcObserver) {
        self.core.add_observer(observer);
    }

    fn remove_observer(&mut self, observer: ArcObserver) {
        self.core.remove_observer(observer);
    }

    fn connection_id(&self) -> Option<String> {
        Some(self.connection_id.clone())
    }
}
