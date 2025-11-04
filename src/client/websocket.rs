//! WebSocket 客户端实现

use crate::common::client_trait::Client;
use crate::common::config::ClientConfig;
use crate::common::connection_state::ConnectionStateManager;
use crate::common::error::Result;
use crate::common::heartbeat::HeartbeatManager;
use crate::common::message_parser::MessageParser;
use crate::common::protocol::Frame;
use crate::common::{generate_id};
use crate::transport::connection::Connection;
use crate::transport::events::{ArcObserver, ConnectionEvent};
use crate::transport::websocket::WebSocketTransport;
use async_trait::async_trait;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;

/// WebSocket 客户端
pub struct WebSocketClient {
    config: ClientConfig,
    connection: Option<Arc<Mutex<Box<dyn Connection>>>>,
    connection_id: String,
    parser: MessageParser,
    observers: Arc<StdMutex<Vec<ArcObserver>>>,
    state_manager: Arc<ConnectionStateManager>,
    heartbeat_manager: Option<HeartbeatManager>,
    reconnect_attempts: u32,
}

impl WebSocketClient {
    /// 创建新的 WebSocket 客户端
    pub fn new(config: ClientConfig) -> Self {
        let parser = MessageParser::new(config.serialization_format, config.compression);
        let connection_id = config.connection_id.clone().unwrap_or_else(generate_id);
        
        Self {
            config,
            connection: None,
            connection_id,
            parser: parser.clone(),
            observers: Arc::new(StdMutex::new(Vec::new())),
            state_manager: Arc::new(ConnectionStateManager::new()),
            heartbeat_manager: None,
            reconnect_attempts: 0,
        }
    }

    /// 创建并连接
    pub async fn connect_with_config(config: ClientConfig) -> Result<Self> {
        let mut client = Self::new(config);
        client.connect().await?;
        Ok(client)
    }

    async fn internal_connect(&mut self) -> Result<()> {
        let url_str = &self.config.server_url;
        
        // WebSocket 客户端：不使用 TLS
        // 使用 ws:// 协议（非 wss://），connect_async 会自动处理
        // tokio-tungstenite 根据 URL 协议自动选择是否使用 TLS
        // ws:// -> 非 TLS 连接，返回 WebSocketStream<TcpStream>（包装在 MaybeTlsStream::Plain 中）
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

        // 添加消息观察者
        let parser_clone = self.parser.clone();
        let observers_clone = Arc::clone(&self.observers);
        let connection_clone = Arc::clone(&connection_arc);
        let conn_id_clone = self.connection_id.clone();
        let state_mgr = Arc::clone(&self.state_manager);

        let message_observer = Arc::new(ClientMessageObserver {
            parser: parser_clone,
            observers: observers_clone,
            connection_id: conn_id_clone,
            state_manager: state_mgr,
            heartbeat_manager: None,
        });
        
        {
            let mut conn = connection_arc.lock().await;
            conn.add_observer(message_observer);
        }

        self.connection = Some(connection_clone);
        
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
        
        // 简化：直接发送，不等待 ACK
        self.send_frame_internal(&connect_frame).await?;

        // 启动心跳
        let mut heartbeat = HeartbeatManager::new(
            self.config.heartbeat_interval,
            self.config.heartbeat_interval * 3, // 3倍间隔作为超时
        );
        
        if let Some(ref conn) = self.connection {
            heartbeat.start(Arc::clone(conn), self.parser.clone());
            self.heartbeat_manager = Some(heartbeat);
        }

        self.state_manager.set_connected();
        self.reconnect_attempts = 0;
        
        Ok(())
    }

    async fn send_frame_internal(&self, frame: &Frame) -> Result<()> {
        let can_send = self.state_manager.get_state().can_send();
        
        if !can_send {
            return Err(crate::common::error::FlareError::connection_failed(
                "Cannot send: connection state is not ready".to_string()
            ));
        }

        let data = self.parser.serialize(frame)?;
        if let Some(ref conn) = self.connection {
            let mut c = conn.lock().await;
            c.send(&data).await?;
        } else {
            return Err(crate::common::error::FlareError::connection_failed("Not connected".to_string()));
        }
        Ok(())
    }

    async fn try_reconnect(&mut self) -> Result<()> {
        if self.reconnect_attempts >= self.config.max_reconnect_attempts {
            return Err(crate::common::error::FlareError::connection_failed(
                format!("Max reconnect attempts ({}) exceeded", self.config.max_reconnect_attempts)
            ));
        }

        self.state_manager.set_reconnecting();
        self.reconnect_attempts += 1;

        // 等待重连间隔
        sleep(self.config.reconnect_interval).await;

        // 断开旧连接
        if let Some(ref conn) = self.connection.take() {
            let mut c = conn.lock().await;
            let _ = c.close().await;
        }

        // 尝试重新连接
        self.internal_connect().await
    }
}

struct ClientMessageObserver {
    parser: MessageParser,
    observers: Arc<StdMutex<Vec<ArcObserver>>>,
    connection_id: String,
    state_manager: Arc<ConnectionStateManager>,
    heartbeat_manager: Option<Arc<StdMutex<HeartbeatManager>>>,
}

impl crate::transport::events::ConnectionObserver for ClientMessageObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
                if let Ok(frame) = self.parser.parse(data) {
                    // 检查是否是 PONG
                    if let Some(cmd) = &frame.command {
                        if let Some(crate::common::protocol::flare::core::commands::command::Type::System(sys_cmd)) = &cmd.r#type {
                            if sys_cmd.r#type == crate::common::protocol::flare::core::commands::system_command::Type::Pong as i32 {
                                // 记录 PONG，更新心跳
                                if let Some(ref hb) = self.heartbeat_manager {
                                    if let Ok(hb_mgr) = hb.lock() {
                                        hb_mgr.record_pong();
                                    }
                                }
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
                // 转发事件
                if let Ok(observers) = self.observers.lock() {
                    for observer in observers.iter() {
                        observer.on_event(event);
                    }
                }
            }
            ConnectionEvent::Disconnected(_) => {
                self.state_manager.set_disconnected();
                // 转发事件
                if let Ok(observers) = self.observers.lock() {
                    for observer in observers.iter() {
                        observer.on_event(event);
                    }
                }
            }
            ConnectionEvent::Error(_e) => {
                self.state_manager.set_failed();
                // 转发事件
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
        let can_connect = self.state_manager.get_state().can_connect();
        
        if !can_connect {
            return Err(crate::common::error::FlareError::protocol_error(
                format!("Cannot connect: state is unavailable")
            ));
        }

        self.state_manager.start_connecting();
        
        match self.internal_connect().await {
            Ok(()) => {
                self.state_manager.set_connected();
                // 通知观察者连接成功
                if let Ok(observers) = self.observers.lock() {
                    for observer in observers.iter() {
                        observer.on_event(&ConnectionEvent::Connected);
                    }
                }
                Ok(())
            }
            Err(e) => {
                self.state_manager.set_failed();
                // 如果允许重连，尝试重连
                if self.config.max_reconnect_attempts > 0 {
                    self.try_reconnect().await
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.state_manager.set_state(crate::common::connection_state::ConnectionState::Disconnecting);

        // 停止心跳
        if let Some(ref mut hb) = self.heartbeat_manager.take() {
            hb.stop();
        }

        if let Some(ref conn) = self.connection.take() {
            let mut c = conn.lock().await;
            c.close().await?;
        }
        
        // 通知观察者断开连接
        if let Ok(observers) = self.observers.lock() {
            for observer in observers.iter() {
                observer.on_event(&ConnectionEvent::Disconnected("Client disconnected".to_string()));
            }
        }

        self.state_manager.set_disconnected();
        Ok(())
    }

    async fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        // 如果未连接，尝试重连
        if !self.is_connected() && self.config.max_reconnect_attempts > 0 {
            if let Err(e) = self.try_reconnect().await {
                return Err(e);
            }
        }
        
        self.send_frame_internal(frame).await
    }

    fn is_connected(&self) -> bool {
        let state_ok = matches!(self.state_manager.get_state(), crate::common::connection_state::ConnectionState::Connected);
        
        state_ok && self.connection.is_some()
            && {
                // 注意：这个方法可能在同步上下文中调用，但 conn 是 tokio::sync::Mutex
                // 需要异步上下文，这里简化为检查连接是否存在
                self.connection.is_some()
            }
    }

    fn add_observer(&mut self, observer: ArcObserver) {
        if let Ok(mut observers) = self.observers.lock() {
            observers.push(observer);
        }
    }

    fn remove_observer(&mut self, observer: ArcObserver) {
        if let Ok(mut observers) = self.observers.lock() {
            observers.retain(|o| !Arc::ptr_eq(o, &observer));
        }
    }

    fn connection_id(&self) -> Option<String> {
        Some(self.connection_id.clone())
    }
}
