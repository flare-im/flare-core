//! QUIC 客户端实现

use crate::common::client_trait::Client;
use crate::common::config::ClientConfig;
use crate::common::connection_state::ConnectionStateManager;
use crate::common::error::Result;
use crate::common::heartbeat::HeartbeatManager;
use crate::common::message_parser::MessageParser;
use crate::common::protocol::{Frame, connect, frame_with_system_command};
use crate::common::{generate_id};
use crate::transport::connection::Connection;
use crate::transport::events::{ArcObserver, ConnectionEvent};
use crate::transport::quic::QUICTransport;
use async_trait::async_trait;
use quinn::Endpoint;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// QUIC 客户端
pub struct QUICClient {
    config: ClientConfig,
    connection: Option<Arc<Mutex<Box<dyn Connection>>>>,
    connection_id: String,
    parser: MessageParser,
    observers: Arc<StdMutex<Vec<ArcObserver>>>,
    state_manager: Arc<ConnectionStateManager>,
    heartbeat_manager: Option<HeartbeatManager>,
    reconnect_attempts: u32,
    endpoint: Option<Endpoint>,
}

impl QUICClient {
    /// 创建新的 QUIC 客户端
    pub fn new(config: ClientConfig) -> Result<Self> {
        let parser = MessageParser::new(config.serialization_format, config.compression);
        let connection_id = config.connection_id.clone().unwrap_or_else(generate_id);
        
        // 创建 QUIC endpoint
        // quinn 0.11: Endpoint::client 已经内置了默认的客户端配置（包括系统根证书）
        // 如果需要自定义配置，可以参考示例代码：
        //   let client_cfg = ClientConfig::with_native_roots();
        //   endpoint.set_default_client_config(client_cfg);
        // 但当前使用默认配置即可，因为 Endpoint::client 已经包含了合适的默认配置
        let endpoint = Endpoint::client("[::]:0".parse().unwrap())
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("Failed to create endpoint: {}", e)))?;

        Ok(Self {
            config,
            connection: None,
            connection_id,
            parser,
            observers: Arc::new(StdMutex::new(Vec::new())),
            state_manager: Arc::new(ConnectionStateManager::new()),
            heartbeat_manager: None,
            reconnect_attempts: 0,
            endpoint: Some(endpoint),
        })
    }

    /// 创建并连接
    pub async fn connect_with_config(config: ClientConfig) -> Result<Self> {
        let mut client = Self::new(config)?;
        client.connect().await?;
        Ok(client)
    }

    async fn internal_connect(&mut self) -> Result<()> {
        let server_addr = self.config.server_url
            .replace("quic://", "")
            .parse::<SocketAddr>()
            .map_err(|e| crate::common::error::FlareError::protocol_error(format!("Invalid address: {}", e)))?;

        let endpoint = self.endpoint.as_ref().ok_or_else(|| {
            crate::common::error::FlareError::connection_failed("Endpoint not initialized".to_string())
        })?;

        // 连接服务器
        let connecting = endpoint.connect(server_addr, "localhost")
            .map_err(|e| crate::common::error::FlareError::connection_failed(e.to_string()))?;

        let quinn_connection = timeout(
            self.config.connect_timeout,
            connecting,
        )
        .await
        .map_err(|_| crate::common::error::FlareError::connection_timeout("Connection timeout".to_string()))?
        .map_err(|e| crate::common::error::FlareError::connection_failed(e.to_string()))?;

        // 打开双向流
        let (send, recv) = quinn_connection.open_bi()
            .await
            .map_err(|e| crate::common::error::FlareError::connection_failed(e.to_string()))?;

        let transport = QUICTransport::new(send, recv);
        let connection: Box<dyn Connection> = Box::new(transport);
        let connection_arc = Arc::new(Mutex::new(connection));

        // 添加消息观察者
        let parser_clone = self.parser.clone();
        let observers_clone = Arc::clone(&self.observers);
        let connection_clone = Arc::clone(&connection_arc);
        let conn_id_clone = self.connection_id.clone();
        let state_mgr = Arc::clone(&self.state_manager);
        let message_observer = Arc::new(QUICMessageObserver {
            parser: parser_clone,
            observers: observers_clone,
            connection_id: conn_id_clone,
            state_manager: state_mgr.clone(),
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
        
        self.send_frame_internal(&connect_frame).await?;

        // 启动心跳
        let mut heartbeat = HeartbeatManager::new(
            self.config.heartbeat_interval,
            self.config.heartbeat_interval * 3,
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

        sleep(self.config.reconnect_interval).await;

        if let Some(ref conn) = self.connection.take() {
            let mut c = conn.lock().await;
            let _ = c.close().await;
        }

        self.internal_connect().await
    }
}

struct QUICMessageObserver {
    parser: MessageParser,
    observers: Arc<StdMutex<Vec<ArcObserver>>>,
    connection_id: String,
    state_manager: Arc<ConnectionStateManager>,
}

impl crate::transport::events::ConnectionObserver for QUICMessageObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
                if let Ok(frame) = self.parser.parse(data) {
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
            ConnectionEvent::Error(_e) => {
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
    impl Client for QUICClient {
        async fn connect(&mut self) -> Result<()> {
            let can_connect = self.state_manager.get_state().can_connect();
            
            if !can_connect {
                return Err(crate::common::error::FlareError::protocol_error(
                    "Cannot connect: connection state is not ready".to_string()
                ));
            }

            self.state_manager.start_connecting();
            
            match self.internal_connect().await {
                Ok(()) => {
                    self.state_manager.set_connected();
                    if let Ok(observers) = self.observers.lock() {
                        for observer in observers.iter() {
                            observer.on_event(&ConnectionEvent::Connected);
                        }
                    }
                    Ok(())
                }
                Err(e) => {
                    self.state_manager.set_failed();
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

