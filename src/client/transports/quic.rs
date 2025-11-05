//! QUIC 客户端实现
//! 
//! 只处理协议层，连接状态管理、心跳、消息路由等功能委托给 ClientCore

use crate::client::transports::{Client, ClientCore};
use crate::client::config::ClientConfig;
use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::common::{generate_id};
use crate::transport::connection::Connection;
use crate::transport::events::{ConnectionEvent, ConnectionObserver, ArcObserver};
use crate::transport::quic::QUICTransport;
use async_trait::async_trait;
use quinn::Endpoint;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tokio::net::lookup_host;

/// QUIC 客户端
/// 
/// 只处理协议层，其他功能委托给 ClientCore
pub struct QUICClient {
    config: ClientConfig,
    connection: Option<Arc<Mutex<Box<dyn Connection>>>>,
    connection_id: String,
    core: ClientCore,
    reconnect_attempts: u32,
    endpoint: Option<Endpoint>,
    _client_config: Option<quinn::ClientConfig>, // 保留用于将来的 TLS 配置
}

impl QUICClient {
    /// 创建新的 QUIC 客户端
    pub fn new(config: ClientConfig) -> Result<Self> {
        let connection_id = config.connection_id.clone().unwrap_or_else(generate_id);
        let core = ClientCore::new(&config);
        
        // 配置 rustls ClientConfig
        use crate::common::cert::create_client_config;
        
        let rustls_config = create_client_config()
            .map_err(|e| crate::common::error::FlareError::protocol_error(
                format!("Failed to create client TLS config: {}", e)
            ))?;
        
        use quinn::crypto::rustls::QuicClientConfig;
        let rustls_config_arc = Arc::new(rustls_config);
        
        let quic_crypto_config = QuicClientConfig::try_from(rustls_config_arc)
            .map_err(|e| crate::common::error::FlareError::protocol_error(
                format!("Failed to create QUIC client config: {}", e)
            ))?;
        
        let client_config = quinn::ClientConfig::new(Arc::new(quic_crypto_config));
        
        let mut endpoint = Endpoint::client("[::]:0".parse().unwrap())
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("Failed to create endpoint: {}", e)))?;
        
        endpoint.set_default_client_config(client_config.clone());

        Ok(Self {
            config,
            connection: None,
            connection_id,
            core,
            reconnect_attempts: 0,
            endpoint: Some(endpoint),
            _client_config: Some(client_config),
        })
    }

    /// 创建并连接
    pub async fn connect_with_config(config: ClientConfig) -> Result<Self> {
        let mut client = Self::new(config)?;
        client.connect().await?;
        Ok(client)
    }
    
    /// 使用 ClientCore 创建（用于 HybridClient）
    pub fn with_core(config: ClientConfig, core: ClientCore) -> Result<Self> {
        let connection_id = config.connection_id.clone().unwrap_or_else(generate_id);
        
        // 配置 rustls ClientConfig
        use crate::common::cert::create_client_config;
        
        let rustls_config = create_client_config()
            .map_err(|e| crate::common::error::FlareError::protocol_error(
                format!("Failed to create client TLS config: {}", e)
            ))?;
        
        use quinn::crypto::rustls::QuicClientConfig;
        let rustls_config_arc = Arc::new(rustls_config);
        
        let quic_crypto_config = QuicClientConfig::try_from(rustls_config_arc)
            .map_err(|e| crate::common::error::FlareError::protocol_error(
                format!("Failed to create QUIC client config: {}", e)
            ))?;
        
        let client_config = quinn::ClientConfig::new(Arc::new(quic_crypto_config));
        
        let mut endpoint = Endpoint::client("[::]:0".parse().unwrap())
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("Failed to create endpoint: {}", e)))?;
        
        endpoint.set_default_client_config(client_config.clone());

        Ok(Self {
            config,
            connection: None,
            connection_id,
            core,
            reconnect_attempts: 0,
            endpoint: Some(endpoint),
            _client_config: Some(client_config),
        })
    }

    async fn internal_connect(&mut self) -> Result<()> {
        let address_str = self.config.server_url
            .replace("quic://", "");
        
        let server_addr = match address_str.parse::<SocketAddr>() {
            Ok(addr) => addr,
            Err(_) => {
                lookup_host(&address_str)
                    .await
                    .map_err(|e| crate::common::error::FlareError::protocol_error(format!("DNS lookup failed: {}", e)))?
                    .next()
                    .ok_or_else(|| crate::common::error::FlareError::protocol_error(format!("No address found for {}", address_str)))?
            }
        };
        
        let hostname = if address_str.starts_with("localhost") || address_str.starts_with("127.0.0.1") {
            "localhost"
        } else {
            address_str
                .split(':')
                .next()
                .unwrap_or("localhost")
        };

        let endpoint = self.endpoint.as_ref().ok_or_else(|| {
            crate::common::error::FlareError::connection_failed("Endpoint not initialized".to_string())
        })?;

        let connecting = endpoint.connect(server_addr, hostname)
            .map_err(|e| {
                crate::common::error::FlareError::connection_failed(e.to_string())
            })?;
        
        let quinn_connection = timeout(
            self.config.connect_timeout,
            connecting,
        )
        .await
        .map_err(|_| {
            crate::common::error::FlareError::connection_timeout("Connection timeout".to_string())
        })?
        .map_err(|e| {
            crate::common::error::FlareError::connection_failed(e.to_string())
        })?;
        
        let (send, recv) = quinn_connection.open_bi()
            .await
            .map_err(|e| {
                crate::common::error::FlareError::connection_failed(e.to_string())
            })?;

        let transport = QUICTransport::new(send, recv);
        let connection: Box<dyn Connection> = Box::new(transport);
        let connection_arc = Arc::new(Mutex::new(connection));

        // 创建消息观察者，委托给 ClientCore
        let core_state_mgr = Arc::clone(&self.core.state_manager);
        let core_parser = self.core.parser.clone();
        let core_observers = Arc::clone(&self.core.observers);
        let core_clone = Arc::new(self.core.clone()); // 用于记录 PONG
        
        let message_observer = Arc::new(QUICMessageObserver {
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
struct QUICMessageObserver {
    state_manager: Arc<crate::client::connection::ConnectionStateManager>,
    parser: crate::common::MessageParser,
    observers: Arc<std::sync::Mutex<Vec<ArcObserver>>>,
    core: Arc<ClientCore>,
}

impl ConnectionObserver for QUICMessageObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
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
impl Client for QUICClient {
    async fn connect(&mut self) -> Result<()> {
        if !self.core.can_connect() {
            return Err(crate::common::error::FlareError::protocol_error(
                "Cannot connect: connection state is not ready".to_string()
            ));
        }

        self.core.state_manager.start_connecting();
        
        match self.internal_connect().await {
            Ok(()) => {
                Ok(())
            }
            Err(e) => {
                self.core.state_manager.set_failed();
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
