//! QUIC 客户端实现

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
use crate::transport::quic::QUICTransport;
use async_trait::async_trait;
use quinn::Endpoint;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tokio::net::lookup_host;

// 使用标准的 rustls 0.23 API 进行证书验证
// 服务器证书已添加到客户端根证书存储中

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
    _client_config: Option<quinn::ClientConfig>, // 保留用于将来的 TLS 配置
}

impl QUICClient {
    /// 创建新的 QUIC 客户端
    pub fn new(config: ClientConfig) -> Result<Self> {
        let parser = MessageParser::new(config.serialization_format, config.compression);
        let connection_id = config.connection_id.clone().unwrap_or_else(generate_id);
        
        // 配置 rustls ClientConfig
        // 注意：在 rustls 0.23 中使用新 API
        // 使用标准的证书验证，将服务器证书添加到根证书存储
        
        use crate::common::cert::create_client_config;
        
        // 创建使用标准证书验证的客户端配置
        let rustls_config = create_client_config()
            .map_err(|e| crate::common::error::FlareError::protocol_error(
                format!("Failed to create client TLS config: {}", e)
            ))?;
        
        // 创建 quinn ClientConfig
        // quinn 0.11 需要使用 QuicClientConfig 包装 rustls::ClientConfig
        use quinn::crypto::rustls::QuicClientConfig;
        let rustls_config_arc = Arc::new(rustls_config);
        
        // QuicClientConfig 实现了 From<Arc<rustls::ClientConfig>>
        let quic_crypto_config = QuicClientConfig::try_from(rustls_config_arc)
            .map_err(|e| crate::common::error::FlareError::protocol_error(
                format!("Failed to create QUIC client config: {}", e)
            ))?;
        
        // quinn::ClientConfig::new 接受实现了 crypto::ClientConfig 的类型
        let client_config = quinn::ClientConfig::new(Arc::new(quic_crypto_config));
        
        // 创建 QUIC endpoint 并设置默认客户端配置
        let mut endpoint = Endpoint::client("[::]:0".parse().unwrap())
            .map_err(|e| crate::common::error::FlareError::connection_failed(format!("Failed to create endpoint: {}", e)))?;
        
        // 设置默认客户端配置（quinn 0.11 API）
        endpoint.set_default_client_config(client_config.clone());

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
            _client_config: Some(client_config),
        })
    }

    /// 创建并连接
    pub async fn connect_with_config(config: ClientConfig) -> Result<Self> {
        let mut client = Self::new(config)?;
        client.connect().await?;
        Ok(client)
    }

    async fn internal_connect(&mut self) -> Result<()> {
        // 解析 URL，支持 hostname:port 格式
        let address_str = self.config.server_url
            .replace("quic://", "");
        
        // 尝试直接解析为 SocketAddr，如果失败则进行 DNS 解析
        let server_addr = match address_str.parse::<SocketAddr>() {
            Ok(addr) => addr,
            Err(_) => {
                // 解析 hostname:port 格式
                lookup_host(&address_str)
                    .await
                    .map_err(|e| crate::common::error::FlareError::protocol_error(format!("DNS lookup failed: {}", e)))?
                    .next()
                    .ok_or_else(|| crate::common::error::FlareError::protocol_error(format!("No address found for {}", address_str)))?
            }
        };
        
        // 提取 hostname 用于 SNI
        // 注意：服务器证书是 "localhost"，所以这里使用 "localhost" 以确保证书验证通过
        let hostname = if address_str.starts_with("localhost") || address_str.starts_with("127.0.0.1") {
            "localhost"
        } else {
            address_str
                .split(':')
                .next()
                .unwrap_or("localhost")
        };
        
        eprintln!("[QUIC Client] Connecting to {} with hostname {}", server_addr, hostname);

        let endpoint = self.endpoint.as_ref().ok_or_else(|| {
            crate::common::error::FlareError::connection_failed("Endpoint not initialized".to_string())
        })?;

        // 连接服务器
        eprintln!("[QUIC Client] Starting connection...");
        let connecting = endpoint.connect(server_addr, hostname)
            .map_err(|e| {
                eprintln!("[QUIC Client] Failed to create connecting: {}", e);
                crate::common::error::FlareError::connection_failed(e.to_string())
            })?;
        
        eprintln!("[QUIC Client] Waiting for connection handshake...");

        let quinn_connection = timeout(
            self.config.connect_timeout,
            connecting,
        )
        .await
        .map_err(|_| {
            eprintln!("[QUIC Client] Connection handshake timeout after {:?}", self.config.connect_timeout);
            crate::common::error::FlareError::connection_timeout("Connection timeout".to_string())
        })?
        .map_err(|e| {
            eprintln!("[QUIC Client] Connection handshake failed: {}", e);
            crate::common::error::FlareError::connection_failed(e.to_string())
        })?;
        
        eprintln!("[QUIC Client] Connection established, opening stream...");

        // 打开双向流
        eprintln!("[QUIC Client] Opening bidirectional stream...");
        let (send, recv) = quinn_connection.open_bi()
            .await
            .map_err(|e| {
                eprintln!("[QUIC Client] Failed to open bidirectional stream: {}", e);
                crate::common::error::FlareError::connection_failed(e.to_string())
            })?;
        
        eprintln!("[QUIC Client] Bidirectional stream opened successfully");

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
                // 解析消息以验证格式，然后通知所有观察者
                if self.parser.parse(data).is_ok() {
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

