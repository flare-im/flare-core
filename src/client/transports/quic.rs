//! QUIC 客户端实现
//! 
//! 只处理协议层，连接状态管理、心跳、消息路由等功能委托给 ClientCore

use crate::client::transports::{Client, ClientCore};
use crate::client::transports::common::{ClientConnectionHelper, ClientMessageObserver};
use crate::client::config::ClientConfig;
use crate::common::error::{FlareError, Result};
use crate::common::protocol::Frame;
use crate::common::generate_id;
use crate::transport::connection::Connection;
use crate::transport::events::{ConnectionEvent, ArcObserver};
use crate::transport::quic::QUICTransport;
use async_trait::async_trait;
use quinn::Endpoint;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::timeout;
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
    _client_config: Option<quinn::ClientConfig>, // 保持 client_config 的生命周期
}

impl QUICClient {
    /// 创建新的 QUIC 客户端
    pub fn new(config: ClientConfig) -> Result<Self> {
        let connection_id = config
            .connection_id
            .clone()
            .unwrap_or_else(generate_id);
        let core = ClientCore::new(&config);
        
        let (endpoint, client_config) = Self::create_quic_endpoint()?;
        
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
        Self::with_core_and_endpoint(config, core, None)
    }
    
    /// 使用 ClientCore 和预创建的 Endpoint 创建（用于协议竞速优化）
    /// 
    /// 如果提供了 endpoint，则使用它；否则创建新的 endpoint
    pub fn with_core_and_endpoint(
        config: ClientConfig,
        core: ClientCore,
        endpoint_opt: Option<(Endpoint, quinn::ClientConfig)>,
    ) -> Result<Self> {
        let connection_id = config
            .connection_id
            .clone()
            .unwrap_or_else(generate_id);
        
        let (endpoint, client_config) = endpoint_opt
            .unwrap_or_else(|| Self::create_quic_endpoint().expect("Failed to create QUIC endpoint"));
        
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
    
    /// 创建 QUIC Endpoint（统一创建逻辑）
    /// 
    /// 公开方法，允许外部预创建 endpoint（用于协议竞速优化）
    pub fn create_quic_endpoint() -> Result<(Endpoint, quinn::ClientConfig)> {
        use crate::common::cert::create_client_config;
        use quinn::crypto::rustls::QuicClientConfig;
        
        let rustls_config = create_client_config()
            .map_err(|e| {
                FlareError::protocol_error(format!("Failed to create client TLS config: {}", e))
            })?;
        
        let rustls_config_arc = Arc::new(rustls_config);
        let quic_crypto_config = QuicClientConfig::try_from(rustls_config_arc)
            .map_err(|e| {
                FlareError::protocol_error(format!("Failed to create QUIC client config: {}", e))
            })?;
        
        let client_config = quinn::ClientConfig::new(Arc::new(quic_crypto_config));
        
        let mut endpoint = Endpoint::client("[::]:0".parse().unwrap())
            .map_err(|e| {
                FlareError::connection_failed(format!("Failed to create endpoint: {}", e))
            })?;
        
        endpoint.set_default_client_config(client_config.clone());
        
        Ok((endpoint, client_config))
    }

    /// 仅建立网络连接（不发送 CONNECT 消息）
    /// 
    /// 用于协议竞速：先建立网络连接，选择最快协议，然后再发送 CONNECT
    pub async fn establish_network_connection(&mut self) -> Result<Arc<Mutex<Box<dyn Connection>>>> {
        let connection = self.establish_quic_connection().await?;
        let connection_arc = Arc::new(Mutex::new(connection));
        // 保存连接，以便后续 connect() 时使用
        self.connection = Some(Arc::clone(&connection_arc));
        Ok(connection_arc)
    }
    
    /// 内部连接实现
    async fn internal_connect(&mut self) -> Result<()> {
        // 如果连接已建立（协议竞速场景），直接发送 CONNECT
        // 否则先建立网络连接
        let connection_arc = if self.connection.is_some() {
            // 连接已建立，直接使用
            self.connection.as_ref().unwrap().clone()
        } else {
            // 建立新的网络连接
            self.establish_network_connection().await?
        };
        
        // 设置连接和观察者（会发送 CONNECT 消息）
        self.setup_connection_with_observer(connection_arc.clone()).await?;
        
        // 启动心跳
        self.core.start_heartbeat(connection_arc.clone()).await;
        
        // 通知连接成功
        self.core.handle_connection_event(&ConnectionEvent::Connected);
        self.reconnect_attempts = 0;
        
        Ok(())
    }
    
    /// 建立 QUIC 连接
    async fn establish_quic_connection(
        &self,
    ) -> Result<Box<dyn Connection>> {
        // 解析服务器地址
        let (server_addr, hostname) = self.parse_server_address().await?;
        
        // 获取 endpoint
        let endpoint = self.endpoint.as_ref().ok_or_else(|| {
            FlareError::connection_failed("Endpoint not initialized".to_string())
        })?;
        
        // 建立连接
        let connecting = endpoint
            .connect(server_addr, &hostname)
            .map_err(|e| FlareError::connection_failed(e.to_string()))?;
        
        let quinn_connection = timeout(self.config.connect_timeout, connecting)
            .await
            .map_err(|_| {
                FlareError::connection_timeout("Connection timeout".to_string())
            })?
            .map_err(|e| FlareError::connection_failed(e.to_string()))?;
        
        // 打开双向流
        let (send, recv) = quinn_connection
            .open_bi()
            .await
            .map_err(|e| FlareError::connection_failed(e.to_string()))?;
        
        let transport = QUICTransport::new(send, recv);
        Ok(Box::new(transport))
    }
    
    /// 解析服务器地址
    async fn parse_server_address(&self) -> Result<(SocketAddr, String)> {
        let address_str = self.config.server_url.replace("quic://", "");
        
        // 尝试直接解析为 SocketAddr
        let server_addr = match address_str.parse::<SocketAddr>() {
            Ok(addr) => addr,
            Err(_) => {
                // DNS 解析
                lookup_host(&address_str)
                    .await
                    .map_err(|e| {
                        FlareError::protocol_error(format!("DNS lookup failed: {}", e))
                    })?
                    .next()
                    .ok_or_else(|| {
                        FlareError::protocol_error(format!("No address found for {}", address_str))
                    })?
            }
        };
        
        // 确定 hostname（返回 String 而不是 &str）
        let hostname = if address_str.starts_with("localhost") || address_str.starts_with("127.0.0.1") {
            "localhost".to_string()
        } else {
            address_str
                .split(':')
                .next()
                .unwrap_or("localhost")
                .to_string()
        };
        
        Ok((server_addr, hostname))
    }
    
    /// 设置连接和观察者
    async fn setup_connection_with_observer(
        &mut self,
        connection: Arc<Mutex<Box<dyn Connection>>>,
    ) -> Result<()> {
        // 创建消息观察者
        let core_clone = Arc::new(self.core.clone());
        let message_observer = Arc::new(ClientMessageObserver::new(core_clone));
        
        // 设置连接并发送 CONNECT 消息
        ClientConnectionHelper::setup_connection_and_send_connect(
            Arc::clone(&connection),
            &mut self.core,
            message_observer,
        )
        .await?;
        
        self.connection = Some(connection);
        Ok(())
    }

    /// 发送 Frame（内部实现）
    async fn send_frame_internal(&self, frame: &Frame) -> Result<()> {
        ClientConnectionHelper::send_frame_internal(
            &self.core,
            self.connection.as_ref(),
            frame,
        )
        .await
    }

    /// 尝试重连
    async fn try_reconnect(&mut self) -> Result<()> {
        // 检查重连次数限制
        if let Some(max) = self.config.max_reconnect_attempts {
            if self.reconnect_attempts >= max {
                return Err(FlareError::connection_failed(format!(
                    "Max reconnect attempts ({}) exceeded",
                    max
                )));
            }
        }
        
        self.core.state_manager.start_connecting();
        self.reconnect_attempts += 1;
        
        // 等待重连间隔
        tokio::time::sleep(self.config.reconnect_interval).await;
        
        // 关闭旧连接
        if let Some(conn) = self.connection.take() {
            let mut c = conn.lock().await;
            let _ = c.close().await;
        }
        
        // 执行连接
        self.internal_connect().await
    }
    
    /// 获取 ClientCore（用于外部访问）
    pub fn core(&self) -> &ClientCore {
        &self.core
    }
}

#[async_trait]
impl Client for QUICClient {
    async fn connect(&mut self) -> Result<()> {
        if !self.core.can_connect() {
            return Err(FlareError::protocol_error(
                "Cannot connect: connection state is not ready".to_string(),
            ));
        }

        self.core.state_manager.start_connecting();
        
        match self.internal_connect().await {
            Ok(()) => Ok(()),
            Err(e) => {
                self.core.state_manager.set_failed();
                if ClientConnectionHelper::can_reconnect(self.config.max_reconnect_attempts) {
                    self.try_reconnect().await
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn disconnect(&mut self) -> Result<()> {
        ClientConnectionHelper::disconnect_internal(
            self.connection.take(),
            &mut self.core,
        )
        .await
    }

    async fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        // 如果未连接，尝试重连
        if !self.is_connected()
            && ClientConnectionHelper::can_reconnect(self.config.max_reconnect_attempts)
        {
            if let Err(e) = self.try_reconnect().await {
                return Err(e);
            }
        }
        
        self.send_frame_internal(frame).await
    }

    fn is_connected(&self) -> bool {
        matches!(
            self.core.state(),
            crate::client::connection::ConnectionState::Connected
        ) && self.connection.is_some()
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
