//! WebSocket 客户端实现
//!
//! 只处理协议层，连接状态管理、心跳、消息路由等功能委托给 ClientCore

use crate::client::config::ClientConfig;
use crate::client::transports::common::{ClientConnectionHelper, ClientMessageObserver};
use crate::client::transports::{Client, ClientCore};
#[cfg(not(target_arch = "wasm32"))]
use crate::common::cert::create_client_config_with_tls;
use crate::common::error::{FlareError, Result};
use crate::common::generate_id;
use crate::common::platform::{sleep, timeout};
use crate::common::protocol::Frame;
use crate::transport::connection::Connection;
use crate::transport::events::{ArcObserver, ConnectionEvent};
#[cfg(not(target_arch = "wasm32"))]
use crate::transport::websocket::WebSocketTransport;
#[cfg(target_arch = "wasm32")]
use crate::transport::websocket_wasm::WebSocketTransport;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
#[cfg(not(target_arch = "wasm32"))]
use tokio_tungstenite::{Connector, connect_async_tls_with_config};

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

    /// 仅建立网络连接（不发送 CONNECT 消息）
    ///
    /// 用于协议竞速：先建立网络连接，选择最快协议，然后再发送 CONNECT
    pub async fn establish_network_connection(
        &mut self,
    ) -> Result<Arc<Mutex<Box<dyn Connection>>>> {
        let connection = self.establish_websocket_connection().await?;
        let connection_arc = Arc::new(Mutex::new(connection));
        // 保存连接，以便后续 connect() 时使用
        self.connection = Some(Arc::clone(&connection_arc));
        Ok(connection_arc)
    }

    /// 内部连接实现
    async fn internal_connect(&mut self) -> Result<()> {
        // 建立 WebSocket 连接
        let connection_arc = self.establish_network_connection().await?;

        // 设置连接和观察者（会发送 CONNECT 消息）
        self.setup_connection_with_observer(connection_arc.clone())
            .await?;

        // 心跳在 CONNECT_ACK 协商完成后由 ClientCore 启动

        // 通知连接成功
        self.core
            .handle_connection_event(&ConnectionEvent::Connected);
        self.reconnect_attempts = 0;

        Ok(())
    }

    /// 建立 WebSocket 连接
    async fn establish_websocket_connection(&self) -> Result<Box<dyn Connection>> {
        let url_str = self
            .config
            .get_protocol_url(&crate::common::config_types::TransportProtocol::WebSocket);

        #[cfg(not(target_arch = "wasm32"))]
        {
            // 入站消息上限 10MB，防超大帧 OOM；与 TCP/QUIC MAX_FRAME_LENGTH 及服务端
            // max_message_size 对齐（tungstenite 默认 64MB 过大，也削弱 gzip 解压炸弹面）。
            let ws_config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
                .max_message_size(Some(10 * 1024 * 1024))
                .max_frame_size(Some(10 * 1024 * 1024));
            let tls_connector = self.tls_connector(&url_str)?;
            let ws_stream_result = timeout(
                self.config.connect_timeout,
                connect_async_tls_with_config(&url_str, Some(ws_config), false, tls_connector),
            )
            .await
            .map_err(|_| FlareError::connection_timeout("Connection timeout".to_string()))?;

            let (ws_stream, _) =
                ws_stream_result.map_err(|e| FlareError::connection_failed(e.to_string()))?;

            let transport = WebSocketTransport::new(ws_stream);
            Ok(Box::new(transport))
        }

        #[cfg(target_arch = "wasm32")]
        {
            let transport = timeout(
                self.config.connect_timeout,
                WebSocketTransport::connect(&url_str),
            )
            .await
            .map_err(|_| FlareError::connection_timeout("Connection timeout".to_string()))??;
            Ok(Box::new(transport))
        }
    }

    /// 设置连接和观察者
    async fn setup_connection_with_observer(
        &mut self,
        connection: Arc<Mutex<Box<dyn Connection>>>,
    ) -> Result<()> {
        // 创建消息观察者（使用 Arc 包装，共享同一个 core 实例）
        // 注意：ClientCore::clone 现在会共享 client_connection，所以可以安全使用
        let core_arc = Arc::new(self.core.clone());
        let message_observer = Arc::new(ClientMessageObserver::new(core_arc));

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
        ClientConnectionHelper::send_frame_internal(&self.core, self.connection.as_ref(), frame)
            .await
    }

    /// 尝试重连
    async fn try_reconnect(&mut self) -> Result<()> {
        // 检查重连次数限制
        if let Some(max) = self.config.max_reconnect_attempts
            && self.reconnect_attempts >= max
        {
            return Err(FlareError::connection_failed(format!(
                "Max reconnect attempts ({}) exceeded",
                max
            )));
        }

        self.core.state_manager.start_connecting();
        self.reconnect_attempts += 1;

        // 等待重连间隔
        sleep(self.config.reconnect_interval).await;

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

    #[cfg(not(target_arch = "wasm32"))]
    fn tls_connector(&self, url: &str) -> Result<Option<Connector>> {
        if url.starts_with("ws://") {
            return Ok(None);
        }
        if !self.config.tls.requires_custom_client_tls() {
            return Ok(None);
        }
        if !url.starts_with("wss://") {
            return Err(FlareError::connection_failed(format!(
                "WebSocket URL must start with ws:// or wss://, got: {url}"
            )));
        }
        let tls_config = create_client_config_with_tls(&self.config.tls)
            .map_err(|error| FlareError::connection_failed(error.to_string()))?;
        Ok(Some(Connector::Rustls(Arc::new(tls_config))))
    }

    /// 发送消息并等待服务端响应（按 Frame.message_id 匹配）
    pub async fn send_frame_and_wait(
        &mut self,
        frame: &Frame,
        timeout_duration: std::time::Duration,
    ) -> Result<Frame> {
        if frame.message_id.is_empty() {
            return Err(FlareError::protocol_error(
                "message_id is empty".to_string(),
            ));
        }

        let rx = self.core.register_pending_response(&frame.message_id).await;
        self.send_frame(frame).await?;

        match timeout(timeout_duration, rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => {
                self.core.cancel_pending_response(&frame.message_id).await;
                Err(FlareError::protocol_error(
                    "Response channel closed".to_string(),
                ))
            }
            Err(_) => {
                self.core.cancel_pending_response(&frame.message_id).await;
                Err(FlareError::protocol_error(format!(
                    "Response timeout for message_id {}",
                    frame.message_id
                )))
            }
        }
    }
}

#[cfg(target_arch = "wasm32")]
unsafe impl Send for WebSocketClient {}
#[cfg(target_arch = "wasm32")]
unsafe impl Sync for WebSocketClient {}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::common::config_types::TlsConfig;
    use std::path::PathBuf;

    #[test]
    fn plain_ws_ignores_custom_client_tls() {
        let mut config = ClientConfig::new("ws://127.0.0.1:60051/ws".to_string()).websocket();
        config.tls = TlsConfig::default().with_ca_cert(PathBuf::from("/tmp/flare-ca.crt"));
        let client = WebSocketClient::new(config);

        let connector = client
            .tls_connector("ws://127.0.0.1:60051/ws")
            .expect("plain ws must not require a TLS connector");

        assert!(connector.is_none());
    }

    #[test]
    fn secure_wss_uses_custom_client_tls() {
        let mut config = ClientConfig::new("wss://127.0.0.1:60051/ws".to_string()).websocket();
        config.tls = TlsConfig::default()
            .with_spki_sha256_pin("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
        let client = WebSocketClient::new(config);

        let connector = client
            .tls_connector("wss://127.0.0.1:60051/ws")
            .expect("wss should accept custom TLS settings");

        assert!(connector.is_some());
    }
}

#[async_trait]
impl Client for WebSocketClient {
    async fn connect(&mut self) -> Result<()> {
        if !self.core.can_connect() {
            return Err(FlareError::protocol_error(
                "Cannot connect: state is unavailable".to_string(),
            ));
        }

        self.core.state_manager.start_connecting();

        match self.internal_connect().await {
            Ok(()) => Ok(()),
            Err(e) => {
                self.core.state_manager.set_failed();
                // 如果允许重连，尝试重连
                if ClientConnectionHelper::can_reconnect(self.config.max_reconnect_attempts) {
                    self.try_reconnect().await
                } else {
                    Err(e)
                }
            }
        }
    }

    fn set_disconnect_requested(&mut self, value: bool) {
        self.core.set_disconnect_requested(value);
    }

    async fn disconnect(&mut self) -> Result<()> {
        ClientConnectionHelper::disconnect_internal(self.connection.take(), &mut self.core).await
    }

    async fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        // 如果未连接，尝试重连
        if !self.is_connected()
            && ClientConnectionHelper::can_reconnect(self.config.max_reconnect_attempts)
        {
            self.try_reconnect().await?;
        }

        self.send_frame_internal(frame).await
    }

    fn is_connected(&self) -> bool {
        matches!(
            self.core.state(),
            crate::client::connection::ConnectionState::Connected
        ) && self.connection.is_some()
            && self.core.is_negotiation_completed()
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
