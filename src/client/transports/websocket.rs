//! WebSocket 客户端实现
//!
//! 只处理协议层，连接状态管理、心跳、消息路由等功能委托给 ClientCore

use crate::client::config::ClientConfig;
use crate::client::transports::common::{ClientConnectionHelper, ClientMessageObserver};
use crate::client::transports::{Client, ClientCore};
use crate::common::error::{FlareError, Result};
use crate::common::generate_id;
use crate::common::protocol::Frame;
use crate::transport::connection::Connection;
use crate::transport::events::{ArcObserver, ConnectionEvent};
use crate::transport::websocket::WebSocketTransport;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::timeout;
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

        // 启动心跳
        self.core.start_heartbeat(connection_arc.clone()).await;

        // 通知连接成功
        self.core
            .handle_connection_event(&ConnectionEvent::Connected);
        self.reconnect_attempts = 0;

        Ok(())
    }

    /// 建立 WebSocket 连接
    async fn establish_websocket_connection(&self) -> Result<Box<dyn Connection>> {
        let url_str = &self.config.server_url;

        let ws_stream_result = timeout(self.config.connect_timeout, connect_async(url_str))
            .await
            .map_err(|_| FlareError::connection_timeout("Connection timeout".to_string()))?;

        let (ws_stream, _) =
            ws_stream_result.map_err(|e| FlareError::connection_failed(e.to_string()))?;

        let transport = WebSocketTransport::new(ws_stream);
        Ok(Box::new(transport))
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
