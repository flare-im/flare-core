//! 客户端构建器通用组件

use crate::client::Client;
use crate::common::config_types::{HeartbeatAppState, HeartbeatConfig, TransportProtocol};
use crate::common::error::Result;
use crate::common::protocol::Frame;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[cfg(not(target_arch = "wasm32"))]
use crate::client::HybridClient;

#[cfg(target_arch = "wasm32")]
use crate::client::WebSocketClient;

/// 客户端包装器 — native 使用 HybridClient，WASM 使用 WebSocketClient。
#[derive(Clone)]
pub struct ClientWrapper {
    #[cfg(not(target_arch = "wasm32"))]
    client: Arc<Mutex<HybridClient>>,
    #[cfg(target_arch = "wasm32")]
    client: Arc<Mutex<WebSocketClient>>,
    reconnect_gate: Arc<Mutex<()>>,
}

impl ClientWrapper {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(client: HybridClient) -> Self {
        Self {
            client: Arc::new(Mutex::new(client)),
            reconnect_gate: Arc::new(Mutex::new(())),
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn new(client: WebSocketClient) -> Self {
        Self {
            client: Arc::new(Mutex::new(client)),
            reconnect_gate: Arc::new(Mutex::new(())),
        }
    }

    pub async fn connect(&self) -> Result<()> {
        let mut client = self.client.lock().await;
        client.connect().await
    }

    pub async fn disconnect(&self) -> Result<()> {
        let mut client = self.client.lock().await;
        client.disconnect().await
    }

    pub async fn send_frame_and_wait(&self, frame: &Frame, timeout: Duration) -> Result<Frame> {
        let mut client = self.client.lock().await;
        client.send_frame_and_wait(frame, timeout).await
    }

    pub async fn send_frame(&self, frame: &Frame) -> Result<()> {
        self.ensure_ready_for_send().await?;
        let mut client = self.client.lock().await;
        client.send_frame(frame).await
    }

    async fn ensure_ready_for_send(&self) -> Result<()> {
        if self.is_send_ready().await {
            return Ok(());
        }

        let _single_flight = self.reconnect_gate.lock().await;
        if self.is_send_ready().await {
            return Ok(());
        }

        {
            let mut client = self.client.lock().await;
            if !client.is_connected() {
                client.connect().await?;
            }
        }

        let core = {
            let client = self.client.lock().await;
            client.core().clone()
        };
        core.wait_for_negotiation(Duration::from_secs(10)).await
    }

    async fn is_send_ready(&self) -> bool {
        let client = self.client.lock().await;
        client.is_connected()
            && client.core().can_send()
            && client.core().is_negotiation_completed()
    }

    pub async fn add_observer(&self, observer: crate::transport::events::ArcObserver) {
        let mut client = self.client.lock().await;
        client.add_observer(observer);
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn set_event_handler(
        &self,
        handler: Option<Arc<dyn crate::client::events::handler::ClientEventHandler>>,
    ) {
        let mut client = self.client.lock().await;
        client.core_mut().set_event_handler(handler);
    }

    pub async fn is_connected_async(&self) -> bool {
        let client = self.client.lock().await;
        client.is_connected()
    }

    pub async fn connection_id_async(&self) -> Option<String> {
        let client = self.client.lock().await;
        client.connection_id()
    }

    pub fn active_protocol(&self) -> TransportProtocol {
        #[cfg(not(target_arch = "wasm32"))]
        {
            crate::client::runtime::run_client_async(async {
                let client = self.client.lock().await;
                client.active_protocol()
            })
        }
        #[cfg(target_arch = "wasm32")]
        {
            TransportProtocol::WebSocket
        }
    }

    pub async fn with_core<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&crate::client::transports::ClientCore) -> R,
    {
        let client = self.client.lock().await;
        f(client.core())
    }

    pub async fn update_heartbeat_config(&self, config: HeartbeatConfig) {
        self.with_core(|core| {
            core.update_heartbeat_config(|current| {
                *current = config;
            });
        })
        .await;
    }

    pub async fn set_heartbeat_app_state(&self, state: HeartbeatAppState) {
        self.with_core(|core| {
            core.set_heartbeat_app_state(state);
        })
        .await;
    }

    pub async fn set_heartbeat_nat_timeout(&self, timeout: Option<Duration>) {
        self.with_core(|core| {
            core.set_heartbeat_nat_timeout(timeout);
        })
        .await;
    }

    pub async fn heartbeat_effective_interval(&self) -> Duration {
        self.with_core(|core| core.heartbeat_effective_interval())
            .await
    }

    /// 获取协商后的消息解析器快照（连接完成后用于解析 `ConnectionEvent::Message` 原始字节）
    pub async fn parser_snapshot(&self) -> crate::common::MessageParser {
        let client = self.client.lock().await;
        client.core().parser.lock().await.clone()
    }

    /// 等待 CONNECT_ACK 协商完成（不长期占用客户端锁，便于 WASM 侧 drain 入站帧）
    pub async fn wait_for_negotiation(&self, timeout: Duration) -> Result<()> {
        let core = {
            let client = self.client.lock().await;
            client.core().clone()
        };
        core.wait_for_negotiation(timeout).await
    }
}
