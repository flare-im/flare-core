//! 客户端构建器通用组件

use crate::client::Client;
use crate::common::config_types::TransportProtocol;
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
pub struct ClientWrapper {
    #[cfg(not(target_arch = "wasm32"))]
    client: Arc<Mutex<HybridClient>>,
    #[cfg(target_arch = "wasm32")]
    client: Arc<Mutex<WebSocketClient>>,
}

impl ClientWrapper {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(client: HybridClient) -> Self {
        Self {
            client: Arc::new(Mutex::new(client)),
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn new(client: WebSocketClient) -> Self {
        Self {
            client: Arc::new(Mutex::new(client)),
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
        let mut client = self.client.lock().await;
        client.send_frame(frame).await
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
