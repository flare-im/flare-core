//! 默认客户端消息观察者
//!
//! 提供通用的客户端消息和事件处理逻辑

use crate::client::connection::ConnectionStateManager;
use crate::client::events::handler::ClientEventHandler;
use crate::client::transports::ClientCore;
use crate::transport::events::{ConnectionEvent, ConnectionObserver};
use std::sync::Arc;
use tracing::{debug, error};

/// 默认客户端消息观察者
///
/// 处理常见的系统命令（CONNECT_ACK, PONG, KICKED）和连接事件
/// 其他命令类型可以委托给可选的 `ClientEventHandler`
///
/// 注意：协商前的消息使用全局共享的 `PRE_NEGOTIATION_PARSER`，
/// 协商后的消息使用 `ClientCore` 中动态更新的 parser
pub struct DefaultClientMessageObserver {
    /// 客户端核心
    core: Arc<ClientCore>,
    /// 状态管理器
    state_manager: Arc<ConnectionStateManager>,
    /// 事件处理器（可选，用于自定义业务逻辑）
    event_handler: Option<Arc<dyn ClientEventHandler>>,
}

impl Clone for DefaultClientMessageObserver {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            state_manager: Arc::clone(&self.state_manager),
            event_handler: self.event_handler.clone(),
        }
    }
}

impl DefaultClientMessageObserver {
    /// 创建新的默认客户端消息观察者
    pub fn new(
        core: Arc<ClientCore>,
        state_manager: Arc<ConnectionStateManager>,
        event_handler: Option<Arc<dyn ClientEventHandler>>,
    ) -> Self {
        Self {
            core,
            state_manager,
            event_handler,
        }
    }
}

impl DefaultClientMessageObserver {
    /// 处理消息事件
    async fn handle_message_event(core: &Arc<ClientCore>, data: Vec<u8>) {
        // 直接转发给 ClientCore 处理
        // ClientCore 会根据协商状态使用正确的 parser
        core.handle_message(data).await;
    }

    /// 处理连接建立事件
    async fn handle_connected_event(
        state_manager: &Arc<ConnectionStateManager>,
        event_handler: Option<Arc<dyn ClientEventHandler>>,
    ) {
        debug!("[DefaultClientObserver] Connection established");
        state_manager.set_connected();

        // 通知事件处理器
        if let Some(handler) = event_handler {
            let event = ConnectionEvent::Connected;
            tokio::spawn(async move {
                let _ = handler.handle_connection_event(&event).await;
            });
        }
    }

    /// 处理连接断开事件
    async fn handle_disconnected_event(
        state_manager: &Arc<ConnectionStateManager>,
        event_handler: Option<Arc<dyn ClientEventHandler>>,
        reason: String,
    ) {
        debug!(
            "[DefaultClientObserver] Connection disconnected: {}",
            reason
        );
        state_manager.set_disconnected();

        // 通知事件处理器
        if let Some(handler) = event_handler {
            let event = ConnectionEvent::Disconnected(reason);
            tokio::spawn(async move {
                let _ = handler.handle_connection_event(&event).await;
            });
        }
    }

    /// 处理错误事件
    async fn handle_error_event(
        state_manager: &Arc<ConnectionStateManager>,
        event_handler: Option<Arc<dyn ClientEventHandler>>,
        error: crate::common::error::FlareError,
    ) {
        error!("[DefaultClientObserver] Connection error: {:?}", error);
        state_manager.set_failed();

        // 通知事件处理器
        if let Some(handler) = event_handler {
            let event = ConnectionEvent::Error(error);
            tokio::spawn(async move {
                let _ = handler.handle_connection_event(&event).await;
            });
        }
    }
}

impl ConnectionObserver for DefaultClientMessageObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
                let core = Arc::clone(&self.core);
                let data_clone = data.clone();
                tokio::spawn(async move {
                    Self::handle_message_event(&core, data_clone).await;
                });
            }
            ConnectionEvent::Connected => {
                let state_manager = Arc::clone(&self.state_manager);
                let event_handler = self.event_handler.clone();
                tokio::spawn(async move {
                    Self::handle_connected_event(&state_manager, event_handler).await;
                });
            }
            ConnectionEvent::Disconnected(reason) => {
                let state_manager = Arc::clone(&self.state_manager);
                let event_handler = self.event_handler.clone();
                let reason_clone = reason.clone();
                tokio::spawn(async move {
                    Self::handle_disconnected_event(&state_manager, event_handler, reason_clone)
                        .await;
                });
            }
            ConnectionEvent::Error(e) => {
                let state_manager = Arc::clone(&self.state_manager);
                let event_handler = self.event_handler.clone();
                let error_clone = e.clone();
                tokio::spawn(async move {
                    Self::handle_error_event(&state_manager, event_handler, error_clone).await;
                });
            }
        }
    }
}
