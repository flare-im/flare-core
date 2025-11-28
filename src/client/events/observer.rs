//! 默认客户端消息观察者
//! 
//! 提供通用的客户端消息和事件处理逻辑

use crate::client::transports::ClientCore;
use crate::client::connection::ConnectionStateManager;
use crate::transport::events::{ConnectionEvent, ConnectionObserver};
use crate::client::events::handler::ClientEventHandler;
use crate::common::MessageParser;
use std::sync::Arc;
use tracing::{debug, error, warn};

/// 默认客户端消息观察者
/// 
/// 处理常见的系统命令（CONNECT_ACK, PONG, KICKED）和连接事件
/// 其他命令类型可以委托给可选的 `ClientEventHandler`
pub struct DefaultClientMessageObserver {
    /// 客户端核心
    core: Arc<ClientCore>,
    /// 状态管理器
    state_manager: Arc<ConnectionStateManager>,
    /// 事件处理器（可选，用于自定义业务逻辑）
    event_handler: Option<Arc<dyn ClientEventHandler>>,
    /// 消息解析器
    parser: MessageParser,
}

impl Clone for DefaultClientMessageObserver {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            state_manager: Arc::clone(&self.state_manager),
            event_handler: self.event_handler.clone(),
            parser: self.parser.clone(),
        }
    }
}

impl DefaultClientMessageObserver {
    /// 创建新的默认客户端消息观察者
    pub fn new(
        core: Arc<ClientCore>,
        state_manager: Arc<ConnectionStateManager>,
        parser: MessageParser,
        event_handler: Option<Arc<dyn ClientEventHandler>>,
    ) -> Self {
        Self {
            core,
            state_manager,
            event_handler,
            parser,
        }
    }
}

impl ConnectionObserver for DefaultClientMessageObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
                // 解析消息
                let frame = match self.parser.parse(data) {
                    Ok(frame) => frame,
                    Err(e) => {
                        warn!("Failed to parse message: {}", e);
                        return;
                    }
                };
                
                // 检查是否是系统命令（CONNECT_ACK, PONG, KICKED 等）
                if let Some(cmd) = &frame.command {
                    if let Some(crate::common::protocol::flare::core::commands::command::Type::System(_sys_cmd)) = &cmd.r#type {
                        // CONNECT_ACK, PONG, KICKED 等系统命令由 ClientCore 处理
                        // 这里只需要转发给 ClientCore
                        let core = Arc::clone(&self.core);
                        let data_clone = data.clone();
                        tokio::spawn(async move {
                            core.handle_message(data_clone).await;
                        });
                        return;
                    }
                }
                
                // 其他命令（Message, Notification, Custom）也由 ClientCore 处理
                // ClientCore 会调用 event_handler 和 message_router
                let core = Arc::clone(&self.core);
                let data_clone = data.clone();
                tokio::spawn(async move {
                    core.handle_message(data_clone).await;
                });
            }
            ConnectionEvent::Connected => {
                debug!("[DefaultClientObserver] Connection established");
                self.state_manager.set_connected();
                
                // 通知事件处理器
                if let Some(ref handler) = self.event_handler {
                    let handler_clone = Arc::clone(handler);
                    let event = event.clone();
                    tokio::spawn(async move {
                        let _ = handler_clone.handle_connection_event(&event).await;
                    });
                }
            }
            ConnectionEvent::Disconnected(reason) => {
                debug!("[DefaultClientObserver] Connection disconnected: {}", reason);
                self.state_manager.set_disconnected();
                
                // 通知事件处理器
                if let Some(ref handler) = self.event_handler {
                    let handler_clone = Arc::clone(handler);
                    let event = event.clone();
                    tokio::spawn(async move {
                        let _ = handler_clone.handle_connection_event(&event).await;
                    });
    }
}
            ConnectionEvent::Error(e) => {
                error!("[DefaultClientObserver] Connection error: {:?}", e);
                self.state_manager.set_failed();
                
                // 通知事件处理器
                if let Some(ref handler) = self.event_handler {
                    let handler_clone = Arc::clone(handler);
                    let event = event.clone();
                    tokio::spawn(async move {
                        let _ = handler_clone.handle_connection_event(&event).await;
                    });
                }
            }
        }
    }
}
