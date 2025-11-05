//! 默认客户端消息观察者
//! 
//! 提供通用的客户端消息和事件处理逻辑

use crate::client::transports::ClientCore;
use crate::client::connection::ConnectionStateManager;
use std::sync::Arc;

/// 默认客户端消息观察者
/// 
/// 处理常见的系统命令（CONNECT_ACK, PONG, KICKED）和连接事件
/// 其他命令类型可以委托给可选的 `ClientEventHandler`
/// 
/// 注意：这个结构目前未使用，消息处理直接在 ClientMessageObserver/QUICMessageObserver 中调用 ClientCore::handle_message
pub struct DefaultClientMessageObserver {
    /// 客户端核心
    _core: Arc<ClientCore>,
    /// 状态管理器
    _state_manager: Arc<ConnectionStateManager>,
}

impl DefaultClientMessageObserver {
    /// 创建新的默认客户端消息观察者
    pub fn new(
        core: Arc<ClientCore>,
        state_manager: Arc<ConnectionStateManager>,
    ) -> Self {
        Self {
            _core: core,
            _state_manager: state_manager,
        }
    }
}

