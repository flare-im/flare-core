//! 传输层事件模块
//!
//! 定义连接事件类型和观察者模式接口，用于在传输层和上层之间传递事件

use crate::common::error::FlareError;
use std::sync::Arc;

/// 连接事件
///
/// 表示连接上发生的各种事件，由 `ConnectionObserver` 使用以响应连接状态和数据接收。
#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    /// 连接成功建立时触发
    Connected,
    /// 连接关闭时触发
    ///
    /// 参数提供断开连接的原因
    Disconnected(String),
    /// 收到新消息时触发
    ///
    /// 负载是字节向量
    Message(Vec<u8>),
    /// 连接上发生非致命错误时触发
    Error(FlareError),
}

impl ConnectionEvent {
    /// 检查是否为连接事件
    pub fn is_connected(&self) -> bool {
        matches!(self, Self::Connected)
    }

    /// 检查是否为断开连接事件
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnected(_))
    }

    /// 检查是否为消息事件
    pub fn is_message(&self) -> bool {
        matches!(self, Self::Message(_))
    }

    /// 检查是否为错误事件
    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error(_))
    }

    /// 获取断开连接的原因（如果是断开连接事件）
    pub fn disconnect_reason(&self) -> Option<&str> {
        match self {
            Self::Disconnected(reason) => Some(reason),
            _ => None,
        }
    }

    /// 获取消息数据（如果是消息事件）
    pub fn message_data(&self) -> Option<&[u8]> {
        match self {
            Self::Message(data) => Some(data),
            _ => None,
        }
    }

    /// 获取错误（如果是错误事件）
    pub fn error(&self) -> Option<&FlareError> {
        match self {
            Self::Error(err) => Some(err),
            _ => None,
        }
    }
}

/// 连接事件观察者
///
/// 实现此 trait 以响应连接建立、断开和接收消息等事件。
/// 观察者通过 `Connection` 实例注册。
pub trait ConnectionObserver: Send + Sync {
    /// 当连接上发生事件时由 `Connection` 调用
    ///
    /// # 参数
    /// - `event`: 发生的事件
    fn on_event(&self, event: &ConnectionEvent);
}

/// 线程安全的、引用计数的观察者类型别名
pub type ArcObserver = Arc<dyn ConnectionObserver>;

// ============================================================================
// 便利观察者实现
// ============================================================================

/// 空观察者（不执行任何操作）
///
/// 用于需要观察者但不需要实际处理的情况
pub struct NoOpObserver;

impl ConnectionObserver for NoOpObserver {
    fn on_event(&self, _event: &ConnectionEvent) {
        // 不执行任何操作
    }
}

impl NoOpObserver {
    /// 创建新的空观察者
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> ArcObserver {
        Arc::new(Self)
    }
}

/// 日志观察者（记录所有事件到日志）
///
/// 用于调试和监控连接事件
pub struct LoggingObserver {
    prefix: String,
}

impl LoggingObserver {
    /// 创建新的日志观察者
    ///
    /// # 参数
    /// - `prefix`: 日志前缀，用于标识不同的观察者实例
    #[allow(clippy::new_ret_no_self)]
    pub fn new(prefix: impl Into<String>) -> ArcObserver {
        Arc::new(Self {
            prefix: prefix.into(),
        })
    }
}

impl ConnectionObserver for LoggingObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Connected => {
                tracing::info!("[{}] Connection established", self.prefix);
            }
            ConnectionEvent::Disconnected(reason) => {
                tracing::info!("[{}] Connection disconnected: {}", self.prefix, reason);
            }
            ConnectionEvent::Message(data) => {
                tracing::debug!("[{}] Message received: {} bytes", self.prefix, data.len());
            }
            ConnectionEvent::Error(err) => {
                tracing::error!("[{}] Connection error: {:?}", self.prefix, err);
            }
        }
    }
}

/// 组合观察者（将事件转发给多个观察者）
///
/// 用于需要多个观察者处理同一事件的情况
pub struct CompositeObserver {
    observers: Vec<ArcObserver>,
}

impl CompositeObserver {
    /// 创建新的组合观察者
    pub fn new() -> Self {
        Self {
            observers: Vec::new(),
        }
    }

    /// 添加观察者
    pub fn add(&mut self, observer: ArcObserver) {
        self.observers.push(observer);
    }

    /// 创建 Arc 包装的组合观察者
    pub fn into_arc(self) -> ArcObserver {
        Arc::new(self)
    }
}

impl ConnectionObserver for CompositeObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        for observer in &self.observers {
            observer.on_event(event);
        }
    }
}

impl Default for CompositeObserver {
    fn default() -> Self {
        Self::new()
    }
}
