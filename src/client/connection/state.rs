//! 连接状态管理模块
//! 
//! 定义连接状态枚举和状态转换逻辑

use std::sync::{Arc, Mutex};
use std::time::Instant;

/// 连接状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// 未连接
    Disconnected,
    /// 正在连接
    Connecting,
    /// 已连接
    Connected,
    /// 正在断开
    Disconnecting,
    /// 连接失败
    Failed,
    /// 重连中
    Reconnecting,
}

impl ConnectionState {
    /// 检查是否可以发送消息
    pub fn can_send(&self) -> bool {
        matches!(self, ConnectionState::Connected)
    }

    /// 检查是否可以连接
    pub fn can_connect(&self) -> bool {
        matches!(self, ConnectionState::Disconnected | ConnectionState::Failed)
    }

    /// 检查是否正在连接过程中
    pub fn is_connecting(&self) -> bool {
        matches!(self, ConnectionState::Connecting | ConnectionState::Reconnecting)
    }
}

/// 连接状态管理器
pub struct ConnectionStateManager {
    state: Arc<Mutex<ConnectionState>>,
    state_changed_at: Arc<Mutex<Instant>>,
    connect_started_at: Arc<Mutex<Option<Instant>>>,
}

impl Clone for ConnectionStateManager {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            state_changed_at: Arc::clone(&self.state_changed_at),
            connect_started_at: Arc::clone(&self.connect_started_at),
        }
    }
}

impl ConnectionStateManager {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ConnectionState::Disconnected)),
            state_changed_at: Arc::new(Mutex::new(Instant::now())),
            connect_started_at: Arc::new(Mutex::new(None)),
        }
    }

    pub fn set_state(&self, new_state: ConnectionState) {
        if let Ok(mut state) = self.state.lock() {
            *state = new_state;
        }
        if let Ok(mut changed_at) = self.state_changed_at.lock() {
            *changed_at = Instant::now();
        }
    }

    pub fn get_state(&self) -> ConnectionState {
        self.state.lock().map(|s| *s).unwrap_or(ConnectionState::Disconnected)
    }

    pub fn start_connecting(&self) {
        self.set_state(ConnectionState::Connecting);
        if let Ok(mut started_at) = self.connect_started_at.lock() {
            *started_at = Some(Instant::now());
        }
    }

    pub fn set_connected(&self) {
        self.set_state(ConnectionState::Connected);
        if let Ok(mut started_at) = self.connect_started_at.lock() {
            *started_at = None;
        }
    }

    pub fn set_disconnected(&self) {
        self.set_state(ConnectionState::Disconnected);
        if let Ok(mut started_at) = self.connect_started_at.lock() {
            *started_at = None;
        }
    }

    pub fn set_failed(&self) {
        self.set_state(ConnectionState::Failed);
        if let Ok(mut started_at) = self.connect_started_at.lock() {
            *started_at = None;
        }
    }

    pub fn set_reconnecting(&self) {
        self.set_state(ConnectionState::Reconnecting);
        if let Ok(mut started_at) = self.connect_started_at.lock() {
            *started_at = Some(Instant::now());
        }
    }

    pub fn state_changed_at(&self) -> Instant {
        self.state_changed_at.lock().map(|t| *t).unwrap_or_else(|_| Instant::now())
    }

    pub fn connect_duration(&self) -> Option<std::time::Duration> {
        self.connect_started_at
            .lock()
            .ok()
            .and_then(|started_at| {
                started_at.map(|start| start.elapsed())
            })
    }
}

impl Default for ConnectionStateManager {
    fn default() -> Self {
        Self::new()
    }
}

