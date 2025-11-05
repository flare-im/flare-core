//! 客户端核心功能
//! 
//! 提供统一的连接状态管理、心跳管理、消息路由等功能，简化客户端实现

use crate::client::connection::ConnectionStateManager;
use crate::client::heartbeat::HeartbeatManager;
use crate::client::router::MessageRouter;
use crate::common::MessageParser;
use crate::client::config::ClientConfig;
use crate::common::protocol::Frame;
use crate::common::error::Result;
use crate::transport::events::{ArcObserver, ConnectionEvent};
use crate::transport::connection::Connection;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex;
use std::time::Duration;

/// 客户端核心功能
/// 
/// 统一管理连接状态、心跳、消息路由，简化客户端实现
pub struct ClientCore {
    /// 连接状态管理器
    pub state_manager: Arc<ConnectionStateManager>,
    /// 消息解析器
    pub parser: MessageParser,
    /// 心跳管理器（可选，通过配置开启）
    /// 使用 Arc<Mutex<>> 以支持并发访问（从同步的观察者中调用）
    heartbeat_manager: Option<Arc<tokio::sync::Mutex<HeartbeatManager>>>,
    /// 消息路由器（可选，通过配置开启）
    message_router: Option<MessageRouter>,
    /// 观察者列表
    pub observers: Arc<StdMutex<Vec<ArcObserver>>>,
    /// 客户端配置
    pub config: ClientConfig,
}

impl ClientCore {
    /// 创建新的客户端核心
    pub fn new(config: &ClientConfig) -> Self {
        let parser = MessageParser::new(config.serialization_format, config.compression);
        
        let message_router = if config.enable_router {
            Some(MessageRouter::new())
        } else {
            None
        };
        
        Self {
            state_manager: Arc::new(ConnectionStateManager::new()),
            parser,
            heartbeat_manager: None,
            message_router,
            observers: Arc::new(StdMutex::new(Vec::new())),
            config: config.clone(),
        }
    }
    
    /// 启动心跳（如果启用）
    pub fn start_heartbeat(
        &mut self,
        connection: Arc<Mutex<Box<dyn Connection>>>,
    ) {
        if !self.config.heartbeat.enabled {
            return;
        }
        
        let mut heartbeat = HeartbeatManager::new(
            self.config.heartbeat.interval,
            self.config.heartbeat.timeout,
        );
        
        heartbeat.start(connection, self.parser.clone());
        self.heartbeat_manager = Some(Arc::new(tokio::sync::Mutex::new(heartbeat)));
    }
    
    /// 停止心跳
    pub fn stop_heartbeat(&mut self) {
        if let Some(ref heartbeat) = self.heartbeat_manager {
            // 需要获取锁来停止心跳
            // 但由于 stop_heartbeat 是 &mut self，我们可以直接 take
            if let Some(mut hb) = self.heartbeat_manager.take() {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::try_current()
                        .map(|handle| handle.block_on(async {
                            let mut hb_guard = hb.lock().await;
                            hb_guard.stop();
                        }))
                        .unwrap_or_else(|_| {
                            tokio::runtime::Runtime::new().unwrap().block_on(async {
                                let mut hb_guard = hb.lock().await;
                                hb_guard.stop();
                            })
                        })
                });
            }
        }
    }
    
    /// 处理接收到的消息
    /// 
    /// 如果启用了路由，使用路由处理；否则直接通知观察者
    pub async fn handle_message(&self, data: Vec<u8>) {
        // 解析消息
        let frame = match self.parser.parse(&data) {
            Ok(frame) => frame,
            Err(e) => {
                tracing::warn!("Failed to parse message: {}", e);
                return;
            }
        };
        
        // 检查是否是 PONG（心跳响应）
        if let Some(cmd) = &frame.command {
            if let Some(crate::common::protocol::flare::core::commands::command::Type::System(sys_cmd)) = &cmd.r#type {
                if sys_cmd.r#type == crate::common::protocol::flare::core::commands::system_command::Type::Pong as i32 {
                    // 记录 PONG，更新心跳
                    self.record_pong();
                }
            }
        }
        
        // 如果启用了路由，使用路由处理
        if let Some(ref router) = self.message_router {
            match router.route(&frame).await {
                Ok(replies) => {
                    // 发送回复（如果需要）
                    // 注意：这里需要连接实例来发送，但 ClientCore 不持有连接
                    // 回复应该通过客户端发送，这里只是路由处理
                    tracing::debug!("Router generated {} replies", replies.len());
                }
                Err(e) => {
                    tracing::warn!("Router error: {}", e);
                }
            }
        }
        
        // 通知所有观察者
        self.notify_observers(&ConnectionEvent::Message(data));
    }
    
    /// 处理连接事件
    pub fn handle_connection_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Connected => {
                self.state_manager.set_connected();
            }
            ConnectionEvent::Disconnected(_) => {
                self.state_manager.set_disconnected();
            }
            ConnectionEvent::Error(_) => {
                self.state_manager.set_failed();
            }
            ConnectionEvent::Message(_) => {
                // 消息处理在 handle_message 中完成
            }
        }
        
        self.notify_observers(event);
    }
    
    /// 添加观察者
    pub fn add_observer(&self, observer: ArcObserver) {
        if let Ok(mut observers) = self.observers.lock() {
            observers.push(observer);
        }
    }
    
    /// 移除观察者
    pub fn remove_observer(&self, observer: ArcObserver) {
        if let Ok(mut observers) = self.observers.lock() {
            observers.retain(|o| !Arc::ptr_eq(o, &observer));
        }
    }
    
    /// 通知所有观察者
    fn notify_observers(&self, event: &ConnectionEvent) {
        if let Ok(observers) = self.observers.lock() {
            for observer in observers.iter() {
                observer.on_event(event);
            }
        }
    }
    
    /// 获取消息路由器（如果启用）
    pub fn router_mut(&mut self) -> Option<&mut MessageRouter> {
        self.message_router.as_mut()
    }
    
    /// 获取消息路由器（只读）
    pub fn router(&self) -> Option<&MessageRouter> {
        self.message_router.as_ref()
    }
    
    /// 获取连接状态
    pub fn state(&self) -> crate::client::connection::ConnectionState {
        self.state_manager.get_state()
    }
    
    /// 检查是否可以发送消息
    pub fn can_send(&self) -> bool {
        self.state_manager.get_state().can_send()
    }
    
    /// 检查是否可以连接
    pub fn can_connect(&self) -> bool {
        self.state_manager.get_state().can_connect()
    }
    
    /// 记录收到 PONG（心跳响应）
    /// 
    /// 由消息观察者调用，用于更新心跳状态
    /// 
    /// 注意：由于观察者是同步的，我们需要异步获取锁
    pub fn record_pong(&self) {
        if let Some(ref heartbeat) = self.heartbeat_manager {
            // HeartbeatManager::record_pong 是 `&self` 方法
            // 但由于我们使用了 Arc<Mutex<>>，需要先获取锁
            // 由于这是从同步上下文调用，使用 block_in_place
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::try_current()
                    .map(|handle| {
                        handle.block_on(async {
                            let hb_guard = heartbeat.lock().await;
                            hb_guard.record_pong();
                        })
                    })
                    .unwrap_or_else(|_| {
                        tokio::runtime::Runtime::new().unwrap().block_on(async {
                            let hb_guard = heartbeat.lock().await;
                            hb_guard.record_pong();
                        })
                    })
            });
        }
    }
}

// 为 ClientCore 实现 Clone（用于共享状态管理器和观察者）
impl Clone for ClientCore {
    fn clone(&self) -> Self {
        Self {
            state_manager: Arc::clone(&self.state_manager),
            parser: self.parser.clone(),
            heartbeat_manager: None, // 心跳管理器不克隆，由主实例管理
            message_router: self.message_router.as_ref().map(|_| MessageRouter::new()), // 路由不克隆，创建新的
            observers: Arc::clone(&self.observers),
            config: self.config.clone(),
        }
    }
}

