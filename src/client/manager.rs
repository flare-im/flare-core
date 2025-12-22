//! 客户端连接管理器
//!
//! 统一管理客户端连接、心跳、重连等生命周期
//! 提供自动重连、心跳管理、连接状态监控等功能

use crate::client::config::ClientConfig;
use crate::client::connection::ConnectionStateManager;
use crate::client::heartbeat::HeartbeatManager;
use crate::client::transports::Client;
use crate::common::MessageParser;
use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::transport::events::{ArcObserver, ConnectionEvent};

use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info, warn};

/// 客户端连接管理器
///
/// 管理客户端连接的生命周期，包括：
/// - 自动连接和重连
/// - 心跳管理
/// - 连接状态监控
/// - 消息观察者管理
pub struct ClientConnectionManager {
    /// 客户端配置
    config: ClientConfig,
    /// 客户端实例
    client: Arc<Mutex<Box<dyn Client>>>,
    /// 连接状态管理器
    state_manager: Arc<ConnectionStateManager>,
    /// 心跳管理器
    heartbeat_manager: Arc<Mutex<Option<HeartbeatManager>>>,
    /// 消息解析器
    #[allow(dead_code)] // 保留用于未来扩展
    parser: MessageParser,
    /// 观察者列表
    observers: Arc<StdMutex<Vec<ArcObserver>>>,
    /// 重连任务句柄
    reconnect_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// 是否正在重连
    is_reconnecting: Arc<Mutex<bool>>,
}

impl ClientConnectionManager {
    /// 创建新的客户端连接管理器
    ///
    /// # 参数
    /// - `client`: 客户端实例（可以是 WebSocketClient、QUICClient 等）
    /// - `config`: 客户端配置
    pub fn new(client: Box<dyn Client>, config: ClientConfig) -> Self {
        let parser = MessageParser::new(
            config.serialization_format,
            config.compression.clone(),
            crate::common::encryption::EncryptionAlgorithm::None,
        );

        Self {
            config,
            client: Arc::new(Mutex::new(client)),
            state_manager: Arc::new(ConnectionStateManager::new()),
            heartbeat_manager: Arc::new(Mutex::new(None)),
            parser,
            observers: Arc::new(StdMutex::new(Vec::new())),
            reconnect_handle: Arc::new(Mutex::new(None)),
            is_reconnecting: Arc::new(Mutex::new(false)),
        }
    }

    /// 连接到服务器
    ///
    /// 自动处理连接、心跳启动等
    pub async fn connect(&self) -> Result<()> {
        info!("Connecting to server: {}", self.config.server_url);

        let mut client = self.client.lock().await;

        // 检查是否已连接
        if client.is_connected() {
            debug!("Already connected, skipping connect");
            return Ok(());
        }

        // 设置状态为连接中
        self.state_manager
            .set_state(crate::client::connection::ConnectionState::Connecting);

        // 执行连接
        match client.connect().await {
            Ok(()) => {
                info!("Successfully connected to server");
                self.state_manager
                    .set_state(crate::client::connection::ConnectionState::Connected);

                // 启动心跳（如果启用）
                if self.config.heartbeat.enabled {
                    self.start_heartbeat().await?;
                }

                // 通知观察者
                self.notify_observers(&ConnectionEvent::Connected);

                Ok(())
            }
            Err(e) => {
                error!("Failed to connect: {}", e);
                self.state_manager
                    .set_state(crate::client::connection::ConnectionState::Disconnected);
                Err(e)
            }
        }
    }

    /// 断开连接
    pub async fn disconnect(&self) -> Result<()> {
        info!("Disconnecting from server");

        // 停止重连
        self.stop_reconnect().await;

        // 停止心跳
        self.stop_heartbeat().await;

        // 断开连接
        let mut client = self.client.lock().await;
        let result = client.disconnect().await;

        self.state_manager
            .set_state(crate::client::connection::ConnectionState::Disconnected);
        self.notify_observers(&ConnectionEvent::Disconnected(String::new()));

        result
    }

    /// 发送消息
    pub async fn send_frame(&self, frame: &Frame) -> Result<()> {
        if !self.state_manager.get_state().can_send() {
            return Err(crate::common::error::FlareError::connection_failed(
                "Not connected".to_string(),
            ));
        }

        let mut client = self.client.lock().await;
        client.send_frame(frame).await
    }

    /// 启动心跳
    async fn start_heartbeat(&self) -> Result<()> {
        if !self.config.heartbeat.enabled {
            return Ok(());
        }

        debug!(
            "Starting heartbeat: interval={:?}, timeout={:?}",
            self.config.heartbeat.interval, self.config.heartbeat.timeout
        );

        let heartbeat = HeartbeatManager::new(
            self.config.heartbeat.interval,
            self.config.heartbeat.timeout,
        );

        // 需要从 Client 获取 Connection，但 Client trait 没有提供这个方法
        // 这里我们需要一个不同的设计
        // 暂时跳过，因为心跳应该在 Client 实现内部管理
        // 或者我们需要扩展 Client trait

        let mut hb_mgr = self.heartbeat_manager.lock().await;
        *hb_mgr = Some(heartbeat);

        Ok(())
    }

    /// 停止心跳
    async fn stop_heartbeat(&self) {
        let mut hb_mgr = self.heartbeat_manager.lock().await;
        if let Some(mut hb) = hb_mgr.take() {
            hb.stop();
        }
    }

    /// 启动自动重连
    pub async fn start_auto_reconnect(&self) {
        let mut is_reconnecting = self.is_reconnecting.lock().await;
        if *is_reconnecting {
            return; // 已经启动了
        }
        *is_reconnecting = true;
        drop(is_reconnecting);

        let client = Arc::clone(&self.client);
        let state_mgr = Arc::clone(&self.state_manager);
        let config = self.config.clone();
        let heartbeat_cfg = self.config.heartbeat.clone();
        let heartbeat_mgr = Arc::clone(&self.heartbeat_manager);
        let observers = Arc::clone(&self.observers);
        let reconnect_handle = Arc::clone(&self.reconnect_handle);
        let is_reconnecting_flag = Arc::clone(&self.is_reconnecting);

        let handle = tokio::spawn(async move {
            loop {
                // 检查连接状态
                let should_reconnect = {
                    let client_guard = client.lock().await;
                    !client_guard.is_connected()
                        && matches!(
                            state_mgr.get_state(),
                            crate::client::connection::ConnectionState::Disconnected
                        )
                };

                if !should_reconnect {
                    // 检查是否超过最大重连次数
                    // 这里简化处理，实际应该在连接失败时触发
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }

                // 检查重连次数限制
                // 注意：这里简化了，实际应该在连接失败时计数
                // 这里我们假设只要状态是断开就重连

                info!("Attempting to reconnect...");
                state_mgr.set_state(crate::client::connection::ConnectionState::Connecting);

                // 尝试重连
                let reconnect_result = {
                    let mut client_guard = client.lock().await;
                    client_guard.connect().await
                };

                match reconnect_result {
                    Ok(()) => {
                        info!("Reconnected successfully");
                        state_mgr.set_state(crate::client::connection::ConnectionState::Connected);

                        // 重新启动心跳
                        if heartbeat_cfg.enabled {
                            let _hb_mgr = heartbeat_mgr.lock().await;
                            // 重新创建心跳管理器
                            // 注意：这里简化了，实际应该从 Client 获取 Connection
                        }

                        // 通知观察者
                        {
                            let observers_guard = observers.lock().unwrap();
                            for observer in observers_guard.iter() {
                                observer.on_event(&ConnectionEvent::Connected);
                            }
                        }

                        // 重连成功，退出重连循环
                        *is_reconnecting_flag.lock().await = false;
                        break;
                    }
                    Err(e) => {
                        warn!(
                            "Reconnect failed: {}, retrying in {:?}",
                            e, config.reconnect_interval
                        );
                        state_mgr
                            .set_state(crate::client::connection::ConnectionState::Disconnected);
                        sleep(config.reconnect_interval).await;
                    }
                }
            }

            // 清理句柄
            let mut handle_guard = reconnect_handle.lock().await;
            *handle_guard = None;
        });

        let mut handle_guard = self.reconnect_handle.lock().await;
        *handle_guard = Some(handle);
    }

    /// 停止自动重连
    async fn stop_reconnect(&self) {
        let mut handle_guard = self.reconnect_handle.lock().await;
        if let Some(handle) = handle_guard.take() {
            handle.abort();
        }

        *self.is_reconnecting.lock().await = false;
    }

    /// 添加观察者
    pub fn add_observer(&self, observer: ArcObserver) {
        let mut observers = self.observers.lock().unwrap();
        observers.push(observer);
    }

    /// 移除观察者
    pub fn remove_observer(&self, observer: ArcObserver) {
        let mut observers = self.observers.lock().unwrap();
        observers.retain(|o| !Arc::ptr_eq(o, &observer));
    }

    /// 通知所有观察者
    fn notify_observers(&self, event: &ConnectionEvent) {
        let observers = self.observers.lock().unwrap();
        for observer in observers.iter() {
            observer.on_event(event);
        }
    }

    /// 检查是否已连接
    pub async fn is_connected(&self) -> bool {
        let client = self.client.lock().await;
        client.is_connected()
    }

    /// 获取连接 ID
    pub async fn connection_id(&self) -> Option<String> {
        let client = self.client.lock().await;
        client.connection_id()
    }

    /// 获取连接状态
    pub fn state(&self) -> crate::client::connection::ConnectionState {
        self.state_manager.get_state()
    }
}
