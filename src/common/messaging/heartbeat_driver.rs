//! 协议无关的心跳驱动
//!
//! - 使用 `FrameFactory` 统一构造 Ping/Pong 帧
//! - 使用 `MessageProcessor` 统一编码/压缩
//! - 仅依赖 `ClientConnection`/`BaseConnection` 的二进制发送能力
//! - 通过包装 `ConnectionEvent` 捕获 Pong，计算 RTT，统一回调

use crate::common::connections::traits::{ClientConnection, BaseConnection, ConnectionEvent};
use crate::common::connections::heartbeat::{HeartbeatConfig, HeartbeatManager, current_epoch_ms};
use crate::common::messaging::MessageProcessor;
use crate::common::protocol::factory::FrameFactory;
use crate::common::protocol::commands::{Command, ControlCmd};
use crate::common::error::FlareError;
use std::sync::{Arc, Mutex};

/// 心跳驱动
pub struct HeartbeatDriver {
    conn: Arc<dyn ClientConnection>,
    manager: HeartbeatManager,
    processor: MessageProcessor,
    /// 用户事件处理器（被包装并转发）
    user_handler: Arc<dyn ConnectionEvent>,
    /// 最近一次 Ping 的消息ID与时间戳
    last_ping: Arc<Mutex<Option<(String, u64)>>>,
    /// 后台任务句柄
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl HeartbeatDriver {
    /// 绑定连接，设置包装后的事件处理器，并启动心跳任务
    pub fn attach_and_start(
        conn: Arc<dyn ClientConnection>,
        user_handler: Arc<dyn ConnectionEvent>,
        hb_config: HeartbeatConfig,
    ) -> Arc<Self> {
        let driver = Arc::new(Self {
            conn: Arc::clone(&conn),
            manager: HeartbeatManager::new(hb_config),
            processor: MessageProcessor::default(),
            user_handler,
            last_ping: Arc::new(Mutex::new(None)),
            task: Mutex::new(None),
        });

        // 设置包装后的事件处理器
        let wrapped = Arc::new(DriverEventWrapper {
            driver: Arc::downgrade(&driver),
            inner: driver.user_handler.clone(),
        });
        conn.set_event_handler(wrapped);

        // 启动后台心跳任务
        driver.start_background_task();
        driver
    }

    fn start_background_task(self: &Arc<Self>) {
        let this = Arc::clone(self);
        let handle = tokio::spawn(async move {
            loop {
                let interval_ms = this.manager.get_interval();
                tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;

                // 发送 Ping
                if let Err(e) = this.send_ping().await {
                    this.user_handler.on_error(e);
                }
            }
        });
        if let Ok(mut g) = self.task.lock() { *g = Some(handle); }
    }

    async fn send_ping(&self) -> Result<(), FlareError> {
        let ping_id = FrameFactory::generate_message_id();
        let frame = FrameFactory::create_ping_frame(ping_id.clone())
            .map_err(|e| FlareError::general_error(e))?;
        let bytes = self.processor.process_send(&frame).await?;

        // 记录发送与时间戳
        self.manager.on_heartbeat_sent();
        if let Ok(mut g) = self.last_ping.lock() {
            *g = Some((ping_id, current_epoch_ms()));
        }

        // 发送并回调
        self.conn.send_bytes(bytes)?;
        self.user_handler.on_heartbeat_ping();
        Ok(())
    }
}

/// 包装事件处理器，拦截 Pong 以计算 RTT，并转发给用户处理器
struct DriverEventWrapper {
    driver: std::sync::Weak<HeartbeatDriver>,
    inner: Arc<dyn ConnectionEvent>,
}

impl ConnectionEvent for DriverEventWrapper {
    fn on_connected(&self) { self.inner.on_connected(); }
    fn on_disconnected(&self, reason: Option<String>) { self.inner.on_disconnected(reason); }
    fn on_error(&self, err: FlareError) { self.inner.on_error(err); }

    fn on_message_received(&self, frame: crate::common::protocol::frame::Frame) {
        // 先拦截 Pong
        if let Command::Control(ControlCmd::Pong) = &frame.command {
            if let Some(driver) = self.driver.upgrade() {
                // 读取最近一次 Ping
                let (match_id, sent_ms) = {
                    if let Ok(g) = driver.last_ping.lock() {
                        if let Some((id, ts)) = &*g { (id.clone(), *ts) } else { (String::new(), 0) }
                    } else { (String::new(), 0) }
                };
                // 如果 message_id 对得上，则计算 RTT
                if !match_id.is_empty() && match_id == frame.message_id {
                    let now = current_epoch_ms();
                    let rtt = now.saturating_sub(sent_ms) as u32;
                    driver.manager.on_heartbeat_success();
                    driver.manager.record_rtt(rtt);
                    self.inner.on_heartbeat_pong(rtt);
                }
            }
        }
        // 再把消息转发给业务层
        self.inner.on_message_received(frame);
    }

    fn on_message_sent(&self, frame: crate::common::protocol::frame::Frame) { self.inner.on_message_sent(frame); }
    fn on_heartbeat_ping(&self) { self.inner.on_heartbeat_ping(); }
    fn on_heartbeat_pong(&self, rtt_ms: u32) { self.inner.on_heartbeat_pong(rtt_ms); }
    fn on_heartbeat_timeout(&self) { self.inner.on_heartbeat_timeout(); }
    fn on_quality_changed(&self, quality: u8) { self.inner.on_quality_changed(quality); }
    fn on_statistics_updated(&self, stats: crate::common::connections::types::ConnectionStats) { self.inner.on_statistics_updated(stats); }
    fn on_reconnect_started(&self) { self.inner.on_reconnect_started(); }
    fn on_reconnected(&self) { self.inner.on_reconnected(); }
    fn on_reconnect_failed(&self, err: FlareError) { self.inner.on_reconnect_failed(err); }
}


