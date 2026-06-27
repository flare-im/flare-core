//! 客户端传输协议公共模块
//!
//! 提供 WebSocket 和 QUIC 客户端共享的逻辑和辅助函数

use crate::client::connection::ConnectionStateManager;
use crate::client::transports::ClientCore;
use crate::common::error::{FlareError, Result};
use crate::common::platform::sleep;
use crate::common::protocol::Frame;
use crate::transport::connection::Connection;
use crate::transport::events::{ConnectionEvent, ConnectionObserver};
use std::sync::Arc;
use tokio::sync::Mutex;

/// 客户端消息观察者（公共实现）
///
/// 委托给 ClientCore 处理消息和连接事件
pub struct ClientMessageObserver {
    core: Arc<ClientCore>,
}

impl ClientMessageObserver {
    /// 创建新的消息观察者
    pub fn new(core: Arc<ClientCore>) -> Self {
        Self { core }
    }
}

impl ConnectionObserver for ClientMessageObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
                let core = Arc::clone(&self.core);
                let data_clone = data.clone();
                #[cfg(target_arch = "wasm32")]
                {
                    // Browser onmessage is sync; queue bytes and drain on the LocalSet task
                    // that is already running `wait_for_negotiation` / `run_async`.
                    core.push_wasm_inbound(data_clone);
                    let core_drain = Arc::clone(&core);
                    crate::client::wasm_tokio::spawn_detached(async move {
                        core_drain.drain_wasm_inbound().await;
                    });
                }
                #[cfg(not(target_arch = "wasm32"))]
                {
                    crate::client::runtime::spawn_client_task(async move {
                        core.handle_message(data_clone).await;
                    });
                }
            }
            ConnectionEvent::Connected
            | ConnectionEvent::Disconnected(_)
            | ConnectionEvent::Error(_) => {
                // 单一生命周期入口：pending 清理、协商状态、上层观察者均由 ClientCore 负责
                self.core.handle_connection_event(event);
            }
        }
    }
}

/// 客户端连接辅助函数
pub struct ClientConnectionHelper;

impl ClientConnectionHelper {
    /// 发送 Frame（内部实现）
    ///
    /// 统一处理消息序列化和发送逻辑
    pub async fn send_frame_internal(
        core: &ClientCore,
        connection: Option<&Arc<Mutex<Box<dyn Connection>>>>,
        frame: &Frame,
    ) -> Result<()> {
        if !core.can_send() {
            return Err(FlareError::connection_failed(
                "Cannot send: connection state is not ready".to_string(),
            ));
        }

        // 检查协商状态
        let negotiation_completed = core.is_negotiation_completed();

        let parser = core.parser.lock().await;
        tracing::trace!(
            "[ClientConnectionHelper] 发送消息: message_id={}, format={:?}",
            frame.message_id,
            parser.default_format()
        );

        // 如果协商未完成，记录警告（但允许发送，因为可能是系统消息）
        if !negotiation_completed {
            tracing::warn!(
                "[ClientConnectionHelper] ⚠️  协商未完成但尝试发送消息: message_id={}, format={:?}, compression={:?}, encryption={:?}",
                frame.message_id,
                parser.default_format(),
                parser.default_compression(),
                parser.default_encryption()
            );
        }

        let data = parser.serialize(frame)?;
        drop(parser);

        let conn =
            connection.ok_or_else(|| FlareError::connection_failed("Not connected".to_string()))?;

        let mut c = conn.lock().await;
        c.send(&data).await?;
        Ok(())
    }

    /// 尝试重连
    ///
    /// 统一处理重连逻辑：检查重连次数、等待间隔、关闭旧连接
    #[allow(dead_code)] // 保留用于未来扩展或外部使用
    pub async fn try_reconnect<F, Fut>(
        reconnect_attempts: &mut u32,
        max_attempts: Option<u32>,
        reconnect_interval: std::time::Duration,
        old_connection: Option<Arc<Mutex<Box<dyn Connection>>>>,
        state_manager: &Arc<ConnectionStateManager>,
        connect_fn: F,
    ) -> Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        // 检查重连次数限制
        if let Some(max) = max_attempts
            && *reconnect_attempts >= max
        {
            return Err(FlareError::connection_failed(format!(
                "Max reconnect attempts ({}) exceeded",
                max
            )));
        }

        state_manager.start_connecting();
        *reconnect_attempts += 1;

        // 等待重连间隔
        sleep(reconnect_interval).await;

        // 关闭旧连接
        if let Some(conn) = old_connection {
            let mut c = conn.lock().await;
            let _ = c.close().await;
        }

        // 执行连接
        connect_fn().await
    }

    /// 断开连接（统一处理）
    pub async fn disconnect_internal(
        connection: Option<Arc<Mutex<Box<dyn Connection>>>>,
        core: &mut ClientCore,
    ) -> Result<()> {
        core.state_manager
            .set_state(crate::client::connection::ConnectionState::Disconnecting);

        // 标记我方主动断开，后续读循环若收到 KICK 不再向观察者通知「被踢」（重复登录/协议竞速场景）
        core.set_disconnect_requested(true);

        // 停止心跳
        core.stop_heartbeat();

        // 先清掉共享连接槽，再用局部 Arc 关闭连接，避免
        // connection -> observer -> cloned ClientCore -> client_connection -> connection
        // 形成强引用环，导致切账号后旧引擎和上层 listener 无法释放。
        core.clear_client_connection();

        let close_result = if let Some(conn) = connection {
            let mut c = conn.lock().await;
            c.close().await
        } else {
            Ok(())
        };

        core.cancel_all_pending_responses().await;

        // 通知连接断开事件
        core.handle_connection_event(&ConnectionEvent::Disconnected(
            "Client disconnected".to_string(),
        ));

        close_result
    }

    /// 检查是否可以重连
    pub fn can_reconnect(max_attempts: Option<u32>) -> bool {
        max_attempts.map(|n| n > 0).unwrap_or(true)
    }

    /// 设置连接观察者并发送 CONNECT 消息
    ///
    /// 统一处理连接设置和 CONNECT 消息发送
    pub async fn setup_connection_and_send_connect(
        connection: Arc<Mutex<Box<dyn Connection>>>,
        core: &mut ClientCore,
        observer: Arc<dyn ConnectionObserver>,
    ) -> Result<()> {
        // 立即设置 client_connection（用于被踢时断开连接）
        core.set_client_connection(Arc::clone(&connection));

        // 添加观察者
        {
            let mut conn = connection.lock().await;
            conn.add_observer(observer);
        }

        // 发送 CONNECT 消息进行协商
        core.send_connect_message(Arc::clone(&connection)).await?;

        Ok(())
    }
}
