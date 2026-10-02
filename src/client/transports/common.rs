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

impl ClientMessageObserver {
    /// 协商完成前读到的帧只可能是 CONNECT_ACK、拒绝 CONNECT 的 ERROR，或服务端停机时的 CLOSE。
    /// ERROR 与 CLOSE 必须在读循环里同步登记：服务端发完它紧接着就关连接，关连接事件是同步处理的，
    /// 而帧的处理在另一个任务里，晚一步就只剩一个没有原因的「连接被关」。
    fn record_pre_negotiation_rejection(&self, data: &[u8]) {
        use crate::common::message::parser::PRE_NEGOTIATION_PARSER;
        use crate::common::protocol::flare::core::commands::command::Type;
        use crate::common::protocol::flare::core::commands::system_command::Type as SystemType;

        if let Ok(frame) = PRE_NEGOTIATION_PARSER.parse(data)
            && let Some(Type::System(system)) =
                frame.command.as_ref().and_then(|c| c.r#type.as_ref())
            && (system.r#type == SystemType::Error as i32
                || system.r#type == SystemType::Close as i32)
        {
            self.core.record_connect_rejection(system);
        }
    }
}

impl ConnectionObserver for ClientMessageObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
                let core = Arc::clone(&self.core);
                let data_clone = data.clone();
                // 帧读出这一刻的协商状态：处理任务可能晚于随后的关连接事件执行（见
                // `ClientCore::handle_received_message`）。
                let negotiated = core.is_negotiation_completed();
                if !negotiated {
                    self.record_pre_negotiation_rejection(data);
                }
                // 记一帧在途，处理完（WASM 为出队处理完或被丢弃）再记完成
                core.inbound_frame_read();
                #[cfg(target_arch = "wasm32")]
                {
                    // Browser onmessage is sync; queue bytes and drain on the LocalSet task
                    // that is already running `wait_for_negotiation` / `run_async`.
                    let _ = negotiated;
                    core.push_wasm_inbound(data_clone);
                    let core_drain = Arc::clone(&core);
                    crate::client::wasm_tokio::spawn_detached(async move {
                        core_drain.drain_wasm_inbound().await;
                    });
                }
                #[cfg(not(target_arch = "wasm32"))]
                {
                    crate::client::runtime::spawn_client_task(async move {
                        core.handle_received_message(data_clone, negotiated).await;
                        core.inbound_frame_done();
                    });
                }
            }
            ConnectionEvent::Disconnected(_) | ConnectionEvent::Error(_)
                if self.core.should_defer_close_until_inbound_drained() =>
            {
                // 协商完成后对端关了连接，而读出的帧还没处理完（典型：服务端发完 KICKED 紧接着关连接）。
                // 等它们处理完再处理断开：被踢先报给观察者，随后的对端关闭不再重复上报。最多等
                // INBOUND_DRAIN_BOUND，处理卡住时照常上报断开。
                let core = Arc::clone(&self.core);
                let event = event.clone();
                crate::client::runtime::spawn_client_task(async move {
                    if !core
                        .wait_inbound_drained(super::client_core::INBOUND_DRAIN_BOUND)
                        .await
                    {
                        tracing::debug!(
                            "[ClientCore] inbound frames still in flight after the close; reporting the close anyway"
                        );
                    }
                    core.handle_connection_event(&event);
                });
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
                "[ClientConnectionHelper] 协商未完成但尝试发送消息: message_id={}, format={:?}, compression={:?}, encryption={:?}",
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
        tracing::info!("[ClientCore] 主动断开（disconnect 请求）");

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

    /// 发送前是否由传输客户端自己重连：未连接、不是我方主动断开、没有被服务端以结束会话的原因踢掉
    /// （[`ClientCore::session_ended_by_server`]）、配置允许发送前重连
    /// （[`ClientConfig::reconnect_on_send`](crate::client::config::ClientConfig::reconnect_on_send)）
    /// 且重连次数不为 0。
    pub fn should_reconnect_before_send(
        connected: bool,
        core: &ClientCore,
        config: &crate::client::config::ClientConfig,
    ) -> bool {
        !connected
            && config.reconnect_on_send
            && !core.is_disconnect_requested()
            && core.session_ended_by_server().is_none()
            && Self::can_reconnect(config.max_reconnect_attempts)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::config::ClientConfig;

    /// 传输客户端发送前自己重连的条件：未连接、允许发送前重连、不是我方主动断开、没有被服务端
    /// 以结束会话的原因踢掉、重连次数不为 0。
    #[test]
    fn reconnect_before_send_honours_the_switch() {
        let config = ClientConfig::default();
        let core = ClientCore::new(&config);
        assert!(ClientConnectionHelper::should_reconnect_before_send(
            false, &core, &config
        ));
        assert!(!ClientConnectionHelper::should_reconnect_before_send(
            true, &core, &config
        ));

        let off = ClientConfig::default().with_reconnect_on_send(false);
        let core_off = ClientCore::new(&off);
        assert!(
            !ClientConnectionHelper::should_reconnect_before_send(false, &core_off, &off),
            "reconnect_on_send = false leaves reconnecting to the caller"
        );

        let no_attempts = ClientConfig::default().with_max_reconnect_attempts(Some(0));
        assert!(!ClientConnectionHelper::should_reconnect_before_send(
            false,
            &ClientCore::new(&no_attempts),
            &no_attempts
        ));

        core.set_disconnect_requested(true);
        assert!(!ClientConnectionHelper::should_reconnect_before_send(
            false, &core, &config
        ));

        // 被服务端以结束会话的原因踢掉（设备冲突、租户不可用）：不再自己重连
        let kicked = ClientCore::new(&config);
        kicked.end_session_by_server(crate::common::kick::DEVICE_CONFLICT_REASON);
        assert!(!ClientConnectionHelper::should_reconnect_before_send(
            false, &kicked, &config
        ));
    }

    struct RecordingObserver(std::sync::Mutex<Vec<String>>);

    impl ConnectionObserver for RecordingObserver {
        fn on_event(&self, event: &ConnectionEvent) {
            if let ConnectionEvent::Disconnected(reason) = event {
                self.0.lock().unwrap().push(reason.clone());
            }
        }
    }

    /// 对端已关闭的连接：本端再关不上报任何事件。
    struct ClosedConnection;

    #[async_trait::async_trait]
    impl Connection for ClosedConnection {
        fn add_observer(&mut self, _observer: crate::transport::events::ArcObserver) {}
        fn remove_observer(&mut self, _observer: crate::transport::events::ArcObserver) {}
        async fn send(&mut self, _data: &[u8]) -> Result<()> {
            Err(FlareError::connection_closed("closed".to_string()))
        }
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
        fn last_active_time(&self) -> crate::common::platform::MonotonicInstant {
            crate::common::platform::monotonic_now()
        }
        fn update_active_time(&mut self) {}
    }

    /// 服务端发完 KICKED 紧接着关连接：读循环先读出 KICKED（交给处理任务），随即同步收到对端关闭。
    /// 断开要等 KICKED 处理完再处理：观察者只收到一次 DEVICE_CONFLICT，前面没有对端关闭。
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn a_close_right_after_a_kick_is_reported_as_the_kick_only() {
        use crate::common::kick::{DEVICE_CONFLICT_REASON, device_conflict_kicked};
        use crate::common::protocol::{Reliability, frame_with_system_command};

        let mut core = ClientCore::new(&ClientConfig::default());
        core.set_client_connection(Arc::new(Mutex::new(
            Box::new(ClosedConnection) as Box<dyn Connection>
        )));
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.update_parser(
            crate::common::protocol::SerializationFormat::Protobuf,
            crate::common::compression::CompressionAlgorithm::Gzip,
            crate::common::encryption::EncryptionAlgorithm::None,
        )
        .await;
        core.stop_heartbeat();
        let recorder = Arc::new(RecordingObserver(std::sync::Mutex::new(Vec::new())));
        core.add_observer(recorder.clone());
        let kicked = frame_with_system_command(
            device_conflict_kicked(&crate::common::device::DevicePlatform::PC, "new-device"),
            Reliability::AtLeastOnce,
        );
        let data = core
            .parser
            .lock()
            .await
            .serialize(&kicked)
            .expect("serialize");

        let observer = ClientMessageObserver::new(Arc::new(core.clone()));
        observer.on_event(&ConnectionEvent::Message(data));
        observer.on_event(&ConnectionEvent::Disconnected(
            "Stream closed by peer".to_string(),
        ));

        for _ in 0..100 {
            if !recorder.0.lock().unwrap().is_empty() && !core.has_inbound_in_flight() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            *recorder.0.lock().unwrap(),
            vec![DEVICE_CONFLICT_REASON.to_string()]
        );
        assert!(!core.can_send());
        assert!(!core.is_negotiation_completed());
    }

    /// 旧版本序列化出的配置没有这个字段：反序列化时取默认值 `true`，行为不变。
    #[test]
    fn configs_without_the_switch_keep_reconnecting_on_send() {
        let mut value = serde_json::to_value(ClientConfig::default()).expect("serialize");
        value
            .as_object_mut()
            .expect("config object")
            .remove("reconnect_on_send");
        let config: ClientConfig = serde_json::from_value(value).expect("deserialize");
        assert!(config.reconnect_on_send);
    }
}
