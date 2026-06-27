//! 客户端核心功能
//!
//! 提供统一的连接状态管理、心跳管理、消息路由等功能，简化客户端实现

use crate::client::config::ClientConfig;
use crate::client::connection::ConnectionStateManager;
use crate::client::heartbeat::HeartbeatManager;
use crate::client::router::MessageRouter;
use crate::common::error::{FlareError, Result};
use crate::common::protocol::Frame;
use crate::common::protocol::flare::core::commands::command::Type;
use crate::common::protocol::flare::core::commands::notification_command::Type as NotificationCommandType;
use crate::common::protocol::flare::core::commands::payload_command::Type as PayloadCommandType;
use crate::common::protocol::flare::core::commands::system_command::Type as SystemCommandType;
use crate::common::{HeartbeatAppState, HeartbeatConfig, MessageParser};
use crate::transport::connection::Connection;
use crate::transport::events::{ArcObserver, ConnectionEvent};
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex as StdMutex, RwLock as StdRwLock,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{Mutex, Notify, oneshot};

#[path = "client_core_connect.rs"]
mod client_core_connect;

const NEGOTIATION_TIMEOUT_HINT: &str =
    "Ensure `flare_chat_server` is running, not `simple_server`.";

/// WASM 入站队列上限：防止浏览器回调洪泛占满内存；丢弃最旧帧并记录日志。
#[cfg(target_arch = "wasm32")]
const MAX_WASM_INBOUND_QUEUE: usize = 512;

fn negotiation_timeout_error(timeout: std::time::Duration) -> FlareError {
    FlareError::connection_timeout(format!(
        "Negotiation timeout after {:?} (CONNECT_ACK not received). {}",
        timeout, NEGOTIATION_TIMEOUT_HINT
    ))
}

#[cfg(not(target_arch = "wasm32"))]
async fn wait_for_negotiation_notify(
    flag: Arc<AtomicBool>,
    failure_reason: Arc<StdMutex<Option<String>>>,
    notify: Arc<Notify>,
    timeout: std::time::Duration,
) -> Result<()> {
    let wait = async move {
        loop {
            if flag.load(Ordering::SeqCst) {
                return Ok(());
            }
            if let Ok(reason) = failure_reason.lock()
                && let Some(msg) = reason.as_ref()
            {
                return Err(FlareError::protocol_error(msg.clone()));
            }
            notify.notified().await;
        }
    };

    match crate::common::platform::timeout(timeout, wait).await {
        Ok(result) => result,
        Err(_) => Err(negotiation_timeout_error(timeout)),
    }
}

/// 客户端核心功能
///
/// 统一管理连接状态、心跳、消息路由，简化客户端实现
pub struct ClientCore {
    /// 连接状态管理器
    pub state_manager: Arc<ConnectionStateManager>,
    /// 消息解析器（使用 Arc<Mutex<>> 以支持协商后更新）
    pub parser: Arc<tokio::sync::Mutex<MessageParser>>,
    /// 心跳管理器（共享，clone 与 observer 路径均可启动/停止）
    heartbeat_manager: Arc<StdMutex<Option<Arc<tokio::sync::Mutex<HeartbeatManager>>>>>,
    /// 运行期心跳策略，供前后台/NAT 探测动态更新。
    heartbeat_config: Arc<StdRwLock<HeartbeatConfig>>,
    /// 消息路由器（可选，通过配置开启）
    message_router: Option<MessageRouter>,
    /// 观察者列表
    pub observers: Arc<StdMutex<Vec<ArcObserver>>>,
    /// 客户端配置
    pub config: ClientConfig,
    /// 事件处理器（可选，用于自定义业务逻辑）
    event_handler: Option<Arc<dyn crate::client::events::handler::ClientEventHandler>>,
    /// 客户端连接（用于断开连接）
    /// 使用 Arc 包装，以便在 clone 时共享同一个连接引用
    #[allow(clippy::type_complexity)]
    client_connection: Arc<std::sync::Mutex<Option<Arc<Mutex<Box<dyn Connection>>>>>>,
    /// 等待响应的请求池（按 message_id 匹配）
    pub(crate) pending_map: Arc<tokio::sync::Mutex<HashMap<String, oneshot::Sender<Frame>>>>,
    /// 协商完成标志
    /// 当收到 CONNECT_ACK 并更新 parser 后，设置为 true
    pub(crate) negotiation_completed: Arc<AtomicBool>,
    /// 用于等待 CONNECT_ACK 完成
    pub(crate) negotiation_notify: Arc<Notify>,
    /// CONNECT_ACK 协商失败原因（`wait_for_negotiation` 立即返回 Err）
    negotiation_failure_reason: Arc<StdMutex<Option<String>>>,
    /// 我方已请求断开（disconnect_internal 中置位）：若随后读循环仍收到 KICK，不向观察者通知「被踢」，避免重复登录/协议竞速时误报
    disconnect_requested: Arc<AtomicBool>,
    /// WASM: browser WebSocket 回调线程同步入队，由 `wait_for_negotiation`/drain 在 LocalSet 内处理
    #[cfg(target_arch = "wasm32")]
    wasm_inbound: Arc<StdMutex<Vec<Vec<u8>>>>,
}

impl ClientCore {
    /// 创建新的客户端核心
    pub fn new(config: &ClientConfig) -> Self {
        let (format, compression) = Self::determine_initial_format(config);
        let parser = MessageParser::new(
            format,
            compression,
            crate::common::encryption::EncryptionAlgorithm::None,
        );

        let message_router = config.enable_router.then(MessageRouter::new);

        Self {
            state_manager: Arc::new(ConnectionStateManager::new()),
            parser: Arc::new(tokio::sync::Mutex::new(parser)),
            heartbeat_manager: Arc::new(StdMutex::new(None)),
            heartbeat_config: Arc::new(StdRwLock::new(config.heartbeat.clone())),
            message_router,
            observers: Arc::new(StdMutex::new(Vec::new())),
            config: config.clone(),
            event_handler: None,
            client_connection: Arc::new(std::sync::Mutex::new(None)),
            pending_map: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            negotiation_completed: Arc::new(AtomicBool::new(false)),
            negotiation_notify: Arc::new(Notify::new()),
            negotiation_failure_reason: Arc::new(StdMutex::new(None)),
            disconnect_requested: Arc::new(AtomicBool::new(false)),
            #[cfg(target_arch = "wasm32")]
            wasm_inbound: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    /// WASM: enqueue bytes from the browser `onmessage` callback (sync context).
    #[cfg(target_arch = "wasm32")]
    pub fn push_wasm_inbound(&self, data: Vec<u8>) {
        if let Ok(mut queue) = self.wasm_inbound.lock() {
            if queue.len() >= MAX_WASM_INBOUND_QUEUE {
                queue.remove(0);
                tracing::warn!(
                    "[ClientCore] wasm inbound queue full (max {}), dropping oldest frame",
                    MAX_WASM_INBOUND_QUEUE
                );
            }
            queue.push(data);
        }
        self.negotiation_notify.notify_waiters();
    }

    /// WASM: process all queued inbound frames on the current LocalSet task.
    #[cfg(target_arch = "wasm32")]
    pub async fn drain_wasm_inbound(&self) {
        let batch: Vec<Vec<u8>> = match self.wasm_inbound.lock() {
            Ok(mut queue) if !queue.is_empty() => queue.drain(..).collect(),
            _ => return,
        };
        for data in batch {
            self.handle_message(data).await;
        }
    }

    /// 标记「我方已请求断开」（disconnect_internal 调用前设置，收到 KICK 时不向观察者通知被踢）
    pub fn set_disconnect_requested(&self, value: bool) {
        self.disconnect_requested.store(value, Ordering::SeqCst);
    }

    /// 协议竞速：与主 core 共享 disconnect / 协商 / pending 状态，避免 loser KICK 误报。
    #[cfg(all(
        not(target_arch = "wasm32"),
        any(feature = "websocket", feature = "quic", feature = "tcp")
    ))]
    pub(crate) fn share_race_state_from(&mut self, shared: &ClientCore) {
        self.observers = Arc::clone(&shared.observers);
        self.pending_map = Arc::clone(&shared.pending_map);
        self.disconnect_requested = Arc::clone(&shared.disconnect_requested);
        self.negotiation_completed = Arc::clone(&shared.negotiation_completed);
        self.negotiation_notify = Arc::clone(&shared.negotiation_notify);
        self.negotiation_failure_reason = Arc::clone(&shared.negotiation_failure_reason);
        self.heartbeat_config = Arc::clone(&shared.heartbeat_config);
    }

    /// 确定初始序列化格式和压缩算法
    fn determine_initial_format(
        config: &ClientConfig,
    ) -> (
        crate::common::protocol::SerializationFormat,
        crate::common::compression::CompressionAlgorithm,
    ) {
        if config.is_force_format() {
            (config.get_serialization_format(), config.get_compression())
        } else {
            // 默认使用 JSON，等待协商后更新
            (
                crate::common::protocol::SerializationFormat::Json,
                crate::common::compression::CompressionAlgorithm::None,
            )
        }
    }

    /// 更新消息解析器（协商完成后调用）
    pub async fn update_parser(
        &self,
        format: crate::common::protocol::SerializationFormat,
        compression: crate::common::compression::CompressionAlgorithm,
        encryption: crate::common::encryption::EncryptionAlgorithm,
    ) {
        let compression_clone = compression.clone();
        let encryption_clone = encryption.clone();
        let mut parser = self.parser.lock().await;
        *parser = MessageParser::new(format, compression, encryption);
        // 标记协商已完成
        self.negotiation_completed.store(true, Ordering::SeqCst);
        self.negotiation_notify.notify_waiters();
        tracing::info!(
            "[ClientCore] ✅ 协商完成，解析器已更新: 最终序列化方式={:?}, 最终压缩方式={:?}, 最终加密方式={:?}, negotiation_completed={}",
            format,
            compression_clone,
            encryption_clone,
            self.negotiation_completed.load(Ordering::SeqCst)
        );
        self.try_start_heartbeat().await;
    }

    /// 设置客户端连接（用于断开连接）
    pub fn set_client_connection(&mut self, connection: Arc<Mutex<Box<dyn Connection>>>) {
        if let Ok(mut conn) = self.client_connection.lock() {
            *conn = Some(connection);
        }
    }

    /// 清空当前连接槽，用于断开连接时打断 connection -> observer -> core -> connection 的强引用链。
    pub fn clear_client_connection(&self) {
        if let Ok(mut conn) = self.client_connection.lock() {
            *conn = None;
        }
    }

    /// 取出当前连接槽；调用方仍可用返回值完成 close，但共享槽会立即释放旧连接。
    pub fn take_client_connection(&self) -> Option<Arc<Mutex<Box<dyn Connection>>>> {
        self.client_connection
            .lock()
            .ok()
            .and_then(|mut conn| conn.take())
    }

    /// 设置事件处理器
    pub fn set_event_handler(
        &mut self,
        handler: Option<Arc<dyn crate::client::events::handler::ClientEventHandler>>,
    ) {
        self.event_handler = handler;
    }

    /// 启动心跳（协商完成后调用；协商前调用会被忽略）
    pub async fn start_heartbeat(&self, connection: Arc<Mutex<Box<dyn Connection>>>) {
        if let Ok(mut conn) = self.client_connection.lock() {
            *conn = Some(Arc::clone(&connection));
        }
        self.try_start_heartbeat().await;
    }

    /// 协商完成后启动心跳（幂等）
    async fn try_start_heartbeat(&self) {
        if !self.current_heartbeat_config().enabled {
            return;
        }
        if !self.negotiation_completed.load(Ordering::SeqCst) {
            return;
        }
        let Ok(mut slot) = self.heartbeat_manager.lock() else {
            return;
        };
        if slot.is_some() {
            return;
        }
        let Some(connection) = self
            .client_connection
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
        else {
            tracing::debug!("[ClientCore] heartbeat deferred: no active connection");
            return;
        };

        let mut heartbeat =
            HeartbeatManager::with_shared_config(Arc::clone(&self.heartbeat_config));
        let parser_ref = Arc::clone(&self.parser);
        heartbeat.start(connection, parser_ref);
        *slot = Some(Arc::new(tokio::sync::Mutex::new(heartbeat)));
        tracing::debug!("[ClientCore] heartbeat started after negotiation");
    }

    /// 停止心跳
    pub fn stop_heartbeat(&self) {
        let taken = self
            .heartbeat_manager
            .lock()
            .ok()
            .and_then(|mut slot| slot.take());
        if let Some(heartbeat) = taken {
            Self::stop_heartbeat_async(heartbeat);
        }
    }

    /// 异步停止心跳（内部辅助函数）
    fn stop_heartbeat_async(heartbeat: Arc<tokio::sync::Mutex<HeartbeatManager>>) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            crate::client::runtime::run_client_async(async {
                let mut hb_guard = heartbeat.lock().await;
                hb_guard.stop();
            });
        }
        #[cfg(target_arch = "wasm32")]
        {
            crate::client::runtime::spawn_client_task(async move {
                let mut hb_guard = heartbeat.lock().await;
                hb_guard.stop();
            });
        }
    }

    /// 处理接收到的消息
    ///
    /// 如果启用了路由，使用路由处理；否则直接通知观察者
    pub async fn handle_message(&self, data: Vec<u8>) {
        // 根据协商完成标志决定使用哪个 parser
        let negotiation_completed = self.negotiation_completed.load(Ordering::SeqCst);

        // 解析消息
        let frame = if !negotiation_completed {
            // 协商未完成：只使用 PRE_NEGOTIATION_PARSER（这个阶段只会收到 CONNECT_ACK）
            use crate::common::message::parser::PRE_NEGOTIATION_PARSER;
            match PRE_NEGOTIATION_PARSER.parse(&data) {
                Ok(frame) => frame,
                Err(e) => {
                    #[cfg(target_arch = "wasm32")]
                    web_sys::console::warn_1(
                        &format!("[flare-core] parse failed pre-negotiation: {e}").into(),
                    );
                    tracing::warn!("Failed to parse message (pre-negotiation): {}", e);
                    return;
                }
            }
        } else {
            // 协商已完成：直接使用协商后的 parser 解析
            match self.parse_message(&data).await {
                Ok(frame) => frame,
                Err(e) => {
                    #[cfg(target_arch = "wasm32")]
                    web_sys::console::warn_1(
                        &format!("[flare-core] parse failed negotiated: {e}").into(),
                    );
                    tracing::warn!("Failed to parse message (negotiated): {}", e);
                    return;
                }
            }
        };
        let is_pending_response = {
            tracing::trace!(
                "[ClientCore] 尝试匹配等待的响应: message_id={}",
                frame.message_id
            );

            if let Some(cmd) = &frame.command
                && let Some(Type::Payload(msg_cmd)) = &cmd.r#type
                && msg_cmd.message_id != frame.message_id
            {
                tracing::warn!(
                    "[ClientCore] PayloadCommand.message_id 和 Frame.message_id 不一致: cmd_id={}, frame_id={}",
                    msg_cmd.message_id,
                    frame.message_id
                );
            }

            // 列出所有等待的 message_id（用于调试）
            let pending_ids: Vec<String> = {
                let pending = self.pending_map.lock().await;
                pending.keys().cloned().collect()
            };
            if !pending_ids.is_empty() {
                tracing::debug!(
                    "[ClientCore] handle_message: 当前等待的响应 message_id 列表: {:?}",
                    pending_ids
                );
            }

            let mut pending = self.pending_map.lock().await;
            if let Some(sender) = pending.remove(&frame.message_id) {
                tracing::debug!(
                    "[ClientCore] ✅ 匹配到等待的响应: message_id={}",
                    frame.message_id
                );
                if sender.send(frame.clone()).is_err() {
                    tracing::warn!(
                        "[ClientCore] 发送响应到等待通道失败: message_id={} (接收者可能已关闭)",
                        frame.message_id
                    );
                    false // 发送失败，继续处理
                } else {
                    tracing::debug!(
                        "[ClientCore] ✅ 响应已发送到等待通道: message_id={}",
                        frame.message_id
                    );
                    true // 发送成功，这是等待的响应，不需要继续处理
                }
            } else {
                tracing::debug!(
                    "[ClientCore] ❌ 未找到等待的响应: message_id={}",
                    frame.message_id
                );
                false // 不是等待的响应，继续处理
            }
        };

        // 如果是等待的响应且已成功发送，直接返回，避免被 MessageListener 重复处理
        // 注意：系统命令（如 CONNECT_ACK）仍然需要被处理，因为它们可能不在 pending_map 中
        if is_pending_response {
            // 仍然需要通知 observers，以便 MessagePipeline 可以处理响应
            // 但不继续处理业务命令，避免被 MessageListener 重复处理
            self.notify_observers(&ConnectionEvent::Message(data));
            return;
        }

        // 处理系统命令（CONNECT_ACK, PONG, KICKED）
        let is_system_command = self.handle_system_commands(&frame).await;

        // 关键修复：即使处理了系统命令，也要通知 observers
        // 这样 MessagePipeline 和 MessageListener 也能收到 CONNECT_ACK 等系统命令
        // 通知所有观察者（包括系统命令，让 MessageListener 也能处理）
        self.notify_observers(&ConnectionEvent::Message(data));

        // 如果是系统命令，处理完并通知 observers 后直接返回
        if is_system_command {
            return;
        }

        // 处理业务命令（Message, Notification）
        self.handle_business_commands(&frame).await;

        // 处理消息路由
        self.handle_message_routing(&frame).await;
    }

    /// 解析消息（内部辅助函数）
    async fn parse_message(&self, data: &[u8]) -> Result<Frame> {
        let parser = self.parser.lock().await;
        parser.parse(data)
    }

    /// 处理系统命令（CONNECT_ACK, PONG, KICKED）
    ///
    /// # 返回
    /// `true` 表示已处理，不需要继续处理；`false` 表示不是系统命令或需要继续处理
    async fn handle_system_commands(&self, frame: &Frame) -> bool {
        let Some(cmd) = &frame.command else {
            return false;
        };

        let Some(Type::System(sys_cmd)) = &cmd.r#type else {
            return false;
        };

        let cmd_type = match SystemCommandType::try_from(sys_cmd.r#type) {
            Ok(t) => t,
            Err(_) => return false,
        };

        match cmd_type {
            SystemCommandType::ConnectAck => {
                self.handle_connect_ack_command(frame).await;
                true
            }
            SystemCommandType::Pong => {
                self.handle_pong_command(frame).await;
                true
            }
            SystemCommandType::Kicked => {
                self.handle_kicked_command(frame).await;
                true
            }
            _ => false,
        }
    }

    /// 处理 CONNECT_ACK 命令
    async fn handle_connect_ack_command(&self, frame: &Frame) {
        // 通知事件处理器
        if let Some(ref handler) = self.event_handler {
            let _ = handler
                .handle_system_command(SystemCommandType::ConnectAck, frame)
                .await;
        }

        // 处理 CONNECT_ACK
        match self.handle_connect_ack(frame) {
            Ok((format, compression, encryption)) => {
                tracing::info!(
                    "[ClientCore] ✅ 收到 CONNECT_ACK: 服务端确定的序列化方式={:?}, 压缩方式={:?}, 加密方式={:?}",
                    format,
                    compression,
                    encryption
                );

                // 更新 parser 为协商后的格式（如果不是强制模式）
                if !self.config.is_force_format() {
                    self.update_parser(format, compression.clone(), encryption.clone())
                        .await;
                    tracing::info!(
                        "[ClientCore] ✅ 解析器已更新为协商后的格式: {:?}, 压缩: {:?}, 加密: {:?}",
                        format,
                        compression,
                        encryption
                    );
                } else {
                    tracing::info!(
                        "[ClientCore] ℹ️  强制模式：继续使用客户端强制指定的格式: {:?}, 压缩: {:?}",
                        self.config.get_serialization_format(),
                        self.config.get_compression()
                    );
                    self.negotiation_completed.store(true, Ordering::SeqCst);
                    self.negotiation_notify.notify_waiters();
                }

                // 发送 NEGOTIATION_READY 命令，通知服务端客户端已准备好按协商方式通信
                // 注意：这里使用协商后的 parser（如果已更新）或 JSON parser（如果还在协商前）
                // 但 NEGOTIATION_READY 应该在协商完成后发送，所以应该使用协商后的 parser
                if let Err(e) = self.send_negotiation_ready().await {
                    tracing::warn!("[ClientCore] 发送 NEGOTIATION_READY 失败: {}", e);
                }

                self.try_start_heartbeat().await;
            }
            Err(e) => {
                self.fail_negotiation(e.to_string()).await;
            }
        }
    }

    /// 处理 PONG 命令
    async fn handle_pong_command(&self, frame: &Frame) {
        // 通知事件处理器
        if let Some(ref handler) = self.event_handler {
            let _ = handler
                .handle_system_command(SystemCommandType::Pong, frame)
                .await;
        }

        // 记录 PONG，更新心跳
        self.record_pong();
    }

    /// 处理 KICKED 命令
    async fn handle_kicked_command(&self, frame: &Frame) {
        let Some(cmd) = &frame.command else {
            return;
        };

        let Some(Type::System(sys_cmd)) = &cmd.r#type else {
            return;
        };

        let reason = sys_cmd.message.clone();
        tracing::warn!("[ClientCore] ⚠️  收到被踢消息: {}", reason);

        // 解析被踢原因（从 metadata 中获取详细信息）
        let kick_reason = Self::parse_kick_reason(&reason, sys_cmd);

        // 通知事件处理器
        if let Some(ref handler) = self.event_handler
            && let Err(e) = handler
                .handle_system_command(SystemCommandType::Kicked, frame)
                .await
        {
            tracing::warn!("[ClientCore] 事件处理器处理 KICKED 失败: {}", e);
        }

        // 更新连接状态为断开（被踢）
        self.state_manager.set_disconnected();

        // 主动断开连接
        self.disconnect_on_kicked().await;

        // 被踢后立刻取消按 message_id 等待的 RPC，避免 send_frame_and_wait 空等至超时
        self.cancel_all_pending_responses().await;

        // 仅在协商完成后且非我方主动断开时，向观察者通知「被踢」语义
        // - 协议竞速：未协商完成的连接收到 KICK 不通知
        // - 重复登录：上层先 disconnect 再建新连接时，disconnect_requested 已置位，读循环后续收到的 KICK 不通知
        let should_notify = self.negotiation_completed.load(Ordering::SeqCst)
            && !self.disconnect_requested.load(Ordering::SeqCst);
        if should_notify {
            self.notify_observers(&ConnectionEvent::Disconnected(kick_reason.clone()));
            tracing::info!("[ClientCore] 连接已断开（被踢）: {}", kick_reason);
        } else {
            tracing::debug!(
                "[ClientCore] 收到 KICKED 但不向观察者通知（协商未完成或我方已请求断开）"
            );
        }
    }

    /// 解析被踢原因（内部辅助函数）
    fn parse_kick_reason(
        base_reason: &str,
        sys_cmd: &crate::common::protocol::SystemCommand,
    ) -> String {
        if let Some(reason_bytes) = sys_cmd.metadata.get("reason")
            && let Ok(reason_str) = String::from_utf8(reason_bytes.clone())
            && reason_str == "device_conflict"
        {
            return format!("设备冲突：{}", base_reason);
        }
        base_reason.to_string()
    }

    /// 断开连接（被踢时调用）
    async fn disconnect_on_kicked(&self) {
        // 尝试从 client_connection 断开
        let client_conn_opt = self.take_client_connection();

        if let Some(client_conn) = client_conn_opt {
            let mut conn = client_conn.lock().await;
            if let Err(e) = conn.close().await {
                tracing::error!("[ClientCore] 断开连接失败: {}", e);
            } else {
                tracing::info!("[ClientCore] ✅ 已主动断开连接（被踢）");
            }
        } else {
            // 如果 client_connection 未设置，记录警告但不阻塞
            // 连接会在底层传输层自动关闭
            tracing::warn!("[ClientCore] ⚠️  客户端连接未设置，等待底层传输层关闭连接");
        }
    }

    /// 处理业务命令（Message, Notification）
    async fn handle_business_commands(&self, frame: &Frame) {
        let Some(ref handler) = self.event_handler else {
            return;
        };

        let Some(cmd) = &frame.command else {
            return;
        };

        match &cmd.r#type {
            Some(Type::Payload(msg_cmd)) => {
                if let Ok(cmd_type) = PayloadCommandType::try_from(msg_cmd.r#type) {
                    let _ = handler.handle_message_command(cmd_type, frame).await;
                }
            }
            Some(Type::Notification(notif_cmd)) => {
                if let Ok(cmd_type) = NotificationCommandType::try_from(notif_cmd.r#type) {
                    let _ = handler.handle_notification_command(cmd_type, frame).await;
                }
            }
            _ => {}
        }
    }

    /// 处理消息路由
    async fn handle_message_routing(&self, frame: &Frame) {
        let Some(ref router) = self.message_router else {
            return;
        };

        match router.route(frame).await {
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

    /// CONNECT_ACK 无效或协商失败：立即唤醒 `wait_for_negotiation` 并上报连接错误。
    async fn fail_negotiation(&self, reason: String) {
        tracing::warn!("[ClientCore] 协商失败: {}", reason);
        if let Ok(mut stored) = self.negotiation_failure_reason.lock() {
            *stored = Some(reason);
        }
        self.negotiation_notify.notify_waiters();
        self.stop_heartbeat();
        self.cancel_all_pending_responses().await;
        if let Ok(reason) = self.negotiation_failure_reason.lock()
            && let Some(msg) = reason.as_ref()
        {
            self.handle_connection_event(&ConnectionEvent::Error(FlareError::protocol_error(
                msg.clone(),
            )));
        }
    }

    fn negotiation_failure_error(&self) -> Option<FlareError> {
        self.negotiation_failure_reason
            .lock()
            .ok()
            .and_then(|reason| {
                reason
                    .as_ref()
                    .map(|msg| FlareError::protocol_error(msg.clone()))
            })
    }

    fn reset_negotiation_state(&self) {
        self.negotiation_completed.store(false, Ordering::SeqCst);
        if let Ok(mut reason) = self.negotiation_failure_reason.lock() {
            *reason = None;
        }
    }

    /// 处理连接事件
    pub fn handle_connection_event(&self, event: &ConnectionEvent) {
        // 通知事件处理器
        if let Some(ref handler) = self.event_handler {
            let handler_clone = Arc::clone(handler);
            let event_clone = event.clone();
            crate::client::runtime::spawn_client_task(async move {
                let _ = handler_clone.handle_connection_event(&event_clone).await;
            });
        }

        // 更新状态
        match event {
            ConnectionEvent::Connected => {
                self.state_manager.set_connected();
                self.reset_negotiation_state();
            }
            ConnectionEvent::Disconnected(_) => {
                self.state_manager.set_disconnected();
                self.reset_negotiation_state();
                let pending = Arc::clone(&self.pending_map);
                crate::client::runtime::spawn_client_task(async move {
                    let mut map = pending.lock().await;
                    if !map.is_empty() {
                        tracing::debug!(
                            count = map.len(),
                            "[ClientCore] connection disconnected: clearing pending response waiters"
                        );
                        map.clear();
                    }
                });
            }
            ConnectionEvent::Error(_) => {
                self.state_manager.set_failed();
                self.reset_negotiation_state();
                let pending = Arc::clone(&self.pending_map);
                crate::client::runtime::spawn_client_task(async move {
                    let mut map = pending.lock().await;
                    if !map.is_empty() {
                        tracing::debug!(
                            count = map.len(),
                            "[ClientCore] connection error: clearing pending response waiters"
                        );
                        map.clear();
                    }
                });
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

    /// 检查协商是否已完成
    pub fn is_negotiation_completed(&self) -> bool {
        self.negotiation_completed
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// 等待 CONNECT_ACK 协商完成（WASM/JS 侧应在发送业务消息前调用）
    pub async fn wait_for_negotiation(&self, timeout: std::time::Duration) -> Result<()> {
        if self.is_negotiation_completed() {
            return Ok(());
        }
        if let Some(err) = self.negotiation_failure_error() {
            return Err(err);
        }

        #[cfg(target_arch = "wasm32")]
        {
            use crate::common::platform::monotonic_now;
            let deadline = monotonic_now() + timeout;
            loop {
                self.drain_wasm_inbound().await;
                if self.is_negotiation_completed() {
                    return Ok(());
                }
                if let Some(err) = self.negotiation_failure_error() {
                    return Err(err);
                }
                if monotonic_now() >= deadline {
                    return Err(negotiation_timeout_error(timeout));
                }
                crate::common::platform::yield_to_event_loop().await;
            }
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            wait_for_negotiation_notify(
                Arc::clone(&self.negotiation_completed),
                Arc::clone(&self.negotiation_failure_reason),
                Arc::clone(&self.negotiation_notify),
                timeout,
            )
            .await
        }
    }

    /// 检查是否可以连接
    pub fn can_connect(&self) -> bool {
        self.state_manager.get_state().can_connect()
    }

    /// 返回当前心跳策略快照。
    pub fn current_heartbeat_config(&self) -> HeartbeatConfig {
        self.heartbeat_config
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_else(|_| self.config.heartbeat.clone())
    }

    /// 当前实际心跳间隔。
    pub fn heartbeat_effective_interval(&self) -> std::time::Duration {
        self.current_heartbeat_config().effective_interval()
    }

    /// 运行期更新心跳策略。启动前更新会影响后续启动，启动后更新会影响下一轮心跳。
    pub fn update_heartbeat_config(&self, update: impl FnOnce(&mut HeartbeatConfig)) {
        if let Ok(mut config) = self.heartbeat_config.write() {
            update(&mut config);
        }
    }

    /// 更新应用前后台状态。
    pub fn set_heartbeat_app_state(&self, state: HeartbeatAppState) {
        self.update_heartbeat_config(|config| {
            config.app_state = state;
        });
    }

    /// 更新 NAT 空闲超时探测结果。
    pub fn set_heartbeat_nat_timeout(&self, timeout: Option<std::time::Duration>) {
        self.update_heartbeat_config(|config| {
            config.nat_timeout = timeout;
        });
    }

    /// 记录收到 PONG（心跳响应）
    ///
    /// 由消息观察者调用，用于更新心跳状态
    ///
    /// 注意：由于观察者是同步的，我们需要异步获取锁
    pub fn record_pong(&self) {
        let heartbeat = match self.heartbeat_manager.lock() {
            Ok(guard) => guard.as_ref().map(Arc::clone),
            Err(_) => None,
        };
        let Some(heartbeat) = heartbeat else {
            return;
        };
        // HeartbeatManager::record_pong 是 `&self` 方法
        // 但由于我们使用了 Arc<Mutex<>>，需要先获取锁
        #[cfg(not(target_arch = "wasm32"))]
        {
            crate::client::runtime::run_client_async(async {
                let hb_guard = heartbeat.lock().await;
                hb_guard.record_pong();
            });
        }
        #[cfg(target_arch = "wasm32")]
        {
            let heartbeat = Arc::clone(&heartbeat);
            crate::client::runtime::spawn_client_task(async move {
                let hb_guard = heartbeat.lock().await;
                hb_guard.record_pong();
            });
        }
    }
}

impl ClientCore {
    /// 注册一个按 message_id 等待的响应通道
    /// 返回 Receiver，调用方可在外部等待
    pub async fn register_pending_response(&self, message_id: &str) -> oneshot::Receiver<Frame> {
        let (tx, rx) = oneshot::channel();
        let mut pending = self.pending_map.lock().await;
        pending.insert(message_id.to_string(), tx);
        rx
    }

    /// 取消等待（超时或主动取消时调用）
    pub async fn cancel_pending_response(&self, message_id: &str) {
        let mut pending = self.pending_map.lock().await;
        pending.remove(message_id);
    }

    /// 连接丢失或被踢时取消全部等待，使 `send_frame_and_wait` 尽快收到通道关闭错误。
    pub async fn cancel_all_pending_responses(&self) {
        let mut pending = self.pending_map.lock().await;
        if !pending.is_empty() {
            tracing::debug!(
                count = pending.len(),
                "[ClientCore] clearing all pending response waiters"
            );
            pending.clear();
        }
    }
}

// 为 ClientCore 实现 Clone（用于共享状态管理器和观察者）
impl Clone for ClientCore {
    fn clone(&self) -> Self {
        Self {
            state_manager: Arc::clone(&self.state_manager),
            parser: Arc::clone(&self.parser),
            heartbeat_manager: Arc::clone(&self.heartbeat_manager),
            heartbeat_config: Arc::clone(&self.heartbeat_config),
            message_router: self.message_router.as_ref().map(|_| MessageRouter::new()), // 路由不克隆，创建新的
            observers: Arc::clone(&self.observers),
            config: self.config.clone(),
            event_handler: self.event_handler.clone(), // 事件处理器可以共享
            client_connection: Arc::clone(&self.client_connection), // 共享连接引用
            pending_map: Arc::clone(&self.pending_map),
            negotiation_completed: Arc::clone(&self.negotiation_completed), // 共享协商完成标志
            negotiation_notify: Arc::clone(&self.negotiation_notify),
            negotiation_failure_reason: Arc::clone(&self.negotiation_failure_reason),
            disconnect_requested: Arc::clone(&self.disconnect_requested),
            #[cfg(target_arch = "wasm32")]
            wasm_inbound: Arc::clone(&self.wasm_inbound),
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod client_core_tests {
    use super::*;
    use crate::common::compression::CompressionAlgorithm;
    use crate::common::encryption::EncryptionAlgorithm;
    use crate::common::protocol::SerializationFormat;
    use std::time::Duration;

    #[tokio::test]
    async fn update_parser_marks_negotiation_completed() {
        let core = ClientCore::new(&ClientConfig::default());
        assert!(!core.is_negotiation_completed());

        core.update_parser(
            SerializationFormat::Protobuf,
            CompressionAlgorithm::Gzip,
            EncryptionAlgorithm::None,
        )
        .await;

        assert!(core.is_negotiation_completed());
    }

    #[tokio::test]
    async fn wait_for_negotiation_returns_after_flag_set() {
        let core = ClientCore::new(&ClientConfig::default());
        let core = Arc::new(core);

        let waiter = {
            let core = Arc::clone(&core);
            tokio::spawn(async move {
                core.wait_for_negotiation(Duration::from_secs(1))
                    .await
                    .expect("negotiation wait")
            })
        };

        tokio::time::sleep(Duration::from_millis(20)).await;
        core.update_parser(
            SerializationFormat::Json,
            CompressionAlgorithm::None,
            EncryptionAlgorithm::None,
        )
        .await;

        waiter.await.expect("wait task");
    }

    #[tokio::test]
    async fn start_heartbeat_before_negotiation_does_not_panic() {
        let core = ClientCore::new(&ClientConfig::default());
        // No connection attached; should no-op safely before negotiation.
        core.start_heartbeat(Arc::new(Mutex::new(
            Box::new(MockConnection) as Box<dyn Connection>
        )))
        .await;
        assert!(!core.is_negotiation_completed());
    }

    #[test]
    fn heartbeat_runtime_policy_is_shared_across_core_clones() {
        let core = ClientCore::new(&ClientConfig::default());
        let cloned = core.clone();

        assert_eq!(core.heartbeat_effective_interval(), Duration::from_secs(30));
        cloned.set_heartbeat_app_state(HeartbeatAppState::Background);
        assert_eq!(
            core.heartbeat_effective_interval(),
            Duration::from_secs(120)
        );

        core.set_heartbeat_nat_timeout(Some(Duration::from_secs(40)));
        assert_eq!(
            cloned.heartbeat_effective_interval(),
            Duration::from_secs(28)
        );
    }

    #[test]
    fn client_connection_take_clears_shared_core_slot() {
        let mut core = ClientCore::new(&ClientConfig::default());
        let cloned = core.clone();
        core.set_client_connection(Arc::new(Mutex::new(
            Box::new(MockConnection) as Box<dyn Connection>
        )));

        assert!(cloned.take_client_connection().is_some());
        assert!(core.take_client_connection().is_none());
    }

    struct MockConnection;

    #[async_trait::async_trait]
    impl Connection for MockConnection {
        fn add_observer(&mut self, _observer: crate::transport::events::ArcObserver) {}
        fn remove_observer(&mut self, _observer: crate::transport::events::ArcObserver) {}
        async fn send(&mut self, _data: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
        fn last_active_time(&self) -> crate::common::platform::MonotonicInstant {
            crate::common::platform::monotonic_now()
        }
        fn update_active_time(&mut self) {}
    }
}
