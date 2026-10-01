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

/// 服务端因租户不可用（停用、删除中）拒绝 CONNECT 或踢线时给出的类型化原因。
///
/// CONNECT 被拒时它映射成 `PERMISSION_DENIED`，与 HTTP 入口、消息摄入对同一原因的错误码一致；
/// 它不是鉴权失败，上层不应为它刷新令牌或自动重连。
pub const TENANT_UNAVAILABLE_REASON: &str = "TENANT_UNAVAILABLE";

/// 把服务端拒绝 CONNECT 时下发的 ERROR 帧（`message` 为类型化原因，`metadata.code` 为错误码）
/// 映射成客户端错误。
///
/// - `TENANT_UNAVAILABLE` → `PERMISSION_DENIED`；
/// - 其余能识别的错误码（如 `AUTHENTICATION_FAILED`、`TOKEN_EXPIRED`，以及服务端暂时放不了行时的
///   `SERVICE_UNAVAILABLE`）原样使用；
/// - 原因认不出时退回 `metadata.code`，再认不出就是 `CONNECTION_REFUSED`。
///
/// 错误文本以类型化原因开头，上层可以据此分辨。
pub fn connect_rejection_error(reason: &str, code: Option<&str>) -> FlareError {
    let reason = reason.trim();
    let code = code.map(str::trim).filter(|code| !code.is_empty());
    let error_code = if reason == TENANT_UNAVAILABLE_REASON {
        crate::common::error::ErrorCode::PermissionDenied
    } else {
        parse_error_code(reason)
            .or_else(|| code.and_then(parse_error_code))
            .unwrap_or(crate::common::error::ErrorCode::ConnectionRefused)
    };
    let label = if reason.is_empty() {
        code.unwrap_or(error_code.as_str())
    } else {
        reason
    };
    FlareError::localized(error_code, format!("{label}: server rejected CONNECT"))
}

fn parse_error_code(value: &str) -> Option<crate::common::error::ErrorCode> {
    serde_json::from_value(serde_json::Value::String(value.to_string())).ok()
}

/// 连接在 CONNECT_ACK 之前被关掉、服务端又没给原因时的错误：这是连接失败，不能猜成令牌被拒。
fn closed_before_connect_ack_error(detail: &str) -> FlareError {
    FlareError::connection_failed(format!(
        "server closed the connection before CONNECT_ACK without a reason ({detail})"
    ))
}

fn negotiation_timeout_error(timeout: std::time::Duration) -> FlareError {
    FlareError::connection_timeout(format!(
        "Negotiation timeout after {:?} (CONNECT_ACK not received). {}",
        timeout, NEGOTIATION_TIMEOUT_HINT
    ))
}

#[cfg(not(target_arch = "wasm32"))]
async fn wait_for_negotiation_notify(
    flag: Arc<AtomicBool>,
    failure: Arc<StdMutex<Option<FlareError>>>,
    notify: Arc<Notify>,
    timeout: std::time::Duration,
) -> Result<()> {
    let wait = async move {
        loop {
            // 先登记唤醒再检查，避免检查与等待之间的通知丢失。
            let notified = notify.notified();
            if flag.load(Ordering::SeqCst) {
                return Ok(());
            }
            if let Ok(failure) = failure.lock()
                && let Some(error) = failure.as_ref()
            {
                return Err(error.clone());
            }
            notified.await;
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
    /// 前台/网络恢复时唤醒心跳循环做一次即时验活（不等一个完整周期），
    /// 用于戳穿半开死连（onclose 从未触发的场景）并触发重连自愈。
    heartbeat_probe_wake: Arc<Notify>,
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
    /// CONNECT_ACK 之前的失败（服务端拒绝、连接被关、CONNECT_ACK 无效）；
    /// `wait_for_negotiation` 立即返回它
    negotiation_failure: Arc<StdMutex<Option<FlareError>>>,
    /// 我方已请求断开（disconnect_internal 中置位）：若随后读循环仍收到 KICK，不向观察者通知「被踢」，避免重复登录/协议竞速时误报
    disconnect_requested: Arc<AtomicBool>,
    /// 上一条连接因服务端停机而断开（收到 CLOSE(SERVER_SHUTDOWN)，或断开原因是停机）：
    /// 下一次重连先随机等一小段（见 [`crate::common::shutdown`]），新连接建立时清除。
    server_shutdown: Arc<AtomicBool>,
    /// WASM: browser WebSocket 回调线程同步入队，由 `wait_for_negotiation`/drain 在 LocalSet 内处理；
    /// 每帧带着入队那一刻的协商状态（见 [`Self::handle_received_message`]）
    #[cfg(target_arch = "wasm32")]
    wasm_inbound: Arc<StdMutex<Vec<(Vec<u8>, bool)>>>,
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
            heartbeat_probe_wake: Arc::new(Notify::new()),
            message_router,
            observers: Arc::new(StdMutex::new(Vec::new())),
            config: config.clone(),
            event_handler: None,
            client_connection: Arc::new(std::sync::Mutex::new(None)),
            pending_map: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            negotiation_completed: Arc::new(AtomicBool::new(false)),
            negotiation_notify: Arc::new(Notify::new()),
            negotiation_failure: Arc::new(StdMutex::new(None)),
            disconnect_requested: Arc::new(AtomicBool::new(false)),
            server_shutdown: Arc::new(AtomicBool::new(false)),
            #[cfg(target_arch = "wasm32")]
            wasm_inbound: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    /// WASM: enqueue bytes from the browser `onmessage` callback (sync context).
    #[cfg(target_arch = "wasm32")]
    pub fn push_wasm_inbound(&self, data: Vec<u8>) {
        let negotiated = self.is_negotiation_completed();
        if let Ok(mut queue) = self.wasm_inbound.lock() {
            if queue.len() >= MAX_WASM_INBOUND_QUEUE {
                queue.remove(0);
                tracing::warn!(
                    "[ClientCore] wasm inbound queue full (max {}), dropping oldest frame",
                    MAX_WASM_INBOUND_QUEUE
                );
            }
            queue.push((data, negotiated));
        }
        self.negotiation_notify.notify_waiters();
    }

    /// WASM: process all queued inbound frames on the current LocalSet task.
    #[cfg(target_arch = "wasm32")]
    pub async fn drain_wasm_inbound(&self) {
        let batch: Vec<(Vec<u8>, bool)> = match self.wasm_inbound.lock() {
            Ok(mut queue) if !queue.is_empty() => queue.drain(..).collect(),
            _ => return,
        };
        for (data, negotiated) in batch {
            self.handle_received_message(data, negotiated).await;
        }
    }

    /// 标记「我方已请求断开」（disconnect_internal 调用前设置，收到 KICK 时不向观察者通知被踢）
    pub fn set_disconnect_requested(&self, value: bool) {
        self.disconnect_requested.store(value, Ordering::SeqCst);
    }

    /// 重连前该等多久：上一条连接因服务端停机而断开时，在
    /// [`SERVER_SHUTDOWN_RECONNECT_MIN`](crate::common::shutdown::SERVER_SHUTDOWN_RECONNECT_MIN)–
    /// [`SERVER_SHUTDOWN_RECONNECT_MAX`](crate::common::shutdown::SERVER_SHUTDOWN_RECONNECT_MAX)
    /// 之间随机取值（只用一次），否则用 `configured`。
    ///
    /// 停机是计划内的：按配置的间隔立即重试，只会撞上还没起来的实例；所有客户端同一刻重连，
    /// 又会把刚起来的实例冲垮。
    pub fn reconnect_delay(&self, configured: std::time::Duration) -> std::time::Duration {
        if self.server_shutdown.swap(false, Ordering::SeqCst) {
            crate::common::shutdown::server_shutdown_reconnect_delay()
        } else {
            configured
        }
    }

    /// 上一条连接是否因服务端停机而断开（新连接建立后复位）。
    pub fn closed_by_server_shutdown(&self) -> bool {
        self.server_shutdown.load(Ordering::SeqCst)
    }

    /// 我方是否已主动断开这个客户端。主动断开后它不能再被「发送前自动重连」拉起来：
    /// 上层（SDK）已经换了新客户端或登出，旧客户端复活会带着旧 token 建出一条上层不知道的连接。
    pub fn is_disconnect_requested(&self) -> bool {
        self.disconnect_requested.load(Ordering::SeqCst)
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
        self.negotiation_failure = Arc::clone(&shared.negotiation_failure);
        self.server_shutdown = Arc::clone(&shared.server_shutdown);
        self.heartbeat_config = Arc::clone(&shared.heartbeat_config);
        self.heartbeat_probe_wake = Arc::clone(&shared.heartbeat_probe_wake);
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
            "[ClientCore] 协商完成，解析器已更新: 最终序列化方式={:?}, 最终压缩方式={:?}, 最终加密方式={:?}, negotiation_completed={}",
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
        heartbeat.start(
            connection,
            parser_ref,
            Arc::clone(&self.heartbeat_probe_wake),
        );
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
        stop_heartbeat_on(heartbeat);
    }

    /// 处理接收到的消息
    ///
    /// 如果启用了路由，使用路由处理；否则直接通知观察者
    pub async fn handle_message(&self, data: Vec<u8>) {
        let negotiated = self.negotiation_completed.load(Ordering::SeqCst);
        self.handle_received_message(data, negotiated).await;
    }

    /// 处理一帧入站数据；`negotiated_at_receipt` 是这帧被读出那一刻的协商状态。
    ///
    /// 入站帧在独立任务里处理，而连接关闭事件在读循环里同步处理：服务端先发 KICKED 再关流时，
    /// EOF 往往先把协商标志复位，KICKED 随后才被解析。若按处理时的标志选解析器，它会被当成
    /// 协商前的 JSON 解析失败而整帧丢掉。解析器按读出时的状态选。
    pub async fn handle_received_message(&self, data: Vec<u8>, negotiated_at_receipt: bool) {
        // 解析消息
        let frame = if !negotiated_at_receipt {
            // 协商未完成：使用 PRE_NEGOTIATION_PARSER（这个阶段只会收到 CONNECT_ACK 或拒绝 CONNECT 的 ERROR）
            use crate::common::message::parser::PRE_NEGOTIATION_PARSER;
            match PRE_NEGOTIATION_PARSER.parse(&data) {
                Ok(frame) => frame,
                // 紧跟在 CONNECT_ACK 之后读出的帧：读出时协商尚未落地，此刻已经落地，按协商结果再解析一次
                Err(_) if self.is_negotiation_completed() => {
                    match self.parse_message(&data).await {
                        Ok(frame) => frame,
                        Err(e) => {
                            tracing::warn!("Failed to parse message (negotiated): {}", e);
                            return;
                        }
                    }
                }
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
                    "[ClientCore] 匹配到等待的响应: message_id={}",
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
                        "[ClientCore] 响应已发送到等待通道: message_id={}",
                        frame.message_id
                    );
                    true // 发送成功，这是等待的响应，不需要继续处理
                }
            } else {
                tracing::debug!(
                    "[ClientCore] 未找到等待的响应: message_id={}",
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
        let is_system_command = self
            .handle_system_commands(&frame, negotiated_at_receipt)
            .await;

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
    async fn handle_system_commands(&self, frame: &Frame, negotiated_at_receipt: bool) -> bool {
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
            // CONNECT_ACK 之前的 ERROR 是服务端拒绝 CONNECT 的原因（读循环里通常已同步登记过，这里幂等）
            SystemCommandType::Error if !negotiated_at_receipt => {
                self.record_connect_rejection(sys_cmd);
                true
            }
            SystemCommandType::Close => {
                self.handle_close_command(frame, sys_cmd, negotiated_at_receipt)
                    .await;
                true
            }
            _ => false,
        }
    }

    /// 登记服务端拒绝 CONNECT 的原因（ERROR 系统帧），并唤醒等待协商的一方。
    ///
    /// 必须早于随后的关连接事件生效：[`ClientMessageObserver`](super::common::ClientMessageObserver)
    /// 在读循环里同步调用它，关连接事件据此报出类型化原因，而不是泛泛的「连接被关」。
    pub(crate) fn record_connect_rejection(
        &self,
        sys_cmd: &crate::common::protocol::SystemCommand,
    ) {
        let code = sys_cmd
            .metadata
            .get("code")
            .and_then(|code| std::str::from_utf8(code).ok());
        let error = connect_rejection_error(&sys_cmd.message, code);
        if let Ok(mut failure) = self.negotiation_failure.lock() {
            if failure.is_some() {
                return;
            }
            tracing::warn!("[ClientCore] CONNECT rejected by server: {}", error);
            *failure = Some(error);
        }
        self.negotiation_notify.notify_waiters();
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
                    "[ClientCore] 收到 CONNECT_ACK: 服务端确定的序列化方式={:?}, 压缩方式={:?}, 加密方式={:?}",
                    format,
                    compression,
                    encryption
                );

                // 更新 parser 为协商后的格式（如果不是强制模式）
                if !self.config.is_force_format() {
                    self.update_parser(format, compression.clone(), encryption.clone())
                        .await;
                    tracing::info!(
                        "[ClientCore] 解析器已更新为协商后的格式: {:?}, 压缩: {:?}, 加密: {:?}",
                        format,
                        compression,
                        encryption
                    );
                } else {
                    tracing::info!(
                        "[ClientCore] 强制模式：继续使用客户端强制指定的格式: {:?}, 压缩: {:?}",
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
        tracing::warn!("[ClientCore] 收到被踢消息: {}", reason);

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

    /// 处理服务端下发的 CLOSE：服务端要关闭这条连接（典型是停机，原因为 `SERVER_SHUTDOWN`）。
    ///
    /// 协商完成后收到：以类型化原因自行断开，观察者收到的断开原因形如
    /// `SERVER_SHUTDOWN: server closed the connection`，上层据此调整重连节奏（停机后先随机等一小段，
    /// 而不是按错误立即重试）；不必等服务端在宽限期满后强行关闭。协商完成前收到：按服务端拒绝 CONNECT
    /// 登记原因（metadata `code` 为 `SERVICE_UNAVAILABLE`，可重试）。
    async fn handle_close_command(
        &self,
        frame: &Frame,
        sys_cmd: &crate::common::protocol::SystemCommand,
        negotiated_at_receipt: bool,
    ) {
        if let Some(ref handler) = self.event_handler
            && let Err(e) = handler
                .handle_system_command(SystemCommandType::Close, frame)
                .await
        {
            tracing::warn!("[ClientCore] 事件处理器处理 CLOSE 失败: {}", e);
        }

        if crate::common::shutdown::is_server_shutdown_close(sys_cmd) {
            self.server_shutdown.store(true, Ordering::SeqCst);
        }
        if !negotiated_at_receipt {
            self.record_connect_rejection(sys_cmd);
            return;
        }

        let reason = Self::close_reason(sys_cmd);
        tracing::info!(reason = %reason, "[ClientCore] server is closing the connection");
        let Some(connection) = self.take_client_connection() else {
            tracing::debug!("[ClientCore] CLOSE received without an active connection");
            return;
        };
        let detail = format!("{reason}: server closed the connection");
        let mut conn = connection.lock().await;
        if let Err(e) = conn.close_with_reason(&detail).await {
            tracing::debug!("[ClientCore] closing after server CLOSE failed: {}", e);
        }
    }

    /// CLOSE 的原因：`message`，没有就用 metadata `reason`，都没有为 `CLOSE`。
    fn close_reason(sys_cmd: &crate::common::protocol::SystemCommand) -> String {
        let message = sys_cmd.message.trim();
        if !message.is_empty() {
            return message.to_string();
        }
        sys_cmd
            .metadata
            .get("reason")
            .and_then(|reason| std::str::from_utf8(reason).ok())
            .map(str::trim)
            .filter(|reason| !reason.is_empty())
            .unwrap_or("CLOSE")
            .to_string()
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
                tracing::info!("[ClientCore] 已主动断开连接（被踢）");
            }
        } else {
            // 如果 client_connection 未设置，记录警告但不阻塞
            // 连接会在底层传输层自动关闭
            tracing::warn!("[ClientCore] 客户端连接未设置，等待底层传输层关闭连接");
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
        let error = FlareError::protocol_error(reason);
        if let Ok(mut stored) = self.negotiation_failure.lock() {
            *stored = Some(error.clone());
        }
        self.negotiation_notify.notify_waiters();
        self.stop_heartbeat();
        self.cancel_all_pending_responses().await;
        self.handle_connection_event(&ConnectionEvent::Error(error));
    }

    fn negotiation_failure_error(&self) -> Option<FlareError> {
        self.negotiation_failure
            .lock()
            .ok()
            .and_then(|failure| failure.clone())
    }

    fn reset_negotiation_state(&self) {
        self.negotiation_completed.store(false, Ordering::SeqCst);
        if let Ok(mut failure) = self.negotiation_failure.lock() {
            *failure = None;
        }
    }

    /// 连接在 CONNECT_ACK 之前没了（关闭或读出错）：立即结束协商等待。
    ///
    /// 服务端登记过拒绝原因（ERROR 帧）就报那个原因；没有原因就是连接失败——
    /// 以前一律报成「令牌被拒」的鉴权失败，心跳失败、踢线后的断开也被误报成令牌问题。
    fn fail_negotiation_on_close(&self, detail: &str) {
        let error = if let Ok(mut failure) = self.negotiation_failure.lock() {
            let error = failure
                .take()
                .unwrap_or_else(|| closed_before_connect_ack_error(detail));
            *failure = Some(error.clone());
            error
        } else {
            return;
        };
        tracing::warn!("[ClientCore] 协商阶段连接断开: {}", error);
        self.negotiation_notify.notify_waiters();
    }

    /// 连接已建立、CONNECT_ACK 还没到、也不是我方主动断开。
    /// 协商失败的原因只在下一条连接建立（[`ConnectionEvent::Connected`]）时清空。
    fn closed_during_negotiation(&self) -> bool {
        matches!(
            self.state_manager.get_state(),
            crate::client::connection::state::ConnectionState::Connected
        ) && !self.is_negotiation_completed()
            && !self.disconnect_requested.load(Ordering::SeqCst)
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
                // 传输层在挂观察者时已经报过一次 Connected；客户端发完 CONNECT 后再报的那次
                // 不能复位协商状态，否则已经到达的 CONNECT_ACK 或拒绝原因会被抹掉。
                let already_connected = matches!(
                    self.state_manager.get_state(),
                    crate::client::connection::state::ConnectionState::Connected
                );
                self.state_manager.set_connected();
                if !already_connected {
                    self.reset_negotiation_state();
                    self.server_shutdown.store(false, Ordering::SeqCst);
                }
            }
            ConnectionEvent::Disconnected(reason) => {
                // 服务端在 CONNECT_ACK 之前就关掉了连接：立即唤醒等协商的一方，别让它干等 10s 超时。
                // 原因以服务端下发的 ERROR 帧为准（租户不可用、鉴权失败……）；没有原因就是连接失败。
                let closed_during_negotiation = self.closed_during_negotiation();
                // WebSocket 关闭帧、QUIC 应用关闭带来的停机原因（没收到或不认得 CLOSE 帧时）
                if crate::common::shutdown::is_server_shutdown_reason(reason) {
                    self.server_shutdown.store(true, Ordering::SeqCst);
                }
                self.state_manager.set_disconnected();
                self.negotiation_completed.store(false, Ordering::SeqCst);
                // 心跳绑在这条连接上：留着它，它会在死连接上继续发 PING，失败时再关一次连接并误报，
                // 同时占着槽位，让下一条连接协商完成后起不了自己的心跳。
                self.stop_heartbeat();
                if closed_during_negotiation {
                    self.fail_negotiation_on_close(reason);
                }
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
            ConnectionEvent::Error(error) => {
                let closed_during_negotiation = self.closed_during_negotiation();
                self.state_manager.set_failed();
                self.negotiation_completed.store(false, Ordering::SeqCst);
                self.stop_heartbeat();
                if closed_during_negotiation {
                    self.fail_negotiation_on_close(&error.to_string());
                }
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
                Arc::clone(&self.negotiation_failure),
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

    /// 更新应用前后台状态。回到前台时额外唤醒心跳做一次即时验活：
    /// 后台期间心跳被浏览器节流甚至冻结，连接可能已被服务端回收成半开死连
    /// （readyState 仍报 OPEN、onclose 从未触发）；不主动戳一下就会一直停在
    /// 「自认为在线、实则发送必失败」的状态，直到用户手动刷新。
    pub fn set_heartbeat_app_state(&self, state: HeartbeatAppState) {
        self.update_heartbeat_config(|config| {
            config.app_state = state;
        });
        if state == HeartbeatAppState::Foreground {
            self.heartbeat_probe_wake.notify_one();
        }
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
        record_pong_on(heartbeat);
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
            heartbeat_probe_wake: Arc::clone(&self.heartbeat_probe_wake),
            message_router: self.message_router.as_ref().map(|_| MessageRouter::new()), // 路由不克隆，创建新的
            observers: Arc::clone(&self.observers),
            config: self.config.clone(),
            event_handler: self.event_handler.clone(), // 事件处理器可以共享
            client_connection: Arc::clone(&self.client_connection), // 共享连接引用
            pending_map: Arc::clone(&self.pending_map),
            negotiation_completed: Arc::clone(&self.negotiation_completed), // 共享协商完成标志
            negotiation_notify: Arc::clone(&self.negotiation_notify),
            negotiation_failure: Arc::clone(&self.negotiation_failure),
            disconnect_requested: Arc::clone(&self.disconnect_requested),
            server_shutdown: Arc::clone(&self.server_shutdown),
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

    /// 服务端停机断开后，下一次重连先在抖动窗口里随机等一段（只用一次）；新连接建立后复位。
    /// 其余断开照常按配置的间隔。
    #[tokio::test]
    async fn reconnect_after_a_server_shutdown_waits_a_jittered_pause_once() {
        use crate::common::shutdown::{
            SERVER_SHUTDOWN_RECONNECT_MAX, SERVER_SHUTDOWN_RECONNECT_MIN,
        };
        let configured = Duration::from_millis(100);
        let core = ClientCore::new(&ClientConfig::default());

        core.handle_connection_event(&ConnectionEvent::Disconnected(
            "WebSocket connection closed by peer".to_string(),
        ));
        assert!(!core.closed_by_server_shutdown());
        assert_eq!(core.reconnect_delay(configured), configured);

        for reason in [
            "SERVER_SHUTDOWN",
            "SERVER_SHUTDOWN: server closed the connection",
        ] {
            core.handle_connection_event(&ConnectionEvent::Disconnected(reason.to_string()));
            assert!(core.closed_by_server_shutdown(), "{reason}");
            let delay = core.reconnect_delay(configured);
            assert!(
                (SERVER_SHUTDOWN_RECONNECT_MIN..=SERVER_SHUTDOWN_RECONNECT_MAX).contains(&delay),
                "{reason}: {delay:?}"
            );
            // 只用一次
            assert_eq!(core.reconnect_delay(configured), configured);
        }

        // 新连接建立即复位
        core.handle_connection_event(&ConnectionEvent::Disconnected(
            "SERVER_SHUTDOWN".to_string(),
        ));
        core.handle_connection_event(&ConnectionEvent::Connected);
        assert!(!core.closed_by_server_shutdown());
        assert_eq!(core.reconnect_delay(configured), configured);
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

/// 拿到心跳管理器并记一次 PONG，**不阻塞、不 panic**。
///
/// 这里曾经用 `run_client_async`（`block_in_place` + `block_on`）去取那把 async 锁，
/// 两个代价：
/// ① current-thread runtime 上 `block_in_place` 直接 panic，而 PONG 是每个心跳周期
///    都要走的路径 —— 任务被打死后心跳记账不再更新，连接会被自己判成超时；
/// ② 多线程 runtime 上不 panic，但每收一个 PONG 就要把本 worker 的任务队列交接给
///    另一个线程，只为了写一个时间戳。
///
/// 而 `HeartbeatManager::record_pong` 只要 `&self`，内部已经是 `std::sync::Mutex`
/// 自带互斥 —— 那把外层 async 锁只是取引用的手段。所以常态走 `try_lock` 内联完成，
/// 只有真撞上锁时才丢给后台任务（顺序无所谓：记的是"最近一次 PONG 的时刻"）。
fn record_pong_on(heartbeat: Arc<tokio::sync::Mutex<HeartbeatManager>>) {
    if let Ok(hb_guard) = heartbeat.try_lock() {
        hb_guard.record_pong();
        return;
    }
    crate::client::runtime::spawn_client_task(async move {
        let hb_guard = heartbeat.lock().await;
        hb_guard.record_pong();
    });
}

/// 停止心跳，同上：常态同步完成，撞锁才异步。
///
/// 停止只是把 stop_tx 发一次信号，重复/延后执行都是幂等的。
fn stop_heartbeat_on(heartbeat: Arc<tokio::sync::Mutex<HeartbeatManager>>) {
    if let Ok(mut hb_guard) = heartbeat.try_lock() {
        hb_guard.stop();
        return;
    }
    crate::client::runtime::spawn_client_task(async move {
        let mut hb_guard = heartbeat.lock().await;
        hb_guard.stop();
    });
}

#[cfg(test)]
mod heartbeat_bridge_tests {
    use super::*;
    use std::time::Duration;

    fn manager() -> Arc<tokio::sync::Mutex<HeartbeatManager>> {
        Arc::new(tokio::sync::Mutex::new(HeartbeatManager::new(
            Duration::from_secs(30),
            Duration::from_secs(90),
        )))
    }

    /// `#[tokio::test]` 默认就是 current-thread —— 正是原实现必 panic 的那个形态。
    /// PONG 每个心跳周期都要走，一旦打死任务，心跳记账不再更新，连接会被自己判成超时。
    #[tokio::test]
    async fn record_pong_works_on_current_thread_runtime() {
        let heartbeat = manager();
        {
            let guard = heartbeat.lock().await;
            guard.mark_ping_for_test();
            assert!(
                guard.is_timeout_after_for_test(Duration::from_secs(0)),
                "先制造一个未응答的 ping，否则断言不出 PONG 有没有记上"
            );
        }

        record_pong_on(Arc::clone(&heartbeat));

        let guard = heartbeat.lock().await;
        assert!(
            !guard.is_timeout_after_for_test(Duration::from_secs(0)),
            "PONG 必须被记上：单线程 runtime 下也不能丢"
        );
    }

    /// 撞锁时不能阻塞、更不能 panic —— 退化成后台补记即可。
    #[tokio::test]
    async fn record_pong_falls_back_to_background_when_lock_is_held() {
        let heartbeat = manager();
        {
            let guard = heartbeat.lock().await;
            guard.mark_ping_for_test();
        }
        let held = heartbeat.clone().lock_owned().await;

        record_pong_on(Arc::clone(&heartbeat));
        drop(held);

        for _ in 0..50 {
            tokio::task::yield_now().await;
            if !heartbeat
                .lock()
                .await
                .is_timeout_after_for_test(Duration::from_secs(0))
            {
                return;
            }
        }
        panic!("撞锁后的 PONG 必须由后台任务补记上");
    }

    #[tokio::test]
    async fn stop_heartbeat_works_on_current_thread_runtime() {
        let heartbeat = manager();
        stop_heartbeat_on(Arc::clone(&heartbeat));
        assert!(
            !heartbeat.lock().await.has_stop_channel_for_test(),
            "停止后 stop 通道必须已被取走"
        );
    }
}

#[cfg(test)]
mod negotiation_rejection_tests {
    use super::*;
    use crate::common::error::ErrorCode;
    use crate::common::protocol::{Reliability, frame_with_system_command};

    fn core() -> ClientCore {
        ClientCore::new(&ClientConfig::default())
    }

    fn rejection(message: &str, code: Option<&str>) -> crate::common::protocol::SystemCommand {
        let metadata =
            code.map(|code| HashMap::from([("code".to_string(), code.as_bytes().to_vec())]));
        crate::common::protocol::builder::error(message.to_string(), metadata)
    }

    /// 服务端在 CONNECT_ACK 之前关连接却没给原因：等协商的一方立刻拿到连接失败，
    /// 不再干等 10s，也不再被猜成令牌被拒（心跳失败、踢线后的断开都走这里）。
    #[test]
    fn close_before_connect_ack_without_a_reason_is_a_connection_failure() {
        let core = core();
        core.handle_connection_event(&ConnectionEvent::Connected);
        assert!(core.negotiation_failure_error().is_none());

        core.handle_connection_event(&ConnectionEvent::Disconnected(
            "heartbeat send failed".into(),
        ));

        let err = core
            .negotiation_failure_error()
            .expect("协商阶段被断开必须立刻记为协商失败");
        assert_eq!(err.code(), Some(ErrorCode::ConnectionFailed), "{err}");
        assert!(err.to_string().contains("before CONNECT_ACK"), "{err}");
        assert!(!err.to_string().contains("TOKEN"), "{err}");
    }

    /// 读出错（如 QUIC 连接被关）同样立即结束协商等待。
    #[test]
    fn read_error_before_connect_ack_fails_negotiation_at_once() {
        let core = core();
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.handle_connection_event(&ConnectionEvent::Error(FlareError::io("connection lost")));
        let err = core.negotiation_failure_error().expect("must fail at once");
        assert_eq!(err.code(), Some(ErrorCode::ConnectionFailed), "{err}");
    }

    /// 服务端先下发 ERROR 帧再关连接：关连接事件保留服务端给的类型化原因。
    #[test]
    fn typed_rejection_survives_the_close_that_follows_it() {
        for (message, expected) in [
            ("TENANT_UNAVAILABLE", ErrorCode::PermissionDenied),
            ("AUTHENTICATION_FAILED", ErrorCode::AuthenticationFailed),
        ] {
            let core = core();
            core.handle_connection_event(&ConnectionEvent::Connected);
            core.record_connect_rejection(&rejection(message, Some("AUTHENTICATION_FAILED")));
            core.handle_connection_event(&ConnectionEvent::Disconnected(
                "Stream closed by peer".into(),
            ));

            let err = core
                .negotiation_failure_error()
                .expect("rejection recorded");
            assert_eq!(err.code(), Some(expected), "{err}");
            assert!(err.to_string().contains(message), "{err}");
        }
    }

    #[test]
    fn connect_rejection_error_maps_typed_reasons() {
        let cases = [
            (
                "TENANT_UNAVAILABLE",
                Some("AUTHENTICATION_FAILED"),
                ErrorCode::PermissionDenied,
            ),
            (
                "AUTHENTICATION_FAILED",
                Some("AUTHENTICATION_FAILED"),
                ErrorCode::AuthenticationFailed,
            ),
            ("TOKEN_EXPIRED", None, ErrorCode::TokenExpired),
            // 暂时性拒绝：认证依赖读不到，客户端应退避重连
            (
                "SERVICE_UNAVAILABLE",
                Some("SERVICE_UNAVAILABLE"),
                ErrorCode::ServiceUnavailable,
            ),
            (
                "TENANT_RUNTIME_UNREADABLE",
                Some("SERVICE_UNAVAILABLE"),
                ErrorCode::ServiceUnavailable,
            ),
            (
                "SOME_NEW_REASON",
                Some("AUTHENTICATION_FAILED"),
                ErrorCode::AuthenticationFailed,
            ),
            ("", None, ErrorCode::ConnectionRefused),
        ];
        for (reason, code, expected) in cases {
            let err = connect_rejection_error(reason, code);
            assert_eq!(err.code(), Some(expected), "{reason}: {err}");
            if !reason.is_empty() {
                assert!(err.to_string().contains(reason), "{err}");
            }
        }
    }

    /// 等协商的一方被 ERROR 帧唤醒，拿到的是类型化错误而不是协议错误。
    #[tokio::test]
    async fn wait_for_negotiation_returns_the_typed_rejection() {
        let core = Arc::new(core());
        core.handle_connection_event(&ConnectionEvent::Connected);
        let waiter = {
            let core = Arc::clone(&core);
            tokio::spawn(async move {
                core.wait_for_negotiation(std::time::Duration::from_secs(5))
                    .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        core.record_connect_rejection(&rejection(
            "TENANT_UNAVAILABLE",
            Some("AUTHENTICATION_FAILED"),
        ));

        let err = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("woken at once")
            .expect("join")
            .expect_err("rejected");
        assert_eq!(err.code(), Some(ErrorCode::PermissionDenied), "{err}");
    }

    /// 我方主动断开（切账号/登出）不是被拒，不能伪造协商失败。
    #[test]
    fn client_requested_disconnect_is_not_a_rejection() {
        let core = core();
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.set_disconnect_requested(true);
        core.handle_connection_event(&ConnectionEvent::Disconnected("Closed by client".into()));
        assert!(core.negotiation_failure_error().is_none());
    }

    /// 协商已经完成后的断线是普通掉线，走重连，不是协商失败。
    #[test]
    fn disconnect_after_negotiation_completed_is_not_a_rejection() {
        let core = core();
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.negotiation_completed.store(true, Ordering::SeqCst);
        core.handle_connection_event(&ConnectionEvent::Disconnected("Connection closed".into()));
        assert!(core.negotiation_failure_error().is_none());
    }

    /// 客户端发完 CONNECT 后补报的那次 Connected 不能复位协商：CONNECT_ACK 可能已经到了。
    #[tokio::test]
    async fn repeated_connected_keeps_a_completed_negotiation() {
        let core = core();
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.update_parser(
            crate::common::protocol::SerializationFormat::Protobuf,
            crate::common::compression::CompressionAlgorithm::Gzip,
            crate::common::encryption::EncryptionAlgorithm::None,
        )
        .await;
        core.handle_connection_event(&ConnectionEvent::Connected);
        assert!(core.is_negotiation_completed());

        // 新的一条连接（先断开再连上）才复位。
        core.handle_connection_event(&ConnectionEvent::Disconnected(
            "Stream closed by peer".into(),
        ));
        core.handle_connection_event(&ConnectionEvent::Connected);
        assert!(!core.is_negotiation_completed());
    }

    /// 连接断开时停掉绑在它上面的心跳，下一条连接协商完成后才能起自己的心跳。
    #[tokio::test]
    async fn disconnect_stops_the_heartbeat_of_that_connection() {
        let mut core = core();
        core.set_client_connection(Arc::new(Mutex::new(
            Box::new(NoopConnection) as Box<dyn Connection>
        )));
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.update_parser(
            crate::common::protocol::SerializationFormat::Json,
            crate::common::compression::CompressionAlgorithm::None,
            crate::common::encryption::EncryptionAlgorithm::None,
        )
        .await;
        assert!(
            core.heartbeat_manager.lock().unwrap().is_some(),
            "heartbeat started"
        );

        core.handle_connection_event(&ConnectionEvent::Disconnected(
            "Stream closed by peer".into(),
        ));
        assert!(
            core.heartbeat_manager.lock().unwrap().is_none(),
            "heartbeat stopped"
        );
    }

    /// 服务端先发 KICKED 再关流：关流事件先把协商标志复位，KICKED 的处理晚一步。
    /// 它仍要按读出时的协商结果解析并交给上层，而不是按协商前 JSON 解析失败后整帧丢掉。
    #[tokio::test]
    async fn kicked_frame_read_before_the_close_still_reaches_observers() {
        let core = core();
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.update_parser(
            crate::common::protocol::SerializationFormat::Protobuf,
            crate::common::compression::CompressionAlgorithm::Gzip,
            crate::common::encryption::EncryptionAlgorithm::None,
        )
        .await;
        let seen = Arc::new(StdMutex::new(Vec::new()));
        core.add_observer(Arc::new(RecordingObserver(Arc::clone(&seen))));
        let kicked = frame_with_system_command(
            crate::common::protocol::builder::kicked("TENANT_UNAVAILABLE".to_string(), None),
            Reliability::AtLeastOnce,
        );
        let data = core
            .parser
            .lock()
            .await
            .serialize(&kicked)
            .expect("serialize");

        let negotiated_at_receipt = core.is_negotiation_completed();
        core.handle_connection_event(&ConnectionEvent::Disconnected(
            "Stream closed by peer".into(),
        ));
        core.handle_received_message(data, negotiated_at_receipt)
            .await;

        let messages = seen
            .lock()
            .unwrap()
            .iter()
            .filter(|event| matches!(event, ConnectionEvent::Message(_)))
            .count();
        assert_eq!(messages, 1, "KICKED frame must reach observers");
    }

    struct RecordingObserver(Arc<StdMutex<Vec<ConnectionEvent>>>);

    impl crate::transport::events::ConnectionObserver for RecordingObserver {
        fn on_event(&self, event: &ConnectionEvent) {
            self.0.lock().unwrap().push(event.clone());
        }
    }

    struct NoopConnection;

    #[async_trait::async_trait]
    impl Connection for NoopConnection {
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
