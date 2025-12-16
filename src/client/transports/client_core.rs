//! 客户端核心功能
//!
//! 提供统一的连接状态管理、心跳管理、消息路由等功能，简化客户端实现

use crate::client::config::ClientConfig;
use crate::client::connection::ConnectionStateManager;
use crate::client::heartbeat::HeartbeatManager;
use crate::client::router::MessageRouter;
use crate::common::MessageParser;
use crate::common::error::{FlareError, Result};
use crate::common::protocol::flare::core::commands::command::Type;
use crate::common::protocol::flare::core::commands::message_command::Type as MessageCommandType;
use crate::common::protocol::flare::core::commands::notification_command::Type as NotificationCommandType;
use crate::common::protocol::flare::core::commands::system_command::Type as SystemCommandType;
use crate::common::protocol::{Frame, Reliability, connect, frame_with_system_command};
use crate::transport::connection::Connection;
use crate::transport::events::{ArcObserver, ConnectionEvent};
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{Mutex, oneshot};

/// 客户端核心功能
///
/// 统一管理连接状态、心跳、消息路由，简化客户端实现
pub struct ClientCore {
    /// 连接状态管理器
    pub state_manager: Arc<ConnectionStateManager>,
    /// 消息解析器（使用 Arc<Mutex<>> 以支持协商后更新）
    pub parser: Arc<tokio::sync::Mutex<MessageParser>>,
    /// 心跳管理器（可选，通过配置开启）
    /// 使用 Arc<Mutex<>> 以支持并发访问（从同步的观察者中调用）
    heartbeat_manager: Option<Arc<tokio::sync::Mutex<HeartbeatManager>>>,
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
    client_connection: Arc<std::sync::Mutex<Option<Arc<Mutex<Box<dyn Connection>>>>>>,
    /// 等待响应的请求池（按 message_id 匹配）
    pub(crate) pending_map: Arc<tokio::sync::Mutex<HashMap<String, oneshot::Sender<Frame>>>>,
}

impl ClientCore {
    /// 创建新的客户端核心
    pub fn new(config: &ClientConfig) -> Self {
        let (format, compression) = Self::determine_initial_format(config);
        let parser = MessageParser::new(format, compression);

        let message_router = config.enable_router.then(|| MessageRouter::new());

        Self {
            state_manager: Arc::new(ConnectionStateManager::new()),
            parser: Arc::new(tokio::sync::Mutex::new(parser)),
            heartbeat_manager: None,
            message_router,
            observers: Arc::new(StdMutex::new(Vec::new())),
            config: config.clone(),
            event_handler: None,
            client_connection: Arc::new(std::sync::Mutex::new(None)),
            pending_map: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
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
    ) {
        let mut parser = self.parser.lock().await;
        *parser = MessageParser::new(format, compression);
        tracing::debug!(
            "[ClientCore] 协商完成，解析器已更新: 最终序列化方式={:?}, 最终压缩方式={:?}",
            format,
            compression
        );
    }

    /// 设置客户端连接（用于断开连接）
    pub fn set_client_connection(&mut self, connection: Arc<Mutex<Box<dyn Connection>>>) {
        if let Ok(mut conn) = self.client_connection.lock() {
            *conn = Some(connection);
        }
    }

    /// 设置事件处理器
    pub fn set_event_handler(
        &mut self,
        handler: Option<Arc<dyn crate::client::events::handler::ClientEventHandler>>,
    ) {
        self.event_handler = handler;
    }

    /// 启动心跳（如果启用）
    pub async fn start_heartbeat(&mut self, connection: Arc<Mutex<Box<dyn Connection>>>) {
        // 保存连接引用（用于断开）
        if let Ok(mut conn) = self.client_connection.lock() {
            *conn = Some(Arc::clone(&connection));
        }

        if !self.config.heartbeat.enabled {
            return;
        }

        let mut heartbeat = HeartbeatManager::new(
            self.config.heartbeat.interval,
            self.config.heartbeat.timeout,
        );

        // 获取当前 parser 的副本用于心跳
        let parser = self.parser.lock().await.clone();
        heartbeat.start(connection, parser);
        self.heartbeat_manager = Some(Arc::new(tokio::sync::Mutex::new(heartbeat)));
    }

    /// 停止心跳
    pub fn stop_heartbeat(&mut self) {
        if let Some(heartbeat) = self.heartbeat_manager.take() {
            Self::stop_heartbeat_async(heartbeat);
        }
    }

    /// 异步停止心跳（内部辅助函数）
    fn stop_heartbeat_async(heartbeat: Arc<tokio::sync::Mutex<HeartbeatManager>>) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::try_current()
                .map(|handle| {
                    handle.block_on(async {
                        let mut hb_guard = heartbeat.lock().await;
                        hb_guard.stop();
                    })
                })
                .unwrap_or_else(|_| {
                    tokio::runtime::Runtime::new().unwrap().block_on(async {
                        let mut hb_guard = heartbeat.lock().await;
                        hb_guard.stop();
                    })
                })
        });
    }

    /// 处理接收到的消息
    ///
    /// 如果启用了路由，使用路由处理；否则直接通知观察者
    pub async fn handle_message(&self, data: Vec<u8>) {
        // 解析消息
        let frame = match self.parse_message(&data).await {
            Ok(frame) => frame,
            Err(e) => {
                tracing::warn!("Failed to parse message: {}", e);
                return;
            }
        };
        // 尝试匹配等待的响应（按 Frame.message_id）
        let is_pending_response = {
            tracing::debug!(
                "[ClientCore] handle_message: 尝试匹配等待的响应, frame.message_id={}",
                frame.message_id
            );
            
            // 检查 MessageCommand 中的 message_id（用于调试）
            if let Some(cmd) = &frame.command {
                if let Some(Type::Message(msg_cmd)) = &cmd.r#type {
                    tracing::debug!(
                        "[ClientCore] handle_message: MessageCommand.message_id={}, Frame.message_id={}",
                        msg_cmd.message_id,
                        frame.message_id
                    );
                    // 如果 MessageCommand.message_id 和 Frame.message_id 不一致，记录警告
                    if msg_cmd.message_id != frame.message_id {
                        tracing::warn!(
                            "[ClientCore] handle_message: MessageCommand.message_id 和 Frame.message_id 不一致! MessageCommand.message_id={}, Frame.message_id={}",
                            msg_cmd.message_id,
                            frame.message_id
                        );
                    }
                }
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
                tracing::info!(
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
                    tracing::info!(
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
                    encryption.as_deref().unwrap_or("none")
                );

                // 更新 parser 为协商后的格式（如果不是强制模式）
                if !self.config.is_force_format() {
                    self.update_parser(format, compression).await;
                    tracing::info!(
                        "[ClientCore] ✅ 解析器已更新为协商后的格式: {:?}, 压缩: {:?}",
                        format,
                        compression
                    );
                } else {
                    tracing::info!(
                        "[ClientCore] ℹ️  强制模式：继续使用客户端强制指定的格式: {:?}, 压缩: {:?}",
                        self.config.get_serialization_format(),
                        self.config.get_compression()
                    );
                }
            }
            Err(e) => {
                tracing::warn!("Failed to handle CONNECT_ACK: {}", e);
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
        if let Some(ref handler) = self.event_handler {
            if let Err(e) = handler
                .handle_system_command(SystemCommandType::Kicked, frame)
                .await
            {
                tracing::warn!("[ClientCore] 事件处理器处理 KICKED 失败: {}", e);
            }
        }

        // 更新连接状态为断开（被踢）
        self.state_manager.set_disconnected();

        // 主动断开连接
        self.disconnect_on_kicked().await;

        // 通知观察者（被踢事件）
        self.notify_observers(&ConnectionEvent::Disconnected(kick_reason.clone()));

        tracing::info!("[ClientCore] 连接已断开（被踢）: {}", kick_reason);
    }

    /// 解析被踢原因（内部辅助函数）
    fn parse_kick_reason(
        base_reason: &str,
        sys_cmd: &crate::common::protocol::SystemCommand,
    ) -> String {
        if let Some(reason_bytes) = sys_cmd.metadata.get("reason") {
            if let Ok(reason_str) = String::from_utf8(reason_bytes.clone()) {
                if reason_str == "device_conflict" {
                    return format!("设备冲突：{}", base_reason);
                }
            }
        }
        base_reason.to_string()
    }

    /// 断开连接（被踢时调用）
    async fn disconnect_on_kicked(&self) {
        // 尝试从 client_connection 断开
        let client_conn_opt = {
            if let Ok(conn_guard) = self.client_connection.lock() {
                conn_guard.clone()
            } else {
                None
            }
        };

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
            Some(Type::Message(msg_cmd)) => {
                if let Ok(cmd_type) = MessageCommandType::try_from(msg_cmd.r#type) {
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

    /// 处理连接事件
    pub fn handle_connection_event(&self, event: &ConnectionEvent) {
        // 通知事件处理器
        if let Some(ref handler) = self.event_handler {
            let handler_clone = Arc::clone(handler);
            let event_clone = event.clone();
            tokio::spawn(async move {
                let _ = handler_clone.handle_connection_event(&event_clone).await;
            });
        }

        // 更新状态
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
        let Some(ref heartbeat) = self.heartbeat_manager else {
            return;
        };

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

    /// 发送 CONNECT 消息进行协商
    ///
    /// # 参数
    /// - `connection`: 连接实例
    ///
    /// # 返回
    /// 发送成功返回 `Ok(())`，失败返回错误
    pub async fn send_connect_message(
        &self,
        connection: Arc<Mutex<Box<dyn Connection>>>,
    ) -> Result<()> {
        let metadata = self.build_connect_metadata();

        // 创建 CONNECT 命令（协商前统一使用 JSON，协商后使用协商结果）
        // 注意：CONNECT 消息的 format 字段应该始终是 JSON（协商前统一使用 JSON）
        // 客户端希望使用的格式通过 metadata 中的 "format" 字段传递
        let connect_cmd = connect(crate::common::protocol::SerializationFormat::Json, metadata);
        let connect_frame = frame_with_system_command(connect_cmd, Reliability::AtLeastOnce);

        // 序列化并发送
        let data = self.serialize_connect_frame(&connect_frame).await?;
        let mut conn = connection.lock().await;
        conn.send(&data).await?;

        self.log_connect_sent();
        Ok(())
    }

    /// 构建 CONNECT 消息的元数据
    fn build_connect_metadata(&self) -> HashMap<String, Vec<u8>> {
        let mut metadata = HashMap::new();

        // 确定要使用的序列化格式和压缩算法
        let (format, compression, should_send_format) = self.determine_connect_format();

        tracing::debug!(
            "[ClientCore] 发送 CONNECT 消息: 请求序列化方式={:?}, 请求压缩方式={:?}, 强制模式={}, 发送format={}",
            format,
            compression,
            self.config.is_force_format(),
            should_send_format
        );

        // 添加序列化格式（仅当客户端指定了格式或强制模式时）
        if should_send_format {
            let format_str = match format {
                crate::common::protocol::SerializationFormat::Protobuf => "protobuf",
                crate::common::protocol::SerializationFormat::Json => "json",
            };
            metadata.insert("format".to_string(), format_str.as_bytes().to_vec());
        }

        // 添加压缩算法（仅当客户端指定了压缩时）
        if compression != crate::common::compression::CompressionAlgorithm::None {
            metadata.insert(
                "compression".to_string(),
                compression.as_str().as_bytes().to_vec(),
            );
        }

        // 添加是否强制指定格式的标记
        if self.config.is_force_format() {
            metadata.insert("force_format".to_string(), b"true".to_vec());
        }

        // 添加设备信息
        Self::add_device_metadata(&mut metadata, &self.config);

        // 添加用户 ID
        if let Some(ref user_id) = self.config.user_id {
            metadata.insert("user_id".to_string(), user_id.as_bytes().to_vec());
        }

        // 添加 token（用于认证）
        if let Some(ref token) = self.config.token {
            metadata.insert("token".to_string(), token.as_bytes().to_vec());
            tracing::debug!("[ClientCore] 已添加 token 到 CONNECT 消息元数据");
        }

        // 添加其他元数据
        for (key, value) in &self.config.metadata {
            metadata.insert(key.clone(), value.as_bytes().to_vec());
        }

        metadata
    }

    /// 确定 CONNECT 消息的格式
    fn determine_connect_format(
        &self,
    ) -> (
        crate::common::protocol::SerializationFormat,
        crate::common::compression::CompressionAlgorithm,
        bool,
    ) {
        if self.config.is_force_format() {
            // 强制模式：直接使用强制格式，必须发送format
            (
                self.config.get_serialization_format(),
                self.config.get_compression(),
                true,
            )
        } else if self.config.serialization_format
            != crate::common::protocol::SerializationFormat::Json
        {
            // 客户端指定了非JSON格式：发送format元数据，服务端优先使用
            (
                self.config.serialization_format,
                self.config.compression,
                true,
            )
        } else {
            // 客户端使用默认JSON：不发送format元数据，让服务端使用默认JSON
            (
                self.config.serialization_format,
                self.config.compression,
                false,
            )
        }
    }

    /// 添加设备信息到元数据（内部辅助函数）
    fn add_device_metadata(metadata: &mut HashMap<String, Vec<u8>>, config: &ClientConfig) {
        let Some(ref device_info) = config.device_info else {
            return;
        };

        metadata.insert(
            "device_id".to_string(),
            device_info.device_id.as_bytes().to_vec(),
        );
        metadata.insert(
            "platform".to_string(),
            device_info.platform.as_str().as_bytes().to_vec(),
        );

        if let Some(ref model) = device_info.model {
            metadata.insert("model".to_string(), model.as_bytes().to_vec());
        }
        if let Some(ref app_version) = device_info.app_version {
            metadata.insert("app_version".to_string(), app_version.as_bytes().to_vec());
        }
        if let Some(ref system_version) = device_info.system_version {
            metadata.insert(
                "system_version".to_string(),
                system_version.as_bytes().to_vec(),
            );
        }

        // 添加其他元数据
        for (key, value) in &device_info.metadata {
            metadata.insert(key.clone(), value.as_bytes().to_vec());
        }
    }

    /// 序列化 CONNECT Frame
    async fn serialize_connect_frame(&self, frame: &Frame) -> Result<Vec<u8>> {
        if self.config.is_force_format() {
            // 强制模式：使用强制格式的 parser
            let parser = self.parser.lock().await;
            parser.serialize(frame)
        } else {
            // 协商模式：使用默认 JSON parser 发送 CONNECT 消息
            MessageParser::json().serialize(frame)
        }
    }

    /// 记录 CONNECT 消息已发送
    fn log_connect_sent(&self) {
        let (format, compression, _) = self.determine_connect_format();

        if self.config.is_force_format() {
            tracing::debug!(
                "[ClientCore] CONNECT 消息已发送（强制模式: format={:?}, compression={:?}）",
                format,
                compression
            );
        } else {
            tracing::debug!(
                "[ClientCore] CONNECT 消息已发送（协商模式: 首选 format={:?}, compression={:?}）",
                format,
                compression
            );
        }
    }

    /// 处理 CONNECT_ACK 消息
    ///
    /// 解析服务器返回的 CONNECT_ACK，确认协商结果
    ///
    /// # 返回
    /// 协商结果：(序列化格式, 压缩算法, 加密方式)
    pub fn handle_connect_ack(
        &self,
        frame: &Frame,
    ) -> Result<(
        crate::common::protocol::SerializationFormat,
        crate::common::compression::CompressionAlgorithm,
        Option<String>,
    )> {
        let cmd = frame
            .command
            .as_ref()
            .and_then(|c| c.r#type.as_ref())
            .and_then(|t| {
                if let Type::System(sys_cmd) = t {
                    Some(sys_cmd)
                } else {
                    None
                }
            })
            .ok_or_else(|| FlareError::protocol_error("Not a CONNECT_ACK message".to_string()))?;

        let cmd_type = SystemCommandType::try_from(cmd.r#type)
            .map_err(|_| FlareError::protocol_error("Invalid system command type".to_string()))?;

        if cmd_type != SystemCommandType::ConnectAck {
            return Err(FlareError::protocol_error(
                "Not a CONNECT_ACK message".to_string(),
            ));
        }

        // 解析协商结果
        let format = crate::common::protocol::SerializationFormat::try_from(cmd.format)
            .unwrap_or(crate::common::protocol::SerializationFormat::Json);

        let compression = Self::parse_compression_from_ack(cmd);
        let encryption = Self::parse_encryption_from_ack(cmd);

        tracing::debug!(
            "[ClientCore] 收到 CONNECT_ACK，协商结果: format={:?}, compression={:?}, encryption={:?}",
            format,
            compression,
            encryption
        );

        // 检查是否有冲突连接通知
        Self::check_conflict_connections(cmd);

        Ok((format, compression, encryption))
    }

    /// 从 CONNECT_ACK 解析压缩算法
    fn parse_compression_from_ack(
        cmd: &crate::common::protocol::SystemCommand,
    ) -> crate::common::compression::CompressionAlgorithm {
        // 优先使用新字段
        if !cmd.compression.is_empty() {
            return crate::common::compression::CompressionAlgorithm::from_str(&cmd.compression)
                .unwrap_or(crate::common::compression::CompressionAlgorithm::None);
        }

        // 兼容旧版本：从 metadata 中读取
        cmd.metadata
            .get("compression")
            .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
            .and_then(|s| crate::common::compression::CompressionAlgorithm::from_str(&s))
            .unwrap_or(crate::common::compression::CompressionAlgorithm::None)
    }

    /// 从 CONNECT_ACK 解析加密方式
    fn parse_encryption_from_ack(cmd: &crate::common::protocol::SystemCommand) -> Option<String> {
        // 优先使用新字段
        if !cmd.encryption.is_empty() {
            return Some(cmd.encryption.clone());
        }

        // 兼容旧版本：从 metadata 中读取
        cmd.metadata
            .get("encryption")
            .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
    }

    /// 检查冲突连接通知
    fn check_conflict_connections(cmd: &crate::common::protocol::SystemCommand) {
        if let Some(conflicts_bytes) = cmd.metadata.get("conflict_connections") {
            if let Ok(conflicts_json) = String::from_utf8(conflicts_bytes.clone()) {
                if let Ok(conflict_connections) =
                    serde_json::from_str::<Vec<String>>(&conflicts_json)
                {
                    if !conflict_connections.is_empty() {
                        tracing::warn!(
                            "[ClientCore] 检测到设备冲突，以下连接被踢掉: {:?}",
                            conflict_connections
                        );
                    }
                }
            }
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
}

// 为 ClientCore 实现 Clone（用于共享状态管理器和观察者）
impl Clone for ClientCore {
    fn clone(&self) -> Self {
        Self {
            state_manager: Arc::clone(&self.state_manager),
            parser: Arc::clone(&self.parser),
            heartbeat_manager: None, // 心跳管理器不克隆，由主实例管理
            message_router: self.message_router.as_ref().map(|_| MessageRouter::new()), // 路由不克隆，创建新的
            observers: Arc::clone(&self.observers),
            config: self.config.clone(),
            event_handler: self.event_handler.clone(), // 事件处理器可以共享
            client_connection: Arc::clone(&self.client_connection), // 共享连接引用
            pending_map: Arc::clone(&self.pending_map),
        }
    }
}
