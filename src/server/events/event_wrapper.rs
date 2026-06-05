use crate::common::Frame;
use crate::server::{ConnectionHandler, ConnectionManagerTrait, ServerEventHandler};
use async_trait::async_trait;
use std::sync::Arc;
use tracing::info;

/// 服务端事件包装器
///
/// 将 `DefaultServerMessageObserver` 的功能直接集成到 `ConnectionHandler` 中，
/// 提供统一的消息和连接事件处理
pub struct ServerMessageWrapper {
    /// 事件处理器，用于处理所有事件和消息
    pub(crate) event_handler: Arc<dyn ServerEventHandler>,
    /// 连接管理器（用于发送响应和更新连接状态）
    connection_manager: Option<Arc<crate::server::connection::ConnectionManager>>,
    /// 设备管理器（用于连接断开时清理设备）
    device_manager: Option<Arc<crate::server::device::DeviceManager>>,
    /// 消息解析器（用于序列化响应）
    parser: crate::common::MessageParser,
}

// 内部处理实现
impl ServerMessageWrapper {
    /// 创建新的服务端消息包装器
    ///
    /// # 参数
    /// - `event_handler`: 事件处理器，用于处理所有事件和消息
    /// - `connection_manager`: 连接管理器（可选，用于发送响应）
    /// - `device_manager`: 设备管理器（可选，用于设备管理）
    /// - `parser`: 消息解析器（用于序列化响应）
    pub fn new(
        event_handler: Arc<dyn ServerEventHandler>,
        connection_manager: Option<Arc<crate::server::connection::ConnectionManager>>,
        device_manager: Option<Arc<crate::server::device::DeviceManager>>,
        parser: crate::common::MessageParser,
    ) -> Self {
        Self {
            event_handler,
            connection_manager,
            device_manager,
            parser,
        }
    }

    /// 统一处理 handler 调用、响应处理和发送
    ///
    /// 封装完整流程：调用 handler → 自动 ACK（如果返回 None）→ 发送响应
    async fn handle_and_send_response<F>(
        &self,
        handler_future: F,
        message_id: String,
        connection_id: &str,
        log_context: &str,
    ) -> crate::common::error::Result<()>
    where
        F: std::future::Future<Output = crate::common::error::Result<Option<Frame>>>,
    {
        let response_frame = match handler_future.await {
            Ok(Some(response)) => {
                tracing::trace!(
                    "[ServerMessageWrapper] {}: 自定义响应: connection_id={}, message_id={}",
                    log_context,
                    connection_id,
                    message_id
                );
                response
            }
            Ok(None) => {
                tracing::trace!(
                    "[ServerMessageWrapper] {}: 自动 ACK: connection_id={}, message_id={}",
                    log_context,
                    connection_id,
                    message_id
                );
                use crate::common::protocol::Reliability;
                use crate::common::protocol::builder::{ack_message, frame_with_payload_command};
                frame_with_payload_command(ack_message(message_id, None), Reliability::AtLeastOnce)
            }
            Err(e) => {
                // Handler 处理失败，发送包含错误信息的 ACK
                tracing::error!(
                    "[ServerMessageWrapper] {}: 处理失败，发送错误 ACK, connection_id={}, message_id={}, error={}",
                    log_context,
                    connection_id,
                    message_id,
                    e
                );
                use crate::common::protocol::Reliability;
                use crate::common::protocol::builder::{ack_message, frame_with_payload_command};
                use std::collections::HashMap;

                // 在 metadata 中放入错误信息
                let mut metadata = HashMap::new();
                metadata.insert("error".to_string(), b"true".to_vec());
                metadata.insert("error_message".to_string(), e.to_string().into_bytes());

                frame_with_payload_command(
                    ack_message(message_id, Some(metadata)),
                    Reliability::AtLeastOnce,
                )
            }
        };

        // 发送响应（如果有连接管理器）
        if let Some(manager) = &self.connection_manager {
            self.send_response_frame_async(response_frame, connection_id, log_context, manager)
                .await;
        }

        Ok(())
    }

    /// 异步发送响应 Frame 到客户端
    async fn send_response_frame_async(
        &self,
        frame_to_send: Frame,
        connection_id: &str,
        log_context: &str,
        manager: &Arc<crate::server::connection::ConnectionManager>,
    ) {
        let manager_trait = Arc::clone(manager) as Arc<dyn ConnectionManagerTrait>;
        // 使用 Arc<str> 避免 String clone，减少内存分配
        let conn_id: Arc<str> = Arc::from(connection_id);
        // message_id 是 String，需要 clone（因为需要移动到异步任务中）
        let message_id = frame_to_send.message_id.clone();
        // 使用 Arc<str> 避免 String clone，减少内存分配
        let log_ctx: Arc<str> = Arc::from(log_context);

        tokio::spawn(async move {
            if let Some((conn, conn_info)) = manager_trait.get_connection(&conn_id).await {
                // 先克隆值用于日志（避免在创建 parser 时移动）
                let format = conn_info.serialization_format;
                let compression = conn_info.compression.clone();
                let encryption = conn_info.encryption.clone();
                let negotiation_completed = conn_info.negotiation_completed;

                // 使用缓存的 parser（避免每次消息发送都创建新的 parser）
                let parser = if negotiation_completed {
                    // 协商已完成，使用缓存的 parser
                    conn_info.cached_parser.clone().unwrap_or_else(|| {
                        // 如果缓存不存在（不应该发生），回退到动态创建
                        tracing::warn!(
                            "[ServerMessageWrapper] 协商已完成但缓存 parser 不存在，回退到动态创建: connection_id={}",
                            conn_id
                        );
                        std::sync::Arc::new(crate::common::MessageParser::new(
                            format,
                            compression.clone(),
                            encryption.clone(),
                        ))
                    })
                } else {
                    // 协商未完成，使用默认的 JSON、不压缩、不加密 parser
                    // 注意：这里可以创建一个全局共享的 parser，但为了简单起见，暂时每次创建
                    // 因为协商未完成的消息很少
                    std::sync::Arc::new(crate::common::MessageParser::new(
                        crate::common::protocol::SerializationFormat::Json,
                        crate::common::compression::CompressionAlgorithm::None,
                        crate::common::encryption::EncryptionAlgorithm::None,
                    ))
                };

                tracing::trace!(
                    "[ServerMessageWrapper] {}: 发送消息: connection_id={}, message_id={}, format={:?}",
                    log_ctx,
                    conn_id,
                    message_id,
                    format
                );

                if let Ok(data) = parser.serialize(&frame_to_send) {
                    let mut c = conn.lock().await;
                    match c.send(&data).await {
                        Ok(_) => tracing::debug!(
                            "[ServerMessageWrapper] {}: 已发送, connection_id={}, message_id={}, data_len={}",
                            log_ctx,
                            conn_id,
                            message_id,
                            data.len()
                        ),
                        Err(e) => tracing::error!(
                            "[ServerMessageWrapper] {}: 发送失败, connection_id={}, message_id={}, error={}",
                            log_ctx,
                            conn_id,
                            message_id,
                            e
                        ),
                    }
                } else {
                    tracing::error!(
                        "[ServerMessageWrapper] {}: 序列化失败, connection_id={}, message_id={}",
                        log_ctx,
                        conn_id,
                        message_id
                    );
                }
            } else {
                tracing::warn!(
                    "[ServerMessageWrapper] {}: 连接不存在，无法发送消息: connection_id={}, message_id={}",
                    log_ctx,
                    conn_id,
                    message_id
                );
            }
        });
    }

    /// 异步更新连接活跃时间
    fn update_connection_active_async(&self, connection_id: &str) {
        if let Some(manager) = &self.connection_manager {
            let manager_trait = Arc::clone(manager) as Arc<dyn ConnectionManagerTrait>;
            // 使用 Arc<str> 避免 String clone，减少内存分配
            let conn_id: Arc<str> = Arc::from(connection_id);
            tokio::spawn(async move {
                let _ = manager_trait.update_connection_active(&conn_id).await;
            });
        }
    }

    /// 处理系统命令
    async fn handle_system_command(
        &self,
        frame: &Frame,
        sys_type: i32,
        connection_id: &str,
    ) -> crate::common::error::Result<()> {
        use crate::common::protocol::flare::core::commands::system_command::Type as SysType;

        match SysType::try_from(sys_type) {
            Ok(SysType::Ping) => {
                // 处理 PING：回复 PONG 并更新连接活跃时间
                self.update_connection_active_async(connection_id);

                // 调用事件处理器，如果返回 None 则使用默认 PONG
                match self.event_handler.handle_ping(frame, connection_id).await {
                    Ok(Some(custom_response)) => {
                        if let Some(manager) = &self.connection_manager {
                            self.send_response_frame_async(
                                custom_response,
                                connection_id,
                                "handle_ping",
                                manager,
                            )
                            .await;
                        }
                    }
                    _ => {
                        // 默认处理：回复 PONG
                        use crate::common::protocol::{
                            Reliability, frame_with_system_command, pong,
                        };
                        let pong_frame =
                            frame_with_system_command(pong(), Reliability::AtLeastOnce);
                        if let Some(manager) = &self.connection_manager {
                            self.send_response_frame_async(
                                pong_frame,
                                connection_id,
                                "handle_ping",
                                manager,
                            )
                            .await;
                        }
                    }
                }
            }
            Ok(SysType::Pong) => {
                // 处理 PONG：更新连接活跃时间
                let _ = self.event_handler.handle_pong(frame, connection_id).await;
                self.update_connection_active_async(connection_id);
            }
            Ok(SysType::Event) => {
                // 处理 System::Event
                self.update_connection_active_async(connection_id);

                // Frame 需要 clone，因为需要移动到异步任务中
                let frame_clone = frame.clone();
                // message_id 是 String，需要 clone（因为需要移动到异步任务中）
                let frame_message_id = frame.message_id.clone();
                // 使用 Arc<str> 避免 String clone，减少内存分配
                let conn_id: Arc<str> = Arc::from(connection_id);
                let wrapper = self.clone_for_async();

                tokio::spawn(async move {
                    if let Err(e) = wrapper
                        .handle_and_send_response(
                            wrapper
                                .event_handler
                                .handle_system_event(&frame_clone, &conn_id),
                            frame_message_id,
                            &conn_id,
                            "handle_system_event",
                        )
                        .await
                    {
                        tracing::error!(
                            "[ServerMessageWrapper] handle_system_event: 处理失败, connection_id={}, error={}",
                            conn_id,
                            e
                        );
                    }
                });
            }
            Ok(SysType::NegotiationReady) => {
                // 处理 NEGOTIATION_READY：标记协商已确认
                self.update_connection_active_async(connection_id);

                if let Some(manager) = &self.connection_manager {
                    let manager_clone = Arc::clone(manager);
                    let conn_id: Arc<str> = Arc::from(connection_id);

                    tokio::spawn(async move {
                        if let Err(e) = (*manager_clone).mark_negotiation_confirmed(&conn_id) {
                            tracing::error!(
                                "[ServerMessageWrapper] 标记协商确认失败: connection_id={}, error={}",
                                conn_id,
                                e
                            );
                        } else {
                            tracing::debug!(
                                "[ServerMessageWrapper] ✅ 协商已确认: connection_id={}",
                                conn_id
                            );
                        }
                    });
                }
            }
            _ => {
                tracing::debug!("[ServerMessageWrapper] 未处理的系统命令类型: {}", sys_type);
            }
        }

        Ok(())
    }

    /// 处理载荷命令
    async fn handle_message_command(
        &self,
        _frame: &Frame,
        command: &crate::common::protocol::PayloadCommand,
        connection_id: &str,
    ) -> crate::common::error::Result<()> {
        let message_id = command.message_id.clone();

        use crate::common::protocol::flare::core::commands::payload_command::Type as PayloadType;

        if let Ok(payload_type) = PayloadType::try_from(command.r#type) {
            let handler_future = match payload_type {
                PayloadType::Message => self.event_handler.handle_message(command, connection_id),
                PayloadType::Event => self.event_handler.handle_event(command, connection_id),
                PayloadType::Ack => self.event_handler.handle_ack(command, connection_id),
                PayloadType::Data => self.event_handler.handle_data(command, connection_id),
                PayloadType::Unspecified => {
                    tracing::warn!(
                        "[ServerMessageWrapper] handle_message_command: Unspecified payload type, connection_id={}",
                        connection_id
                    );
                    return Ok(());
                }
            };

            // 统一处理并发送响应
            self.handle_and_send_response(
                handler_future,
                message_id,
                connection_id,
                "handle_message_command",
            )
            .await?;

            return Ok(());
        }

        tracing::error!(
            "[ServerMessageWrapper] handle_message_command: 无法识别载荷类型, connection_id={}, message_id={}",
            connection_id,
            message_id
        );
        Err(crate::common::error::FlareError::general_error(
            "Unknown message type",
        ))
    }

    /// 处理通知命令
    async fn handle_notification_command(
        &self,
        frame: &Frame,
        command: &crate::common::protocol::NotificationCommand,
        connection_id: &str,
    ) -> crate::common::error::Result<()> {
        // 统一处理并发送响应
        self.handle_and_send_response(
            self.event_handler
                .handle_notification_command(command, connection_id),
            frame.message_id.clone(),
            connection_id,
            "handle_notification_command",
        )
        .await
    }

    /// 处理自定义命令
    async fn handle_custom_command(
        &self,
        frame: &Frame,
        command: &crate::common::protocol::flare::core::commands::CustomCommand,
        connection_id: &str,
    ) -> crate::common::error::Result<()> {
        self.update_connection_active_async(connection_id);

        let cmd_name = command.name.clone();
        let log_ctx = format!("handle_custom_command[{}]", cmd_name);

        // 统一处理并发送响应
        self.handle_and_send_response(
            self.event_handler
                .handle_custom_command(command, connection_id),
            frame.message_id.clone(),
            connection_id,
            &log_ctx,
        )
        .await
    }

    /// 克隆用于异步任务（只克隆必要的字段）
    fn clone_for_async(&self) -> Self {
        Self {
            event_handler: self.event_handler.clone(),
            connection_manager: self.connection_manager.clone(),
            device_manager: self.device_manager.clone(),
            parser: self.parser.clone(),
        }
    }
}

impl Clone for ServerMessageWrapper {
    fn clone(&self) -> Self {
        self.clone_for_async()
    }
}

/// 实现连接处理的接口
#[async_trait]
impl ConnectionHandler for ServerMessageWrapper {
    /// 处理消息
    async fn handle_frame(
        &self,
        frame: &Frame,
        connection_id: &str,
    ) -> crate::common::error::Result<Option<Frame>> {
        // 根据命令类型路由到相应的处理方法
        if let Some(cmd) = &frame.command {
            match &cmd.r#type {
                Some(crate::common::protocol::flare::core::commands::command::Type::System(
                    sys_cmd,
                )) => {
                    // 处理系统命令（CONNECT 由 ServerCore 处理，这里只处理 PING/PONG/Event/NEGOTIATION_READY）
                    let sys_type = sys_cmd.r#type;
                    // CONNECT 命令由 ServerCore 统一处理，这里跳过
                    use crate::common::protocol::flare::core::commands::system_command::Type as SysType;
                    if sys_type != SysType::Connect as i32 {
                        let wrapper = self.clone_for_async();
                        let frame_clone = frame.clone();
                        // 使用 Arc<str> 避免 String clone，减少内存分配
                        let conn_id: Arc<str> = Arc::from(connection_id);
                        tokio::spawn(async move {
                            if let Err(e) = wrapper
                                .handle_system_command(&frame_clone, sys_type, &conn_id)
                                .await
                            {
                                tracing::error!("[ServerMessageWrapper] 处理系统命令失败: {}", e);
                            }
                        });
                    }
                }
                Some(crate::common::protocol::flare::core::commands::command::Type::Payload(
                    msg_cmd,
                )) => {
                    // 处理载荷命令
                    let wrapper = self.clone_for_async();
                    let msg_cmd_clone = msg_cmd.clone();
                    let frame_clone = frame.clone();
                    // 使用 Arc<str> 避免 String clone，减少内存分配
                    let conn_id: Arc<str> = Arc::from(connection_id);
                    tokio::spawn(async move {
                        if let Err(e) = wrapper
                            .handle_message_command(&frame_clone, &msg_cmd_clone, &conn_id)
                            .await
                        {
                            tracing::error!("[ServerMessageWrapper] 处理消息命令失败: {}", e);
                        }
                    });
                }
                Some(
                    crate::common::protocol::flare::core::commands::command::Type::Notification(
                        notif_cmd,
                    ),
                ) => {
                    // 处理通知命令
                    let wrapper = self.clone_for_async();
                    let notif_cmd_clone = notif_cmd.clone();
                    let frame_clone = frame.clone();
                    // 使用 Arc<str> 避免 String clone，减少内存分配
                    let conn_id: Arc<str> = Arc::from(connection_id);
                    tokio::spawn(async move {
                        if let Err(e) = wrapper
                            .handle_notification_command(&frame_clone, &notif_cmd_clone, &conn_id)
                            .await
                        {
                            tracing::error!("[ServerMessageWrapper] 处理通知命令失败: {}", e);
                        }
                    });
                }
                Some(crate::common::protocol::flare::core::commands::command::Type::Custom(
                    custom_cmd,
                )) => {
                    // 处理自定义命令
                    let wrapper = self.clone_for_async();
                    let custom_cmd_clone = custom_cmd.clone();
                    let frame_clone = frame.clone();
                    // 使用 Arc<str> 避免 String clone，减少内存分配
                    let conn_id: Arc<str> = Arc::from(connection_id);
                    tokio::spawn(async move {
                        if let Err(e) = wrapper
                            .handle_custom_command(&frame_clone, &custom_cmd_clone, &conn_id)
                            .await
                        {
                            tracing::error!("[ServerMessageWrapper] 处理自定义命令失败: {}", e);
                        }
                    });
                }
                None => {
                    tracing::debug!("[ServerMessageWrapper] 未处理的命令类型");
                }
            }
        }

        // 消息处理是异步的，这里不返回响应
        Ok(None)
    }

    async fn on_connect(&self, connection_id: &str) -> crate::common::error::Result<()> {
        info!("[ServerMessageWrapper] ✅ 新连接: {}", connection_id);
        self.event_handler.on_connect(connection_id).await
    }

    async fn on_disconnect(&self, connection_id: &str) -> crate::common::error::Result<()> {
        info!("[ServerMessageWrapper] ❌ 连接断开: {}", connection_id);

        // 通知事件处理器
        let _ = self.event_handler.on_disconnect(connection_id, None).await;

        // 清理设备（如果有设备管理器）
        if let (Some(device_mgr), Some(manager)) = (&self.device_manager, &self.connection_manager)
        {
            let manager_trait = Arc::clone(manager) as Arc<dyn ConnectionManagerTrait>;
            let device_mgr_clone = device_mgr.clone();
            // 使用 Arc<str> 避免 String clone，减少内存分配
            let conn_id: Arc<str> = Arc::from(connection_id);

            tokio::spawn(async move {
                // 获取连接信息（包括 user_id）
                if let Some((_, conn_info)) = manager_trait.get_connection(&conn_id).await
                    && let Some(user_id) = conn_info.user_id
                {
                    if let Err(e) = device_mgr_clone.remove_device(&user_id, &conn_id).await {
                        tracing::debug!(
                            "[ServerMessageWrapper] Failed to remove device from DeviceManager: {}",
                            e
                        );
                    } else {
                        tracing::info!(
                            "[ServerMessageWrapper] Successfully removed device from DeviceManager: user_id={}, connection_id={}",
                            user_id,
                            conn_id
                        );
                    }
                }
            });
        }

        Ok(())
    }
}

// 扩展方法（不在 trait 中）
impl ServerMessageWrapper {
    /// 处理错误事件
    ///
    /// # 参数
    /// - `connection_id`: 连接 ID
    /// - `error`: 错误信息
    pub(crate) async fn on_error(
        &self,
        connection_id: &str,
        error: &str,
    ) -> crate::common::error::Result<()> {
        tracing::error!(
            "[ServerMessageWrapper] ❌ 连接错误: connection_id={}, error={}",
            connection_id,
            error
        );

        // 通知事件处理器
        if let Err(e) = self.event_handler.on_error(connection_id, error).await {
            tracing::error!(
                "[ServerMessageWrapper] 事件处理器处理错误失败: connection_id={}, error={}",
                connection_id,
                e
            );
        }

        Ok(())
    }
}
