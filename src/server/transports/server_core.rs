//! 服务器核心功能
//!
//! 提供统一的连接管理和心跳检测功能，简化服务器实现

use crate::common::MessageParser;
use crate::common::compression::CompressionAlgorithm;
use crate::common::error::Result;
use crate::common::protocol::{Frame, Reliability, SerializationFormat, frame_with_system_command};
use crate::server::auth::Authenticator;
use crate::server::config::ServerConfig;
use crate::server::connection::{
    ConnectionManager, ConnectionManagerTrait, device_handler, negotiation,
};
use crate::server::device::DeviceManager;
use crate::server::events::factory::ServerMessageObserverFactory;
use crate::server::events::handler::ServerEventHandler;
use crate::server::handle::ServerHandle;
use crate::server::heartbeat::HeartbeatDetector;
use crate::server::transports::ConnectionHandler;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

/// 服务器核心功能
///
/// 统一管理连接和心跳检测，简化服务器实现
pub struct ServerCore {
    /// 连接管理器
    pub connection_manager: Arc<ConnectionManager>,
    /// 消息解析器
    pub parser: MessageParser,
    /// 心跳检测器（可选，使用 Mutex 包装以支持内部可变性）
    heartbeat_detector: Arc<tokio::sync::Mutex<Option<HeartbeatDetector>>>,
    /// 设备管理器（可选，用于设备冲突管理）
    device_manager: Option<Arc<DeviceManager>>,
    /// 事件处理器（可选，用于细化的命令处理）
    event_handler: Option<Arc<dyn ServerEventHandler>>,
    /// 认证器（可选，如果提供则启用认证）
    authenticator: Option<Arc<dyn Authenticator>>,
    /// 是否启用认证（从配置读取）
    auth_enabled: bool,
    /// 认证超时时间（从配置读取）
    auth_timeout: Duration,
    /// 默认序列化格式（用于协商）
    default_serialization_format: SerializationFormat,
    /// 默认压缩算法（用于协商）
    default_compression: CompressionAlgorithm,
    /// 观察者工厂（用于创建连接观察者）
    observer_factory: Arc<dyn ServerMessageObserverFactory>,
}

impl ServerCore {
    /// 获取默认序列化格式
    pub fn default_serialization_format(&self) -> SerializationFormat {
        self.default_serialization_format
    }

    /// 获取默认压缩算法
    pub fn default_compression(&self) -> CompressionAlgorithm {
        self.default_compression
    }

    /// 设置默认序列化格式
    #[must_use]
    pub fn with_default_format(mut self, format: SerializationFormat) -> Self {
        self.default_serialization_format = format;
        self
    }

    /// 设置默认压缩算法
    #[must_use]
    pub fn with_default_compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.default_compression = compression;
        self
    }

    /// 创建新的服务器核心
    pub fn new(config: &ServerConfig, connection_manager: Option<Arc<ConnectionManager>>) -> Self {
        Self::with_observer_factory(
            config,
            connection_manager,
            Arc::new(crate::server::events::DefaultServerMessageObserverFactory::new()),
        )
    }

    /// 使用指定的观察者工厂创建服务器核心
    ///
    /// # 参数
    /// - `config`: 服务器配置
    /// - `connection_manager`: 可选的连接管理器
    /// - `observer_factory`: 观察者工厂
    ///
    /// # 示例
    /// ```rust,no_run
    /// use flare_core::server::events::factory::ServerMessageObserverFactory;
    ///
    /// // 使用自定义工厂
    /// let factory = Arc::new(MyCustomObserverFactory::new());
    /// let core = ServerCore::with_observer_factory(&config, None, factory);
    /// ```
    pub fn with_observer_factory(
        config: &ServerConfig,
        connection_manager: Option<Arc<ConnectionManager>>,
        observer_factory: Arc<dyn ServerMessageObserverFactory>,
    ) -> Self {
        let connection_manager =
            connection_manager.unwrap_or_else(|| Arc::new(ConnectionManager::new()));

        let parser = MessageParser::new(
            config.default_serialization_format,
            config.default_compression,
        );

        Self {
            connection_manager,
            parser,
            heartbeat_detector: Arc::new(tokio::sync::Mutex::new(None)),
            device_manager: None,
            event_handler: None,
            authenticator: None,
            auth_enabled: config.auth_enabled,
            auth_timeout: config.auth_timeout,
            default_serialization_format: config.default_serialization_format,
            default_compression: config.default_compression,
            observer_factory,
        }
    }

    /// 设置设备管理器
    #[must_use]
    pub fn with_device_manager(mut self, device_manager: Option<Arc<DeviceManager>>) -> Self {
        self.device_manager = device_manager;
        self
    }

    /// 获取设备管理器
    pub fn device_manager(&self) -> Option<Arc<DeviceManager>> {
        self.device_manager.clone()
    }

    /// 设置事件处理器
    #[must_use]
    pub fn with_event_handler(
        mut self,
        event_handler: Option<Arc<dyn ServerEventHandler>>,
    ) -> Self {
        self.event_handler = event_handler;
        self
    }

    /// 设置观察者工厂
    ///
    /// # 参数
    /// - `factory`: 观察者工厂
    pub fn set_observer_factory(&mut self, factory: Arc<dyn ServerMessageObserverFactory>) {
        self.observer_factory = factory;
    }

    /// 获取观察者工厂
    pub fn observer_factory(&self) -> &Arc<dyn ServerMessageObserverFactory> {
        &self.observer_factory
    }

    /// 创建连接观察者
    ///
    /// # 参数
    /// - `handler`: 连接处理器
    /// - `connection_id`: 连接 ID
    /// - `core_arc`: ServerCore 的 Arc 包装（由调用方提供，避免循环引用）
    ///
    /// # 返回
    /// 创建的观察者实例
    pub fn create_observer_with_core(
        &self,
        handler: Arc<dyn ConnectionHandler>,
        connection_id: String,
        core_arc: Arc<ServerCore>,
    ) -> Arc<dyn crate::transport::events::ConnectionObserver> {
        // 创建 ServerCore 的引用（避免循环引用）
        let core_ref = Arc::new(crate::server::events::factory::ServerCoreRef {
            device_manager: self.device_manager.clone(),
            event_handler: self.event_handler.clone(),
        });

        self.observer_factory.create_observer(
            handler,
            Arc::clone(&self.connection_manager),
            self.parser.clone(),
            connection_id,
            core_ref,
            core_arc,
        )
    }

    /// 获取事件处理器
    pub fn event_handler(&self) -> Option<Arc<dyn ServerEventHandler>> {
        self.event_handler.clone()
    }

    /// 设置事件处理器（可变引用版本，用于已经创建的实例）
    pub fn set_event_handler(&mut self, event_handler: Option<Arc<dyn ServerEventHandler>>) {
        self.event_handler = event_handler;
    }

    /// 设置设备管理器（可变引用版本，用于已经创建的实例）
    pub fn set_device_manager(&mut self, device_manager: Option<Arc<DeviceManager>>) {
        self.device_manager = device_manager;
    }

    /// 设置认证器
    #[must_use]
    pub fn with_authenticator(mut self, authenticator: Option<Arc<dyn Authenticator>>) -> Self {
        self.authenticator = authenticator;
        self
    }

    /// 获取认证器
    pub fn authenticator(&self) -> Option<Arc<dyn Authenticator>> {
        self.authenticator.clone()
    }

    /// 设置认证器（可变引用版本，用于已经创建的实例）
    pub fn set_authenticator(&mut self, authenticator: Option<Arc<dyn Authenticator>>) {
        self.authenticator = authenticator;
    }

    /// 检查是否启用认证
    pub fn auth_enabled(&self) -> bool {
        self.auth_enabled && self.authenticator.is_some()
    }

    /// 获取认证超时时间
    pub fn auth_timeout(&self) -> Duration {
        self.auth_timeout
    }

    /// 启动心跳检测
    pub fn start_heartbeat(&self, config: &ServerConfig) {
        let manager_trait = Arc::clone(&self.connection_manager) as Arc<dyn ConnectionManagerTrait>;
        let timeout = config.connection_timeout;
        let check_interval =
            Duration::from_secs(timeout.as_secs() / 3).max(Duration::from_secs(10));

        let mut detector = HeartbeatDetector::new(manager_trait, timeout, check_interval);
        detector.start();

        // 使用 Mutex 设置 heartbeat_detector
        let detector_arc = Arc::clone(&self.heartbeat_detector);
        tokio::spawn(async move {
            let mut guard = detector_arc.lock().await;
            *guard = Some(detector);
        });
    }

    /// 停止心跳检测
    pub fn stop_heartbeat(&self) {
        let detector_arc = Arc::clone(&self.heartbeat_detector);
        tokio::spawn(async move {
            let mut guard = detector_arc.lock().await;
            if let Some(ref mut detector) = *guard {
                detector.stop();
            }
        });
    }

    /// 获取连接管理器 trait
    pub fn connection_manager_trait(&self) -> Arc<dyn ConnectionManagerTrait> {
        Arc::clone(&self.connection_manager) as Arc<dyn ConnectionManagerTrait>
    }

    /// 向指定连接发送消息
    ///
    /// 使用连接协商后的序列化格式和压缩算法
    pub async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        let manager_trait = self.connection_manager_trait();
        manager_trait
            .send_frame_to(connection_id, frame, None)
            .await
    }

    /// 向指定用户的所有连接发送消息
    ///
    /// 每个连接使用其协商后的序列化格式和压缩算法
    pub async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        let manager_trait = self.connection_manager_trait();
        manager_trait.send_frame_to_user(user_id, frame, None).await
    }

    /// 广播消息到所有连接
    ///
    /// 每个连接使用其协商后的序列化格式和压缩算法
    pub async fn broadcast(&self, frame: &Frame) -> Result<()> {
        let manager_trait = self.connection_manager_trait();
        manager_trait.broadcast_frame(frame, None).await
    }

    /// 广播消息到所有连接，排除指定连接
    ///
    /// 每个连接使用其协商后的序列化格式和压缩算法
    pub async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        let manager_trait = self.connection_manager_trait();
        manager_trait
            .broadcast_frame_except(frame, exclude_connection_id, None)
            .await
    }

    /// 获取连接数量
    pub fn connection_count(&self) -> usize {
        self.connection_manager.connection_count()
    }

    /// 获取用户数量
    pub fn user_count(&self) -> usize {
        self.connection_manager.stats().total_users
    }

    /// 断开指定连接
    pub async fn disconnect(&self, connection_id: &str) -> Result<()> {
        let manager_trait = self.connection_manager_trait();
        manager_trait.remove_connection(connection_id).await
    }

    /// 获取所有连接 ID（异步）
    pub async fn list_connections(&self) -> Vec<String> {
        let manager_trait = self.connection_manager_trait();
        manager_trait.list_connections().await
    }

    /// 处理 CONNECT 消息（协商）
    ///
    /// # 参数
    /// - `frame`: CONNECT 消息的 Frame
    /// - `connection_id`: 连接 ID
    ///
    /// # 返回
    /// 协商结果，包含：
    /// - `ack_frame`: 需要发送的 CONNECT_ACK Frame
    /// - `parser`: 基于协商结果创建的 MessageParser
    pub async fn handle_connect_message(
        &self,
        frame: &Frame,
        connection_id: &str,
    ) -> Result<(Frame, MessageParser)> {
        // 1. 解析协商信息
        let negotiation = negotiation::parse_connect_message(frame)?;

        // 2. 确定最终使用的序列化格式和压缩算法
        let (final_format, final_compression) = self.determine_negotiation_result(&negotiation);

        self.log_negotiation_details(connection_id, &negotiation, final_format, final_compression);

        // 3. Token 验证（如果启用认证）- 先完成认证
        let auth_user_id = self
            .authenticate_connection(frame, connection_id, &negotiation)
            .await?;

        // 4. 更新连接信息（在认证后）
        self.update_connection_info(
            connection_id,
            &negotiation,
            final_format,
            final_compression,
            &auth_user_id,
        )
        .await;

        // 5. 标记连接为已验证
        self.mark_connection_authenticated(connection_id, &auth_user_id)
            .await;

        // 6. 延迟处理设备冲突（在认证完成后，避免同时踢掉两个连接）
        // 使用小延迟确保第一个连接先完成认证，再处理第二个连接的冲突
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let conflict_connections = self
            .handle_device_conflict(connection_id, &negotiation)
            .await;

        // 7. 创建 CONNECT_ACK
        let ack_frame =
            self.create_connect_ack(final_format, final_compression, &conflict_connections);

        // 8. 创建基于协商结果的 MessageParser
        let parser = MessageParser::new(final_format, final_compression);

        Ok((ack_frame, parser))
    }

    /// 完整处理 CONNECT 消息（协商、发送 ACK、调用 handler）
    ///
    /// 这是一个统一的处理方法，将协商、发送 ACK 和调用 handler 的逻辑集中在一起
    ///
    /// # 参数
    /// - `frame`: CONNECT 消息的 Frame
    /// - `connection_id`: 连接 ID
    /// - `connection`: 连接实例（用于发送 CONNECT_ACK）
    /// - `handler`: 连接处理器（用于调用 on_connect）
    ///
    /// # 返回
    /// 处理成功返回 `Ok(())`，失败返回错误
    pub async fn handle_connect_complete(
        &self,
        frame: &Frame,
        connection_id: &str,
        connection: Arc<tokio::sync::Mutex<Box<dyn crate::transport::connection::Connection>>>,
        handler: Arc<dyn ConnectionHandler>,
    ) -> Result<()> {
        // 1. 处理协商（内部会处理设备冲突、更新连接信息等）
        let (ack_frame, negotiation_parser) =
            self.handle_connect_message(frame, connection_id).await?;

        // 记录最终协商结果
        let final_format = negotiation_parser.default_format();
        let final_compression = negotiation_parser.default_compression();
        debug!(
            "[ServerCore] ✅ 协商完成: connection_id={}, 最终序列化方式={:?}, 最终压缩方式={:?}",
            connection_id, final_format, final_compression
        );

        // 2. 使用协商后的解析器序列化 CONNECT_ACK 并发送
        let ack_data = negotiation_parser.serialize(&ack_frame)?;
        {
            let mut conn = connection.lock().await;
            conn.send(&ack_data).await?;
        }
        debug!(
            "[ServerCore] CONNECT_ACK 已发送: connection_id={}",
            connection_id
        );

        // 3. 通知连接建立（在协商完成后）
        handler.on_connect(connection_id).await?;

        Ok(())
    }

    // ============================================================================
    // 内部辅助方法
    // ============================================================================

    /// 确定协商结果（内部辅助方法）
    fn determine_negotiation_result(
        &self,
        negotiation: &negotiation::NegotiationResult,
    ) -> (SerializationFormat, CompressionAlgorithm) {
        // 协商规则：
        // - 如果客户端强制指定（force_format=true），使用客户端格式
        // - 如果客户端指定了格式但未强制，优先使用客户端格式（如果服务端支持）
        // - 如果客户端未指定格式，使用服务端默认格式（JSON）
        let final_format = if negotiation.is_forced {
            negotiation.serialization_format
        } else if negotiation.serialization_format != SerializationFormat::Json {
            negotiation.serialization_format
        } else {
            self.default_serialization_format
        };

        let final_compression = if negotiation.is_forced {
            negotiation.compression
        } else if negotiation.compression != CompressionAlgorithm::None {
            negotiation.compression
        } else {
            self.default_compression
        };

        (final_format, final_compression)
    }

    /// 记录协商详情（内部辅助方法）
    fn log_negotiation_details(
        &self,
        connection_id: &str,
        negotiation: &negotiation::NegotiationResult,
        final_format: SerializationFormat,
        final_compression: CompressionAlgorithm,
    ) {
        debug!(
            "[ServerCore] 📥 收到 CONNECT 消息: connection_id={}",
            connection_id
        );
        info!(
            "[ServerCore] 📋 协商详情: 客户端请求={:?}, 客户端压缩={:?}, 强制模式={}, 服务端默认={:?}, 服务端默认压缩={:?}, 最终格式={:?}, 最终压缩={:?}, device={:?}, user_id={:?}",
            negotiation.serialization_format,
            negotiation.compression,
            negotiation.is_forced,
            self.default_serialization_format,
            self.default_compression,
            final_format,
            final_compression,
            negotiation.device_info.as_ref().map(|d| &d.platform),
            negotiation.user_id
        );
    }

    /// 处理设备冲突（内部辅助方法）
    async fn handle_device_conflict(
        &self,
        connection_id: &str,
        negotiation: &negotiation::NegotiationResult,
    ) -> Vec<String> {
        let mut conflict_connections = Vec::new();

        debug!(
            "[ServerCore] 设备冲突检测条件: device_manager={}, device_info={}, user_id={}",
            self.device_manager.is_some(),
            negotiation.device_info.is_some(),
            negotiation.user_id.is_some()
        );

        if let (Some(device_mgr), Some(device_info)) =
            (&self.device_manager, &negotiation.device_info)
        {
            if let Some(user_id) = &negotiation.user_id {
                info!(
                    "[ServerCore] 🔍 开始设备冲突检测: user_id={}, connection_id={}, platform={:?}",
                    user_id, connection_id, device_info.platform
                );

                let manager_trait = self.connection_manager_trait();
                let platform = device_info.platform.clone();

                match device_handler::handle_device_conflict(
                    Some(Arc::clone(device_mgr)),
                    user_id,
                    connection_id,
                    &platform,
                    device_info,
                    manager_trait,
                )
                .await
                {
                    Ok(conflict_result) => {
                        conflict_connections = conflict_result.conflict_connections;

                        // 防御性检查：确保冲突连接列表不包含新连接本身
                        conflict_connections.retain(|conn_id| {
                            if conn_id == connection_id {
                                error!(
                                    "[ServerCore] ❌ 错误：冲突连接列表包含新连接ID，已过滤: connection_id={}",
                                    connection_id
                                );
                                false
                            } else {
                                true
                            }
                        });

                        if !conflict_connections.is_empty() {
                            info!(
                                "[ServerCore] ⚠️  检测到设备冲突: user_id={}, 新连接={}, 将踢掉 {} 个旧连接: {:?}",
                                user_id,
                                connection_id,
                                conflict_connections.len(),
                                conflict_connections
                            );
                        } else {
                            debug!(
                                "[ServerCore] ✅ 无设备冲突: user_id={}, platform={:?}, 新连接={}",
                                user_id, platform, connection_id
                            );
                        }
                    }
                    Err(e) => {
                        error!("[ServerCore] 设备冲突处理失败: {}", e);
                    }
                }
            } else {
                debug!("[ServerCore] 跳过设备冲突检测: user_id 为空");
            }
        } else {
            debug!(
                "[ServerCore] 跳过设备冲突检测: device_manager={}, device_info={}",
                self.device_manager.is_some(),
                negotiation.device_info.is_some()
            );
        }

        conflict_connections
    }

    /// 认证连接（内部辅助方法）
    async fn authenticate_connection(
        &self,
        frame: &Frame,
        connection_id: &str,
        negotiation: &negotiation::NegotiationResult,
    ) -> Result<Option<String>> {
        let auth_user_id = negotiation.user_id.clone();
        let auth_enabled = self.auth_enabled();

        if !auth_enabled {
            debug!("[ServerCore] 跳过 token 验证: 认证未启用");
            return Ok(auth_user_id);
        }

        let Some(authenticator) = &self.authenticator else {
            return Ok(auth_user_id);
        };

        // 从 CONNECT 消息的 metadata 中提取 token
        let token = Self::extract_token_from_frame(frame);

        let Some(token) = token else {
            error!(
                "[ServerCore] ❌ 未提供 token: connection_id={}",
                connection_id
            );
            return Err(crate::common::error::FlareError::authentication_failed(
                "未提供 token".to_string(),
            ));
        };

        debug!(
            "[ServerCore] 🔐 开始验证 token: connection_id={}",
            connection_id
        );

        let metadata = Self::extract_system_command_metadata(frame);

        match authenticator
            .authenticate(
                &token,
                connection_id,
                negotiation.device_info.as_ref(),
                metadata,
            )
            .await
        {
            Ok(auth_result) => {
                if auth_result.authenticated {
                    debug!(
                        "[ServerCore] ✅ Token 验证成功: connection_id={}, user_id={:?}",
                        connection_id, auth_result.user_id
                    );
                    Ok(auth_result.user_id)
                } else {
                    let error_msg = auth_result
                        .error_message
                        .unwrap_or_else(|| "Token 验证失败".to_string());
                    error!(
                        "[ServerCore] ❌ Token 验证失败: connection_id={}, error={}",
                        connection_id, error_msg
                    );
                    Err(crate::common::error::FlareError::authentication_failed(
                        error_msg,
                    ))
                }
            }
            Err(e) => {
                error!(
                    "[ServerCore] ❌ Token 验证过程出错: connection_id={}, error={}",
                    connection_id, e
                );
                Err(crate::common::error::FlareError::authentication_failed(
                    format!("验证过程出错: {}", e),
                ))
            }
        }
    }

    /// 从 Frame 中提取 token（内部辅助方法）
    fn extract_token_from_frame(frame: &Frame) -> Option<String> {
        frame.command.as_ref().and_then(|cmd| {
            if let Some(crate::common::protocol::flare::core::commands::command::Type::System(
                sys_cmd,
            )) = &cmd.r#type
            {
                sys_cmd
                    .metadata
                    .get("token")
                    .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
            } else {
                None
            }
        })
    }

    /// 从 Frame 中提取系统命令的 metadata（内部辅助方法）
    fn extract_system_command_metadata(
        frame: &Frame,
    ) -> Option<&std::collections::HashMap<String, Vec<u8>>> {
        frame.command.as_ref().and_then(|cmd| {
            if let Some(crate::common::protocol::flare::core::commands::command::Type::System(
                sys_cmd,
            )) = &cmd.r#type
            {
                Some(&sys_cmd.metadata)
            } else {
                None
            }
        })
    }

    /// 更新连接信息（内部辅助方法）
    async fn update_connection_info(
        &self,
        connection_id: &str,
        negotiation: &negotiation::NegotiationResult,
        final_format: SerializationFormat,
        final_compression: CompressionAlgorithm,
        auth_user_id: &Option<String>,
    ) {
        let manager = Arc::clone(&self.connection_manager);

        if let Err(e) = manager.update_connection_negotiation(
            connection_id,
            negotiation.device_info.clone(),
            final_format,
            final_compression,
            auth_user_id.clone(),
        ) {
            error!("[ServerCore] 更新连接协商信息失败: {}", e);
            return;
        }

        // 验证更新是否成功
        if let Some(user_id) = auth_user_id {
            debug!(
                "[ServerCore] 已更新连接协商信息: connection_id={}, user_id={}",
                connection_id, user_id
            );

            if let Some((_, conn_info)) = manager.get_connection(connection_id) {
                if let Some(ref updated_user_id) = conn_info.user_id {
                    debug!(
                        "[ServerCore] ✅ 验证成功: 连接信息中的 user_id={}",
                        updated_user_id
                    );
                } else {
                    error!("[ServerCore] ❌ 验证失败: 连接信息中的 user_id 仍为 None");
                }
            }
        }
    }

    /// 标记连接为已验证（内部辅助方法）
    async fn mark_connection_authenticated(
        &self,
        connection_id: &str,
        auth_user_id: &Option<String>,
    ) {
        let manager = Arc::clone(&self.connection_manager);
        let manager_trait = manager as Arc<dyn ConnectionManagerTrait>;
        let auth_enabled = self.auth_enabled();

        if let Err(e) = manager_trait
            .set_connection_authenticated(connection_id, auth_user_id.clone())
            .await
        {
            error!("[ServerCore] 标记连接为已验证失败: {}", e);
            return;
        }

        if auth_enabled {
            debug!(
                "[ServerCore] ✅ 连接已标记为已验证（认证通过）: connection_id={}, user_id={:?}",
                connection_id, auth_user_id
            );
        } else {
            debug!(
                "[ServerCore] ✅ 连接已标记为已验证（无需认证）: connection_id={}, user_id={:?}",
                connection_id, auth_user_id
            );
        }
    }

    /// 创建 CONNECT_ACK（内部辅助方法）
    fn create_connect_ack(
        &self,
        final_format: SerializationFormat,
        final_compression: CompressionAlgorithm,
        conflict_connections: &[String],
    ) -> Frame {
        let mut ack_metadata = std::collections::HashMap::new();

        // 如果有冲突连接，通知客户端
        if !conflict_connections.is_empty() {
            let conflicts_json =
                serde_json::to_string(conflict_connections).unwrap_or_else(|_| "[]".to_string());
            ack_metadata.insert(
                "conflict_connections".to_string(),
                conflicts_json.into_bytes(),
            );
        }

        // 创建 CONNECT_ACK，包含完整的协商结果：格式、压缩、加密
        // 加密方式目前为 "none"（传输层TLS已提供加密，应用层暂不加密）
        let connect_ack_cmd = negotiation::create_connect_ack(
            final_format,
            final_compression,
            Some("none"), // 加密方式：目前为 none，为未来扩展预留
            Some(ack_metadata),
        );

        frame_with_system_command(connect_ack_cmd, Reliability::AtLeastOnce)
    }
}

/// 让 ServerCore 实现 ServerHandle trait
/// 这样可以在任何需要发送消息的地方注入 ServerCore，而不需要整个 Server
#[async_trait]
impl ServerHandle for ServerCore {
    async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        self.send_to(connection_id, frame).await
    }

    async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        self.send_to_user(user_id, frame).await
    }

    async fn broadcast(&self, frame: &Frame) -> Result<()> {
        self.broadcast(frame).await
    }

    async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        self.broadcast_except(frame, exclude_connection_id).await
    }

    async fn disconnect(&self, connection_id: &str) -> Result<()> {
        self.disconnect(connection_id).await
    }

    fn connection_count(&self) -> usize {
        self.connection_count()
    }

    fn user_count(&self) -> usize {
        self.user_count()
    }
}
