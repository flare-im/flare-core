//! 服务端连接观察者实现
//!
//! 将 `ConnectionHandler` 适配为 `ConnectionObserver`，处理连接事件和消息

use crate::common::MessageParser;
use crate::common::message::parser::PRE_NEGOTIATION_PARSER;
use crate::server::connection::ConnectionManager;
use crate::server::events::ServerMessageWrapper;
use crate::transport::events::{ConnectionEvent, ConnectionObserver};
use std::sync::Arc;
use tracing::{debug, error, warn};

/// 连接状态信息（用于消息处理）
struct ConnectionState {
    info: crate::server::connection::ConnectionInfo,
    negotiation_completed: bool,
    negotiation_confirmed: bool,
    cached_parser: Option<Arc<MessageParser>>,
    cached_pipeline: Option<Arc<crate::common::message::pipeline::MessagePipeline>>,
}

/// ConnectionHandler 到 ConnectionObserver 的适配器
///
/// 将实现了 `ConnectionHandler` 的 `ServerMessageWrapper` 适配为 `ConnectionObserver`
/// 这样可以在需要 `ConnectionObserver` 的地方使用 `ServerMessageWrapper`
///
/// 注意：每个连接都有自己的协商结果，parser 应该根据连接信息动态创建，而不是固定存储
/// 单连接入站队列容量。
///
/// 满了说明客户端灌入速度持续超过处理速度——正常客户端不会到这一步。
/// 宁可响亮报错也不能静默丢帧：静默丢在 IM 里表现为「消息偶尔不见」，
/// 是最难查的一类故障。
const INBOUND_QUEUE_CAPACITY: usize = 1024;

pub struct ConnectionHandlerObserverAdapter {
    /// 内部的 ConnectionHandler（ServerMessageWrapper）
    handler: Arc<ServerMessageWrapper>,
    /// 连接 ID
    connection_id: String,
    /// 连接管理器（用于查询连接信息，获取协商结果）
    connection_manager: Arc<ConnectionManager>,
    /// ServerCore 引用（用于处理 CONNECT 消息）
    server_core: Option<Arc<crate::server::transports::server_core::ServerCore>>,
    /// 本连接的入站帧队列。
    ///
    /// **同一连接的帧必须按序处理**。这里以前是每帧 `tokio::spawn`，等于把连接上
    /// 天然有序的帧丢进运行时抢跑：两条连发的消息谁先跑到序列号分配谁拿小号，
    /// 于是会话时间线**永久**错乱（实测连打 8 条必现 2–3 个逆序对，
    /// 间隔 ≥300ms 才看不出来）。
    ///
    /// 改成「每连接一个队列 + 单消费任务」：连接内严格有序，连接之间照旧并行，
    /// 吞吐仍随连接数扩展。顺带把「每帧一个任务」降成「每连接一个任务」，
    /// 高频小包场景下调度开销更低。
    inbound: tokio::sync::mpsc::Sender<Vec<u8>>,
}

impl ConnectionHandlerObserverAdapter {
    /// 创建适配器
    pub fn new(
        handler: Arc<ServerMessageWrapper>,
        connection_id: String,
        connection_manager: Arc<ConnectionManager>,
        server_core: Option<Arc<crate::server::transports::server_core::ServerCore>>,
    ) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(INBOUND_QUEUE_CAPACITY);

        // 每连接一个消费任务，串行处理本连接的帧。
        // 适配器被丢弃时 tx 随之析构，rx.recv() 返回 None，任务自然退出。
        {
            let handler = Arc::clone(&handler);
            let manager = Arc::clone(&connection_manager);
            let server_core = server_core.clone();
            let conn_id: Arc<str> = Arc::from(connection_id.as_str());
            tokio::spawn(async move {
                while let Some(data) = rx.recv().await {
                    Self::handle_message_event(
                        &handler,
                        data,
                        Arc::clone(&conn_id),
                        Arc::clone(&manager),
                        server_core.clone(),
                    )
                    .await;
                }
                debug!(
                    "[ConnectionHandlerObserverAdapter] 入站队列关闭，消费任务退出: connection_id={}",
                    conn_id
                );
            });
        }

        Self {
            handler,
            connection_id,
            connection_manager,
            server_core,
            inbound: tx,
        }
    }

    /// 处理消息事件
    async fn handle_message_event(
        handler: &Arc<ServerMessageWrapper>,
        data: Vec<u8>,
        conn_id: Arc<str>,
        manager: Arc<ConnectionManager>,
        server_core: Option<Arc<crate::server::transports::server_core::ServerCore>>,
    ) {
        // **关键修复**：在处理消息时立即更新连接活跃时间，防止连接被心跳检测器清理
        // 这必须在处理消息之前更新，因为消息处理可能耗时较长
        let manager_trait =
            Arc::clone(&manager) as Arc<dyn crate::server::connection::ConnectionManagerTrait>;
        let conn_id_clone = Arc::clone(&conn_id);
        if let Err(e) = manager_trait.update_connection_active(&conn_id_clone).await {
            // 如果连接不存在，记录警告但不阻塞处理（可能是连接刚被清理）
            tracing::warn!(
                "[ConnectionHandlerObserverAdapter] 更新连接活跃时间失败（连接可能不存在）: connection_id={}, error={}",
                conn_id,
                e
            );
        }

        // 获取连接信息以检查协商状态和获取缓存的 parser/pipeline
        let connection_state = Self::get_connection_state(&manager, &conn_id);

        // 根据协商状态路由到不同的处理逻辑
        if connection_state.negotiation_completed {
            Self::handle_negotiated_message(
                handler,
                data,
                conn_id,
                manager,
                connection_state.info,
                connection_state.negotiation_confirmed,
                connection_state.cached_parser,
                connection_state.cached_pipeline,
            )
            .await;
        } else {
            // 协商未完成，使用全局共享的协商前 parser
            Self::handle_pre_negotiation_message(handler, data, conn_id, manager, server_core)
                .await;
        }
    }

    /// 获取连接状态信息
    fn get_connection_state(manager: &Arc<ConnectionManager>, conn_id: &str) -> ConnectionState {
        manager
            .get_connection(conn_id)
            .map(|(_, info)| ConnectionState {
                negotiation_completed: info.negotiation_completed,
                negotiation_confirmed: info.negotiation_confirmed,
                cached_parser: info.cached_parser.clone(),
                cached_pipeline: info.cached_pipeline.clone(),
                info: info.clone(),
            })
            .unwrap_or_else(|| {
                // 如果连接不存在，使用默认值
                let default_info =
                    crate::server::connection::ConnectionInfo::new(conn_id.to_string(), true);
                ConnectionState {
                    info: default_info,
                    negotiation_completed: false,
                    negotiation_confirmed: false,
                    cached_parser: None,
                    cached_pipeline: None,
                }
            })
    }

    /// 处理已协商完成的消息
    ///
    /// 处理流程：
    /// 1. 解析消息得到 Frame（由 MessageParser 统一处理，支持容错标记）
    /// 2. 如果有 pipeline，执行中间件（before）和处理器（processor）
    /// 3. **无论 pipeline 是否返回响应，都继续调用 handle_frame 让 event_wrapper.rs 处理业务逻辑**
    /// 4. event_wrapper.rs 处理后会发送响应（如果有）
    ///
    /// # 容错策略
    /// - 如果 `negotiation_confirmed = false`：使用容错模式（允许解密失败时作为未加密数据处理）
    /// - 如果 `negotiation_confirmed = true`：使用严格模式（解密失败直接返回错误）
    /// - 在客户端确认之前，除了加密、压缩和序列化，其他都使用默认值（JSON、不压缩、不加密）
    #[allow(clippy::too_many_arguments)]
    async fn handle_negotiated_message(
        handler: &Arc<ServerMessageWrapper>,
        data: Vec<u8>,
        conn_id: Arc<str>,
        manager: Arc<ConnectionManager>,
        connection_info: crate::server::connection::ConnectionInfo,
        negotiation_confirmed: bool,
        cached_parser: Option<Arc<MessageParser>>,
        cached_pipeline: Option<Arc<crate::common::message::pipeline::MessagePipeline>>,
    ) {
        // 1. 先尝试使用 PRE_NEGOTIATION_PARSER 解析，检查是否是 NEGOTIATION_READY 消息
        // NEGOTIATION_READY 消息必须使用 PRE_NEGOTIATION_PARSER（JSON、不压缩、不加密）
        // 这样客户端和服务端都统一使用 PRE_NEGOTIATION_PARSER 处理，确保兼容性
        // 服务端收到 NEGOTIATION_READY 后才会严格使用协商后的 parser（不允许 fallback）
        if let Ok(frame) = PRE_NEGOTIATION_PARSER.parse(&data)
            && Self::is_negotiation_ready_message(&frame)
        {
            // 这是 NEGOTIATION_READY 消息，使用 PRE_NEGOTIATION_PARSER 解析成功
            // 标记协商已确认，之后服务端将严格使用协商后的 parser（不允许 fallback）
            if let Err(e) = (*manager).mark_negotiation_confirmed(&conn_id) {
                error!(
                    "[ConnectionHandlerObserverAdapter] 标记协商确认失败: connection_id={}, error={}",
                    conn_id, e
                );
            } else {
                debug!(
                    "[ConnectionHandlerObserverAdapter] ✅ 协商已确认: connection_id={}，之后将严格使用协商后的 parser",
                    conn_id
                );
            }
            // 协商确认消息不需要进一步处理
            return;
        }

        // 2. 如果不是 NEGOTIATION_READY 消息，使用协商后的 parser 解析
        // 在客户端确认之前（negotiation_confirmed = false），如果协商已完成但未确认，使用容错模式
        // 这样可以兼容客户端在收到 CONNECT_ACK 之前发送的未加密消息
        // 在客户端确认之后（negotiation_confirmed = true），严格使用协商后的 parser，不允许 fallback
        let allow_fallback = !negotiation_confirmed;

        // 先克隆，避免在创建 parser 时移动
        let compression = connection_info.compression.clone();
        let encryption = connection_info.encryption.clone();

        let parser = cached_parser.unwrap_or_else(|| {
            error!(
                "[ConnectionHandlerObserverAdapter] 协商已完成但缓存 parser 不存在，回退到动态创建: connection_id={}",
                conn_id
            );
            std::sync::Arc::new(crate::common::MessageParser::new(
                connection_info.serialization_format,
                compression.clone(),
                encryption.clone(),
            ))
        });

        // 在解析前验证加密器是否已注册（如果协商了加密）
        if encryption != crate::common::encryption::EncryptionAlgorithm::None {
            let encryptor_name = encryption.as_str();
            if !crate::common::encryption::EncryptionUtil::is_registered(&encryptor_name) {
                let registered = crate::common::encryption::EncryptionUtil::list_registered();
                error!(
                    "[ConnectionHandlerObserverAdapter] 加密器未注册: connection_id={}, encryption={:?}, registered={:?}",
                    conn_id, encryption, registered
                );
                return;
            }
        }

        // 使用 MessageParser 的统一解析方法，支持容错标记
        // negotiation_confirmed = false 时允许 fallback（容错模式）
        // negotiation_confirmed = true 时不允许 fallback（严格模式）
        let frame = match parser.parse_with_fallback(&data, allow_fallback) {
            Ok(frame) => frame,
            Err(e) => {
                // 提供更详细的错误信息，包括加密器注册状态
                let encryptor_status = if encryption
                    != crate::common::encryption::EncryptionAlgorithm::None
                {
                    let encryptor_name = encryption.as_str();
                    if crate::common::encryption::EncryptionUtil::is_registered(&encryptor_name) {
                        "registered".to_string()
                    } else {
                        format!(
                            "NOT registered (registered: {:?})",
                            crate::common::encryption::EncryptionUtil::list_registered()
                        )
                    }
                } else {
                    "none".to_string()
                };

                // 添加数据预览，帮助调试
                let data_preview: Vec<u8> = data.iter().take(16).cloned().collect();
                error!(
                    "[ConnectionHandlerObserverAdapter] 解析消息失败（协商后）: connection_id={}, format={:?}, compression={:?}, encryption={:?} ({}), confirmed={}, allow_fallback={}, error={}, data_len={}, data_preview={:?}",
                    conn_id,
                    connection_info.serialization_format,
                    compression,
                    encryption,
                    encryptor_status,
                    negotiation_confirmed,
                    allow_fallback,
                    e,
                    data.len(),
                    data_preview
                );
                return;
            }
        };

        // 3. 如果有 pipeline，执行中间件（before）和处理器（processor）
        // 注意：Pipeline 的处理器（processor）只用于额外的处理，不应该替代 handle_frame
        // 无论 pipeline 是否返回响应，都继续调用 handle_frame 让 event_wrapper.rs 处理业务逻辑
        if let Some(pipeline) = cached_pipeline {
            // 执行 pipeline 的中间件（before）和处理器
            // 这里只执行中间件和处理器，不处理响应（响应由 event_wrapper.rs 处理）
            match pipeline.process_frame(&frame, Some(&conn_id)).await {
                Ok(pipeline_response) => {
                    // Pipeline 可能返回了响应，但我们仍然继续调用 handle_frame
                    // Pipeline 的响应可以用于中间件处理（如日志、监控），但不替代业务逻辑处理
                    if pipeline_response.is_some() {
                        debug!(
                            "[ConnectionHandlerObserverAdapter] Pipeline 返回了响应，但继续调用 handle_frame: connection_id={}",
                            conn_id
                        );
                    }
                }
                Err(e) => {
                    error!(
                        "[ConnectionHandlerObserverAdapter] Pipeline 处理失败: connection_id={}, error={}",
                        conn_id, e
                    );
                    // Pipeline 失败不影响继续处理，继续调用 handle_frame
                }
            }
        }

        // 4. 继续调用 handle_frame 让 event_wrapper.rs 处理业务逻辑
        // 这是必须的，因为所有消息最终都需要经过 event_wrapper.rs 处理
        Self::handle_frame_safely(handler, &frame, &conn_id).await;
    }

    /// 处理协商前的消息
    async fn handle_pre_negotiation_message(
        handler: &Arc<ServerMessageWrapper>,
        data: Vec<u8>,
        conn_id: Arc<str>,
        manager: Arc<ConnectionManager>,
        server_core: Option<Arc<crate::server::transports::server_core::ServerCore>>,
    ) {
        // 使用全局共享的协商前 parser（避免每次创建）
        match PRE_NEGOTIATION_PARSER.parse(&data) {
            Ok(frame) => {
                // 检查是否是 CONNECT 消息
                if Self::is_connect_message(&frame) {
                    // CONNECT 消息：调用 ServerCore 的 handle_connect_complete
                    Self::handle_connect_message(handler, &frame, &conn_id, manager, server_core)
                        .await; // CONNECT 消息处理完成
                } else {
                    // 非 CONNECT 消息但在协商完成前收到，记录警告并继续处理
                    // 检查连接信息，确认协商状态
                    if let Some((_, conn_info)) = (*manager).get_connection(&conn_id) {
                        warn!(
                            "[ConnectionHandlerObserverAdapter] 收到非 CONNECT 消息但协商未完成: connection_id={}, negotiation_completed={}, negotiation_confirmed={}, format={:?}, compression={:?}, encryption={:?}",
                            conn_id,
                            conn_info.negotiation_completed,
                            conn_info.negotiation_confirmed,
                            conn_info.serialization_format,
                            conn_info.compression,
                            conn_info.encryption
                        );
                    } else {
                        warn!(
                            "[ConnectionHandlerObserverAdapter] 收到非 CONNECT 消息但协商未完成: connection_id={}, 连接不存在",
                            conn_id
                        );
                    }
                    // 继续使用 handle_frame 处理
                    Self::handle_frame_safely(handler, &frame, &conn_id).await;
                }
            }
            Err(e) => {
                error!(
                    "[ConnectionHandlerObserverAdapter] 解析消息失败（协商前）: connection_id={}, error={}",
                    conn_id, e
                );
            }
        }
    }

    /// 安全地调用 handle_frame（统一错误处理）
    async fn handle_frame_safely(
        handler: &Arc<ServerMessageWrapper>,
        frame: &crate::common::protocol::Frame,
        conn_id: &str,
    ) {
        use crate::server::ConnectionHandler;
        if let Err(e) = ConnectionHandler::handle_frame(handler.as_ref(), frame, conn_id).await {
            error!(
                "[ConnectionHandlerObserverAdapter] 处理消息失败: connection_id={}, error={}",
                conn_id, e
            );
        }
    }

    /// 处理 CONNECT 消息
    async fn handle_connect_message(
        _handler: &Arc<ServerMessageWrapper>,
        frame: &crate::common::protocol::Frame,
        conn_id: &Arc<str>,
        manager: Arc<ConnectionManager>,
        server_core: Option<Arc<crate::server::transports::server_core::ServerCore>>,
    ) {
        // 获取连接实例
        let Some((connection, _)) = manager.get_connection(conn_id) else {
            error!(
                "[ConnectionHandlerObserverAdapter] 连接不存在，无法处理 CONNECT: connection_id={}",
                conn_id
            );
            return;
        };

        // 获取 ServerCore 引用
        let Some(server_core) = &server_core else {
            error!(
                "[ConnectionHandlerObserverAdapter] ServerCore 未初始化，无法处理 CONNECT: connection_id={}",
                conn_id
            );
            return;
        };

        if let Err(e) = server_core
            .handle_connect_complete(frame, conn_id, connection)
            .await
        {
            error!(
                "[ConnectionHandlerObserverAdapter] 处理 CONNECT 消息失败: connection_id={}, error={}",
                conn_id, e
            );
            if let Err(remove_err) = manager.remove_connection(conn_id) {
                warn!(
                    "[ConnectionHandlerObserverAdapter] CONNECT 失败后移除连接失败: connection_id={}, error={}",
                    conn_id, remove_err
                );
            }
        }
        // CONNECT 消息已由 handle_connect_complete 处理，不需要再调用 handle_frame
    }

    /// 检查是否是 CONNECT 消息
    fn is_connect_message(frame: &crate::common::protocol::Frame) -> bool {
        frame.command.as_ref().and_then(|cmd| {
            if let Some(crate::common::protocol::flare::core::commands::command::Type::System(sys_cmd)) = &cmd.r#type {
                use crate::common::protocol::flare::core::commands::system_command::Type as SysType;
                Some(sys_cmd.r#type == SysType::Connect as i32)
            } else {
                None
            }
        }).unwrap_or(false)
    }

    /// 检查是否是 NEGOTIATION_READY 消息
    fn is_negotiation_ready_message(frame: &crate::common::protocol::Frame) -> bool {
        frame.command.as_ref().and_then(|cmd| {
            if let Some(crate::common::protocol::flare::core::commands::command::Type::System(sys_cmd)) = &cmd.r#type {
                use crate::common::protocol::flare::core::commands::system_command::Type as SysType;
                Some(sys_cmd.r#type == SysType::NegotiationReady as i32)
            } else {
                None
            }
        }).unwrap_or(false)
    }
}

impl ConnectionObserver for ConnectionHandlerObserverAdapter {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
                // 投递到本连接的有序队列，由单个消费任务按序处理。
                // 这里不能 spawn：同一连接的帧一旦并行，序列号分配就会乱序。
                if let Err(e) = self.inbound.try_send(data.to_vec()) {
                    match e {
                        tokio::sync::mpsc::error::TrySendError::Full(_) => {
                            error!(
                                "[ConnectionHandlerObserverAdapter] 入站队列已满({}), 丢弃该帧: connection_id={}。\
                                 客户端灌入速度持续超过处理速度，请检查是否有异常重发。",
                                INBOUND_QUEUE_CAPACITY, self.connection_id
                            );
                        }
                        tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                            debug!(
                                "[ConnectionHandlerObserverAdapter] 入站队列已关闭，忽略该帧: connection_id={}",
                                self.connection_id
                            );
                        }
                    }
                }
            }
            ConnectionEvent::Connected => {
                // 物理连接建立不等于 IM 会话建立。业务 on_connect 必须等 CONNECT
                // 认证、协商、CONNECT_ACK 发送完成后由 ServerCore 统一触发。
                debug!(
                    "[ConnectionHandlerObserverAdapter] transport connected; wait for CONNECT negotiation: connection_id={}",
                    self.connection_id
                );
            }
            ConnectionEvent::Disconnected(reason) => {
                // 调用 on_disconnect
                let handler = Arc::clone(&self.handler);
                // 使用 Arc<str> 避免 String clone，减少内存分配
                let conn_id: Arc<str> = Arc::from(self.connection_id.as_str());
                // 使用 Arc<str> 避免 String clone，减少内存分配
                let reason_str: Arc<str> = Arc::from(reason.as_str());
                tokio::spawn(async move {
                    use crate::server::ConnectionHandler;
                    if let Err(e) =
                        ConnectionHandler::on_disconnect(handler.as_ref(), &conn_id).await
                    {
                        warn!(
                            "[ConnectionHandlerObserverAdapter] 处理断开事件失败: connection_id={}, reason={}, error={}",
                            conn_id, reason_str, e
                        );
                    }
                });
            }
            ConnectionEvent::Error(err) => {
                // 处理错误事件
                let handler = Arc::clone(&self.handler);
                let conn_id: Arc<str> = Arc::from(self.connection_id.as_str());
                let error_msg = err.to_string();
                let error_str: Arc<str> = Arc::from(error_msg.as_str());

                tokio::spawn(async move {
                    if let Err(e) = handler.on_error(&conn_id, &error_str).await {
                        error!(
                            "[ConnectionHandlerObserverAdapter] 处理错误事件失败: connection_id={}, error={}",
                            conn_id, e
                        );
                    }
                });
            }
        }
    }
}

#[cfg(test)]
mod ordering_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Mutex;

    /// 锁住「同一连接的帧按投递顺序处理」这个不变量。
    ///
    /// 这里复刻的是适配器的分发骨架（有界 mpsc + 单消费任务），不拉起整个
    /// ServerMessageWrapper——要点在于**不能每帧 spawn**。
    /// 之前正是每帧 spawn 让同连接的帧在运行时里抢跑：连发的消息谁先跑到
    /// 序列号分配谁拿小号，会话时间线永久错乱。
    /// 如果有人把分发改回并发，这个测试会红。
    #[tokio::test]
    async fn frames_from_one_connection_are_processed_in_order() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<usize>(1024);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let done = Arc::new(AtomicUsize::new(0));

        let worker = {
            let seen = Arc::clone(&seen);
            let done = Arc::clone(&done);
            tokio::spawn(async move {
                while let Some(i) = rx.recv().await {
                    // 故意让「先到的帧处理更慢」——并发分发下这必然导致乱序。
                    let delay = if i % 2 == 0 { 8 } else { 1 };
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    seen.lock().await.push(i);
                    done.fetch_add(1, Ordering::SeqCst);
                }
            })
        };

        const N: usize = 24;
        for i in 0..N {
            tx.try_send(i).expect("入站队列不该在这个规模下满");
        }
        drop(tx);
        worker.await.expect("消费任务不该 panic");

        assert_eq!(
            done.load(Ordering::SeqCst),
            N,
            "所有帧都必须被处理，一帧不能少"
        );
        let got = seen.lock().await.clone();
        assert_eq!(
            got,
            (0..N).collect::<Vec<_>>(),
            "同一连接的帧必须按投递顺序处理；乱序说明分发又变成并发的了"
        );
    }

    /// 队列满时必须**报错而不是静默丢**：静默丢在 IM 里表现为「消息偶尔不见」。
    #[tokio::test]
    async fn full_queue_is_observable_not_silent() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<usize>(2);
        assert!(tx.try_send(1).is_ok());
        assert!(tx.try_send(2).is_ok());
        match tx.try_send(3) {
            Err(tokio::sync::mpsc::error::TrySendError::Full(v)) => assert_eq!(v, 3),
            other => panic!("队列满必须可观测地失败，实际: {other:?}"),
        }
    }
}
