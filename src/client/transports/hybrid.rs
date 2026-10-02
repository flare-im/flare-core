//! 混合客户端接口
//!
//! 支持单个协议或多协议竞速
//! 统一管理连接状态、心跳、消息路由等功能

use crate::client::config::ClientConfig;
use crate::client::transports::{Client, ClientCore};
use crate::common::config_types::TransportProtocol;
use crate::common::error::{FlareError, Result};
use crate::common::platform::{MonotonicInstant, monotonic_now, timeout as platform_timeout};
use crate::common::protocol::Frame;
use crate::transport::events::ArcObserver;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[cfg(feature = "quic")]
use crate::client::transports::quic::QUICClient;
#[cfg(feature = "tcp")]
use crate::client::transports::tcp::TCPClient;
#[cfg(feature = "websocket")]
use crate::client::transports::websocket::WebSocketClient;

/// 混合客户端
///
/// 支持单个协议连接或多协议竞速
/// 统一管理连接状态、心跳、消息路由等功能
pub struct HybridClient {
    /// 内部客户端（根据配置动态选择）
    inner: Arc<Mutex<Box<dyn Client>>>,
    /// 最近一次**成功读到**的连接状态。
    ///
    /// `is_connected` 是同步 trait 方法，撞上 `inner` 锁时没有阻塞以外的选择——
    /// 而阻塞在这里两头不讨好（见 `is_connected` 处的说明）。缓存让它可以无锁降级。
    last_known_connected: Arc<std::sync::atomic::AtomicBool>,
    /// 使用的协议
    active_protocol: TransportProtocol,
    /// 客户端核心功能（统一管理连接状态、心跳、消息路由）
    core: ClientCore,
}

/// 连接结果类型别名
type ConnectionResult = Result<Box<dyn Client>>;

/// 连接任务结果
type ConnectionTaskResult = (TransportProtocol, usize, ConnectionResult, Duration);

/// 竞速中各协议的建连任务。`JoinSet` 被丢弃时中止其中全部任务：竞速超时（整个竞速 future
/// 被丢弃）时，还在握手的连接随之中止，不在后台拖到连接超时。
type RaceTasks = tokio::task::JoinSet<ConnectionTaskResult>;

/// 成功的连接信息
type SuccessfulConnection = (usize, TransportProtocol, Box<dyn Client>, Duration);

/// 失败的连接信息
type FailedConnection = (usize, TransportProtocol, FlareError, Duration);

impl HybridClient {
    /// 创建新的混合客户端
    ///
    /// # 参数
    /// - `config`: 客户端配置
    ///
    /// # 返回
    /// 混合客户端实例
    pub fn new(config: ClientConfig) -> Result<Self> {
        let core = ClientCore::new(&config);
        let protocols = config.get_protocols();

        if protocols.len() == 1 {
            Self::create_single_protocol(config, protocols[0], core)
        } else {
            Self::create_race_mode_placeholder(config, protocols[0], core)
        }
    }

    /// 创建单个协议客户端
    fn create_single_protocol(
        config: ClientConfig,
        protocol: TransportProtocol,
        core: ClientCore,
    ) -> Result<Self> {
        let mut single_config = config;
        single_config.transport = protocol;
        single_config.transports = None;
        // 与竞速模式一致：优先使用 with_protocol_url 注册的地址，避免裸 host:port 缺少 scheme。
        single_config.server_url = single_config.get_protocol_url(&protocol);

        let client = Self::create_protocol_client(single_config, core.clone())?;

        Ok(Self {
            inner: Arc::new(Mutex::new(client)),
            last_known_connected: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            active_protocol: protocol,
            core,
        })
    }

    /// 创建竞速模式的占位符客户端
    ///
    /// 返回一个占位符，实际连接在 connect_with_race 时完成
    fn create_race_mode_placeholder(
        config: ClientConfig,
        default_protocol: TransportProtocol,
        core: ClientCore,
    ) -> Result<Self> {
        let default_config = ClientConfig {
            server_url: config.get_protocol_url(&default_protocol),
            transport: default_protocol,
            ..config
        };

        let client = Self::create_protocol_client(default_config, core.clone())?;

        Ok(Self {
            inner: Arc::new(Mutex::new(client)),
            last_known_connected: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            active_protocol: default_protocol,
            core,
        })
    }

    /// 创建协议客户端（统一创建逻辑）
    fn create_protocol_client(config: ClientConfig, core: ClientCore) -> Result<Box<dyn Client>> {
        match config.transport {
            TransportProtocol::WebSocket => {
                #[cfg(feature = "websocket")]
                {
                    Ok(Box::new(WebSocketClient::with_core(config, core)))
                }
                #[cfg(not(feature = "websocket"))]
                {
                    let _ = (config, core);
                    Err(Self::transport_feature_disabled(
                        TransportProtocol::WebSocket,
                    ))
                }
            }
            TransportProtocol::QUIC => {
                #[cfg(feature = "quic")]
                {
                    QUICClient::with_core(config, core).map(|c| Box::new(c) as Box<dyn Client>)
                }
                #[cfg(not(feature = "quic"))]
                {
                    let _ = (config, core);
                    Err(Self::transport_feature_disabled(TransportProtocol::QUIC))
                }
            }
            TransportProtocol::TCP => {
                #[cfg(feature = "tcp")]
                {
                    Ok(Box::new(TCPClient::with_core(config, core)))
                }
                #[cfg(not(feature = "tcp"))]
                {
                    let _ = (config, core);
                    Err(Self::transport_feature_disabled(TransportProtocol::TCP))
                }
            }
        }
    }

    #[allow(dead_code)]
    fn transport_feature_disabled(protocol: TransportProtocol) -> FlareError {
        FlareError::operation_not_supported(format!("{protocol:?} transport feature is disabled"))
    }

    /// 协议竞速连接
    ///
    /// 并行为每个协议建立网络连接，第一个建好的协议胜出，立即在它上面发 CONNECT：不等其余协议，
    /// 也不等排在前面、还没建好的协议。未选中的协议交给后台收尾
    /// （[`Self::close_race_losers_in_background`]）。
    ///
    /// 此前竞速要等所有未选中的连接收尾：UDP 被静默丢弃时 QUIC 握手要等到连接超时（默认 30 s），
    /// 整个竞速在竞速时限（默认 5 s）处失败，已经连上的 WebSocket 也一起丢掉。
    async fn race_connect(
        config: ClientConfig,
        shared_core: ClientCore,
    ) -> Result<(Box<dyn Client>, TransportProtocol)> {
        let protocols = config.get_protocols();
        let race_start = monotonic_now();

        let mut tasks = Self::spawn_connection_tasks(config, &protocols, &shared_core);
        let (winner, errors) = Self::wait_for_first_connection(&mut tasks).await;
        Self::close_race_losers_in_background(tasks);

        // 在胜出的连接上发送 CONNECT
        Self::process_race_results(winner, errors, shared_core, race_start).await
    }

    /// 为所有协议创建连接任务
    fn spawn_connection_tasks(
        config: ClientConfig,
        protocols: &[TransportProtocol],
        shared_core: &ClientCore,
    ) -> RaceTasks {
        let mut tasks = RaceTasks::new();
        for (index, protocol) in protocols.iter().enumerate() {
            let protocol_url = config.get_protocol_url(protocol);
            tracing::debug!(
                "协议竞速: [{}] {:?} 使用地址: {}",
                index,
                protocol,
                protocol_url
            );

            let protocol_config = ClientConfig {
                server_url: protocol_url.clone(),
                transport: *protocol,
                transports: None,
                ..config.clone()
            };

            // 为每个协议创建独立的 core 副本，但共享竞速关键状态
            let mut protocol_core = ClientCore::new(&protocol_config);
            protocol_core.share_race_state_from(shared_core);

            let protocol_clone = *protocol;
            let protocol_index = index;

            tasks.spawn(async move {
                let start_time = monotonic_now();
                tracing::debug!(
                    "开始建立网络连接: {:?} (编号: {}, 地址: {})",
                    protocol_clone,
                    protocol_index,
                    protocol_url
                );

                // 仅建立网络连接，不发送 CONNECT（用于协议竞速）
                let network_result = Self::establish_protocol_network(
                    protocol_clone,
                    protocol_config,
                    protocol_core,
                    protocol_index,
                )
                .await;

                let elapsed = start_time.elapsed();

                // 将网络连接结果转换为 ConnectionResult 格式
                let client_result = network_result.map(|(client, _)| client);

                (protocol_clone, protocol_index, client_result, elapsed)
            });
        }
        tasks
    }

    /// 仅建立网络连接（不发送 CONNECT，用于协议竞速）
    async fn establish_protocol_network(
        protocol: TransportProtocol,
        config: ClientConfig,
        core: ClientCore,
        priority: usize,
    ) -> Result<(Box<dyn Client>, Duration)> {
        match protocol {
            TransportProtocol::WebSocket => {
                #[cfg(feature = "websocket")]
                {
                    let (client, elapsed) =
                        Self::establish_websocket_network(config, core, priority).await?;
                    Ok((Box::new(client), elapsed))
                }
                #[cfg(not(feature = "websocket"))]
                {
                    let _ = (config, core, priority);
                    Err(Self::transport_feature_disabled(
                        TransportProtocol::WebSocket,
                    ))
                }
            }
            TransportProtocol::QUIC => {
                #[cfg(feature = "quic")]
                {
                    let (client, elapsed) =
                        Self::establish_quic_network(config, core, priority).await?;
                    Ok((Box::new(client), elapsed))
                }
                #[cfg(not(feature = "quic"))]
                {
                    let _ = (config, core, priority);
                    Err(Self::transport_feature_disabled(TransportProtocol::QUIC))
                }
            }
            TransportProtocol::TCP => {
                #[cfg(feature = "tcp")]
                {
                    let (client, elapsed) =
                        Self::establish_tcp_network(config, core, priority).await?;
                    Ok((Box::new(client), elapsed))
                }
                #[cfg(not(feature = "tcp"))]
                {
                    let _ = (config, core, priority);
                    Err(Self::transport_feature_disabled(TransportProtocol::TCP))
                }
            }
        }
    }

    /// 仅建立 WebSocket 网络连接（不发送 CONNECT 消息）
    ///
    /// 用于协议竞速：先建立网络连接，选择最快协议，然后再发送 CONNECT
    #[cfg(feature = "websocket")]
    async fn establish_websocket_network(
        config: ClientConfig,
        core: ClientCore,
        priority: usize,
    ) -> Result<(WebSocketClient, Duration)> {
        let start_time = monotonic_now();
        let mut client = WebSocketClient::with_core(config, core);

        // 仅建立网络连接，不发送 CONNECT
        // establish_network_connection 会保存连接到 client.connection
        let _connection_arc = client.establish_network_connection().await?;
        let elapsed = start_time.elapsed();

        tracing::debug!(
            "WebSocket 网络连接建立成功 (优先级: {}, 耗时: {:?})",
            priority,
            elapsed
        );
        Ok((client, elapsed))
    }

    /// 仅建立 QUIC 网络连接（不发送 CONNECT 消息）
    ///
    /// 用于协议竞速：先建立网络连接，选择最快协议，然后再发送 CONNECT
    ///
    /// 优化：将 endpoint 创建移到计时开始之前，只测量网络连接建立时间
    /// 这样可以更公平地比较 QUIC 和 WebSocket 的网络连接速度
    #[cfg(feature = "quic")]
    async fn establish_quic_network(
        config: ClientConfig,
        core: ClientCore,
        priority: usize,
    ) -> Result<(QUICClient, Duration)> {
        // 在计时开始之前创建 endpoint（排除 endpoint 创建时间）
        // 这样可以更公平地比较网络连接时间，而不是包含 endpoint 创建时间
        let (endpoint, client_config) = match QUICClient::create_quic_endpoint_with_tls(&config.tls)
        {
            Ok(ep) => ep,
            Err(e) => {
                tracing::warn!(
                    "QUIC endpoint 创建失败 (优先级: {}, 地址: {}): {}",
                    priority,
                    config.server_url,
                    e
                );
                return Err(e);
            }
        };

        // 现在开始计时（只测量网络连接建立时间）
        let start_time = monotonic_now();

        let mut client = match QUICClient::with_core_and_endpoint(
            config.clone(),
            core,
            Some((endpoint, client_config)),
        ) {
            Ok(client) => client,
            Err(e) => {
                let elapsed = start_time.elapsed();
                tracing::warn!(
                    "QUIC 客户端创建失败 (优先级: {}, 耗时: {:?}, 地址: {}): {}",
                    priority,
                    elapsed,
                    config.server_url,
                    e
                );
                return Err(e);
            }
        };

        // 仅建立网络连接，不发送 CONNECT
        let _connection_arc = client.establish_network_connection().await?;
        let elapsed = start_time.elapsed();

        tracing::debug!(
            "QUIC 网络连接建立成功 (优先级: {}, 耗时: {:?})",
            priority,
            elapsed
        );
        Ok((client, elapsed))
    }

    /// 仅建立 TCP 网络连接（不发送 CONNECT 消息）
    #[cfg(feature = "tcp")]
    async fn establish_tcp_network(
        config: ClientConfig,
        core: ClientCore,
        priority: usize,
    ) -> Result<(TCPClient, Duration)> {
        let start_time = monotonic_now();
        let mut client = TCPClient::with_core(config, core);
        let _connection_arc = client.establish_network_connection().await?;
        let elapsed = start_time.elapsed();
        tracing::debug!(
            "TCP 网络连接建立成功 (优先级: {}, 耗时: {:?})",
            priority,
            elapsed
        );
        Ok((client, elapsed))
    }

    /// 等第一个建好网络连接的协议：它一建好就胜出、立即返回，不再等还在建连的其余协议。
    ///
    /// 返回胜出的连接（全部失败时为 `None`）与此前已经失败的协议；还没结束的任务留在 `tasks` 里，
    /// 由调用方交给 [`Self::close_race_losers_in_background`]。
    async fn wait_for_first_connection(
        tasks: &mut RaceTasks,
    ) -> (Option<SuccessfulConnection>, Vec<FailedConnection>) {
        let mut errors = Vec::new();

        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok((protocol, protocol_index, Ok(client), elapsed)) => {
                    let ms = elapsed.as_secs_f64() * 1000.0;
                    tracing::info!(
                        "第一个建好的连接: {:?} (编号: {}, 耗时: {:.3}ms)，不再等其余 {} 个协议",
                        protocol,
                        protocol_index,
                        ms,
                        tasks.len()
                    );
                    return (Some((protocol_index, protocol, client, elapsed)), errors);
                }
                Ok((protocol, protocol_index, Err(e), elapsed)) => {
                    let ms = elapsed.as_secs_f64() * 1000.0;
                    tracing::warn!(
                        "连接失败: {:?} (编号: {}, 耗时: {:.3}ms): {}",
                        protocol,
                        protocol_index,
                        ms,
                        e
                    );
                    errors.push((protocol_index, protocol, e, elapsed));
                }
                Err(join_err) => {
                    tracing::error!("Task join error: {:?}", join_err);
                }
            }
        }

        (None, errors)
    }

    /// 在后台收尾未选中的协议，不占竞速的时间：还在建连的任务中止（丢弃时连同半建好的连接一起释放），
    /// 已经建好的连接与共享状态脱钩后关闭（[`Self::close_race_loser`]）。
    ///
    /// 未选中的连接没发过 CONNECT（服务端不会按设备冲突踢它），也没有挂消息观察者，与选中连接上的
    /// CONNECT 并行关闭不会产生上层可见的事件。
    fn close_race_losers_in_background(mut tasks: RaceTasks) {
        if tasks.is_empty() {
            return;
        }
        tasks.abort_all();
        tokio::spawn(async move {
            while let Some(joined) = tasks.join_next().await {
                match joined {
                    Ok((protocol, protocol_index, Ok(client), _elapsed)) => {
                        tracing::debug!(
                            "关闭未选中的连接: {:?} (编号: {})",
                            protocol,
                            protocol_index
                        );
                        Self::close_race_loser(client).await;
                    }
                    Ok((_protocol, _index, Err(_), _elapsed)) => {}
                    Err(join_err) if join_err.is_cancelled() => {}
                    Err(join_err) => {
                        tracing::debug!("[HybridClient] race task join error: {:?}", join_err);
                    }
                }
            }
        });
    }

    /// 关闭一个未选中、已经建好网络连接的客户端（客户端主动断开）。
    ///
    /// 先与共享状态脱钩（`detach_from_race`）再置位主动断开、关闭：它关闭时的事件只到它自己的观察者，
    /// 主动断开标记也只置在它自己身上，不会让选中的连接此后被踢时不通知观察者、发送前不再重连。
    async fn close_race_loser(mut client: Box<dyn Client>) {
        client.detach_from_race();
        client.set_disconnect_requested(true);
        if let Err(e) = client.disconnect().await {
            tracing::debug!("[HybridClient] race cleanup disconnect: {}", e);
        }
    }

    /// 处理竞速结果：在胜出的连接上发送 CONNECT；全部失败时汇总各协议的错误。
    async fn process_race_results(
        winner: Option<SuccessfulConnection>,
        errors: Vec<FailedConnection>,
        shared_core: ClientCore,
        race_start: MonotonicInstant,
    ) -> Result<(Box<dyn Client>, TransportProtocol)> {
        // 打印已结束协议的耗时信息
        Self::log_protocol_timings(&winner, &errors);

        let Some((selected_index, selected_protocol, mut selected_client, selected_elapsed)) =
            winner
        else {
            // 所有协议都失败了
            return Self::build_all_failed_error(errors);
        };

        let selected_ms = selected_elapsed.as_secs_f64() * 1000.0;
        let total_ms = race_start.elapsed().as_secs_f64() * 1000.0;
        tracing::info!(
            "协议竞速成功: {:?} (编号: {}, 网络连接耗时: {:.3}ms, 总竞速时间: {:.3}ms)",
            selected_protocol,
            selected_index,
            selected_ms,
            total_ms
        );

        // 发送 CONNECT（复用竞速建好的网络连接）
        tracing::debug!(
            "发送 CONNECT 消息: {:?} (编号: {})",
            selected_protocol,
            selected_index
        );
        selected_client.connect().await?;

        // 同步连接状态到共享的 core
        shared_core.state_manager.set_connected();

        Ok((selected_client, selected_protocol))
    }

    /// 打印已结束协议的耗时信息（胜出时还在建连、已被中止的协议不在其中）
    fn log_protocol_timings(winner: &Option<SuccessfulConnection>, errors: &[FailedConnection]) {
        tracing::info!("协议竞速耗时统计：");

        // 收集已结束协议的结果（胜出 + 失败）
        let mut all_results: Vec<(usize, TransportProtocol, Duration, Option<String>)> = Vec::new();

        if let Some((index, protocol, _, elapsed)) = winner {
            all_results.push((*index, *protocol, *elapsed, None));
        }

        for (index, protocol, error, elapsed) in errors {
            all_results.push((*index, *protocol, *elapsed, Some(error.to_string())));
        }

        // 按编号排序
        all_results.sort_by_key(|(index, _, _, _)| *index);

        for (index, protocol, elapsed, error_opt) in all_results {
            let protocol_name = match protocol {
                TransportProtocol::WebSocket => "WebSocket",
                TransportProtocol::QUIC => "QUIC",
                TransportProtocol::TCP => "TCP",
            };
            let ms = elapsed.as_secs_f64() * 1000.0;

            match error_opt {
                None => {
                    tracing::info!(
                        "  [{:2}] {:12} 成功 - {:.3}ms (已选中)",
                        index,
                        protocol_name,
                        ms
                    );
                }
                Some(err) => {
                    // 截断错误信息，避免日志过长
                    let err_short = if err.chars().count() > 50 {
                        format!("{}...", err.chars().take(50).collect::<String>())
                    } else {
                        err
                    };
                    tracing::info!(
                        "  [{:2}] {:12} 失败 - {:.3}ms - {}",
                        index,
                        protocol_name,
                        ms,
                        err_short
                    );
                }
            }
        }
    }

    /// 构建所有协议都失败的错误信息
    fn build_all_failed_error(
        errors: Vec<FailedConnection>,
    ) -> Result<(Box<dyn Client>, TransportProtocol)> {
        let mut sorted_errors = errors;
        sorted_errors.sort_by_key(|(index, _protocol, _error, _elapsed)| *index);

        let error_details: Vec<String> = sorted_errors
            .iter()
            .map(|(index, protocol, e, elapsed)| {
                format!("[{}] {:?} (耗时: {:?}): {}", index, protocol, elapsed, e)
            })
            .collect();

        let error_msg = format!(
            "所有协议连接都失败（按优先级顺序）: {}",
            error_details.join(", ")
        );

        tracing::error!("{}", error_msg);
        Err(FlareError::connection_failed(error_msg))
    }

    /// 获取当前使用的协议
    pub fn active_protocol(&self) -> TransportProtocol {
        self.active_protocol
    }

    /// 获取 ClientCore（用于外部访问）
    pub fn core(&self) -> &ClientCore {
        &self.core
    }

    /// 获取 ClientCore 的可变引用（用于外部修改）
    pub fn core_mut(&mut self) -> &mut ClientCore {
        &mut self.core
    }
}

#[async_trait]
impl Client for HybridClient {
    async fn connect(&mut self) -> Result<()> {
        let mut client = self.inner.lock().await;
        client.connect().await
    }

    async fn disconnect(&mut self) -> Result<()> {
        let mut client = self.inner.lock().await;
        client.disconnect().await
    }

    async fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        let mut client = self.inner.lock().await;
        client.send_frame(frame).await
    }

    fn is_connected(&self) -> bool {
        use std::sync::atomic::Ordering;
        match self.inner.try_lock() {
            Ok(client) => {
                let connected = client.is_connected();
                self.last_known_connected
                    .store(connected, Ordering::Relaxed);
                connected
            }
            // 撞锁不阻塞，退回上一次观察到的状态。
            //
            // 这里原来是 `block_in_place` + `blocking_lock`，注释说它"避免阻塞 Tokio
            // 运行时"——恰恰相反：`block_in_place` 要把本 worker 的整个任务队列交接给
            // 另一个线程，而 `blocking_lock` 随后把这个线程真的挂住，只为读一个布尔；
            // 在 current-thread runtime 上它更是直接 panic。
            //
            // 而"拿不到锁"本身就意味着底层正在收发，缓存值是那一刻的真实状态；
            // 每次成功读取都会刷新它，所以最多只会陈旧一个临界区的时间。
            Err(_) => self.last_known_connected.load(Ordering::Relaxed),
        }
    }

    fn add_observer(&mut self, observer: ArcObserver) {
        // 通过 ClientCore 添加观察者
        self.core.add_observer(observer);
    }

    fn remove_observer(&mut self, observer: ArcObserver) {
        self.core.remove_observer(observer);
    }

    fn connection_id(&self) -> Option<String> {
        if let Ok(client) = self.inner.try_lock() {
            client.connection_id()
        } else {
            None
        }
    }
}

impl std::fmt::Debug for HybridClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridClient")
            .field("active_protocol", &self.active_protocol)
            .finish_non_exhaustive()
    }
}

/// 创建混合客户端的便捷函数
impl HybridClient {
    /// 发送消息并等待服务端响应（按 Frame.message_id 匹配）
    pub async fn send_frame_and_wait(&mut self, frame: &Frame, timeout: Duration) -> Result<Frame> {
        if frame.message_id.is_empty() {
            return Err(FlareError::protocol_error(
                "message_id is empty".to_string(),
            ));
        }

        tracing::debug!(
            "[HybridClient] send_frame_and_wait: 注册等待响应, message_id={}, frame.message_id={}",
            frame.message_id,
            frame.message_id
        );

        // 在发送前注册等待器
        let rx = self.core.register_pending_response(&frame.message_id).await;
        tracing::debug!(
            "[HybridClient] send_frame_and_wait: 已注册等待响应, message_id={}",
            frame.message_id
        );

        // 发送消息
        {
            let mut client = self.inner.lock().await;
            client.send_frame(frame).await?;
        }

        tracing::debug!(
            "[HybridClient] send_frame_and_wait: 消息已发送, 等待响应, message_id={}, timeout={:?}",
            frame.message_id,
            timeout
        );

        // 等待响应或超时
        match platform_timeout(timeout, rx).await {
            Ok(Ok(resp)) => {
                tracing::debug!(
                    "[HybridClient] send_frame_and_wait: 收到响应, message_id={}, resp.message_id={}",
                    frame.message_id,
                    resp.message_id
                );
                Ok(resp)
            }
            Ok(Err(_)) => {
                // 发送失败（通道已关闭），清理等待项
                tracing::debug!(
                    "[HybridClient] send_frame_and_wait: 响应通道已关闭, message_id={}",
                    frame.message_id
                );
                self.core.cancel_pending_response(&frame.message_id).await;
                Err(FlareError::protocol_error(
                    "Response channel closed".to_string(),
                ))
            }
            Err(_) => {
                // 超时，清理等待项
                tracing::warn!(
                    "[HybridClient] send_frame_and_wait: 响应超时, message_id={}",
                    frame.message_id
                );
                self.core.cancel_pending_response(&frame.message_id).await;
                Err(FlareError::protocol_error(format!(
                    "Response timeout for message_id {}",
                    frame.message_id
                )))
            }
        }
    }

    /// 使用配置创建并连接（单协议）
    pub async fn connect_with_config(config: ClientConfig) -> Result<Self> {
        let mut client = Self::new(config)?;
        client.connect().await?;
        Ok(client)
    }

    /// 使用配置创建并连接（协议竞速）
    pub async fn connect_with_race(config: ClientConfig) -> Result<Self> {
        if !config.is_race_mode() {
            return Self::connect_with_config(config).await;
        }

        let core = ClientCore::new(&config);
        let race_timeout = config.race_timeout.unwrap_or(Duration::from_secs(5));

        // 超时时整个竞速 future 被丢弃：还在建连的任务随 `RaceTasks` 一起中止，不等它们收尾。
        let race_result =
            platform_timeout(race_timeout, Self::race_connect(config, core.clone())).await;

        let (client, protocol) = match race_result {
            Ok(result) => result?,
            Err(_) => {
                return Err(FlareError::connection_failed(format!(
                    "Protocol race timed out after {:?}",
                    race_timeout
                )));
            }
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(client)),
            last_known_connected: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            active_protocol: protocol,
            core,
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod hybrid_race_tests {
    use super::*;
    #[cfg(feature = "websocket")]
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct TrackedRaceClient {
        disconnected: Arc<AtomicBool>,
        disconnect_requested: Arc<AtomicBool>,
    }

    impl TrackedRaceClient {
        fn new(disconnected: Arc<AtomicBool>, disconnect_requested: Arc<AtomicBool>) -> Self {
            Self {
                disconnected,
                disconnect_requested,
            }
        }
    }

    #[async_trait]
    impl Client for TrackedRaceClient {
        async fn connect(&mut self) -> Result<()> {
            Ok(())
        }

        async fn disconnect(&mut self) -> Result<()> {
            self.disconnected.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn send_frame(&mut self, _frame: &Frame) -> Result<()> {
            Ok(())
        }

        fn is_connected(&self) -> bool {
            !self.disconnected.load(Ordering::SeqCst)
        }

        fn add_observer(&mut self, _observer: ArcObserver) {}

        fn remove_observer(&mut self, _observer: ArcObserver) {}

        fn set_disconnect_requested(&mut self, value: bool) {
            self.disconnect_requested.store(value, Ordering::SeqCst);
        }
    }

    fn ok_client(
        disconnected: Arc<AtomicBool>,
        disconnect_requested: Arc<AtomicBool>,
    ) -> Box<dyn Client> {
        Box::new(TrackedRaceClient::new(disconnected, disconnect_requested))
    }

    /// 任务的 future 被丢弃（中止，或正常结束）时置位。
    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    async fn wait_for_flag(flag: &AtomicBool, what: &str) {
        let started = std::time::Instant::now();
        while !flag.load(Ordering::SeqCst) {
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "{what} not observed within 2s"
            );
            crate::common::platform::sleep(Duration::from_millis(5)).await;
        }
    }

    /// 第一个建好的连接一建好就胜出：不等还在建连的协议（这里要 30 s 才结束，相当于 UDP 被静默丢弃时
    /// QUIC 握手到连接超时），之后那个任务在后台被中止。此前竞速等所有未选中的连接收尾，在竞速时限
    /// 处整体失败。
    #[tokio::test]
    async fn the_first_connection_wins_without_waiting_for_the_others() {
        let winner_disconnected = Arc::new(AtomicBool::new(false));
        let winner_requested = Arc::new(AtomicBool::new(false));
        let handshake_dropped = Arc::new(AtomicBool::new(false));

        let mut tasks = RaceTasks::new();
        tasks.spawn({
            let handshake_dropped = Arc::clone(&handshake_dropped);
            async move {
                let _dropped = DropFlag(handshake_dropped);
                crate::common::platform::sleep(Duration::from_secs(30)).await;
                (
                    TransportProtocol::QUIC,
                    0,
                    Err(FlareError::connection_timeout("Connection timeout")),
                    Duration::from_secs(30),
                )
            }
        });
        tasks.spawn({
            let winner_disconnected = Arc::clone(&winner_disconnected);
            let winner_requested = Arc::clone(&winner_requested);
            async move {
                crate::common::platform::sleep(Duration::from_millis(5)).await;
                (
                    TransportProtocol::WebSocket,
                    1,
                    Ok(ok_client(winner_disconnected, winner_requested)),
                    Duration::from_millis(5),
                )
            }
        });

        let started = std::time::Instant::now();
        let (winner, errors) = HybridClient::wait_for_first_connection(&mut tasks).await;
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the race must not wait for the slower protocol: {:?}",
            started.elapsed()
        );
        let (index, protocol, _client, _elapsed) = winner.expect("WebSocket wins");
        assert_eq!((index, protocol), (1, TransportProtocol::WebSocket));
        assert!(errors.is_empty());
        assert_eq!(tasks.len(), 1, "the QUIC handshake is still in flight");

        HybridClient::close_race_losers_in_background(tasks);
        wait_for_flag(&handshake_dropped, "the in-flight handshake being aborted").await;
        assert!(
            !winner_disconnected.load(Ordering::SeqCst),
            "closing the losers must not touch the winner"
        );
        assert!(!winner_requested.load(Ordering::SeqCst));
    }

    /// 失败的协议不影响后来建好的协议胜出，失败原因随结果带回；全部失败时没有胜出者。
    #[tokio::test]
    async fn failures_before_the_first_connection_are_collected() {
        let mut tasks = RaceTasks::new();
        tasks.spawn(async {
            (
                TransportProtocol::QUIC,
                0,
                Err(FlareError::connection_failed("refused")),
                Duration::from_millis(1),
            )
        });
        tasks.spawn(async {
            crate::common::platform::sleep(Duration::from_millis(50)).await;
            (
                TransportProtocol::WebSocket,
                1,
                Ok(ok_client(
                    Arc::new(AtomicBool::new(false)),
                    Arc::new(AtomicBool::new(false)),
                )),
                Duration::from_millis(50),
            )
        });
        let (winner, errors) = HybridClient::wait_for_first_connection(&mut tasks).await;
        assert_eq!(winner.map(|w| w.1), Some(TransportProtocol::WebSocket));
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].1, TransportProtocol::QUIC);

        let mut all_failed = RaceTasks::new();
        for (index, protocol) in [TransportProtocol::QUIC, TransportProtocol::WebSocket]
            .into_iter()
            .enumerate()
        {
            all_failed.spawn(async move {
                (
                    protocol,
                    index,
                    Err(FlareError::connection_failed("refused")),
                    Duration::ZERO,
                )
            });
        }
        let (winner, errors) = HybridClient::wait_for_first_connection(&mut all_failed).await;
        assert!(winner.is_none());
        assert_eq!(errors.len(), 2);
    }

    /// 胜出时已经建好、还没取走结果的未选中连接：在后台以「我方主动断开」关闭。失败的任务直接忽略。
    #[tokio::test]
    async fn established_losers_are_closed_in_the_background() {
        let loser_disconnected = Arc::new(AtomicBool::new(false));
        let loser_requested = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));

        let mut tasks = RaceTasks::new();
        tasks.spawn({
            let loser_disconnected = Arc::clone(&loser_disconnected);
            let loser_requested = Arc::clone(&loser_requested);
            let finished = Arc::clone(&finished);
            async move {
                let _finished = DropFlag(finished);
                (
                    TransportProtocol::QUIC,
                    0,
                    Ok(ok_client(loser_disconnected, loser_requested)),
                    Duration::from_millis(1),
                )
            }
        });
        tasks.spawn(async {
            (
                TransportProtocol::WebSocket,
                1,
                Err(FlareError::connection_failed("mock failure")),
                Duration::ZERO,
            )
        });
        // 让建好的那个任务先跑完：中止只对还没结束的任务生效
        wait_for_flag(&finished, "the loser task finishing").await;

        HybridClient::close_race_losers_in_background(tasks);
        wait_for_flag(&loser_disconnected, "the established loser being closed").await;
        assert!(loser_requested.load(Ordering::SeqCst));
    }

    /// 竞速耗时日志按字符截断错误信息：截断处落在多字节字符中间时不能 panic。这里第 50 个字节
    /// 落在一个汉字中间（「错误 [CONNECTION_FAILED] 」占 27 字节，之后每个汉字 3 字节）。
    #[test]
    fn race_timing_log_truncates_non_ascii_errors_on_a_char_boundary() {
        let error =
            FlareError::connection_failed("读取证书失败读取证书失败读取证书失败读取证书失败");
        assert!(!error.to_string().is_char_boundary(50));
        HybridClient::log_protocol_timings(
            &None,
            &[(0, TransportProtocol::QUIC, error, Duration::from_millis(1))],
        );
    }

    /// 记下共享观察者列表收到的事件。
    #[cfg(feature = "websocket")]
    #[derive(Default)]
    struct SharedObserverEvents(StdMutex<Vec<crate::transport::events::ConnectionEvent>>);

    #[cfg(feature = "websocket")]
    impl crate::transport::events::ConnectionObserver for SharedObserverEvents {
        fn on_event(&self, event: &crate::transport::events::ConnectionEvent) {
            self.0.lock().unwrap().push(event.clone());
        }
    }

    /// 关掉未选中的连接之前先与共享状态脱钩：它关闭时的断开事件不到选中连接的观察者，它置位的主动
    /// 断开标记也不留在共享标记上。此前两者都落在共享状态上：选中的连接一直带着「我方已主动断开」，
    /// 被踢时不通知观察者。
    #[cfg(feature = "websocket")]
    #[tokio::test]
    async fn closing_race_losers_leaves_the_shared_state_alone() {
        let config = ClientConfig::new("ws://127.0.0.1:9/ws".to_string()).websocket();
        let shared = ClientCore::new(&config);
        let events = Arc::new(SharedObserverEvents::default());
        shared.add_observer(events.clone());

        let loser = || {
            let mut core = ClientCore::new(&config);
            core.share_race_state_from(&shared);
            Box::new(WebSocketClient::with_core(config.clone(), core)) as Box<dyn Client>
        };

        HybridClient::close_race_loser(loser()).await;

        let late = loser();
        let finished = Arc::new(AtomicBool::new(false));
        let mut tasks = RaceTasks::new();
        tasks.spawn({
            let finished = Arc::clone(&finished);
            async move {
                let _finished = DropFlag(finished);
                (
                    TransportProtocol::WebSocket,
                    1,
                    Ok(late),
                    Duration::from_millis(1),
                )
            }
        });
        wait_for_flag(&finished, "the late loser task finishing").await;
        HybridClient::close_race_losers_in_background(tasks);
        // 后台收尾没有可等的句柄：给它一点时间跑完
        crate::common::platform::sleep(Duration::from_millis(100)).await;

        assert!(
            !shared.is_disconnect_requested(),
            "a loser's requested disconnect must not stick to the selected connection"
        );
        let seen = events.0.lock().unwrap().clone();
        assert!(
            seen.is_empty(),
            "a loser's close must not reach the selected connection's observers: {seen:?}"
        );
    }
}
