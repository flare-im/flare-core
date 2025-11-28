//! 混合客户端接口
//! 
//! 支持单个协议或多协议竞速
//! 统一管理连接状态、心跳、消息路由等功能

use crate::client::transports::{Client, ClientCore};
use crate::client::config::ClientConfig;
use crate::common::config_types::TransportProtocol;
use crate::common::error::{FlareError, Result};
use crate::common::protocol::Frame;
use crate::transport::events::ArcObserver;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use futures_util::future::select_all;

use crate::client::transports::websocket::WebSocketClient;
use crate::client::transports::quic::QUICClient;

/// 混合客户端
/// 
/// 支持单个协议连接或多协议竞速
/// 统一管理连接状态、心跳、消息路由等功能
pub struct HybridClient {
    /// 内部客户端（根据配置动态选择）
    inner: Arc<Mutex<Box<dyn Client>>>,
    /// 使用的协议
    active_protocol: TransportProtocol,
    /// 客户端核心功能（统一管理连接状态、心跳、消息路由）
    core: ClientCore,
}

/// 连接结果类型别名
type ConnectionResult = Result<Box<dyn Client>>;

/// 连接任务结果
type ConnectionTaskResult = (TransportProtocol, usize, ConnectionResult, Duration);

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
        
        let client = Self::create_protocol_client(single_config, core.clone())?;
        
        Ok(Self {
            inner: Arc::new(Mutex::new(client)),
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
            server_url: config.server_url.clone(),
            transport: default_protocol,
            ..config
        };
        
        let client = Self::create_protocol_client(default_config, core.clone())?;
        
        Ok(Self {
            inner: Arc::new(Mutex::new(client)),
            active_protocol: default_protocol,
            core,
        })
    }
    
    /// 创建协议客户端（统一创建逻辑）
    fn create_protocol_client(
        config: ClientConfig,
        core: ClientCore,
    ) -> Result<Box<dyn Client>> {
        match config.transport {
            TransportProtocol::WebSocket => {
                Ok(Box::new(WebSocketClient::with_core(config, core)))
            }
            TransportProtocol::QUIC => {
                QUICClient::with_core(config, core).map(|c| Box::new(c) as Box<dyn Client>)
            }
            TransportProtocol::TCP => {
                Err(FlareError::protocol_error(
                    "TCP transport not yet implemented".to_string()
                ))
            }
        }
    }
    
    /// 协议竞速连接
    /// 
    /// 并行连接多个协议，选择最快成功的连接
    /// 如果多个连接几乎同时成功（时间差 < 100ms），则使用优先级最高的
    /// 协议列表的顺序就是优先级顺序（index 越小优先级越高）
    async fn race_connect(
        config: ClientConfig,
        shared_core: ClientCore,
    ) -> Result<(Box<dyn Client>, TransportProtocol)> {
        let protocols = config.get_protocols();
        let race_start = Instant::now();
        
        // 创建所有协议的连接任务
        let handles = Self::spawn_connection_tasks(config, &protocols, &shared_core);
        
        // 等待并处理连接结果
        let (first_success, successful_clients, errors) = 
            Self::wait_for_connections(handles).await;
        
        // 处理连接结果（选择最快协议，然后发送 CONNECT）
        Self::process_race_results(
            first_success,
            successful_clients,
            errors,
            shared_core,
            race_start,
        ).await
    }
    
    /// 为所有协议创建连接任务
    fn spawn_connection_tasks(
        config: ClientConfig,
        protocols: &[TransportProtocol],
        shared_core: &ClientCore,
    ) -> Vec<tokio::task::JoinHandle<ConnectionTaskResult>> {
        let mut handles = Vec::with_capacity(protocols.len());
        
        for (index, protocol) in protocols.iter().enumerate() {
            let protocol_url = config.get_protocol_url(protocol);
            tracing::debug!("协议竞速: [{}] {:?} 使用地址: {}", index, protocol, protocol_url);
            
            let protocol_config = ClientConfig {
                server_url: protocol_url.clone(),
                transport: *protocol,
                transports: None,
                ..config.clone()
            };
            
            // 为每个协议创建独立的 core 副本，但共享观察者
            let mut protocol_core = ClientCore::new(&protocol_config);
            protocol_core.observers = Arc::clone(&shared_core.observers);
            
            let protocol_clone = *protocol;
            let protocol_index = index;
            
            let handle = tokio::spawn(async move {
                let start_time = Instant::now();
                tracing::debug!(
                    "开始建立网络连接: {:?} (优先级: {}, 地址: {})",
                    protocol_clone, protocol_index, protocol_url
                );
                
                // 仅建立网络连接，不发送 CONNECT（用于协议竞速）
                let network_result = Self::establish_protocol_network(
                    protocol_clone,
                    protocol_config,
                    protocol_core,
                    protocol_index,
                ).await;
                
                let elapsed = start_time.elapsed();
                
                // 将网络连接结果转换为 ConnectionResult 格式
                let client_result = network_result.map(|(client, _)| client)
                    .map_err(|e| e);
                
                (protocol_clone, protocol_index, client_result, elapsed)
            });
            
            handles.push(handle);
        }
        
        handles
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
                let (client, elapsed) = Self::establish_websocket_network(config, core, priority).await?;
                Ok((Box::new(client), elapsed))
            }
            TransportProtocol::QUIC => {
                let (client, elapsed) = Self::establish_quic_network(config, core, priority).await?;
                Ok((Box::new(client), elapsed))
            }
            TransportProtocol::TCP => {
                Err(FlareError::protocol_error(
                    "TCP transport not yet implemented".to_string()
                ))
            }
        }
    }
    
    /// 连接单个协议（完整流程：建立连接 + 发送 CONNECT）
    async fn connect_protocol(
        protocol: TransportProtocol,
        config: ClientConfig,
        core: ClientCore,
        priority: usize,
    ) -> ConnectionResult {
        match protocol {
            TransportProtocol::WebSocket => {
                Self::connect_websocket(config, core, priority).await
            }
            TransportProtocol::QUIC => {
                Self::connect_quic(config, core, priority).await
            }
            TransportProtocol::TCP => {
                Err(FlareError::protocol_error(
                    "TCP transport not yet implemented".to_string()
                ))
            }
        }
    }
    
    /// 仅建立 WebSocket 网络连接（不发送 CONNECT 消息）
    /// 
    /// 用于协议竞速：先建立网络连接，选择最快协议，然后再发送 CONNECT
    async fn establish_websocket_network(
        config: ClientConfig,
        core: ClientCore,
        priority: usize,
    ) -> Result<(WebSocketClient, Duration)> {
        let start_time = Instant::now();
        let mut client = WebSocketClient::with_core(config, core);
        
        // 仅建立网络连接，不发送 CONNECT
        // establish_network_connection 会保存连接到 client.connection
        let _connection_arc = client.establish_network_connection().await?;
        let elapsed = start_time.elapsed();
        
        tracing::debug!(
            "WebSocket 网络连接建立成功 (优先级: {}, 耗时: {:?})",
            priority, elapsed
        );
        Ok((client, elapsed))
    }
    
    /// 仅建立 QUIC 网络连接（不发送 CONNECT 消息）
    /// 
    /// 用于协议竞速：先建立网络连接，选择最快协议，然后再发送 CONNECT
    /// 
    /// 优化：将 endpoint 创建移到计时开始之前，只测量网络连接建立时间
    /// 这样可以更公平地比较 QUIC 和 WebSocket 的网络连接速度
    async fn establish_quic_network(
        config: ClientConfig,
        core: ClientCore,
        priority: usize,
    ) -> Result<(QUICClient, Duration)> {
        // 在计时开始之前创建 endpoint（排除 endpoint 创建时间）
        // 这样可以更公平地比较网络连接时间，而不是包含 endpoint 创建时间
        let (endpoint, client_config) = match QUICClient::create_quic_endpoint() {
            Ok(ep) => ep,
            Err(e) => {
                tracing::warn!(
                    "QUIC endpoint 创建失败 (优先级: {}, 地址: {}): {}",
                    priority, config.server_url, e
                );
                return Err(e);
            }
        };
        
        // 现在开始计时（只测量网络连接建立时间）
        let start_time = Instant::now();
        
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
                    priority, elapsed, config.server_url, e
                );
                return Err(e);
            }
        };
        
        // 仅建立网络连接，不发送 CONNECT
        let _connection_arc = client.establish_network_connection().await?;
        let elapsed = start_time.elapsed();
        
        tracing::debug!(
            "QUIC 网络连接建立成功 (优先级: {}, 耗时: {:?})",
            priority, elapsed
        );
        Ok((client, elapsed))
    }
    
    /// 连接 WebSocket 协议（完整流程：建立连接 + 发送 CONNECT）
    async fn connect_websocket(
        config: ClientConfig,
        core: ClientCore,
        priority: usize,
    ) -> ConnectionResult {
        let start_time = Instant::now();
        let mut client = WebSocketClient::with_core(config, core);
        
        match client.connect().await {
            Ok(_) => {
                let elapsed = start_time.elapsed();
                tracing::debug!(
                    "WebSocket 连接成功 (优先级: {}, 耗时: {:?})",
                    priority, elapsed
                );
                Ok(Box::new(client))
            }
            Err(e) => {
                let elapsed = start_time.elapsed();
                tracing::warn!(
                    "WebSocket 连接失败 (优先级: {}, 耗时: {:?}): {}",
                    priority, elapsed, e
                );
                Err(e)
            }
        }
    }
    
    /// 连接 QUIC 协议（完整流程：建立连接 + 发送 CONNECT）
    async fn connect_quic(
        config: ClientConfig,
        core: ClientCore,
        priority: usize,
    ) -> ConnectionResult {
        let start_time = Instant::now();
        
        let mut client = match QUICClient::with_core(config.clone(), core) {
            Ok(client) => client,
            Err(e) => {
                let elapsed = start_time.elapsed();
                tracing::warn!(
                    "QUIC 客户端创建失败 (优先级: {}, 耗时: {:?}, 地址: {}): {}",
                    priority, elapsed, config.server_url, e
                );
                return Err(e);
            }
        };
        
        match client.connect().await {
            Ok(_) => {
                let elapsed = start_time.elapsed();
                tracing::debug!(
                    "QUIC 连接成功 (优先级: {}, 耗时: {:?})",
                    priority, elapsed
                );
                Ok(Box::new(client))
            }
            Err(e) => {
                let elapsed = start_time.elapsed();
                tracing::warn!(
                    "QUIC 连接失败 (优先级: {}, 耗时: {:?}, 地址: {}): {}",
                    priority, elapsed, config.server_url, e
                );
                Err(e)
            }
        }
    }
    
    /// 等待所有连接任务完成，收集结果
    async fn wait_for_connections(
        handles: Vec<tokio::task::JoinHandle<ConnectionTaskResult>>,
    ) -> (
        Option<SuccessfulConnection>,
        Vec<SuccessfulConnection>,
        Vec<FailedConnection>,
    ) {
        const TIME_THRESHOLD: Duration = Duration::from_millis(100);
        
        let mut remaining_handles = handles;
        let mut first_success: Option<SuccessfulConnection> = None;
        let mut successful_clients = Vec::new();
        let mut errors = Vec::new();
        let total_protocols = remaining_handles.len();
        
        tracing::debug!("开始协议竞速，共 {} 个协议", total_protocols);
        
        while !remaining_handles.is_empty() {
            let (result, _index, remaining) = select_all(remaining_handles).await;
            remaining_handles = remaining;
            
            match result {
                Ok((protocol, protocol_index, client_result, elapsed)) => {
                    match client_result {
                        Ok(client) => {
                            if first_success.is_none() {
                                // 第一个成功的连接，立即返回
                                first_success = Some((protocol_index, protocol, client, elapsed));
                                let ms = elapsed.as_secs_f64() * 1000.0;
                                tracing::info!(
                                    "🏆 第一个成功的连接: {:?} (优先级: {}, 耗时: {:.3}ms)",
                                    protocol, protocol_index, ms
                                );
                                tracing::debug!("第一个连接成功，立即返回，不再等待其他连接");
                                break;
                            } else {
                                // 检查是否在时间阈值内（几乎同时成功）
                                let first_elapsed = first_success.as_ref().unwrap().3;
                                if elapsed <= first_elapsed + TIME_THRESHOLD {
                                    successful_clients.push((protocol_index, protocol, client, elapsed));
                                    let ms = elapsed.as_secs_f64() * 1000.0;
                                    tracing::debug!(
                                        "⚡ 几乎同时成功的连接: {:?} (优先级: {}, 耗时: {:.3}ms)",
                                        protocol, protocol_index, ms
                                    );
                                } else {
                                    // 连接太慢，关闭它
                                    let ms = elapsed.as_secs_f64() * 1000.0;
                                    tracing::debug!(
                                        "🐌 连接太慢，关闭: {:?} (优先级: {}, 耗时: {:.3}ms)",
                                        protocol, protocol_index, ms
                                    );
                                    Self::disconnect_client_async(protocol, protocol_index, elapsed);
                                }
                            }
                        }
                        Err(e) => {
                            let error_msg = e.to_string();
                            errors.push((protocol_index, protocol, e, elapsed));
                            let ms = elapsed.as_secs_f64() * 1000.0;
                            tracing::warn!(
                                "连接失败: {:?} (优先级: {}, 耗时: {:.3}ms): {}",
                                protocol, protocol_index, ms, error_msg
                            );
                        }
                    }
                }
                Err(join_err) => {
                    tracing::error!("Task join error: {:?}", join_err);
                }
            }
        }
        
        (first_success, successful_clients, errors)
    }
    
    /// 异步关闭客户端（不阻塞）
    fn disconnect_client_async(
        protocol: TransportProtocol,
        priority: usize,
        elapsed: Duration,
    ) {
        tracing::debug!(
            "🐌 连接太慢，关闭: {:?} (优先级: {}, 耗时: {:?})",
            protocol, priority, elapsed
        );
        // 注意：这里无法获取 client，因为它在 Result 中
        // 实际关闭会在 process_race_results 中处理
    }
    
    /// 处理竞速结果，选择最佳连接，然后发送 CONNECT 消息
    async fn process_race_results(
        first_success: Option<SuccessfulConnection>,
        mut successful_clients: Vec<SuccessfulConnection>,
        errors: Vec<FailedConnection>,
        shared_core: ClientCore,
        race_start: Instant,
    ) -> Result<(Box<dyn Client>, TransportProtocol)> {
        // 打印所有协议的耗时信息
        Self::log_protocol_timings(&first_success, &successful_clients, &errors);
        
        if let Some((first_index, first_protocol, first_client, first_elapsed)) = first_success {
            // 合并所有成功的连接，选择优先级最高的
            successful_clients.push((first_index, first_protocol, first_client, first_elapsed));
            successful_clients.sort_by_key(|(index, _protocol, _client, _elapsed)| *index);
            
            // 选择优先级最高的（index 最小的）
            let (selected_index, selected_protocol, mut selected_client, selected_elapsed) = 
                successful_clients.remove(0);
            
            let selected_ms = selected_elapsed.as_secs_f64() * 1000.0;
            let total_ms = race_start.elapsed().as_secs_f64() * 1000.0;
            tracing::info!(
                "✅ 协议竞速成功: {:?} (优先级: {}, 网络连接耗时: {:.3}ms, 总竞速时间: {:.3}ms)",
                selected_protocol, selected_index, selected_ms, total_ms
            );
            
            // 关闭其他未选中的连接
            Self::disconnect_unselected_clients(successful_clients);
            
            // 现在发送 CONNECT 消息（网络连接已建立）
            tracing::debug!(
                "📤 发送 CONNECT 消息: {:?} (优先级: {})",
                selected_protocol, selected_index
            );
            selected_client.connect().await?;
            
            // 同步连接状态到共享的 core
            shared_core.state_manager.set_connected();
            
            Ok((selected_client, selected_protocol))
        } else {
            // 所有协议都失败了
            Self::build_all_failed_error(errors)
        }
    }
    
    /// 打印所有协议的耗时信息
    fn log_protocol_timings(
        first_success: &Option<SuccessfulConnection>,
        successful_clients: &[SuccessfulConnection],
        errors: &[FailedConnection],
    ) {
        tracing::info!("📊 协议竞速耗时统计：");
        
        // 收集所有协议的结果（成功 + 失败）
        let mut all_results: Vec<(usize, TransportProtocol, Option<Duration>, Option<String>)> = Vec::new();
        
        // 添加成功的连接
        if let Some((index, protocol, _, elapsed)) = first_success {
            all_results.push((*index, *protocol, Some(*elapsed), None));
        }
        
        for (index, protocol, _, elapsed) in successful_clients {
            all_results.push((*index, *protocol, Some(*elapsed), None));
        }
        
        // 添加失败的连接
        for (index, protocol, error, elapsed) in errors {
            all_results.push((*index, *protocol, Some(*elapsed), Some(error.to_string())));
        }
        
        // 按优先级排序
        all_results.sort_by_key(|(index, _, _, _)| *index);
        
        // 打印每个协议的耗时
        for (index, protocol, elapsed_opt, error_opt) in all_results {
            let protocol_name = match protocol {
                TransportProtocol::WebSocket => "WebSocket",
                TransportProtocol::QUIC => "QUIC",
                TransportProtocol::TCP => "TCP",
            };
            
            match (elapsed_opt, error_opt) {
                (Some(elapsed), None) => {
                    let ms = elapsed.as_secs_f64() * 1000.0;
                    // 检查是否是第一个成功的（会被选中）
                    let is_first = first_success.as_ref()
                        .map(|(idx, _, _, _)| *idx == index)
                        .unwrap_or(false);
                    if is_first {
                        tracing::info!(
                            "  [{:2}] {:12} ✅ 成功 - {:.3}ms ⭐ (已选中)",
                            index, protocol_name, ms
                        );
                    } else {
                        tracing::info!(
                            "  [{:2}] {:12} ✅ 成功 - {:.3}ms",
                            index, protocol_name, ms
                        );
                    }
                }
                (Some(elapsed), Some(err)) => {
                    let ms = elapsed.as_secs_f64() * 1000.0;
                    // 截断错误信息，避免日志过长
                    let err_short = if err.len() > 50 {
                        format!("{}...", &err[..50])
                    } else {
                        err
                    };
                    tracing::info!(
                        "  [{:2}] {:12} ❌ 失败 - {:.3}ms - {}",
                        index, protocol_name, ms, err_short
                    );
                }
                _ => {}
            }
        }
    }
    
    /// 关闭未选中的连接
    fn disconnect_unselected_clients(clients: Vec<SuccessfulConnection>) {
        if clients.is_empty() {
            return;
        }
        
        tracing::info!("正在关闭 {} 个未选中的连接...", clients.len());
        
        for (index, protocol, mut client, elapsed) in clients {
            tracing::debug!(
                "关闭未选中的连接: {:?} (优先级: {}, 耗时: {:?})",
                protocol, index, elapsed
            );
            
            // 异步关闭，不阻塞
            tokio::spawn(async move {
                if let Err(e) = client.disconnect().await {
                    tracing::warn!("关闭 {:?} 连接时出错: {}", protocol, e);
                } else {
                    tracing::debug!("✅ {:?} 连接已关闭", protocol);
                }
            });
        }
        
        tracing::info!("所有未选中的连接已关闭");
    }
    
    /// 构建所有协议都失败的错误信息
    fn build_all_failed_error(errors: Vec<FailedConnection>) -> Result<(Box<dyn Client>, TransportProtocol)> {
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
        
        tracing::error!("❌ {}", error_msg);
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
    
    /// 解析基础 URL，提取主机和端口
    #[allow(dead_code)] // 保留用于未来扩展
    fn parse_base_url(url: &str) -> (String, u16) {
        let url = url
            .trim_start_matches("ws://")
            .trim_start_matches("wss://")
            .trim_start_matches("quic://")
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        
        if let Some(colon_pos) = url.rfind(':') {
            let host = url[..colon_pos].to_string();
            if let Ok(port) = url[colon_pos + 1..].parse::<u16>() {
                return (host, port);
            }
        }
        
        (url.to_string(), 8080)
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
        // 优先使用 try_lock 避免阻塞，如果无法立即获取锁则使用 block_in_place
        // 这样可以避免在异步运行时中直接使用 blocking_lock 导致的 panic
        match self.inner.try_lock() {
            Ok(client) => client.is_connected(),
            Err(_) => {
                // 如果无法立即获取锁，使用 block_in_place 在专用线程中执行
                // 这会将阻塞操作移到专用线程，避免阻塞 Tokio 运行时
                tokio::task::block_in_place(|| {
                    let client = self.inner.blocking_lock();
                    client.is_connected()
                })
            }
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
        
        let result = tokio::time::timeout(
            race_timeout,
            Self::race_connect(config, core.clone())
        )
        .await
        .map_err(|_| FlareError::connection_failed(
            format!("Protocol race timed out after {:?}", race_timeout)
        ))?;
        
        let (client, protocol) = result?;
        
        Ok(Self {
            inner: Arc::new(Mutex::new(client)),
            active_protocol: protocol,
            core,
        })
    }
}
