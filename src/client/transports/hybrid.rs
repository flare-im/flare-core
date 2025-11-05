//! 混合客户端接口
//! 
//! 支持单个协议或多协议竞速
//! 统一管理连接状态、心跳、消息路由等功能

use crate::client::transports::{Client, ClientCore};
use crate::client::config::ClientConfig;
use crate::common::config_types::TransportProtocol;
use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::transport::events::ArcObserver;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;

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

impl HybridClient {
    /// 创建新的混合客户端
    /// 
    /// # 参数
    /// - `config`: 客户端配置
    /// 
    /// # 返回
    /// 混合客户端实例
    pub fn new(config: ClientConfig) -> Result<Self> {
        // 创建共享的 ClientCore
        let core = ClientCore::new(&config);
        
        let protocols = config.get_protocols();
        
        if protocols.len() == 1 {
            // 单个协议模式
            Self::create_single_protocol(config, protocols[0], core)
        } else {
            // 多协议竞速模式
            // 返回一个占位符，实际连接在 connect_with_race 时完成
            let default_config = ClientConfig {
                server_url: config.server_url.clone(),
                transport: protocols[0],
                ..config.clone()
            };
            let client: Box<dyn Client> = match protocols[0] {
                TransportProtocol::WebSocket => Box::new(WebSocketClient::with_core(default_config, core.clone())),
                TransportProtocol::QUIC => Box::new(QUICClient::with_core(default_config, core.clone())?),
                TransportProtocol::TCP => {
                    return Err(crate::common::error::FlareError::protocol_error(
                        "TCP transport not yet implemented".to_string()
                    ));
                }
            };
            
            Ok(Self {
                inner: Arc::new(Mutex::new(client)),
                active_protocol: protocols[0],
                core,
            })
        }
    }
    
    /// 创建单个协议客户端
    fn create_single_protocol(config: ClientConfig, protocol: TransportProtocol, core: ClientCore) -> Result<Self> {
        let mut single_config = config.clone();
        single_config.transport = protocol;
        single_config.transports = None;
        
        let client: Box<dyn Client> = match protocol {
            TransportProtocol::WebSocket => Box::new(WebSocketClient::with_core(single_config, core.clone())),
            TransportProtocol::QUIC => Box::new(QUICClient::with_core(single_config, core.clone())?),
            TransportProtocol::TCP => {
                return Err(crate::common::error::FlareError::protocol_error(
                    "TCP transport not yet implemented".to_string()
                ));
            }
        };
        
        Ok(Self {
            inner: Arc::new(Mutex::new(client)),
            active_protocol: protocol,
            core,
        })
    }
    
        /// 协议竞速连接
    /// 
    /// 同时尝试多个协议，按照优先级顺序选择第一个成功的
    /// 协议列表的顺序就是优先级顺序
    async fn race_connect(config: ClientConfig, shared_core: ClientCore) -> Result<(Box<dyn Client>, TransportProtocol)> {
        let protocols = config.get_protocols();
        
        // 为每个协议创建连接任务
        let mut handles = Vec::new();
        for (index, protocol) in protocols.iter().enumerate() {
            // 使用配置的协议地址，如果没有配置则使用默认地址
            let protocol_url = config.get_protocol_url(protocol);
            tracing::debug!("协议竞速: [{}] {:?} 使用地址: {}", index, protocol, protocol_url);
            
            let protocol_config = ClientConfig {
                server_url: protocol_url.clone(),
                transport: *protocol,
                transports: None,
                ..config.clone()
            };
            
            // 每个协议使用独立的 core 副本
            // 关键：为协议竞速创建独立的 ConnectionStateManager，避免状态冲突
            // 但共享观察者，这样消息通知可以正常工作
            let mut protocol_core = ClientCore::new(&protocol_config);
            // 共享观察者（消息通知）
            protocol_core.observers = Arc::clone(&shared_core.observers);
            // 使用独立的状态管理器，避免多个协议并发连接时的状态冲突
            
            let protocol_clone = *protocol;
            let protocol_index = index;
            
            let handle = tokio::spawn(async move {
                tracing::debug!("开始连接协议: {:?} (优先级: {}, 地址: {})", protocol_clone, protocol_index, protocol_url);
                let client_result: Result<Box<dyn Client>> = match protocol_clone {
                    TransportProtocol::WebSocket => {
                        let mut client = WebSocketClient::with_core(protocol_config, protocol_core);
                        match client.connect().await {
                            Ok(_) => {
                                tracing::debug!("WebSocket 连接成功 (优先级: {})", protocol_index);
                                Ok(Box::new(client))
                            }
                            Err(e) => {
                                tracing::warn!("WebSocket 连接失败 (优先级: {}): {}", protocol_index, e);
                                Err(e)
                            }
                        }
                    }
                    TransportProtocol::QUIC => {
                        match QUICClient::with_core(protocol_config.clone(), protocol_core) {
                            Ok(mut client) => {
                                match client.connect().await {
                                    Ok(_) => {
                                        tracing::debug!("QUIC 连接成功 (优先级: {})", protocol_index);
                                        Ok(Box::new(client))
                                    }
                                    Err(e) => {
                                        tracing::warn!("QUIC 连接失败 (优先级: {}, 地址: {}): {}", protocol_index, protocol_config.server_url, e);
                                        Err(e)
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!("QUIC 客户端创建失败 (优先级: {}, 地址: {}): {}", protocol_index, protocol_config.server_url, e);
                                Err(e)
                            }
                        }
                    }
                    TransportProtocol::TCP => {
                        Err(crate::common::error::FlareError::protocol_error(
                            "TCP transport not yet implemented".to_string()
                        ))
                    }
                };
                
                (protocol_clone, protocol_index, client_result)
            });
            
            handles.push(handle);
        }
        
        // 按照优先级顺序检查成功的连接
        // 使用一个 Vec 来跟踪每个协议的结果，保持优先级顺序
        use futures_util::future::join_all;
        let all_results: Vec<std::result::Result<(TransportProtocol, usize, Result<Box<dyn Client>>), tokio::task::JoinError>> = join_all(handles).await;
        
        // 收集所有结果，按优先级顺序存储
        // 同时收集所有成功的连接，用于后续关闭未选中的连接
        let mut successful_clients: Vec<(usize, TransportProtocol, Box<dyn Client>)> = Vec::new();
        let mut errors: Vec<(usize, TransportProtocol, crate::common::error::FlareError)> = Vec::new();
        let mut result_summary: Vec<(usize, TransportProtocol, bool, Option<String>)> = Vec::new(); // 用于日志显示
        
        for result in all_results {
            match result {
                Ok((protocol, index, client_result)) => {
                    match client_result {
                        Ok(client) => {
                            // 保存成功的连接，用于后续选择或关闭
                            successful_clients.push((index, protocol, client));
                            result_summary.push((index, protocol, true, None));
                        }
                        Err(e) => {
                            let error_msg = e.to_string();
                            errors.push((index, protocol, e));
                            result_summary.push((index, protocol, false, Some(error_msg)));
                        }
                    }
                }
                Err(join_err) => {
                    eprintln!("Task join error: {:?}", join_err);
                }
            }
        }
        
        // 按照优先级顺序（索引顺序）排序结果
        successful_clients.sort_by_key(|(index, _protocol, _client)| *index);
        result_summary.sort_by_key(|(index, _protocol, _success, _error)| *index);
        errors.sort_by_key(|(index, _protocol, _error)| *index);
        
        // 记录所有协议的结果（用于调试）
        tracing::info!("协议竞速结果汇总（按优先级顺序）:");
        for (index, protocol, success, error_msg) in &result_summary {
            if *success {
                tracing::info!("  [{}] {:?}: ✅ 成功", index, protocol);
            } else {
                if let Some(msg) = error_msg {
                    tracing::warn!("  [{}] {:?}: ❌ 失败 - {}", index, protocol, msg);
                } else {
                    tracing::warn!("  [{}] {:?}: ❌ 失败", index, protocol);
                }
            }
        }
        
        // 策略：优先保留优先级高的，如果优先级高的失败，再保留连接速度最快的
        // 按照优先级顺序检查成功的连接
        if successful_clients.is_empty() {
        // 所有协议都失败了
            let error_details: Vec<String> = errors.iter()
                .map(|(index, protocol, e)| format!("[{}] {:?}: {}", index, protocol, e))
                .collect();
            let error_msg = format!("所有协议连接都失败（按优先级顺序）: {}", error_details.join(", "));
            tracing::error!("❌ {}", error_msg);
            return Err(crate::common::error::FlareError::connection_failed(error_msg));
        }
        
        // 选择优先级最高的（第一个）
        let (selected_index, selected_protocol, selected_client) = successful_clients.remove(0);
        
        tracing::info!("✅ 协议竞速成功: {:?} (优先级: {})", selected_protocol, selected_index);
        
        // 关闭其他所有成功的连接（未选中的）
        // 重要：必须关闭未选中的连接，避免多个连接同时运行导致消息重复接收
        if !successful_clients.is_empty() {
            tracing::info!("正在关闭 {} 个未选中的连接...", successful_clients.len());
            for (index, protocol, mut client) in successful_clients {
                tracing::debug!("关闭未选中的连接: {:?} (优先级: {})", protocol, index);
                if let Err(e) = client.disconnect().await {
                    tracing::warn!("关闭 {:?} 连接时出错: {}", protocol, e);
                } else {
                    tracing::debug!("✅ {:?} 连接已关闭", protocol);
                }
            }
            tracing::info!("所有未选中的连接已关闭");
        }
        
        // 同步连接状态到共享的 core（用于最终状态管理）
        shared_core.state_manager.set_connected();
        
        // 返回选中的客户端
        Ok((selected_client, selected_protocol))
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
        let client = self.inner.blocking_lock();
        client.is_connected()
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
        
        // 创建共享的 ClientCore
        let core = ClientCore::new(&config);
        
        let race_timeout = config.race_timeout.unwrap_or(Duration::from_secs(5));
        
        let result = tokio::time::timeout(
            race_timeout,
            Self::race_connect(config, core.clone())
        )
        .await
        .map_err(|_| crate::common::error::FlareError::connection_failed(
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

