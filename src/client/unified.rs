//! 统一客户端接口
//! 
//! 支持单个协议或多协议竞速

use crate::common::client_trait::Client;
use crate::common::config::{ClientConfig, TransportProtocol};
use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::transport::events::ArcObserver;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;

use super::websocket::WebSocketClient;
use super::quic::QUICClient;

/// 统一客户端
/// 
/// 支持单个协议连接或多协议竞速
pub struct UnifiedClient {
    /// 内部客户端（根据配置动态选择）
    inner: Arc<Mutex<Box<dyn Client>>>,
    /// 使用的协议
    active_protocol: TransportProtocol,
}

impl UnifiedClient {
    /// 创建新的统一客户端
    /// 
    /// # 参数
    /// - `config`: 客户端配置
    /// 
    /// # 返回
    /// 统一客户端实例
    pub fn new(config: ClientConfig) -> Result<Self> {
        let protocols = config.get_protocols();
        
        if protocols.len() == 1 {
            // 单个协议模式
            Self::create_single_protocol(config, protocols[0])
        } else {
            // 多协议竞速模式
            // 返回一个占位符，实际连接在 connect 时完成
            // 这里先创建一个默认的，实际会在 connect 时选择最快的
            let default_config = ClientConfig {
                server_url: config.server_url.clone(),
                transport: protocols[0],
                ..config.clone()
            };
            let client: Box<dyn Client> = match protocols[0] {
                TransportProtocol::WebSocket => Box::new(WebSocketClient::new(default_config)),
                TransportProtocol::QUIC => Box::new(QUICClient::new(default_config)?),
                TransportProtocol::TCP => {
                    return Err(crate::common::error::FlareError::protocol_error(
                        "TCP transport not yet implemented".to_string()
                    ));
                }
            };
            
            Ok(Self {
                inner: Arc::new(Mutex::new(client)),
                active_protocol: protocols[0],
            })
        }
    }
    
    /// 创建单个协议客户端
    fn create_single_protocol(config: ClientConfig, protocol: TransportProtocol) -> Result<Self> {
        let mut single_config = config.clone();
        single_config.transport = protocol;
        single_config.transports = None;
        
        let client: Box<dyn Client> = match protocol {
            TransportProtocol::WebSocket => Box::new(WebSocketClient::new(single_config)),
            TransportProtocol::QUIC => Box::new(QUICClient::new(single_config)?),
            TransportProtocol::TCP => {
                return Err(crate::common::error::FlareError::protocol_error(
                    "TCP transport not yet implemented".to_string()
                ));
            }
        };
        
        Ok(Self {
            inner: Arc::new(Mutex::new(client)),
            active_protocol: protocol,
        })
    }
    
    /// 协议竞速连接
    /// 
    /// 同时尝试多个协议，选择最先成功的
    async fn race_connect(config: ClientConfig) -> Result<(Box<dyn Client>, TransportProtocol)> {
        let protocols = config.get_protocols();
        
        // 为每个协议创建连接任务
        let mut handles = Vec::new();
        for protocol in protocols.clone() {
            let protocol_config = ClientConfig {
                server_url: config.server_url.clone(),
                transport: protocol,
                transports: None,
                ..config.clone()
            };
            
            let handle = tokio::spawn(async move {
                let client_result: Result<Box<dyn Client>> = match protocol {
                    TransportProtocol::WebSocket => {
                        let mut client = WebSocketClient::new(protocol_config);
                        match client.connect().await {
                            Ok(_) => Ok(Box::new(client)),
                            Err(e) => Err(e),
                        }
                    }
                    TransportProtocol::QUIC => {
                        match QUICClient::new(protocol_config) {
                            Ok(mut client) => {
                                match client.connect().await {
                                    Ok(_) => Ok(Box::new(client)),
                                    Err(e) => Err(e),
                                }
                            }
                            Err(e) => Err(e),
                        }
                    }
                    TransportProtocol::TCP => {
                        Err(crate::common::error::FlareError::protocol_error(
                            "TCP transport not yet implemented".to_string()
                        ))
                    }
                };
                
                (protocol, client_result)
            });
            
            handles.push(handle);
        }
        
        // 使用 futures_util::future::select_all 等待第一个完成的连接
        use futures_util::future::select_all;
        use std::pin::Pin;
        
        // 将所有 handles 转换为 futures
        // handle.await 返回 std::result::Result<(TransportProtocol, Result<Box<dyn Client>, FlareError>), JoinError>
        type RaceResult = (TransportProtocol, Result<Box<dyn Client>>);
        
        let mut race_futures: Vec<Pin<Box<dyn std::future::Future<Output = std::result::Result<RaceResult, tokio::task::JoinError>> + Send>>> = handles
            .into_iter()
            .map(|handle| {
                Box::pin(async move { handle.await }) as Pin<Box<dyn std::future::Future<Output = std::result::Result<RaceResult, tokio::task::JoinError>> + Send>>
            })
            .collect();
        
        // 使用 select_all 循环等待第一个成功的连接
        let mut errors = Vec::new();
        
        while !race_futures.is_empty() {
            let (result, _index, remaining) = select_all(race_futures).await;
            race_futures = remaining;
            
            match result {
                Ok((protocol, Ok(client))) => {
                    // 成功！取消其他任务（通过不再等待它们）
                    return Ok((client, protocol));
                }
                Ok((protocol, Err(e))) => {
                    errors.push((protocol, e));
                    // 继续等待其他协议
                }
                Err(join_err) => {
                    eprintln!("Task join error: {:?}", join_err);
                    // 继续等待其他协议
                }
            }
        }
        
        // 所有协议都失败了
        Err(crate::common::error::FlareError::connection_failed(
            format!("All protocol connections failed: {:?}", errors)
        ))
    }
    
    /// 获取当前使用的协议
    pub fn active_protocol(&self) -> TransportProtocol {
        self.active_protocol
    }
}

#[async_trait]
impl Client for UnifiedClient {
    async fn connect(&mut self) -> Result<()> {
        // 如果客户端还未连接，则尝试连接
        // 对于单协议模式，直接连接
        // 对于竞速模式，应该使用 connect_with_race
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
        // 由于 inner 是 Arc<Mutex<>>，我们需要异步访问
        // 这里返回一个保守的值，或者使用阻塞锁
        // 暂时使用 blocking_lock
        let client = self.inner.blocking_lock();
        client.is_connected()
    }
    
    fn add_observer(&mut self, observer: ArcObserver) {
        // 注意：blocking_lock 在异步上下文可能会导致 panic
        // 使用 try_lock 并丢弃错误，这在示例中使用应该是安全的
        if let Ok(mut client) = self.inner.try_lock() {
            client.add_observer(observer);
        }
    }
    
    fn remove_observer(&mut self, observer: ArcObserver) {
        if let Ok(mut client) = self.inner.try_lock() {
            client.remove_observer(observer);
        }
    }
    
    fn connection_id(&self) -> Option<String> {
        if let Ok(client) = self.inner.try_lock() {
            client.connection_id()
        } else {
            None
        }
    }
}

/// 创建统一客户端的便捷函数
impl UnifiedClient {
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
        
        let race_timeout = config.race_timeout.unwrap_or(Duration::from_secs(5));
        
        // 添加超时控制
        let result = tokio::time::timeout(
            race_timeout,
            Self::race_connect(config)
        )
        .await
        .map_err(|_| crate::common::error::FlareError::connection_failed(
            format!("Protocol race timed out after {:?}", race_timeout)
        ))?;
        
        let (client, protocol) = result?;
        
        Ok(Self {
            inner: Arc::new(Mutex::new(client)),
            active_protocol: protocol,
        })
    }
}
