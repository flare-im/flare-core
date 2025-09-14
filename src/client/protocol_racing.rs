//! 协议竞速模块
//! 
//! 实现客户端协议竞速功能，支持同时尝试多种协议并选择最优的连接

use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use crate::common::{
    error::Result,
    connections::{
        types::{ConnectionConfig, Transport},
        traits::{ClientConnection, ConnectionFactory as ConnectionFactoryTrait},
        factory::ConnectionFactory,
    },
};

/// 协议竞速结果
pub struct RacingResult {
    /// 获胜的连接
    pub connection: Box<dyn ClientConnection>,
    /// 获胜的传输类型
    pub protocol_type: Transport,
    /// 连接建立耗时（毫秒）
    pub connect_time_ms: u64,
}

// 手动实现Debug trait，因为dyn ClientConnection不实现Debug
impl std::fmt::Debug for RacingResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RacingResult")
            .field("protocol_type", &self.protocol_type)
            .field("connect_time_ms", &self.connect_time_ms)
            .finish()
    }
}

/// 协议竞速器
pub struct ProtocolRacer {
    /// 连接工厂
    factory: ConnectionFactory,
    /// 竞速超时时间（毫秒）
    timeout_ms: u64,
}

impl ProtocolRacer {
    /// 创建新的协议竞速器
    pub fn new(timeout_ms: u64) -> Self {
        Self {
            factory: ConnectionFactory::new(),
            timeout_ms,
        }
    }

    /// 执行协议竞速
    /// 
    /// # 参数
    /// * `base_config` - 基础连接配置
    /// * `protocol_addresses` - 协议地址映射
    /// * `protocols` - 要竞速的协议列表
    /// 
    /// # 返回值
    /// 返回竞速结果或错误
    pub async fn race(
        &self,
        base_config: ConnectionConfig,
        protocol_addresses: std::collections::HashMap<Transport, String>,
        protocols: Vec<Transport>,
    ) -> Result<RacingResult> {
        info!("开始协议竞速，协议数量: {}", protocols.len());
        
        // 为每种协议创建配置
        let mut protocol_configs = Vec::new();
        
        for protocol in &protocols {
            if let Some(address) = protocol_addresses.get(protocol) {
                let mut config = base_config.clone();
                config.transport = *protocol;
                config.remote_addr = address.clone();
                
                protocol_configs.push((config, *protocol));
            } else {
                warn!("未找到协议 {:?} 的地址配置，跳过该协议", protocol);
            }
        }
        
        // 同时启动所有协议的连接尝试
        let mut handles = Vec::new();
        let start_time = Instant::now();
        
        for (config, protocol_type) in protocol_configs {
            let factory = self.factory.clone_box();
            let handle = tokio::spawn(async move {
                let connect_start = Instant::now();
                match factory.create_client_connection(config).await {
                    Ok(connection) => {
                        match connection.connect().await {
                            Ok(_) => {
                                let connect_time = connect_start.elapsed().as_millis() as u64;
                                debug!("协议 {:?} 连接成功，耗时: {}ms", protocol_type, connect_time);
                                Ok((connection, protocol_type, connect_time))
                            }
                            Err(e) => {
                                warn!("协议 {:?} 连接失败: {}", protocol_type, e);
                                Err(e)
                            }
                        }
                    }
                    Err(e) => {
                        warn!("协议 {:?} 创建连接失败: {}", protocol_type, e);
                        Err(e)
                    }
                }
            });
            handles.push(handle);
        }
        
        // 等待第一个成功的连接或全部失败
        let timeout_duration = Duration::from_millis(self.timeout_ms);
        
        // 使用 tokio::select 等待第一个完成的连接
        let mut successful_connections = Vec::new();
        let mut failed_count = 0;
        let total_count = handles.len();
        
        // 等待所有任务完成或超时
        let results = tokio::time::timeout(timeout_duration, async {
            let mut results = Vec::new();
            for handle in handles {
                results.push(handle.await);
            }
            results
        }).await;
        
        match results {
            Ok(task_results) => {
                for task_result in task_results {
                    match task_result {
                        Ok(Ok((connection, protocol_type, connect_time))) => {
                            // 如果是QUIC协议，优先选择
                            if protocol_type == Transport::Quic {
                                info!("QUIC协议获胜，优先选择");
                                return Ok(RacingResult {
                                    connection,
                                    protocol_type,
                                    connect_time_ms: connect_time,
                                });
                            }
                            // 否则添加到成功列表中
                            successful_connections.push((connection, protocol_type, connect_time));
                        }
                        Ok(Err(_)) => {
                            failed_count += 1;
                        }
                        Err(_) => {
                            failed_count += 1;
                        }
                    }
                }
            }
            Err(_) => {
                warn!("协议竞速超时");
            }
        }
        
        // 如果有成功的连接，选择最快的
        if !successful_connections.is_empty() {
            // 按连接时间排序，选择最快的
            let mut sorted_connections = successful_connections;
            sorted_connections.sort_by_key(|c| c.2);
            
            let fastest = sorted_connections.into_iter().next().unwrap();
            let total_time = start_time.elapsed().as_millis() as u64;
            
            info!("协议竞速完成，最快协议: {:?}, 连接时间: {}ms, 总耗时: {}ms", 
                  fastest.1, fastest.2, total_time);
            
            return Ok(RacingResult {
                connection: fastest.0,
                protocol_type: fastest.1,
                connect_time_ms: fastest.2,
            });
        }
        
        // 如果所有连接都失败了，返回错误
        Err(crate::common::error::FlareError::connection_failed(
            format!("所有协议连接尝试都失败了 (成功: 0, 失败: {}, 总计: {})", failed_count, total_count)
        ))
    }

    /// 简化的竞速方法，自动选择常见协议组合
    pub async fn race_auto(
        &self, 
        base_config: ConnectionConfig,
        protocol_addresses: std::collections::HashMap<Transport, String>
    ) -> Result<RacingResult> {
        // 优先级：QUIC > WebSocket
        let protocols = vec![Transport::Quic, Transport::WebSocket];
        self.race(base_config, protocol_addresses, protocols).await
    }
}