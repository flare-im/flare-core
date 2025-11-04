//! 统一服务端接口
//! 
//! 支持单个协议或多协议同时监听

use crate::server::config::ServerConfig;
use crate::common::config_types::TransportProtocol;
use crate::common::error::Result;
use crate::common::protocol::Frame;
use super::{Server, ConnectionHandler};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;
use tracing::{debug, error};

use super::websocket::WebSocketServer;
use super::quic::QUICServer;
use crate::server::connection::ConnectionManager;

/// 统一服务端
/// 
/// 支持单个协议或多协议同时监听
pub struct UnifiedServer {
    /// 内部服务器列表
    servers: Vec<Arc<Mutex<Box<dyn Server>>>>,
    /// 使用的协议列表
    protocols: Vec<TransportProtocol>,
    /// 是否正在运行
    is_running: Arc<AtomicBool>,
}

impl UnifiedServer {
    /// 创建新的统一服务端
    /// 
    /// # 参数
    /// - `config`: 服务端配置
    /// - `handler`: 连接处理器
    /// 
    /// # 返回
    /// 统一服务端实例
    pub fn new(config: ServerConfig, handler: Arc<dyn ConnectionHandler>) -> Result<Self> {
        Self::with_connection_manager(config, handler, None)
    }
    
    /// 使用指定的连接管理器创建统一服务端
    /// 
    /// # 参数
    /// - `config`: 服务端配置
    /// - `handler`: 连接处理器
    /// - `connection_manager`: 可选的连接管理器，如果为 None，则每个协议创建独立的
    /// 
    /// # 返回
    /// 统一服务端实例
    pub fn with_connection_manager(
        config: ServerConfig,
        handler: Arc<dyn ConnectionHandler>,
        connection_manager: Option<Arc<crate::server::connection::ConnectionManager>>,
    ) -> Result<Self> {
        let protocols = config.get_protocols();
        let mut servers = Vec::new();
        
        for protocol in &protocols {
            let mut server_config = config.clone();
            server_config.transport = *protocol;
            server_config.transports = None;
            
            // 为不同协议调整地址（如果需要）
            // QUIC 使用不同的端口，避免与 WebSocket 冲突
            // 只有在多协议模式下才调整端口
            let bind_address = match protocol {
                TransportProtocol::QUIC if protocols.len() > 1 => {
                    // 如果配置的地址包含端口，为 QUIC 使用下一个端口号
                    // 例如：0.0.0.0:8080 -> 0.0.0.0:8081
                    if let Some(colon_pos) = config.bind_address.rfind(':') {
                        if let Ok(port) = config.bind_address[colon_pos + 1..].parse::<u16>() {
                            format!("{}:{}", &config.bind_address[..colon_pos], port + 1)
                        } else {
                            config.bind_address.clone()
                        }
                    } else {
                        config.bind_address.clone()
                    }
                }
                _ => config.bind_address.clone(),
            };
            server_config.bind_address = bind_address;
            
                        let server: Box<dyn Server> = match protocol {
                TransportProtocol::WebSocket => {
                    Box::new(WebSocketServer::with_connection_manager(
                        server_config,
                        Arc::clone(&handler),
                        connection_manager.clone(),
                    ))                                                                         
                }
                TransportProtocol::QUIC => {
                    Box::new(QUICServer::with_connection_manager(
                        server_config,
                        Arc::clone(&handler),
                        connection_manager.clone(),
                    )?)                                                                             
                }
                TransportProtocol::TCP => {
                    return Err(crate::common::error::FlareError::protocol_error(
                        "TCP transport not yet implemented".to_string()
                    ));
                }
            };
            
            servers.push(Arc::new(Mutex::new(server)));
        }
        
        Ok(Self {
            servers,
            protocols,
            is_running: Arc::new(AtomicBool::new(false)),
        })
    }
    
    /// 获取使用的协议列表
    pub fn protocols(&self) -> &[TransportProtocol] {
        &self.protocols
    }
}

#[async_trait::async_trait]
impl Server for UnifiedServer {
    async fn start(&mut self) -> Result<()> {
        let mut started_count = 0;
        let mut errors = Vec::new();
        
        // 启动所有服务器
        for server in &self.servers {
            let mut s = server.lock().await;
            match s.start().await {
                Ok(_) => {
                    started_count += 1;
                }
                Err(e) => {
                    error!("Failed to start server: {:?}", e);
                    errors.push(e);
                }
            }
        }
        
        // 如果所有服务器都启动失败，返回错误
        if started_count == 0 && !errors.is_empty() {
            self.is_running.store(false, Ordering::SeqCst);
            return Err(errors.remove(0));
        }
        
        // 如果至少有一个服务器启动成功，标记为运行状态
        if started_count > 0 {
            self.is_running.store(true, Ordering::SeqCst);
        }
        
        Ok(())
    }
    
    async fn stop(&mut self) -> Result<()> {
        self.is_running.store(false, Ordering::SeqCst);
        
        // 停止所有服务器
        for server in &self.servers {
            let mut s = server.lock().await;
            if let Err(e) = s.stop().await {
                error!("Failed to stop server: {:?}", e);
            }
        }
        
        Ok(())
    }
    
    async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        // 尝试在所有服务器上发送，找到包含该连接的服务器
        // 注意：不要在持有异步锁的情况下调用 is_running()
        for server in &self.servers {
            // 先检查是否运行（使用 block_in_place）
            let is_running = tokio::task::block_in_place(|| {
                let s = server.blocking_lock();
                s.is_running()
            });
            
            if is_running {
                let s = server.lock().await;
                if let Ok(_) = s.send_to(connection_id, frame).await {
                    return Ok(());
                }
            }
        }
        
        Err(crate::common::error::FlareError::protocol_error(
            format!("Connection {} not found on any server", connection_id)
        ))
    }
    
    async fn send_to_user(&self, user_id: &str, frame: &Frame) -> Result<()> {
        // 在所有服务器上发送
        // 注意：不要在持有异步锁的情况下调用 is_running()
        let server_refs: Vec<Arc<tokio::sync::Mutex<Box<dyn Server>>>> = {
            self.servers.iter()
                .filter_map(|server| {
                    let is_running = tokio::task::block_in_place(|| {
                        let s = server.blocking_lock();
                        s.is_running()
                    });
                    if is_running {
                        Some(Arc::clone(server))
                    } else {
                        None
                    }
                })
                .collect()
        };
        
        let mut last_error = None;
        for server in &server_refs {
            let s = server.lock().await;
            if let Err(e) = s.send_to_user(user_id, frame).await {
                last_error = Some(e);
            }
        }
        
        // 如果至少有一个服务器成功，就返回成功
        if last_error.is_none() {
            Ok(())
        } else {
            Err(last_error.unwrap_or_else(|| crate::common::error::FlareError::protocol_error(
                "Failed to send to user".to_string()
            )))
        }
    }
    
    async fn broadcast(&self, frame: &Frame) -> Result<()> {
        debug!("[DEBUG UnifiedServer] broadcast 开始");
        // 在所有服务器上广播
        // 注意：不要在持有异步锁的情况下调用 is_running()，因为它可能使用 block_in_place
        // 解决方案：先获取所有需要广播的服务器列表，然后释放锁，再广播
        debug!("[DEBUG UnifiedServer] broadcast: 检查服务器状态");
        let server_refs: Vec<Arc<tokio::sync::Mutex<Box<dyn Server>>>> = {
            debug!("[DEBUG UnifiedServer] broadcast: 开始迭代服务器");
            let refs: Vec<_> = self.servers.iter()
                .enumerate()
                .filter_map(|(idx, server)| {
                    debug!("[DEBUG UnifiedServer] broadcast: 检查服务器 {} (索引 {})", idx, idx);
                    // 使用 block_in_place 来安全地检查 is_running
                    debug!("[DEBUG UnifiedServer] broadcast: 调用 block_in_place 检查 is_running");
                    let is_running = tokio::task::block_in_place(|| {
                        debug!("[DEBUG UnifiedServer] broadcast: block_in_place 内部，获取 blocking_lock");
                        let s = server.blocking_lock();
                        debug!("[DEBUG UnifiedServer] broadcast: blocking_lock 已获取，调用 is_running");
                        let result = s.is_running();
                        debug!("[DEBUG UnifiedServer] broadcast: is_running 返回: {}", result);
                        result
                    });
                    debug!("[DEBUG UnifiedServer] broadcast: block_in_place 完成，is_running={}", is_running);
                    if is_running {
                        debug!("[DEBUG UnifiedServer] broadcast: 服务器 {} 正在运行，添加到列表", idx);
                        Some(Arc::clone(server))
                    } else {
                        debug!("[DEBUG UnifiedServer] broadcast: 服务器 {} 未运行，跳过", idx);
                        None
                    }
                })
                .collect();
            debug!("[DEBUG UnifiedServer] broadcast: 找到 {} 个运行的服务器", refs.len());
            refs
        };
        
        // 现在释放锁，对所有服务器进行广播
        debug!("[DEBUG UnifiedServer] broadcast: 开始对 {} 个服务器进行广播", server_refs.len());
        let mut last_error = None;
        for (idx, server) in server_refs.iter().enumerate() {
            debug!("[DEBUG UnifiedServer] broadcast: 广播到服务器 {} (索引 {})", idx, idx);
            let s = server.lock().await;
            debug!("[DEBUG UnifiedServer] broadcast: 服务器 {} 锁已获取", idx);
            if let Err(e) = s.broadcast(frame).await {
                error!("[DEBUG UnifiedServer] broadcast: 服务器 {} 广播失败: {:?}", idx, e);
                last_error = Some(e);
            } else {
                debug!("[DEBUG UnifiedServer] broadcast: 服务器 {} 广播成功", idx);
            }
        }
        
        debug!("[DEBUG UnifiedServer] broadcast 完成");
        if last_error.is_none() {
            Ok(())
        } else {
            Err(last_error.unwrap_or_else(|| crate::common::error::FlareError::protocol_error(
                "Failed to broadcast".to_string()
            )))
        }
    }
    
    async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        debug!("[DEBUG UnifiedServer] broadcast_except 开始: exclude={}", exclude_connection_id);
        // 在所有服务器上广播，排除指定连接
        let server_refs: Vec<Arc<tokio::sync::Mutex<Box<dyn Server>>>> = {
            self.servers.iter()
                .filter_map(|server| {
                    let is_running = tokio::task::block_in_place(|| {
                        let s = server.blocking_lock();
                        s.is_running()
                    });
                    if is_running {
                        Some(Arc::clone(server))
                    } else {
                        None
                    }
                })
                .collect()
        };
        
        debug!("[DEBUG UnifiedServer] broadcast_except: 找到 {} 个运行的服务器", server_refs.len());
        let mut last_error = None;
        for (idx, server) in server_refs.iter().enumerate() {
            debug!("[DEBUG UnifiedServer] broadcast_except: 广播到服务器 {} (排除 {})", idx, exclude_connection_id);
            let s = server.lock().await;
            if let Err(e) = s.broadcast_except(frame, exclude_connection_id).await {
                error!("[DEBUG UnifiedServer] broadcast_except: 服务器 {} 广播失败: {:?}", idx, e);
                last_error = Some(e);
            } else {
                debug!("[DEBUG UnifiedServer] broadcast_except: 服务器 {} 广播成功", idx);
            }
        }
        
        debug!("[DEBUG UnifiedServer] broadcast_except 完成");
        if last_error.is_none() {
            Ok(())
        } else {
            Err(last_error.unwrap_or_else(|| crate::common::error::FlareError::protocol_error(
                "Failed to broadcast_except".to_string()
            )))
        }
    }
    
    fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }
    
    fn connection_count(&self) -> usize {
        // 返回所有服务器的连接总数
        // 注意：此方法可能从异步上下文中调用，使用 block_in_place 避免阻塞运行时
        tokio::task::block_in_place(|| {
            self.servers.iter()
                .map(|s| {
                    let server = s.blocking_lock();
                    server.connection_count()
                })
                .sum()
        })
    }
    
    fn user_count(&self) -> usize {
        // 返回所有服务器的用户总数（可能有重复，但简化处理）
        tokio::task::block_in_place(|| {
            self.servers.iter()
                .map(|s| {
                    let server = s.blocking_lock();
                    server.user_count()
                })
                .sum()
        })
    }
    
    async fn disconnect(&self, connection_id: &str) -> Result<()> {
        // 尝试在所有服务器上断开连接
        // 由于我们不知道连接在哪个服务器上，我们在所有服务器上尝试断开
        // 注意：不要在持有异步锁的情况下调用 is_running()
        let server_refs: Vec<Arc<tokio::sync::Mutex<Box<dyn Server>>>> = {
            self.servers.iter()
                .filter_map(|server| {
                    let is_running = tokio::task::block_in_place(|| {
                        let s = server.blocking_lock();
                        s.is_running()
                    });
                    if is_running {
                        Some(Arc::clone(server))
                    } else {
                        None
                    }
                })
                .collect()
        };
        
        let mut last_error = None;
        for server in &server_refs {
            let s = server.lock().await;
            // 尝试断开，如果连接不存在会返回错误，但我们继续尝试其他服务器
            match s.disconnect(connection_id).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    // 记录错误但继续尝试
                    last_error = Some(e);
                }
            }
        }
        
        // 如果所有服务器都没有找到连接
        Err(last_error.unwrap_or_else(|| crate::common::error::FlareError::protocol_error(
            format!("Connection {} not found on any server", connection_id)
        )))
    }
}

