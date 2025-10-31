//! 统一服务端接口
//! 
//! 支持单个协议或多协议同时监听

use crate::common::config::{ServerConfig, TransportProtocol};
use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::common::server_trait::{Server, ConnectionHandler};
use std::sync::Arc;
use tokio::sync::Mutex;

use super::websocket::WebSocketServer;
use super::quic::QUICServer;

/// 统一服务端
/// 
/// 支持单个协议或多协议同时监听
pub struct UnifiedServer {
    /// 内部服务器列表
    servers: Vec<Arc<Mutex<Box<dyn Server>>>>,
    /// 使用的协议列表
    protocols: Vec<TransportProtocol>,
    /// 是否正在运行
    is_running: Arc<tokio::sync::Mutex<bool>>,
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
        let protocols = config.get_protocols();
        let mut servers = Vec::new();
        
        for protocol in &protocols {
            let mut server_config = config.clone();
            server_config.transport = *protocol;
            server_config.transports = None;
            
            // 为不同协议调整地址（如果需要）
            // 例如：WebSocket 使用 :8080，QUIC 使用 :8081
            // 这里简化处理，使用相同地址
            
            let server: Box<dyn Server> = match protocol {
                TransportProtocol::WebSocket => {
                    Box::new(WebSocketServer::new(server_config, Arc::clone(&handler)))
                }
                TransportProtocol::QUIC => {
                    Box::new(QUICServer::new(server_config, Arc::clone(&handler))?)
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
            is_running: Arc::new(tokio::sync::Mutex::new(false)),
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
        *self.is_running.lock().await = true;
        
        // 启动所有服务器
        for server in &self.servers {
            let mut s = server.lock().await;
            if let Err(e) = s.start().await {
                eprintln!("Failed to start server: {:?}", e);
                // 继续启动其他服务器
            }
        }
        
        Ok(())
    }
    
    async fn stop(&mut self) -> Result<()> {
        *self.is_running.lock().await = false;
        
        // 停止所有服务器
        for server in &self.servers {
            let mut s = server.lock().await;
            if let Err(e) = s.stop().await {
                eprintln!("Failed to stop server: {:?}", e);
            }
        }
        
        Ok(())
    }
    
    async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        // 尝试在所有服务器上发送，找到包含该连接的服务器
        for server in &self.servers {
            let s = server.lock().await;
            if s.is_running() {
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
        let mut last_error = None;
        for server in &self.servers {
            let s = server.lock().await;
            if s.is_running() {
                if let Err(e) = s.send_to_user(user_id, frame).await {
                    last_error = Some(e);
                }
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
        // 在所有服务器上广播
        let mut last_error = None;
        for server in &self.servers {
            let s = server.lock().await;
            if s.is_running() {
                if let Err(e) = s.broadcast(frame).await {
                    last_error = Some(e);
                }
            }
        }
        
        if last_error.is_none() {
            Ok(())
        } else {
            Err(last_error.unwrap_or_else(|| crate::common::error::FlareError::protocol_error(
                "Failed to broadcast".to_string()
            )))
        }
    }
    
    fn is_running(&self) -> bool {
        *self.is_running.blocking_lock()
    }
    
    fn connection_count(&self) -> usize {
        // 返回所有服务器的连接总数
        self.servers.iter()
            .map(|s| {
                let server = s.blocking_lock();
                server.connection_count()
            })
            .sum()
    }
    
    fn user_count(&self) -> usize {
        // 返回所有服务器的用户总数（可能有重复，但简化处理）
        self.servers.iter()
            .map(|s| {
                let server = s.blocking_lock();
                server.user_count()
            })
            .sum()
    }
    
    async fn disconnect(&self, connection_id: &str) -> Result<()> {
        // 尝试在所有服务器上断开连接
        // 由于我们不知道连接在哪个服务器上，我们在所有服务器上尝试断开
        let mut last_error = None;
        for server in &self.servers {
            let s = server.lock().await;
            if s.is_running() {
                // 尝试断开，如果连接不存在会返回错误，但我们继续尝试其他服务器
                match s.disconnect(connection_id).await {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        // 记录错误但继续尝试
                        last_error = Some(e);
                    }
                }
            }
        }
        
        // 如果所有服务器都没有找到连接
        Err(last_error.unwrap_or_else(|| crate::common::error::FlareError::protocol_error(
            format!("Connection {} not found on any server", connection_id)
        )))
    }
}

