//! Flare 服务端核心模块
//!
//! 提供统一的QUIC和WebSocket服务端

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn, error};

use crate::common::{
    error::Result, protocol::ProtocolSelection,
};

use super::{
    config::ServerConfig,
    websocket_server::WebSocketServer,
    quic_server::QuicServer,
};

/// Flare 服务端
/// 提供统一的QUIC和WebSocket服务端
pub struct FlareIMServer {
    /// 配置
    config: ServerConfig,
    /// WebSocket服务器
    websocket_server: Option<WebSocketServer>,
    /// QUIC服务器
    quic_server: Option<QuicServer>,
    /// 运行状态
    running: Arc<RwLock<bool>>,
    /// 消息发送器
    message_sender: Option<tokio::sync::mpsc::Sender<crate::common::protocol::UnifiedProtocolMessage>>,
}

impl FlareIMServer {
    /// 创建新的 Flare 服务端
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config,
            websocket_server: None,
            quic_server: None,
            running: Arc::new(RwLock::new(false)),
            message_sender: None,
        }
    }
    
    /// 使用默认配置创建新的 Flare 服务端
    pub fn with_default_config(config: ServerConfig) -> Self {
        Self::new(config)
    }
    
    /// 设置消息发送器
    pub fn set_message_sender(&mut self, sender: tokio::sync::mpsc::Sender<crate::common::protocol::UnifiedProtocolMessage>) {
        self.message_sender = Some(sender);
    }

    /// 启动服务端
    pub async fn start(&mut self) -> Result<()> {
        {
            let mut running = self.running.write().await;
            if *running {
                return Ok(());
            }
            *running = true;
        }
        
        // 根据协议选择启动相应的服务器
        match self.config.protocol.selection {
            ProtocolSelection::WebSocketOnly => {
                info!("启动 WebSocket 服务器");
                let ws_server = WebSocketServer::new("127.0.0.1:4000".to_string(), false);
                // TODO: 实现消息发送器设置
                ws_server.start().await?;
                self.websocket_server = Some(ws_server);
            }
            ProtocolSelection::QuicOnly => {
                info!("启动 QUIC 服务器");
                let mut quic_server = QuicServer::new(self.config.clone());
                quic_server.start().await?;
                self.quic_server = Some(quic_server);
            }
            ProtocolSelection::Auto => {
                info!("启动 QUIC 和 WebSocket 服务器");
                
                // 启动WebSocket服务器
                let ws_server = WebSocketServer::new("127.0.0.1:4000".to_string(), false);
                // TODO: 实现消息发送器设置
                if let Err(e) = ws_server.start().await {
                    warn!("启动WebSocket服务器失败: {}", e);
                } else {
                    self.websocket_server = Some(ws_server);
                }
                
                // 启动QUIC服务器
                let mut quic_server = QuicServer::new(self.config.clone());
                if let Err(e) = quic_server.start().await {
                    warn!("启动QUIC服务器失败: {}", e);
                } else {
                    self.quic_server = Some(quic_server);
                }
                
                // 检查是否至少有一个服务器启动成功
                if self.websocket_server.is_none() && self.quic_server.is_none() {
                    return Err(crate::common::error::FlareError::InvalidConfiguration(
                        "无法启动任何服务器".to_string()
                    ));
                }
            }
        }
        
        info!("服务端启动成功");
        Ok(())
    }

    /// 停止服务端
    pub async fn stop(&mut self) -> Result<()> {
        {
            let mut running = self.running.write().await;
            if !*running {
                return Ok(());
            }
            *running = false;
        }
        
        // 停止所有服务器
        if let Some(_ws_server) = self.websocket_server.take() {
            // TODO: 实现 WebSocket 服务器停止
            warn!("WebSocket 服务器停止功能尚未实现");
        }
        
        if let Some(mut quic_server) = self.quic_server.take() {
            if let Err(e) = quic_server.stop().await {
                warn!("停止 QUIC 服务器时出错: {:?}", e);
            }
        }
        
        info!("服务端已停止");
        Ok(())
    }

    /// 检查服务端是否正在运行
    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }

    /// 获取配置
    pub fn config(&self) -> &ServerConfig {
        &self.config
    }
    
    /// 获取WebSocket服务器
    pub fn get_websocket_server(&self) -> Option<&WebSocketServer> {
        self.websocket_server.as_ref()
    }
    
    /// 获取QUIC服务器
    pub fn get_quic_server(&self) -> Option<&QuicServer> {
        self.quic_server.as_ref()
    }
    
    /// 获取当前连接数
    pub async fn get_total_connection_count(&self) -> usize {
        let mut total = 0;
        
        if let Some(_ws_server) = &self.websocket_server {
            // WebSocket服务器暂时没有连接计数方法
            // total += ws_server.get_connection_count().await;
        }
        
        if let Some(quic_server) = &self.quic_server {
            total += quic_server.get_connection_count().await;
        }
        
        total
    }
    
    /// 广播消息到所有连接
    pub async fn broadcast_message(&self, message: crate::common::protocol::UnifiedProtocolMessage) -> Result<()> {
        let mut success_count = 0;
        let mut error_count = 0;
        
        // 广播到WebSocket连接
        if let Some(_ws_server) = &self.websocket_server {
            // WebSocket服务器暂时没有广播方法
            // 这里可以添加WebSocket广播逻辑
        }
        
        // 广播到QUIC连接
        if let Some(quic_server) = &self.quic_server {
            match quic_server.broadcast_message(message.clone()).await {
                Ok(_) => success_count += 1,
                Err(e) => {
                    error!("QUIC广播失败: {}", e);
                    error_count += 1;
                }
            }
        }
        
        info!("广播完成: 成功 {} 个服务器, 失败 {} 个服务器", success_count, error_count);
        Ok(())
    }
} 

 