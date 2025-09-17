//! 连接工厂实现
//! 
//! 提供创建不同类型连接的工厂模式实现

use std::sync::Arc;

use crate::common::{error::Result, connections::{
    types::{ConnectionConfig, Transport, ConnectionRole},
    traits::{ClientConnection, ServerConnection, ConnectionFactory as ConnectionFactoryTrait, ConnectionEvent},
    quic::QuicConnection,
    websocket::WebSocketConnection,
    builder::ConnectionBuilder,
}, FrameSerializer};
use crate::Connection;

/// 连接工厂
pub struct ConnectionFactory;

impl ConnectionFactory {
    /// 创建新的连接工厂
    pub fn new() -> Self {
        Self
    }
    
    /// 从构建器创建连接（根据配置自动判断是客户端还是服务端）
    pub async fn create_from_builder(&self, builder: ConnectionBuilder) -> Result<Box<dyn Connection>> {
        let (config, custom_serializer) = builder.build();
        self.create(config, custom_serializer).await
    }

    /// 从配置创建连接（根据配置自动判断是客户端还是服务端）
    pub async fn create(&self, config: ConnectionConfig, custom_serializer: Option<Arc<Box<dyn FrameSerializer>>>) -> Result<Box<dyn Connection>> {
        match config.transport {
            Transport::Quic => {
                if let Some(serializer) = custom_serializer {
                    Ok(Box::new(QuicConnection::with_serializer(config, serializer)))
                } else {
                    Ok(Box::new(QuicConnection::new(config)))
                }
            }
            Transport::WebSocket => {
                if let Some(serializer) = custom_serializer {
                    Ok(Box::new(WebSocketConnection::with_serializer(config, serializer)))
                } else {
                    Ok(Box::new(WebSocketConnection::new(config)))
                }
            }
            Transport::Tcp | Transport::Udp => {
                Err(crate::common::error::FlareError::connection_failed(
                    format!("{:?} 传输暂未实现", config.transport)
                ))
            }
        }
    }
}

#[async_trait::async_trait]
impl ConnectionFactoryTrait for ConnectionFactory {
    async fn create_client_connection(&self, config: ConnectionConfig) -> Result<Box<dyn ClientConnection>> {
        // 确保是客户端配置
        if config.role != ConnectionRole::Client {
            return Err(crate::common::error::FlareError::connection_failed(
                "只能为客户端角色创建客户端连接"
            ));
        }

        match config.transport {
            Transport::Quic => {
                Ok(Box::new(QuicConnection::new(config)))
            }
            Transport::WebSocket => {
                Ok(Box::new(WebSocketConnection::new(config)))
            }
            Transport::Tcp | Transport::Udp => {
                Err(crate::common::error::FlareError::connection_failed(
                    format!("{:?} 传输暂未实现", config.transport)
                ))
            }
        }
    }
    
    async fn create_server_connection(&self, config: ConnectionConfig) -> Result<Box<dyn ServerConnection>> {
        // 确保是服务端配置
        if config.role != ConnectionRole::Server {
            return Err(crate::common::error::FlareError::connection_failed(
                "只能为服务端角色创建服务端连接"
            ));
        }
        
        match config.transport {
            Transport::Quic => {
                Ok(Box::new(QuicConnection::new(config)))
            }
            Transport::WebSocket => {
                Ok(Box::new(WebSocketConnection::new(config)))
            }
            Transport::Tcp | Transport::Udp => {
                Err(crate::common::error::FlareError::connection_failed(
                    format!("{:?} 传输暂未实现", config.transport)
                ))
            }
        }
    }

    fn supported_types(&self) -> Vec<Transport> {
        vec![Transport::WebSocket, Transport::Quic]
    }

    fn supports_config(&self, config: &ConnectionConfig) -> bool {
        config.validate().is_ok()
    }

    fn clone_box(&self) -> Box<dyn ConnectionFactoryTrait> {
        Box::new(Self::new())
    }
}

impl Default for ConnectionFactory {
    fn default() -> Self {
        Self::new()
    }
}

/// 原始连接处理器
/// 
/// 负责从原始网络连接创建服务端连接
pub struct RawConnectionHandler;

impl RawConnectionHandler {
    /// 从 WebSocket 原始连接创建服务端连接
    pub async fn from_websocket(
        tcp_stream: tokio::net::TcpStream,
        config: ConnectionConfig,
    ) -> Result<Box<dyn ServerConnection>> {
        use tokio_tungstenite::accept_async;
        
        // 接受 WebSocket 握手
        let ws_stream = accept_async(tcp_stream).await
            .map_err(|e| crate::common::error::FlareError::connection_failed(
                format!("WebSocket 握手失败: {}", e)
            ))?;
        
        // 创建 WebSocket 连接
        let mut connection = WebSocketConnection::new(config);
        
        // 设置 WebSocket 流到连接中
        connection.set_connection(ws_stream).await;
        
        // 启动消息处理任务
        connection.start_task().await
            .map_err(|e| crate::common::error::FlareError::connection_failed(
                format!("启动消息接收任务失败: {}", e)
            ))?;
        
        Ok(Box::new(connection))
    }
    
    /// 从 WebSocket 原始连接创建服务端连接，并设置事件处理器
    pub async fn from_websocket_with_handler(
        tcp_stream: tokio::net::TcpStream,
        config: ConnectionConfig,
        handler: Arc<dyn ConnectionEvent>,
    ) -> Result<Box<dyn ServerConnection>> {
        use tokio_tungstenite::accept_async;
        
        // 接受 WebSocket 握手
        let ws_stream = accept_async(tcp_stream).await
            .map_err(|e| crate::common::error::FlareError::connection_failed(
                format!("WebSocket 握手失败: {}", e)
            ))?;
        
        // 创建 WebSocket 连接
        let mut connection = WebSocketConnection::new(config);
        
        // 设置事件处理器
        connection.set_event_handler(handler).await;
        
        // 设置 WebSocket 流到连接中
        connection.set_connection(ws_stream).await;
        
        // 启动消息处理任务
        connection.start_task().await
            .map_err(|e| crate::common::error::FlareError::connection_failed(
                format!("启动消息接收任务失败: {}", e)
            ))?;
        
        Ok(Box::new(connection))
    }
    
    /// 从 WebSocket 原始连接创建服务端连接（使用Arc包装的事件处理器）
    pub async fn from_websocket_with_handler_arc(
        tcp_stream: tokio::net::TcpStream,
        config: ConnectionConfig,
        handler: Arc<dyn ConnectionEvent>,
    ) -> Result<Arc<dyn ServerConnection>> {
        use tokio_tungstenite::accept_async;
        
        // 接受 WebSocket 握手
        let ws_stream = accept_async(tcp_stream).await
            .map_err(|e| crate::common::error::FlareError::connection_failed(
                format!("WebSocket 握手失败: {}", e)
            ))?;
        
        // 创建 WebSocket 连接
        let mut connection = WebSocketConnection::new(config);
        
        // 设置事件处理器
        connection.set_event_handler(handler).await;
        
        // 设置 WebSocket 流到连接中
        connection.set_connection(ws_stream).await;
        
        // 注意：不在这里启动任务，而是在accept方法中启动
        // 这样可以确保任务在正确的时机启动
        
        Ok(Arc::new(connection))
    }
    
    /// 从 QUIC 原始连接创建服务端连接
    pub async fn from_quic(
        quic_connection: quinn::Connection,
        config: ConnectionConfig,
    ) -> Result<Box<dyn ServerConnection>> {
        // 创建 QUIC 连接
        let mut connection = QuicConnection::new(config);
        
        // 设置 QUIC 连接到连接中
        connection.set_connection(quic_connection).await;
        
        Ok(Box::new(connection))
    }
    
    /// 从 QUIC 原始连接创建服务端连接，并设置事件处理器
    pub async fn from_quic_with_handler(
        quic_connection: quinn::Connection,
        config: ConnectionConfig,
        handler: Arc<dyn ConnectionEvent>,
    ) -> Result<Box<dyn ServerConnection>> {
        // 创建 QUIC 连接
        let mut connection = QuicConnection::new(config);
        
        // 设置事件处理器
        connection.set_event_handler(handler).await;
        
        // 设置 QUIC 连接到连接中
        connection.set_connection(quic_connection).await;

        Ok(Box::new(connection))
    }
    
    /// 从 QUIC 原始连接创建服务端连接（使用Arc包装的事件处理器）
    pub async fn from_quic_with_handler_arc(
        quic_connection: quinn::Connection,
        config: ConnectionConfig,
        handler: Arc<dyn ConnectionEvent>,
    ) -> Result<Arc<dyn ServerConnection>> {
        // 创建 QUIC 连接
        let mut connection = QuicConnection::new(config);
        
        // 设置事件处理器
        connection.set_event_handler(handler).await;
        
        // 设置 QUIC 连接到连接中
        connection.set_connection(quic_connection).await;
        
        // 启动消息处理任务
        connection.start_task().await
            .map_err(|e| crate::common::error::FlareError::connection_failed(
                format!("启动消息接收任务失败: {}", e)
            ))?;

        Ok(Arc::new(connection))
    }
}