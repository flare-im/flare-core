//! WebSocket服务端连接实现
//!
//! 基于通用连接抽象层包装WebSocket服务端专有特性

use crate::common::connections::config::ConnectionConfig;
use crate::common::connections::enums::{ConnectionState, Transport};
use crate::common::connections::traits::{BaseConnection, ServerConnection, ConnectionEvent};
use crate::common::connections::websocket::WebSocketServerConn;
use crate::common::protocol::frame::Frame;
use crate::common::error::FlareError;
use crate::common::messaging::MessageProcessor;
use std::sync::Arc;

/// WebSocket服务端连接
pub struct WebSocketServerConnection {
    /// WebSocket服务端连接实现
    connection: Arc<WebSocketServerConn>,
}

impl WebSocketServerConnection {
    /// 从配置创建WebSocket服务端连接
    ///
    /// # 参数
    /// * `config` - 连接配置
    ///
    /// # 返回值
    /// WebSocket服务端连接实例
    pub fn from_config(config: ConnectionConfig) -> Self {
        // 确保配置指定了WebSocket传输协议
        let mut ws_config = config.clone();
        ws_config.transport = Transport::WebSocket;
        
        let connection = Arc::new(WebSocketServerConn::from_config(ws_config));
        Self { connection }
    }

    /// 从WebSocket流创建WebSocket服务端连接
    ///
    /// # 参数
    /// * `stream` - WebSocket流
    /// * `config` - 连接配置
    ///
    /// # 返回值
    /// WebSocket服务端连接实例
    pub fn from_websocket_stream(
        stream: tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        config: ConnectionConfig,
    ) -> Self {
        // 确保配置指定了WebSocket传输协议
        let mut ws_config = config.clone();
        ws_config.transport = Transport::WebSocket;
        
        let connection = Arc::new(WebSocketServerConn::from_websocket_stream(stream, ws_config));
        Self { connection }
    }

    /// 接受客户端连接
    ///
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    pub fn accept(&self) -> Result<(), FlareError> {
        self.connection.accept()
    }

    /// 关闭连接
    ///
    /// # 参数
    /// * `reason` - 关闭连接的原因（可选）
    ///
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    pub fn close(&self, reason: Option<String>) -> Result<(), FlareError> {
        self.connection.close(reason)
    }

    /// 发送消息（便利方法）
    ///
    /// 使用 MessageProcessor 处理 Frame（编码+压缩），然后通过连接发送二进制数据。
    ///
    /// # 参数
    /// * `frame` - 要发送的消息帧
    ///
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    pub fn send_message(&self, frame: Frame) -> Result<(), FlareError> {
        // 使用 MessageProcessor 处理 Frame → 二进制
        let processor = MessageProcessor::default();
        let bytes = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                processor.process_send(&frame).await
            })
        })?;
        
        // 连接层只负责发送二进制数据
        self.connection.send_bytes(bytes)
    }

    /// 设置事件处理器
    ///
    /// # 参数
    /// * `handler` - 事件处理器实例
    pub fn set_event_handler(&self, handler: Arc<dyn ConnectionEvent>) {
        self.connection.set_event_handler(handler);
    }

    /// 获取连接状态
    ///
    /// # 返回值
    /// 当前连接状态
    pub fn state(&self) -> ConnectionState {
        self.connection.state()
    }

    /// 获取统计信息
    ///
    /// # 返回值
    /// 连接的统计信息
    pub fn stats(&self) -> crate::common::connections::types::ConnectionStats {
        self.connection.stats()
    }

    /// 获取连接ID
    ///
    /// # 返回值
    /// 连接的唯一标识符字符串
    pub fn id(&self) -> String {
        self.connection.id()
    }

    /// 检查连接是否已建立
    ///
    /// # 返回值
    /// 如果连接已建立返回true，否则返回false
    pub fn is_connected(&self) -> bool {
        matches!(self.state(), ConnectionState::Connected)
    }
}

impl BaseConnection for WebSocketServerConnection {
    fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), FlareError> {
        self.connection.send_bytes(bytes)
    }
    
    fn set_event_handler(&self, handler: Arc<dyn ConnectionEvent>) {
        self.connection.set_event_handler(handler);
    }
    
    fn state(&self) -> ConnectionState {
        self.connection.state()
    }
    
    fn ready(&self) -> Result<(), FlareError> {
        self.connection.ready()
    }
    
    fn connected(&self) -> Result<(), FlareError> {
        self.connection.connected()
    }
    
    fn set_state(&self, state: ConnectionState) -> Result<(), FlareError> {
        self.connection.set_state(state)
    }
    
    fn stats(&self) -> crate::common::connections::types::ConnectionStats {
        self.connection.stats()
    }
    
    fn last_activity_epoch_ms(&self) -> u64 {
        self.connection.last_activity_epoch_ms()
    }
    
    fn id(&self) -> String {
        self.connection.id()
    }
}

impl ServerConnection for WebSocketServerConnection {
    fn accept(&self) -> Result<(), FlareError> {
        self.connection.accept()
    }

    fn close(&self, reason: Option<String>) -> Result<(), FlareError> {
        self.connection.close(reason)
    }
}