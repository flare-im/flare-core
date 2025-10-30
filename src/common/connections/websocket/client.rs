//! WebSocket 客户端连接实现

use crate::common::connections::traits::{ClientConnection, ConnectionEvent};
use crate::common::connections::config::ConnectionConfig;
use crate::common::error::FlareError;
use crate::common::connections::websocket::base::WebSocketBaseConn;
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::MaybeTlsStream;
use std::sync::Arc;

/// WebSocket 客户端连接结构
pub struct WebSocketClientConn {
    /// 基础WebSocket连接
    base: Arc<WebSocketBaseConn>,
}

impl WebSocketClientConn {
    /// 从配置创建WebSocket客户端连接
    /// 
    /// # 参数
    /// * `config` - 连接配置信息
    /// 
    /// # 返回值
    /// 新创建的WebSocketClientConn实例
    pub fn from_config(config: ConnectionConfig) -> Self {
        let base = Arc::new(WebSocketBaseConn::from_config(config));
        
        Self {
            base,
        }
    }
    
    /// 从WebSocket流创建WebSocket客户端连接
    /// 
    /// # 参数
    /// * `stream` - WebSocket流
    /// * `config` - 连接配置信息
    /// 
    /// # 返回值
    /// 新创建的WebSocketClientConn实例
    pub fn from_websocket_stream(stream: WebSocketStream<MaybeTlsStream<TcpStream>>, config: ConnectionConfig) -> Self {
        let base = Arc::new(WebSocketBaseConn::from_websocket_stream(stream, config));
        
        Self {
            base,
        }
    }
    
    /// 获取基础连接核心
    /// 
    /// # 返回值
    /// 基础连接核心的引用
    pub fn base(&self) -> &Arc<WebSocketBaseConn> {
        &self.base
    }
}

// 实现 BaseConnection trait（通过委托给 base）
impl crate::common::connections::traits::BaseConnection for WebSocketClientConn {
    fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), FlareError> {
        self.base.send_bytes(bytes)
    }
    
    fn set_event_handler(&self, handler: Arc<dyn ConnectionEvent>) {
        self.base.set_event_handler(handler);
    }
    
    fn state(&self) -> crate::common::connections::enums::ConnectionState {
        self.base.state()
    }
    
    fn ready(&self) -> Result<(), FlareError> {
        self.base.ready()
    }
    
    fn connected(&self) -> Result<(), FlareError> {
        self.base.connected()
    }
    
    fn set_state(&self, state: crate::common::connections::enums::ConnectionState) -> Result<(), FlareError> {
        self.base.set_state(state)
    }
    
    fn stats(&self) -> crate::common::connections::types::ConnectionStats {
        self.base.stats()
    }
    
    fn last_activity_epoch_ms(&self) -> u64 {
        self.base.last_activity_epoch_ms()
    }
    
    fn id(&self) -> String {
        self.base.id()
    }
}

impl ClientConnection for WebSocketClientConn {
    /// 连接客户端
    /// 
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    fn connect(&self) -> Result<(), FlareError> {
        // 启动接收消息任务
        self.base.start_receive_task()?;
        
        // 通知连接已建立
        if let Some(h) = self.base.get_event_handler() {
            h.on_connected();
        }
        
        Ok(())
    }
    
    /// 断开客户端连接
    /// 
    /// # 参数
    /// * `reason` - 断开连接的原因（可选）
    /// 
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    fn disconnect(&self, reason: Option<String>) -> Result<(), FlareError> {
        if let Some(h) = self.base.get_event_handler() {
            h.on_disconnected(reason);
        }
        Ok(())
    }
}

