//! WebSocket 服务端连接实现

use crate::common::connections::traits::{ServerConnection, ConnectionEvent};
use crate::common::connections::config::ConnectionConfig;
use crate::common::error::FlareError;
use crate::common::connections::websocket::base::WebSocketBaseConn;
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::MaybeTlsStream;
use std::sync::Arc;

/// WebSocket 服务端连接结构
pub struct WebSocketServerConn {
    /// 基础WebSocket连接
    base: Arc<WebSocketBaseConn>,
}

impl WebSocketServerConn {
    /// 从配置创建WebSocket服务端连接
    /// 
    /// # 参数
    /// * `config` - 连接配置信息
    /// 
    /// # 返回值
    /// 新创建的WebSocketServerConn实例
    pub fn from_config(config: ConnectionConfig) -> Self {
        let base = Arc::new(WebSocketBaseConn::from_config(config));
        
        Self {
            base,
        }
    }
    
    /// 从WebSocket流创建WebSocket服务端连接
    /// 
    /// # 参数
    /// * `stream` - WebSocket流
    /// * `config` - 连接配置信息
    /// 
    /// # 返回值
    /// 新创建的WebSocketServerConn实例
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
impl crate::common::connections::traits::BaseConnection for WebSocketServerConn {
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

impl ServerConnection for WebSocketServerConn {
    /// 接受服务端连接
    /// 
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    fn accept(&self) -> Result<(), FlareError> {
        // 启动接收消息任务
        self.base.start_receive_task()?;
        
        // 通知连接已接受
        if let Some(h) = self.base.get_event_handler() {
            h.on_connected();
        }
        
        Ok(())
    }
    
    /// 关闭服务端连接
    /// 
    /// # 参数
    /// * `reason` - 关闭连接的原因（可选）
    /// 
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    fn close(&self, reason: Option<String>) -> Result<(), FlareError> {
        if let Some(h) = self.base.get_event_handler() {
            h.on_disconnected(reason);
        }
        Ok(())
    }
}

