//! QUIC 服务端连接实现

use crate::common::connections::traits::{ServerConnection, ConnectionEvent};
use crate::common::connections::config::ConnectionConfig;
use crate::common::error::FlareError;
use crate::common::connections::quic::base::QuicBaseConn;
use std::sync::Arc;

/// QUIC 服务端连接结构
pub struct QuicServerConn {
    /// 基础QUIC连接
    base: Arc<QuicBaseConn>,
}

impl QuicServerConn {
    pub fn from_config(config: ConnectionConfig) -> Self {
        let base = Arc::new(QuicBaseConn::from_config(config));
        
        Self {
            base,
        }
    }

    /// 从原生 quinn::Connection 构造服务端连接（支持真实读写桥接）
    pub fn from_quinn_connection(conn: quinn::Connection, config: ConnectionConfig) -> Self {
        let base = Arc::new(QuicBaseConn::from_quinn_connection(conn, config));
        
        Self {
            base,
        }
    }
    
    /// 获取基础连接核心
    /// 
    /// # 返回值
    /// 基础连接核心的引用
    pub fn base(&self) -> &Arc<QuicBaseConn> {
        &self.base
    }
}

// 实现 BaseConnection trait（通过委托给 base）
impl crate::common::connections::traits::BaseConnection for QuicServerConn {
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

impl ServerConnection for QuicServerConn {
    fn accept(&self) -> Result<(), FlareError> {
        // 通知连接已接受
        if let Some(h) = self.base.get_event_handler() {
            h.on_connected();
        }
        Ok(())
    }
    
    fn close(&self, reason: Option<String>) -> Result<(), FlareError> {
        if let Some(h) = self.base.get_event_handler() {
            h.on_disconnected(reason);
        }
        Ok(())
    }
}

