//! QUIC客户端连接实现
//!
//! 基于通用连接抽象层包装QUIC客户端专有特性

use crate::common::connections::config::ConnectionConfig;
use crate::common::connections::enums::{ConnectionState, Transport};
use crate::common::connections::traits::{BaseConnection, ClientConnection, ConnectionEvent};
use crate::common::connections::quic::QuicClientConn;
use crate::common::protocol::frame::Frame;
use crate::common::error::FlareError;
use crate::common::messaging::MessageProcessor;
use std::sync::Arc;

/// QUIC客户端连接
pub struct QuicClient {
    /// QUIC客户端连接实现
    connection: Arc<QuicClientConn>,
}

impl QuicClient {
    /// 创建新的QUIC客户端连接
    ///
    /// # 参数
    /// * `config` - 连接配置
    ///
    /// # 返回值
    /// QUIC客户端连接实例或错误
    pub fn new(config: ConnectionConfig) -> Result<Self, FlareError> {
        // 确保配置指定了QUIC传输协议
        let mut quic_config = config.clone();
        quic_config.transport = Transport::Quic;
        
        let connection = Arc::new(QuicClientConn::from_config(quic_config));
        Ok(Self { connection })
    }

    /// 连接到服务器
    ///
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    pub fn connect(&self) -> Result<(), FlareError> {
        self.connection.connect()
    }

    /// 断开连接
    ///
    /// # 参数
    /// * `reason` - 断开连接的原因（可选）
    ///
    /// # 返回值
    /// 操作结果，成功返回Ok(())，失败返回相应的错误
    pub fn disconnect(&self, reason: Option<String>) -> Result<(), FlareError> {
        self.connection.disconnect(reason)
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

impl BaseConnection for QuicClient {
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

impl ClientConnection for QuicClient {
    fn connect(&self) -> Result<(), FlareError> {
        self.connection.connect()
    }

    fn disconnect(&self, reason: Option<String>) -> Result<(), FlareError> {
        self.connection.disconnect(reason)
    }
}