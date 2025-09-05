//! 消息解析器
//! 
//! 用于统一处理来自不同协议（QUIC/WebSocket）的消息数据，
//! 并触发相应的连接事件。

use std::sync::Arc;
use tracing::{debug, info, error};

use crate::common::{
    error::Result,
    protocol::Frame,
    connections::{
        event::ConnectionEvent,
        traits::ConnectionStats,
    },
    serialization::FrameSerializer,
};

/// 消息解析器
/// 
/// 负责解析从不同协议接收到的原始数据，并触发相应的连接事件。
/// 每个连接应创建一个独立的实例以确保状态隔离。
pub struct MessageParser {
    /// 连接ID
    connection_id: String,
    /// 事件处理器
    event_handler: Arc<dyn ConnectionEvent>,
    /// 统计信息
    stats: Arc<tokio::sync::RwLock<ConnectionStats>>,
    /// 序列化器
    serializer: Arc<Box<dyn FrameSerializer>>,
}

impl MessageParser {
    /// 创建新的消息解析器
    /// 
    /// 每个连接应创建一个独立的实例以确保状态隔离。
    pub fn new(
        connection_id: String,
        event_handler: Arc<dyn ConnectionEvent>,
        stats: Arc<tokio::sync::RwLock<ConnectionStats>>,
        serializer: Arc<Box<dyn FrameSerializer>>,
    ) -> Self {
        Self {
            connection_id,
            event_handler,
            stats,
            serializer,
        }
    }

    /// 解析并处理接收到的数据
    /// 
    /// 该方法负责：
    /// 1. 使用序列化器解析原始数据为Frame
    /// 2. 更新统计信息
    /// 3. 触发相应的连接事件
    pub async fn parse_and_handle(&self, data: Vec<u8>) {
        // 使用序列化器解析消息
        let frame = match self.serializer.deserialize(&data).await {
            Ok(frame) => {
                debug!("成功解析消息: {:?}", frame.get_message_type());
                frame
            },
            Err(e) => {
                error!("消息反序列化失败: {}，创建默认数据消息", e);
                // 如果解析失败，创建简单的数据消息
                Frame::new(
                    crate::common::protocol::MessageType::Data,
                    0,
                    crate::common::protocol::Reliability::AtLeastOnce,
                    data,
                )
            }
        };

        // 更新统计信息
        {
            let mut stats = self.stats.write().await;
            stats.messages_received += 1;
        }

        // 克隆需要的变量
        let handler = Arc::clone(&self.event_handler);
        let id = self.connection_id.clone();
        let frame_clone = frame.clone();

        // 触发消息接收事件
        tokio::spawn(async move {
            handler.on_message_received(&id, &frame_clone).await;
            
            // 如果是心跳消息，触发心跳接收事件
            if frame_clone.is_heartbeat() {
                handler.on_heartbeat_received(&id).await;
            }
        });
    }

    /// 处理WebSocket的Ping消息
    /// 
    /// 专门用于处理WebSocket协议的Ping消息
    pub async fn handle_websocket_ping(&self) {
        // 克隆需要的变量
        let handler = Arc::clone(&self.event_handler);
        let id = self.connection_id.clone();

        // 触发心跳接收事件
        tokio::spawn(async move {
            // 创建心跳消息帧来表示收到ping
            let heartbeat_frame = crate::common::protocol::Frame::heartbeat();
            handler.on_heartbeat_received(&id).await;
            // 同时触发消息接收事件
            handler.on_message_received(&id, &heartbeat_frame).await;
        });
    }

    /// 处理WebSocket的Pong消息
    /// 
    /// 专门用于处理WebSocket协议的Pong消息
    pub async fn handle_websocket_pong(&self) {
        // 克隆需要的变量
        let handler = Arc::clone(&self.event_handler);
        let id = self.connection_id.clone();

        // 触发心跳发送事件
        tokio::spawn(async move {
            handler.on_heartbeat_sent(&id).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{
        protocol::{MessageType, Reliability},
        connections::event::DefConnectionEventHandler,
    };

    #[tokio::test]
    async fn test_message_parser_creation() {
        let handler = Arc::new(DefConnectionEventHandler::default());
        let stats = Arc::new(tokio::sync::RwLock::new(ConnectionStats::default()));
        let serializer = Arc::new(crate::common::serialization::factory::json_serializer());
        
        let parser = MessageParser::new(
            "test-connection".to_string(),
            handler,
            stats,
            serializer,
        );
        
        assert_eq!(parser.connection_id, "test-connection");
    }

    #[tokio::test]
    async fn test_parse_valid_message() {
        let handler = Arc::new(DefConnectionEventHandler::default());
        let stats = Arc::new(tokio::sync::RwLock::new(ConnectionStats::default()));
        let serializer = Arc::new(crate::common::serialization::factory::json_serializer());
        
        let parser = MessageParser::new(
            "test-connection".to_string(),
            handler,
            stats,
            serializer,
        );
        
        // 创建测试消息
        let frame = Frame::new(
            MessageType::Data,
            1,
            Reliability::AtLeastOnce,
            b"test data".to_vec(),
        );
        
        // 序列化消息
        let data = crate::common::serialization::factory::json_serializer()
            .serialize(&frame)
            .await
            .unwrap();
        
        // 解析消息（在实际应用中，这会触发事件）
        parser.parse_and_handle(data).await;
    }
}