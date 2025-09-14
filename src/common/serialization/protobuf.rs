//! Protocol Buffers序列化器实现
//!
//! 提供高效的Protobuf二进制序列化支持，适合跨语言、有版本要求的通信

use async_trait::async_trait;
use prost::Message;
use std::sync::{Arc, RwLock};

use crate::common::{
    error::{Result, FlareError},
    protocol::{Frame, MessageType, Reliability, ProtobufFrame, ProtobufMessageType, ProtobufReliability},
    serialization::traits::{
        FrameSerializer, SerializationFormat, SerializationConfig,
        ConfigurableSerializer, SerializerFeature,
    },
};

/// Protocol Buffers序列化器实现
#[derive(Debug)]
pub struct ProtobufSerializer {
    /// 序列化配置
    config: Arc<RwLock<SerializationConfig>>,
}

impl ProtobufSerializer {
    /// 创建新的Protobuf序列化器
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(SerializationConfig::default())),
        }
    }
    
    /// 创建带配置的Protobuf序列化器
    pub fn with_config(config: SerializationConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
        }
    }
    
    /// 检查消息大小限制
    fn check_size_limit(&self, size: usize) -> Result<()> {
        if let Ok(config) = self.config.read() {
            if let Some(max_size) = config.max_message_size {
                if size > max_size {
                    return Err(FlareError::general_error(
                        format!("消息大小({})超过限制({})", size, max_size)
                    ));
                }
            }
        }
        Ok(())
    }
    
    /// 获取支持的特性列表
    pub fn supported_features() -> Vec<SerializerFeature> {
        vec![
            SerializerFeature::BinaryFormat,
            SerializerFeature::SchemaValidation,
        ]
    }
    
    /// 将Serde Frame转换为Protobuf Frame
    fn frame_to_proto(frame: &Frame) -> ProtobufFrame {
        ProtobufFrame {
            message_type: Self::message_type_to_proto(frame.message_type) as i32,
            message_id: frame.message_id,
            reliability: Self::reliability_to_proto(frame.reliability) as i32,
            timestamp: frame.timestamp,
            payload: frame.payload.clone(),
            session_id: frame.session_id.clone(),
            priority: frame.priority as u32,
            compression: frame.compression.map(|c| c as u32),
            encrypted: frame.encrypted,
            metadata: frame.metadata.clone().unwrap_or_default(),
        }
    }
    
    /// 将Protobuf Frame转换为Frame
    fn proto_to_frame(proto_frame: ProtobufFrame) -> Result<Frame> {
        Ok(Frame {
            message_type: Self::proto_to_message_type(proto_frame.message_type)?,
            message_id: proto_frame.message_id,
            reliability: Self::proto_to_reliability(proto_frame.reliability)?,
            timestamp: proto_frame.timestamp,
            payload: proto_frame.payload,
            session_id: proto_frame.session_id,
            priority: proto_frame.priority as u8,
            compression: proto_frame.compression.map(|c| c as u8),
            encrypted: proto_frame.encrypted,
            metadata: if proto_frame.metadata.is_empty() {
                None
            } else {
                Some(proto_frame.metadata)
            },
        })
    }
    
    /// 将MessageType转换为Protobuf MessageType
    fn message_type_to_proto(message_type: MessageType) -> ProtobufMessageType {
        match message_type {
            MessageType::Heartbeat => ProtobufMessageType::Heartbeat,
            MessageType::HeartbeatAck => ProtobufMessageType::HeartbeatAck,
            MessageType::Connect => ProtobufMessageType::Connect,
            MessageType::ConnectAck => ProtobufMessageType::ConnectAck,
            MessageType::Disconnect => ProtobufMessageType::Disconnect,
            MessageType::DisconnectAck => ProtobufMessageType::DisconnectAck,
            MessageType::Data => ProtobufMessageType::Data,
            MessageType::DataAck => ProtobufMessageType::DataAck,
            MessageType::Message => ProtobufMessageType::Message,
            MessageType::MessageAck => ProtobufMessageType::MessageAck,
            MessageType::Resend => ProtobufMessageType::Resend,
            MessageType::Error => ProtobufMessageType::Error,
            MessageType::Notification => ProtobufMessageType::Notification,
            MessageType::CustomEvent => ProtobufMessageType::CustomEvent,
            MessageType::CustomMessage => ProtobufMessageType::CustomMessage,
            MessageType::AuthRequest => ProtobufMessageType::AuthRequest,
            MessageType::AuthResponse => ProtobufMessageType::AuthResponse,
        }
    }
    
    /// 将Protobuf MessageType转换为MessageType
    fn proto_to_message_type(proto_type: i32) -> Result<MessageType> {
        match ProtobufMessageType::try_from(proto_type) {
            Ok(ProtobufMessageType::Heartbeat) => Ok(MessageType::Heartbeat),
            Ok(ProtobufMessageType::HeartbeatAck) => Ok(MessageType::HeartbeatAck),
            Ok(ProtobufMessageType::Connect) => Ok(MessageType::Connect),
            Ok(ProtobufMessageType::ConnectAck) => Ok(MessageType::ConnectAck),
            Ok(ProtobufMessageType::Disconnect) => Ok(MessageType::Disconnect),
            Ok(ProtobufMessageType::DisconnectAck) => Ok(MessageType::DisconnectAck),
            Ok(ProtobufMessageType::Data) => Ok(MessageType::Data),
            Ok(ProtobufMessageType::DataAck) => Ok(MessageType::DataAck),
            Ok(ProtobufMessageType::Message) => Ok(MessageType::Message),
            Ok(ProtobufMessageType::MessageAck) => Ok(MessageType::MessageAck),
            Ok(ProtobufMessageType::Resend) => Ok(MessageType::Resend),
            Ok(ProtobufMessageType::Error) => Ok(MessageType::Error),
            Ok(ProtobufMessageType::Notification) => Ok(MessageType::Notification),
            Ok(ProtobufMessageType::CustomEvent) => Ok(MessageType::CustomEvent),
            Ok(ProtobufMessageType::CustomMessage) => Ok(MessageType::CustomMessage),
            Ok(ProtobufMessageType::AuthRequest) => Ok(MessageType::AuthRequest),
            Ok(ProtobufMessageType::AuthResponse) => Ok(MessageType::AuthResponse),
            Ok(ProtobufMessageType::Unknown) | Err(_) => Err(FlareError::deserialization_failed(
                "无效的消息类型".to_string()
            )),
        }
    }
    
    /// 将Reliability转换为Protobuf Reliability
    fn reliability_to_proto(reliability: Reliability) -> ProtobufReliability {
        match reliability {
            Reliability::BestEffort => ProtobufReliability::BestEffort,
            Reliability::AtLeastOnce => ProtobufReliability::AtLeastOnce,
            Reliability::ExactlyOnce => ProtobufReliability::ExactlyOnce,
            Reliability::Ordered => ProtobufReliability::Ordered,
        }
    }
    
    /// 将Protobuf Reliability转换为Reliability
    fn proto_to_reliability(proto_reliability: i32) -> Result<Reliability> {
        match ProtobufReliability::try_from(proto_reliability) {
            Ok(ProtobufReliability::BestEffort) => Ok(Reliability::BestEffort),
            Ok(ProtobufReliability::AtLeastOnce) => Ok(Reliability::AtLeastOnce),
            Ok(ProtobufReliability::ExactlyOnce) => Ok(Reliability::ExactlyOnce),
            Ok(ProtobufReliability::Ordered) => Ok(Reliability::Ordered),
            Err(_) => Err(FlareError::deserialization_failed(
                "无效的可靠性级别".to_string()
            )),
        }
    }
}

impl Default for ProtobufSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ProtobufSerializer {
    fn clone(&self) -> Self {
        let config = self.config.read()
            .map(|c| c.clone())
            .unwrap_or_default();
        
        Self {
            config: Arc::new(RwLock::new(config)),
        }
    }
}

#[async_trait]
impl FrameSerializer for ProtobufSerializer {
    fn format(&self) -> SerializationFormat {
        SerializationFormat::Protobuf
    }
    
    async fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
        // 将Frame转换为Protobuf格式
        let proto_frame = Self::frame_to_proto(frame);
        
        // Protobuf序列化
        let mut buf = Vec::new();
        proto_frame.encode(&mut buf)
            .map_err(|e| FlareError::serialization_error(format!("Protobuf序列化失败: {}", e)))?;
        
        // 检查大小限制
        self.check_size_limit(buf.len())?;
        
        Ok(buf)
    }
    
    async fn deserialize(&self, data: &[u8]) -> Result<Frame> {
        // Protobuf反序列化
        let proto_frame = ProtobufFrame::decode(data)
            .map_err(|e| FlareError::deserialization_failed(format!("Protobuf反序列化失败: {}", e)))?;
        
        // 将Protobuf Frame转换为Frame
        Self::proto_to_frame(proto_frame)
    }
    
    fn name(&self) -> &'static str {
        "ProtobufSerializer"
    }
    
    fn version(&self) -> &'static str {
        "1.0.0"
    }
    
    fn description(&self) -> &'static str {
        "Protocol Buffers格式消息帧序列化器，高效二进制格式，支持模式验证和版本兼容"
    }
    
    fn config(&self) -> SerializationConfig {
        self.config.read()
            .map(|c| c.clone())
            .unwrap_or_default()
    }
    
    fn set_config(&mut self, config: SerializationConfig) -> Result<()> {
        if let Ok(mut current_config) = self.config.write() {
            *current_config = config;
            Ok(())
        } else {
            Err(FlareError::general_error("无法获取配置写锁"))
        }
    }
    
    fn clone_box(&self) -> Box<dyn FrameSerializer> {
        Box::new(self.clone())
    }
    
    fn supports_compression(&self) -> bool {
        true // Protobuf可以与压缩算法结合
    }
    
    fn supported_compression_algorithms(&self) -> Vec<&'static str> {
        vec!["gzip", "lz4", "snappy"]
    }
    
    fn mime_type(&self) -> &'static str {
        "application/x-protobuf"
    }
    
    fn file_extension(&self) -> &'static str {
        "proto"
    }
}

#[async_trait]
impl ConfigurableSerializer for ProtobufSerializer {
    fn update_config(&mut self, config: SerializationConfig) -> Result<()> {
        self.set_config(config)
    }
    
    fn configurable_params(&self) -> Vec<&'static str> {
        vec![
            "max_message_size",
            "enable_compression",
            "compression_level",
        ]
    }
    
    fn validate_config(&self, config: &SerializationConfig) -> Result<()> {
        // 验证Protobuf序列化器特定的配置
        if config.pretty_format {
            return Err(FlareError::general_error(
                "Protobuf是二进制格式，不支持美化格式"
            ));
        }
        
        if let Some(max_size) = config.max_message_size {
            if max_size == 0 {
                return Err(FlareError::general_error(
                    "最大消息大小不能为0"
                ));
            }
        }
        
        if config.enable_compression {
            if let Some(level) = config.compression_level {
                if level > 9 {
                    return Err(FlareError::general_error(
                        "压缩级别不能超过9"
                    ));
                }
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::{Frame, MessageType, Reliability};
    
    #[tokio::test]
    async fn test_protobuf_serializer_basic() {
        let serializer = ProtobufSerializer::new();
        
        let frame = Frame::new(
            MessageType::Data,
            12345,
            Reliability::AtLeastOnce,
            b"test message".to_vec(),
        );
        
        // 测试序列化
        let serialized = serializer.serialize(&frame).await.unwrap();
        assert!(!serialized.is_empty());
        
        // 测试反序列化
        let deserialized = serializer.deserialize(&serialized).await.unwrap();
        assert_eq!(deserialized.get_message_id(), frame.get_message_id());
        assert_eq!(deserialized.get_message_type(), frame.get_message_type());
        assert_eq!(deserialized.get_reliability(), frame.get_reliability());
        assert_eq!(deserialized.get_payload(), frame.get_payload());
    }
    
    #[tokio::test]
    async fn test_protobuf_size_efficiency() {
        let serializer = ProtobufSerializer::new();
        
        let frame = Frame::new(
            MessageType::Heartbeat,
            1,
            Reliability::BestEffort,
            Vec::new(), // 空载荷
        );
        
        let protobuf_data = serializer.serialize(&frame).await.unwrap();
        let json_data = serde_json::to_vec(&frame).unwrap();
        
        println!("Protobuf大小: {} 字节", protobuf_data.len());
        println!("JSON大小: {} 字节", json_data.len());
        
        // Protobuf对于小消息应该相对紧凑
        assert!(protobuf_data.len() > 0);
    }
    
    #[tokio::test]
    async fn test_protobuf_different_message_types() {
        let serializer = ProtobufSerializer::new();
        
        let message_types = vec![
            (MessageType::Data, Reliability::AtLeastOnce),
            (MessageType::Heartbeat, Reliability::BestEffort),
            (MessageType::ConnectAck, Reliability::ExactlyOnce),
            (MessageType::Error, Reliability::AtLeastOnce),
        ];
        
        for (msg_type, reliability) in message_types {
            let frame = Frame::new(
                msg_type,
                42,
                reliability,
                format!("test payload for {:?}", msg_type).into_bytes(),
            );
            
            let serialized = serializer.serialize(&frame).await.unwrap();
            let deserialized = serializer.deserialize(&serialized).await.unwrap();
            
            assert_eq!(deserialized.get_message_type(), frame.get_message_type());
            assert_eq!(deserialized.get_reliability(), frame.get_reliability());
            assert_eq!(deserialized.get_payload(), frame.get_payload());
        }
    }
    
    #[tokio::test]
    async fn test_protobuf_performance() {
        let serializer = ProtobufSerializer::new();
        
        let frame = Frame::new(
            MessageType::Data,
            1,
            Reliability::AtLeastOnce,
            vec![0u8; 512], // 512字节数据
        );
        
        // 性能测试 - 应该满足超低延迟要求
        let iterations = 100;
        let start = std::time::Instant::now();
        
        for _ in 0..iterations {
            let serialized = serializer.serialize(&frame).await.unwrap();
            let _ = serializer.deserialize(&serialized).await.unwrap();
        }
        
        let duration = start.elapsed();
        let avg_per_op = duration / iterations;
        
        println!("Protobuf平均操作时间: {:?}", avg_per_op);
        
        // 每次序列化+反序列化应该远小于15ms
        assert!(avg_per_op.as_millis() < 15); // 小于15ms
    }
}