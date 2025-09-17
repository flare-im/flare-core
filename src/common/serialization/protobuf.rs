//! Protocol Buffers序列化器实现
//!
//! 提供高效的Protobuf二进制序列化支持，适合跨语言、有版本要求的通信

use async_trait::async_trait;
use prost::Message;
use std::sync::{Arc, RwLock};

use crate::common::{
    error::{Result, FlareError},
    protocol::{Frame, ProtobufFrame, ProtocolConverter},
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
        // 使用ProtocolConverter将Rust Frame转换为Proto Frame
        let proto_frame = ProtocolConverter::rust_to_proto_frame(frame)
            .map_err(|e| FlareError::serialization_error(format!("Frame转换为Proto失败: {}", e)))?;
        
        // 使用prost序列化Proto Frame为二进制数据
        let mut buf = Vec::new();
        proto_frame.encode(&mut buf)
            .map_err(|e| FlareError::serialization_error(format!("Proto Frame序列化失败: {}", e)))?;
        
        // 检查大小限制
        self.check_size_limit(buf.len())?;
        
        Ok(buf)
    }
    
    async fn deserialize(&self, data: &[u8]) -> Result<Frame> {
        // 使用prost反序列化二进制数据为Proto Frame
        let proto_frame = ProtobufFrame::decode(data)
            .map_err(|e| FlareError::deserialization_failed(format!("Proto Frame反序列化失败: {}", e)))?;
        
        // 使用ProtocolConverter将Proto Frame转换为Rust Frame
        let frame = ProtocolConverter::proto_to_rust_frame(&proto_frame)
            .map_err(|e| FlareError::deserialization_failed(format!("Proto转换为Frame失败: {}", e)))?;
        
        Ok(frame)
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
    use crate::common::protocol::{Frame, Reliability};
    use crate::common::protocol::factory::FrameFactory;
    use std::time::{SystemTime, UNIX_EPOCH};
    
    #[tokio::test]
    async fn test_protobuf_serializer_basic() {
        let serializer = ProtobufSerializer::new();
        
        let message_id = FrameFactory::generate_message_id();
        let frame = FrameFactory::create_ping_frame(message_id.clone()).unwrap();
        
        // 添加测试数据到元数据中
        let mut frame_with_metadata = frame.clone();
        FrameFactory::add_metadata(&mut frame_with_metadata, "test_data".to_string(), b"test message".to_vec());
        
        // 测试序列化
        let serialized = serializer.serialize(&frame_with_metadata).await.unwrap();
        assert!(!serialized.is_empty());
        
        // 测试反序列化
        let deserialized = serializer.deserialize(&serialized).await.unwrap();
        assert_eq!(deserialized.message_id, frame_with_metadata.message_id);
        assert_eq!(deserialized.reliability, frame_with_metadata.reliability);
    }
    
    #[tokio::test]
    async fn test_protobuf_size_efficiency() {
        let serializer = ProtobufSerializer::new();
        
        let message_id = FrameFactory::generate_message_id();
        let frame = FrameFactory::create_ping_frame(message_id.clone()).unwrap();
        
        let protobuf_data = serializer.serialize(&frame).await.unwrap();
        
        println!("Protobuf大小: {} 字节", protobuf_data.len());
        
        // Protobuf对于小消息应该相对紧凑
        assert!(protobuf_data.len() > 0);
    }
    
    #[tokio::test]
    async fn test_protobuf_different_reliability_levels() {
        let serializer = ProtobufSerializer::new();
        
        let reliability_levels = vec![
            Reliability::BestEffort,
            Reliability::AtLeastOnce,
            Reliability::ExactlyOnce,
            Reliability::Ordered,
        ];
        
        for reliability in reliability_levels {
            let message_id = FrameFactory::generate_message_id();
            let frame = FrameFactory::create_ping_frame(message_id.clone()).unwrap();
            
            // 添加测试数据到元数据中
            let mut frame_with_metadata = frame.clone();
            FrameFactory::add_metadata(&mut frame_with_metadata, "test_data".to_string(), format!("test payload for {:?}", reliability).into_bytes());
            
            let serialized = serializer.serialize(&frame_with_metadata).await.unwrap();
            let deserialized = serializer.deserialize(&serialized).await.unwrap();
            
            assert_eq!(deserialized.reliability, frame_with_metadata.reliability);
        }
    }
    
    #[tokio::test]
    async fn test_protobuf_performance() {
        let serializer = ProtobufSerializer::new();
        
        let message_id = FrameFactory::generate_message_id();
        let frame = FrameFactory::create_ping_frame(message_id.clone()).unwrap();
        
        // 添加512字节数据到元数据中
        let mut frame_with_data = frame.clone();
        FrameFactory::add_metadata(&mut frame_with_data, "data".to_string(), vec![0u8; 512]);
        
        // 性能测试 - 应该满足超低延迟要求
        let iterations = 100;
        let start = std::time::Instant::now();
        
        for _ in 0..iterations {
            let serialized = serializer.serialize(&frame_with_data).await.unwrap();
            let _ = serializer.deserialize(&serialized).await.unwrap();
        }
        
        let duration = start.elapsed();
        let avg_per_op = duration / iterations;
        
        println!("Protobuf平均操作时间: {:?}", avg_per_op);
        
        // 每次序列化+反序列化应该远小于15ms
        assert!(avg_per_op.as_millis() < 15); // 小于15ms
    }
}