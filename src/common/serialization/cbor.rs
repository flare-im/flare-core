//! CBOR序列化器实现
//!
//! 提供CBOR (Concise Binary Object Representation) 序列化支持，
//! RFC 7049标准，适合IoT和资源受限环境

use async_trait::async_trait;
use std::sync::{Arc, RwLock};

use crate::common::{
    error::{Result, FlareError},
    protocol::Frame,
    serialization::traits::{
        FrameSerializer, SerializationFormat, SerializationConfig,
        ConfigurableSerializer, SerializerFeature,
    },
};

/// CBOR序列化器实现
#[derive(Debug)]
pub struct CborSerializer {
    /// 序列化配置
    config: Arc<RwLock<SerializationConfig>>,
}

impl CborSerializer {
    /// 创建新的CBOR序列化器
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(SerializationConfig::default())),
        }
    }
    
    /// 创建带配置的CBOR序列化器
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
    
    /// CBOR编码 - 使用serde_cbor库
    fn encode_cbor(&self, frame: &Frame) -> Result<Vec<u8>> {
        serde_cbor::to_vec(frame)
            .map_err(|e| FlareError::serialization_error(format!("CBOR序列化失败: {}", e)))
    }
    
    /// CBOR解码 - 使用serde_cbor库
    fn decode_cbor(&self, data: &[u8]) -> Result<Frame> {
        serde_cbor::from_slice(data)
            .map_err(|e| FlareError::deserialization_failed(format!("CBOR反序列化失败: {}", e)))
    }
    
    /// 获取支持的特性列表
    pub fn supported_features() -> Vec<SerializerFeature> {
        vec![
            SerializerFeature::BinaryFormat,
            SerializerFeature::SelfDescribing,
        ]
    }
}

impl Default for CborSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for CborSerializer {
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
impl FrameSerializer for CborSerializer {
    fn format(&self) -> SerializationFormat {
        SerializationFormat::Cbor
    }
    
    async fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
        // CBOR序列化
        let result = self.encode_cbor(frame);
        
        match result {
            Ok(data) => {
                // 检查大小限制
                self.check_size_limit(data.len())?;
                Ok(data)
            }
            Err(e) => {
                Err(e)
            }
        }
    }
    
    async fn deserialize(&self, data: &[u8]) -> Result<Frame> {
        // 检查大小限制
        self.check_size_limit(data.len())?;
        
        // CBOR反序列化
        let result = self.decode_cbor(data);
        
        match result {
            Ok(frame) => Ok(frame),
            Err(e) => {
                Err(e)
            }
        }
    }
    
    fn name(&self) -> &'static str {
        "CborSerializer"
    }
    
    fn version(&self) -> &'static str {
        "1.0.0"
    }
    
    fn description(&self) -> &'static str {
        "CBOR格式消息帧序列化器，紧凑二进制格式，适合IoT和资源受限环境"
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
        true // CBOR可以与压缩算法结合
    }
    
    fn supported_compression_algorithms(&self) -> Vec<&'static str> {
        vec!["gzip", "lz4"]
    }
    
    fn mime_type(&self) -> &'static str {
        "application/cbor"
    }
    
    fn file_extension(&self) -> &'static str {
        "cbor"
    }
}

#[async_trait]
impl ConfigurableSerializer for CborSerializer {
    fn update_config(&mut self, config: SerializationConfig) -> Result<()> {
        self.set_config(config)
    }
    
    fn configurable_params(&self) -> Vec<&'static str> {
        vec![
            "max_message_size",
            "enable_compression",
        ]
    }
    
    fn validate_config(&self, config: &SerializationConfig) -> Result<()> {
        // 验证CBOR序列化器特定的配置
        if config.pretty_format {
            return Err(FlareError::general_error(
                "CBOR是二进制格式，不支持美化格式"
            ));
        }
        
        if let Some(max_size) = config.max_message_size {
            if max_size == 0 {
                return Err(FlareError::general_error(
                    "最大消息大小不能为0"
                ));
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::factory::FrameFactory;
    use std::time::{SystemTime, UNIX_EPOCH};
    
    #[tokio::test]
    async fn test_cbor_serializer_basic() {
        let serializer = CborSerializer::new();
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
            
        let message_id = FrameFactory::generate_message_id();
        let frame = FrameFactory::create_ping_frame(message_id.clone()).unwrap();
        
        // 添加测试数据到元数据中
        let mut frame_with_metadata = frame.clone();
        FrameFactory::add_metadata(&mut frame_with_metadata, "test_data".to_string(), b"Hello CBOR!".to_vec());
        
        // 测试序列化
        let serialized = serializer.serialize(&frame_with_metadata).await.unwrap();
        assert!(!serialized.is_empty());
        
        // 测试反序列化
        let deserialized = serializer.deserialize(&serialized).await.unwrap();
        assert_eq!(deserialized.message_id, frame_with_metadata.message_id);
        assert_eq!(deserialized.reliability, frame_with_metadata.reliability);
    }
    
    #[tokio::test]
    async fn test_cbor_validation() {
        let serializer = CborSerializer::new();
        
        // 有效的CBOR数据应该能成功反序列化
        let valid_data = vec![0xA4, 0x64, 0x74, 0x79, 0x70, 0x65]; // map + "type"
        let result = serializer.deserialize(&valid_data).await;
        // 我们不直接测试validate方法，而是测试deserialize是否成功
        assert!(result.is_err()); // 这个数据实际上不是有效的Frame
        
        // 无效的数据应该反序列化失败
        let invalid_data = vec![0xFF, 0xFF];
        let result = serializer.deserialize(&invalid_data).await;
        assert!(result.is_err());
        
        // 空数据应该反序列化失败
        let result = serializer.deserialize(&[]).await;
        assert!(result.is_err());
    }
    
    #[tokio::test]
    async fn test_cbor_compact_size() {
        let serializer = CborSerializer::new();
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        // 测试小消息的紧凑性
        let message_id = FrameFactory::generate_message_id();
        let small_frame = FrameFactory::create_ping_frame(message_id.clone()).unwrap();
        
        let cbor_data = serializer.serialize(&small_frame).await.unwrap();
        let json_data = serde_json::to_vec(&small_frame).unwrap();
        
        println!("CBOR大小: {} 字节", cbor_data.len());
        println!("JSON大小: {} 字节", json_data.len());
        
        // CBOR对于小消息应该更紧凑
        assert!(cbor_data.len() < json_data.len());
    }
    
    #[tokio::test]
    async fn test_cbor_different_payload_sizes() {
        let serializer = CborSerializer::new();
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        let payload_sizes = vec![0, 10, 23, 24, 100, 255, 256, 1000];
        
        for size in payload_sizes {
            let payload = vec![0x42u8; size]; // 填充字节
            let message_id = FrameFactory::generate_message_id();
            let frame = FrameFactory::create_ping_frame(message_id.clone()).unwrap();
            
            // 添加载荷数据到元数据中
            let mut frame_with_payload = frame.clone();
            FrameFactory::add_metadata(&mut frame_with_payload, "payload".to_string(), payload.clone());
            
            let serialized = serializer.serialize(&frame_with_payload).await.unwrap();
            let deserialized = serializer.deserialize(&serialized).await.unwrap();
            
            // 验证元数据中的载荷数据
            let deserialized_payload = deserialized.metadata.as_ref().unwrap().get("payload").unwrap();
            assert_eq!(deserialized_payload.len(), size);
            assert_eq!(deserialized_payload, &payload);
            
            println!("载荷{}字节 -> CBOR{}字节", size, serialized.len());
        }
    }
}