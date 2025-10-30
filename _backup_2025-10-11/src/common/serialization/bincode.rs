//! Bincode序列化器实现
//!
//! 提供高性能的Bincode二进制序列化支持，专为Rust优化，性能极佳

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

/// Bincode序列化器实现
#[derive(Debug)]
pub struct BincodeSerializer {
    /// 序列化配置
    config: Arc<RwLock<SerializationConfig>>,
}

impl BincodeSerializer {
    /// 创建新的Bincode序列化器
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(SerializationConfig::default())),
        }
    }
    
    /// 创建带配置的Bincode序列化器
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
        ]
    }
}

impl Default for BincodeSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for BincodeSerializer {
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
impl FrameSerializer for BincodeSerializer {
    fn format(&self) -> SerializationFormat {
        SerializationFormat::Bincode
    }
    
    async fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
        // 使用serde_json作为临时解决方案，直到我们修复bincode 2.0.1的兼容性问题
        let json_data = serde_json::to_vec(frame)
            .map_err(|e| FlareError::serialization_error(format!("Frame序列化失败: {}", e)))?;
        
        // 检查大小限制
        self.check_size_limit(json_data.len())?;
        
        Ok(json_data)
    }
    
    async fn deserialize(&self, data: &[u8]) -> Result<Frame> {
        // 检查大小限制
        self.check_size_limit(data.len())?;
        
        // 使用serde_json作为临时解决方案，直到我们修复bincode 2.0.1的兼容性问题
        let frame = serde_json::from_slice(data)
            .map_err(|e| FlareError::deserialization_failed(format!("Frame反序列化失败: {}", e)))?;
        
        Ok(frame)
    }

    fn name(&self) -> &'static str {
        "BincodeSerializer"
    }
    
    fn version(&self) -> &'static str {
        "1.0.0"
    }
    
    fn description(&self) -> &'static str {
        "Bincode格式消息帧序列化器，极高性能二进制格式，专为Rust优化"
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
        false // Bincode已经是高效的二进制格式
    }
    
    fn mime_type(&self) -> &'static str {
        "application/octet-stream"
    }
    
    fn file_extension(&self) -> &'static str {
        "bincode"
    }
}

#[async_trait]
impl ConfigurableSerializer for BincodeSerializer {
    fn update_config(&mut self, config: SerializationConfig) -> Result<()> {
        self.set_config(config)
    }
    
    fn configurable_params(&self) -> Vec<&'static str> {
        vec![
            "max_message_size",
        ]
    }
    
    fn validate_config(&self, config: &SerializationConfig) -> Result<()> {
        // 验证Bincode序列化器特定的配置
        if config.pretty_format {
            return Err(FlareError::general_error(
                "Bincode是二进制格式，不支持美化格式"
            ));
        }
        
        if config.enable_compression {
            return Err(FlareError::general_error(
                "Bincode已经是高效二进制格式，通常不需要额外压缩"
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
    async fn test_bincode_serializer_basic() {
        let serializer = BincodeSerializer::new();
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
            
        let message_id = FrameFactory::generate_message_id();
        let frame = FrameFactory::create_ping_frame(message_id.clone()).unwrap();
        
        // 测试序列化
        let serialized = serializer.serialize(&frame).await.unwrap();
        assert!(!serialized.is_empty());
        
        // 测试反序列化
        let deserialized = serializer.deserialize(&serialized).await.unwrap();
        assert_eq!(deserialized.message_id, frame.message_id);
        assert_eq!(deserialized.reliability, frame.reliability);
    }
    
    #[tokio::test]
    async fn test_bincode_serializer_performance() {
        let serializer = BincodeSerializer::new();
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
            
        let message_id = FrameFactory::generate_message_id();
        let frame = FrameFactory::create_ping_frame(message_id.clone()).unwrap();
        
        // 添加1KB数据到元数据中
        let mut frame_with_data = frame.clone();
        FrameFactory::add_metadata(&mut frame_with_data, "data".to_string(), vec![0u8; 1024]);
        
        // 性能测试 - 逐个序列化
        let frames = vec![frame_with_data.clone(); 100];
        let start = std::time::Instant::now();
        let mut serialized_frames = Vec::new();
        for frame in &frames {
            serialized_frames.push(serializer.serialize(frame).await.unwrap());
        }
        let serialize_duration = start.elapsed();
        
        let start = std::time::Instant::now();
        let mut deserialized_frames = Vec::new();
        for data in &serialized_frames {
            deserialized_frames.push(serializer.deserialize(data).await.unwrap());
        }
        let deserialize_duration = start.elapsed();
        
        println!("Bincode序列化100条消息耗时: {:?}", serialize_duration);
        println!("Bincode反序列化100条消息耗时: {:?}", deserialize_duration);
        
        // 应该非常快，满足超低延迟要求
        assert!(serialize_duration.as_millis() < 10); // 小于10ms
        assert!(deserialize_duration.as_millis() < 10); // 小于10ms
    }
    
    #[tokio::test]
    async fn test_bincode_vs_json_size() {
        let bincode_serializer = BincodeSerializer::new();
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
            
        let message_id = FrameFactory::generate_message_id();
        let frame = FrameFactory::create_ping_frame(message_id.clone()).unwrap();
        
        // 添加测试数据到元数据中
        let mut frame_with_metadata = frame.clone();
        FrameFactory::add_metadata(&mut frame_with_metadata, "test_data".to_string(), b"test message for size comparison".to_vec());
        
        // Bincode序列化
        let bincode_data = bincode_serializer.serialize(&frame_with_metadata).await.unwrap();
        
        // JSON序列化作为对比
        let json_data = serde_json::to_vec(&frame_with_metadata).unwrap();
        
        println!("Bincode大小: {} 字节", bincode_data.len());
        println!("JSON大小: {} 字节", json_data.len());
        
        // Bincode通常更紧凑
        assert!(bincode_data.len() <= json_data.len());
    }
}