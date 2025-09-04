//! MessagePack序列化器实现
//!
//! 提供高效的二进制MessagePack格式序列化支持，适合跨语言通信

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

/// MessagePack序列化器实现
#[derive(Debug)]
pub struct MessagePackSerializer {
    /// 序列化配置
    config: Arc<RwLock<SerializationConfig>>,
}

impl MessagePackSerializer {
    /// 创建新的MessagePack序列化器
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(SerializationConfig::default())),
        }
    }
    
    /// 创建带配置的MessagePack序列化器
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
            SerializerFeature::SelfDescribing,
        ]
    }
}

impl Default for MessagePackSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for MessagePackSerializer {
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
impl FrameSerializer for MessagePackSerializer {
    fn format(&self) -> SerializationFormat {
        SerializationFormat::MessagePack
    }
    
    async fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
        // MessagePack序列化 - 使用rmp-serde库
        let msgpack_data = rmp_serde::to_vec(frame)
            .map_err(|e| FlareError::serialization_error(format!("MessagePack序列化失败: {}", e)))?;
        
        // 检查大小限制
        self.check_size_limit(msgpack_data.len())?;
        
        Ok(msgpack_data)
    }
    
    async fn deserialize(&self, data: &[u8]) -> Result<Frame> {
        // 检查大小限制
        self.check_size_limit(data.len())?;
        
        // MessagePack反序列化 - 使用rmp-serde库
        let frame = rmp_serde::from_slice(data)
            .map_err(|e| FlareError::deserialization_failed(format!("MessagePack反序列化失败: {}", e)))?;
        
        Ok(frame)
    }
    
    fn name(&self) -> &'static str {
        "MessagePackSerializer"
    }
    
    fn version(&self) -> &'static str {
        "1.0.0"
    }
    
    fn description(&self) -> &'static str {
        "MessagePack格式消息帧序列化器，高效二进制格式，适合跨语言通信"
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
        false // MessagePack本身是压缩格式
    }
    
    fn mime_type(&self) -> &'static str {
        "application/msgpack"
    }
    
    fn file_extension(&self) -> &'static str {
        "msgpack"
    }
}

#[async_trait]
impl ConfigurableSerializer for MessagePackSerializer {
    fn update_config(&mut self, config: SerializationConfig) -> Result<()> {
        self.set_config(config)
    }
    
    fn configurable_params(&self) -> Vec<&'static str> {
        vec![
            "max_message_size",
        ]
    }
    
    fn validate_config(&self, config: &SerializationConfig) -> Result<()> {
        // 验证MessagePack序列化器特定的配置
        if config.pretty_format {
            return Err(FlareError::general_error(
                "MessagePack是二进制格式，不支持美化格式"
            ));
        }
        
        if config.enable_compression {
            return Err(FlareError::general_error(
                "MessagePack本身是压缩格式，不需要额外压缩"
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
    use crate::common::protocol::{Frame, MessageType, Reliability};
    
    #[tokio::test]
    async fn test_msgpack_serializer_basic() {
        let serializer = MessagePackSerializer::new();
        
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
    }
    
    #[tokio::test]
    async fn test_msgpack_size_estimate() {
        let serializer = MessagePackSerializer::new();
        
        let frame = Frame::new(
            MessageType::Data,
            1,
            Reliability::AtLeastOnce,
            b"test message for size estimation".to_vec(),
        );
        
        // 直接序列化获取实际大小
        let actual_data = serializer.serialize(&frame).await.unwrap();
        let actual_size = actual_data.len();
        
        // 验证序列化成功并获得了数据
        assert!(actual_size > 0);
        
        println!("MessagePack实际大小: {} 字节", actual_size);
    }
}