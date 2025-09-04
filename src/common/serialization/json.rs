//! JSON序列化器实现
//!
//! 提供默认的JSON格式序列化支持

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

/// JSON序列化器实现
#[derive(Debug)]
pub struct JsonSerializer {
    /// 序列化配置
    config: Arc<RwLock<SerializationConfig>>,
}

impl JsonSerializer {
    /// 创建新的JSON序列化器
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(SerializationConfig::default())),
        }
    }
    
    /// 创建美化格式的JSON序列化器
    pub fn pretty() -> Self {
        let mut config = SerializationConfig::default();
        config.pretty_format = true;
        
        Self {
            config: Arc::new(RwLock::new(config)),
        }
    }
    
    /// 创建带配置的JSON序列化器
    pub fn with_config(config: SerializationConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
        }
    }
    
    /// 设置是否使用美化格式
    pub fn set_pretty(&mut self, pretty: bool) -> Result<()> {
        if let Ok(mut config) = self.config.write() {
            config.pretty_format = pretty;
            Ok(())
        } else {
            Err(FlareError::general_error("无法获取配置写锁"))
        }
    }
    
    /// 获取是否使用美化格式
    pub fn is_pretty(&self) -> bool {
        self.config.read()
            .map(|config| config.pretty_format)
            .unwrap_or(false)
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
            SerializerFeature::PrettyFormat,
            SerializerFeature::TextFormat,
            SerializerFeature::SelfDescribing,
        ]
    }
}

impl Default for JsonSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for JsonSerializer {
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
impl FrameSerializer for JsonSerializer {
    fn format(&self) -> SerializationFormat {
        SerializationFormat::Json
    }
    
    async fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
        let config = self.config.read()
            .map_err(|_| FlareError::general_error("无法获取配置读锁"))?;
        
        let result = if config.pretty_format {
            serde_json::to_vec_pretty(frame)
        } else {
            serde_json::to_vec(frame)
        };
        
        match result {
            Ok(data) => {
                // 检查大小限制
                self.check_size_limit(data.len())?;
                Ok(data)
            }
            Err(e) => {
                Err(FlareError::serialization_error(
                    format!("JSON序列化失败: {}", e)
                ))
            }
        }
    }
    
    async fn deserialize(&self, data: &[u8]) -> Result<Frame> {
        // 检查大小限制
        self.check_size_limit(data.len())?;
        
        let result = serde_json::from_slice(data);
        
        match result {
            Ok(frame) => Ok(frame),
            Err(e) => {
                Err(FlareError::deserialization_failed(
                    format!("JSON反序列化失败: {}", e)
                ))
            }
        }
    }
    
    fn name(&self) -> &'static str {
        "JsonSerializer"
    }
    
    fn version(&self) -> &'static str {
        "1.0.0"
    }
    
    fn description(&self) -> &'static str {
        "JSON格式消息帧序列化器"
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
        false // JSON序列化器本身不支持压缩，可以在外层添加
    }
    
    fn mime_type(&self) -> &'static str {
        "application/json"
    }
    
    fn file_extension(&self) -> &'static str {
        "json"
    }
}

#[async_trait]
impl ConfigurableSerializer for JsonSerializer {
    fn update_config(&mut self, config: SerializationConfig) -> Result<()> {
        self.set_config(config)
    }
    
    fn configurable_params(&self) -> Vec<&'static str> {
        vec![
            "pretty_format",
            "max_message_size",
        ]
    }
    
    fn validate_config(&self, config: &SerializationConfig) -> Result<()> {
        // 验证JSON序列化器特定的配置
        if config.enable_compression {
            return Err(FlareError::general_error(
                "JSON序列化器不支持内置压缩功能"
            ));
        }
        
        if let Some(max_size) = config.max_message_size {
            if max_size == 0 {
                return Err(FlareError::general_error(
                    "最大消息大小不能为0"
                ));
            }
            
            if max_size > 1024 * 1024 * 1024 {  // 1GB
                return Err(FlareError::general_error(
                    "最大消息大小不能超过1GB"
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
    async fn test_json_serializer_basic() {
        let serializer = JsonSerializer::new();
        
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
    async fn test_json_serializer_pretty() {
        let mut serializer = JsonSerializer::pretty();
        
        let frame = Frame::new(
            MessageType::Heartbeat,
            0,
            Reliability::BestEffort,
            Vec::new(),
        );
        
        let serialized = serializer.serialize(&frame).await.unwrap();
        let json_str = String::from_utf8(serialized).unwrap();
        
        // 美化格式应该包含换行符
        assert!(json_str.contains('\n'));
    }
    
    #[tokio::test]
    async fn test_json_serializer_size_limit() {
        let mut config = SerializationConfig::default();
        config.max_message_size = Some(10); // 非常小的限制
        
        let mut serializer = JsonSerializer::with_config(config);
        
        let frame = Frame::new(
            MessageType::Data,
            1,
            Reliability::AtLeastOnce,
            b"this is a very long message that exceeds the limit".to_vec(),
        );
        
        // 应该因为大小限制失败
        let result = serializer.serialize(&frame).await;
        assert!(result.is_err());
    }
}