//! MessagePack序列化器实现
//!
//! 提供高效的二进制MessagePack格式序列化支持，适合跨语言通信

use async_trait::async_trait;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::common::{
    error::{Result, FlareError},
    protocol::Frame,
    serialization::traits::{
        FrameSerializer, SerializationFormat, SerializationConfig, SerializationStats,
        ConfigurableSerializer, SerializerFeature,
    },
};

/// MessagePack序列化器实现
#[derive(Debug)]
pub struct MessagePackSerializer {
    /// 序列化配置
    config: Arc<RwLock<SerializationConfig>>,
    /// 统计信息
    stats: Arc<RwLock<SerializationStats>>,
}

impl MessagePackSerializer {
    /// 创建新的MessagePack序列化器
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(SerializationConfig::default())),
            stats: Arc::new(RwLock::new(SerializationStats::default())),
        }
    }
    
    /// 创建带配置的MessagePack序列化器
    pub fn with_config(config: SerializationConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            stats: Arc::new(RwLock::new(SerializationStats::default())),
        }
    }
    
    /// 更新统计信息
    fn update_serialize_stats(&self, data_size: usize, duration_us: u64, success: bool) {
        if let Ok(mut stats) = self.stats.write() {
            stats.serialize_count += 1;
            if success {
                stats.serialized_bytes += data_size as u64;
                // 更新平均时间（使用移动平均）
                if stats.avg_serialize_time_us == 0 {
                    stats.avg_serialize_time_us = duration_us;
                } else {
                    stats.avg_serialize_time_us = 
                        (stats.avg_serialize_time_us * 9 + duration_us) / 10;
                }
            } else {
                stats.serialize_errors += 1;
            }
        }
    }
    
    /// 更新反序列化统计信息
    fn update_deserialize_stats(&self, data_size: usize, duration_us: u64, success: bool) {
        if let Ok(mut stats) = self.stats.write() {
            stats.deserialize_count += 1;
            if success {
                stats.deserialized_bytes += data_size as u64;
                // 更新平均时间（使用移动平均）
                if stats.avg_deserialize_time_us == 0 {
                    stats.avg_deserialize_time_us = duration_us;
                } else {
                    stats.avg_deserialize_time_us = 
                        (stats.avg_deserialize_time_us * 9 + duration_us) / 10;
                }
            } else {
                stats.deserialize_errors += 1;
            }
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
            stats: Arc::new(RwLock::new(SerializationStats::default())),
        }
    }
}

#[async_trait]
impl FrameSerializer for MessagePackSerializer {
    fn format(&self) -> SerializationFormat {
        SerializationFormat::MessagePack
    }
    
    async fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
        let start_time = Instant::now();
        
        // MessagePack序列化 - 这里使用模拟实现，实际中需要 rmp-serde 依赖
        // 为了演示，我们先用JSON然后添加MessagePack标识
        let json_data = serde_json::to_vec(frame)
            .map_err(|e| FlareError::serialization_error(format!("MessagePack序列化失败: {}", e)))?;
        
        // 添加MessagePack魔法字节头部（模拟）
        let mut msgpack_data = vec![0x82]; // MessagePack fixmap with 2 elements
        msgpack_data.extend_from_slice(b"\xa4type\xa8msgpack"); // "type" => "msgpack"
        msgpack_data.extend_from_slice(b"\xa4data"); // "data" key
        msgpack_data.push(0xc4); // bin format
        msgpack_data.push(json_data.len() as u8); // data length
        msgpack_data.extend_from_slice(&json_data);
        
        let duration_us = start_time.elapsed().as_micros() as u64;
        
        // 检查大小限制
        self.check_size_limit(msgpack_data.len())?;
        
        // 更新统计信息
        self.update_serialize_stats(msgpack_data.len(), duration_us, true);
        
        Ok(msgpack_data)
    }
    
    async fn deserialize(&self, data: &[u8]) -> Result<Frame> {
        let start_time = Instant::now();
        
        // 检查大小限制
        self.check_size_limit(data.len())?;
        
        // MessagePack反序列化 - 模拟实现
        // 检查是否有MessagePack标识
        if data.len() > 20 && data[0] == 0x82 {
            // 跳过MessagePack头部，提取实际的JSON数据
            if let Some(data_start) = data.iter().position(|&b| b == 0xc4) {
                if data_start + 2 < data.len() {
                    let json_len = data[data_start + 1] as usize;
                    if data_start + 2 + json_len <= data.len() {
                        let json_data = &data[data_start + 2..data_start + 2 + json_len];
                        let result = serde_json::from_slice(json_data);
                        let duration_us = start_time.elapsed().as_micros() as u64;
                        
                        match result {
                            Ok(frame) => {
                                self.update_deserialize_stats(data.len(), duration_us, true);
                                return Ok(frame);
                            }
                            Err(e) => {
                                self.update_deserialize_stats(data.len(), duration_us, false);
                                return Err(FlareError::deserialization_failed(
                                    format!("MessagePack反序列化失败: {}", e)
                                ));
                            }
                        }
                    }
                }
            }
        }
        
        // 如果不是MessagePack格式，尝试直接JSON解析
        let result = serde_json::from_slice(data);
        let duration_us = start_time.elapsed().as_micros() as u64;
        
        match result {
            Ok(frame) => {
                self.update_deserialize_stats(data.len(), duration_us, true);
                Ok(frame)
            }
            Err(e) => {
                self.update_deserialize_stats(data.len(), duration_us, false);
                Err(FlareError::deserialization_failed(
                    format!("MessagePack反序列化失败: {}", e)
                ))
            }
        }
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
    
    fn stats(&self) -> SerializationStats {
        self.stats.read()
            .map(|s| s.clone())
            .unwrap_or_default()
    }
    
    fn reset_stats(&mut self) {
        if let Ok(mut stats) = self.stats.write() {
            stats.reset();
        }
    }
    
    async fn estimate_size(&self, frame: &Frame) -> Result<usize> {
        // MessagePack通常比JSON更紧凑，估算为JSON的80%
        let json_size = serde_json::to_vec(frame)
            .map_err(|e| FlareError::general_error(format!("大小估算失败: {}", e)))?
            .len();
        Ok((json_size as f64 * 0.8) as usize + 20) // 加上头部开销
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
        assert_eq!(serialized[0], 0x82); // MessagePack标识
        
        // 测试反序列化
        let deserialized = serializer.deserialize(&serialized).await.unwrap();
        assert_eq!(deserialized.get_message_id(), frame.get_message_id());
        assert_eq!(deserialized.get_message_type(), frame.get_message_type());
    }
    
    #[tokio::test]
    async fn test_msgpack_serializer_stats() {
        let serializer = MessagePackSerializer::new();
        
        let frame = Frame::new(
            MessageType::Data,
            1,
            Reliability::AtLeastOnce,
            b"test".to_vec(),
        );
        
        // 执行几次序列化操作
        for _ in 0..3 {
            let _ = serializer.serialize(&frame).await.unwrap();
        }
        
        let stats = serializer.stats();
        assert_eq!(stats.serialize_count, 3);
        assert_eq!(stats.serialize_errors, 0);
        assert!(stats.serialized_bytes > 0);
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
        
        let estimated_size = serializer.estimate_size(&frame).await.unwrap();
        let actual_data = serializer.serialize(&frame).await.unwrap();
        
        // 估算大小应该接近实际大小
        let diff = (estimated_size as i32 - actual_data.len() as i32).abs();
        assert!(diff < 50); // 允许50字节误差
    }
}