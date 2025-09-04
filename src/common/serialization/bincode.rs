//! Bincode序列化器实现
//!
//! 提供高性能的Bincode二进制序列化支持，专为Rust优化，性能极佳

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

/// Bincode序列化器实现
#[derive(Debug)]
pub struct BincodeSerializer {
    /// 序列化配置
    config: Arc<RwLock<SerializationConfig>>,
    /// 统计信息
    stats: Arc<RwLock<SerializationStats>>,
}

impl BincodeSerializer {
    /// 创建新的Bincode序列化器
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(SerializationConfig::default())),
            stats: Arc::new(RwLock::new(SerializationStats::default())),
        }
    }
    
    /// 创建带配置的Bincode序列化器
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
            stats: Arc::new(RwLock::new(SerializationStats::default())),
        }
    }
}

#[async_trait]
impl FrameSerializer for BincodeSerializer {
    fn format(&self) -> SerializationFormat {
        SerializationFormat::Bincode
    }
    
    async fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
        let start_time = Instant::now();
        
        // Bincode序列化 - 使用现有的Frame::to_bytes()方法
        // 这应该是最高性能的序列化方式，专为超低延迟优化
        let result = frame.to_bytes();
        
        let duration_us = start_time.elapsed().as_micros() as u64;
        
        match result {
            Ok(data) => {
                // 检查大小限制
                self.check_size_limit(data.len())?;
                
                // 更新统计信息
                self.update_serialize_stats(data.len(), duration_us, true);
                
                Ok(data)
            }
            Err(e) => {
                // 更新统计信息
                self.update_serialize_stats(0, duration_us, false);
                
                Err(FlareError::serialization_error(
                    format!("Bincode序列化失败: {}", e)
                ))
            }
        }
    }
    
    async fn deserialize(&self, data: &[u8]) -> Result<Frame> {
        let start_time = Instant::now();
        
        // 检查大小限制
        self.check_size_limit(data.len())?;
        
        // Bincode反序列化 - 使用现有的Frame::from_bytes()方法
        let result = Frame::from_bytes(data);
        let duration_us = start_time.elapsed().as_micros() as u64;
        
        match result {
            Ok(frame) => {
                // 更新统计信息
                self.update_deserialize_stats(data.len(), duration_us, true);
                Ok(frame)
            }
            Err(e) => {
                // 更新统计信息
                self.update_deserialize_stats(data.len(), duration_us, false);
                
                Err(FlareError::deserialization_failed(
                    format!("Bincode反序列化失败: {}", e)
                ))
            }
        }
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
        // Bincode大小估算 - 实际序列化获取精确大小
        let data = self.serialize(frame).await?;
        Ok(data.len())
    }
    
    async fn serialize_batch(&self, frames: &[Frame]) -> Result<Vec<Vec<u8>>> {
        // Bincode批量序列化优化
        let mut results = Vec::with_capacity(frames.len());
        let start_time = Instant::now();
        
        for frame in frames {
            match frame.to_bytes() {
                Ok(data) => {
                    self.check_size_limit(data.len())?;
                    results.push(data);
                }
                Err(e) => {
                    return Err(FlareError::serialization_error(
                        format!("批量Bincode序列化失败: {}", e)
                    ));
                }
            }
        }
        
        let duration_us = start_time.elapsed().as_micros() as u64;
        let total_size: usize = results.iter().map(|data| data.len()).sum();
        
        // 更新批量统计
        if let Ok(mut stats) = self.stats.write() {
            stats.serialize_count += frames.len() as u64;
            stats.serialized_bytes += total_size as u64;
            // 更新平均时间
            let avg_time = duration_us / frames.len() as u64;
            if stats.avg_serialize_time_us == 0 {
                stats.avg_serialize_time_us = avg_time;
            } else {
                stats.avg_serialize_time_us = 
                    (stats.avg_serialize_time_us * 9 + avg_time) / 10;
            }
        }
        
        Ok(results)
    }
    
    async fn deserialize_batch(&self, data_vec: &[Vec<u8>]) -> Result<Vec<Frame>> {
        // Bincode批量反序列化优化
        let mut results = Vec::with_capacity(data_vec.len());
        let start_time = Instant::now();
        
        for data in data_vec {
            self.check_size_limit(data.len())?;
            match Frame::from_bytes(data) {
                Ok(frame) => results.push(frame),
                Err(e) => {
                    return Err(FlareError::deserialization_failed(
                        format!("批量Bincode反序列化失败: {}", e)
                    ));
                }
            }
        }
        
        let duration_us = start_time.elapsed().as_micros() as u64;
        let total_size: usize = data_vec.iter().map(|data| data.len()).sum();
        
        // 更新批量统计
        if let Ok(mut stats) = self.stats.write() {
            stats.deserialize_count += data_vec.len() as u64;
            stats.deserialized_bytes += total_size as u64;
            // 更新平均时间
            let avg_time = duration_us / data_vec.len() as u64;
            if stats.avg_deserialize_time_us == 0 {
                stats.avg_deserialize_time_us = avg_time;
            } else {
                stats.avg_deserialize_time_us = 
                    (stats.avg_deserialize_time_us * 9 + avg_time) / 10;
            }
        }
        
        Ok(results)
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
    use crate::common::protocol::{Frame, MessageType, Reliability};
    
    #[tokio::test]
    async fn test_bincode_serializer_basic() {
        let serializer = BincodeSerializer::new();
        
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
    async fn test_bincode_serializer_performance() {
        let serializer = BincodeSerializer::new();
        
        let frame = Frame::new(
            MessageType::Data,
            1,
            Reliability::AtLeastOnce,
            vec![0u8; 1024], // 1KB数据
        );
        
        // 性能测试 - 批量操作
        let frames = vec![frame; 100];
        let start = std::time::Instant::now();
        let serialized_batch = serializer.serialize_batch(&frames).await.unwrap();
        let serialize_duration = start.elapsed();
        
        let start = std::time::Instant::now();
        let _deserialized_batch = serializer.deserialize_batch(&serialized_batch).await.unwrap();
        let deserialize_duration = start.elapsed();
        
        println!("Bincode批量序列化100条消息耗时: {:?}", serialize_duration);
        println!("Bincode批量反序列化100条消息耗时: {:?}", deserialize_duration);
        
        // 应该非常快，满足超低延迟要求
        assert!(serialize_duration.as_millis() < 10); // 小于10ms
        assert!(deserialize_duration.as_millis() < 10); // 小于10ms
    }
    
    #[tokio::test]
    async fn test_bincode_vs_json_size() {
        let bincode_serializer = BincodeSerializer::new();
        
        let frame = Frame::new(
            MessageType::Data,
            12345,
            Reliability::AtLeastOnce,
            b"test message for size comparison".to_vec(),
        );
        
        // Bincode序列化
        let bincode_data = bincode_serializer.serialize(&frame).await.unwrap();
        
        // JSON序列化作为对比
        let json_data = serde_json::to_vec(&frame).unwrap();
        
        println!("Bincode大小: {} 字节", bincode_data.len());
        println!("JSON大小: {} 字节", json_data.len());
        
        // Bincode通常更紧凑
        assert!(bincode_data.len() <= json_data.len());
    }
    
    #[tokio::test]
    async fn test_bincode_stats_tracking() {
        let serializer = BincodeSerializer::new();
        
        let frame = Frame::new(
            MessageType::Heartbeat,
            1,
            Reliability::BestEffort,
            Vec::new(),
        );
        
        // 执行多次操作
        for _ in 0..10 {
            let data = serializer.serialize(&frame).await.unwrap();
            let _ = serializer.deserialize(&data).await.unwrap();
        }
        
        let stats = serializer.stats();
        assert_eq!(stats.serialize_count, 10);
        assert_eq!(stats.deserialize_count, 10);
        assert_eq!(stats.serialize_errors, 0);
        assert_eq!(stats.deserialize_errors, 0);
        assert!(stats.avg_serialize_time_us > 0);
        assert!(stats.avg_deserialize_time_us > 0);
    }
}