//! CBOR序列化器实现
//!
//! 提供CBOR (Concise Binary Object Representation) 序列化支持，
//! RFC 7049标准，适合IoT和资源受限环境

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

/// CBOR序列化器实现
#[derive(Debug)]
pub struct CborSerializer {
    /// 序列化配置
    config: Arc<RwLock<SerializationConfig>>,
    /// 统计信息
    stats: Arc<RwLock<SerializationStats>>,
}

impl CborSerializer {
    /// 创建新的CBOR序列化器
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(SerializationConfig::default())),
            stats: Arc::new(RwLock::new(SerializationStats::default())),
        }
    }
    
    /// 创建带配置的CBOR序列化器
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
    
    /// CBOR编码 - 简化实现
    fn encode_cbor(&self, frame: &Frame) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        
        // CBOR map with 4 items (major type 5, additional info 4)
        buf.push(0xA4);
        
        // 字段1: "type" => message_type
        buf.extend_from_slice(b"\x64type"); // text string "type"
        buf.push(0x00 + frame.get_message_type() as u8); // unsigned integer
        
        // 字段2: "id" => message_id  
        buf.extend_from_slice(b"\x62id"); // text string "id"
        let msg_id = frame.get_message_id();
        if msg_id <= 23 {
            buf.push(msg_id as u8); // direct encoding
        } else if msg_id <= 255 {
            buf.push(0x18); // uint8
            buf.push(msg_id as u8);
        } else {
            buf.push(0x19); // uint16
            buf.extend_from_slice(&(msg_id as u16).to_be_bytes());
        }
        
        // 字段3: "reliability" => reliability
        buf.extend_from_slice(b"\x6Areliability"); // text string "reliability"
        buf.push(0x00 + frame.get_reliability() as u8); // unsigned integer
        
        // 字段4: "payload" => byte string
        buf.extend_from_slice(b"\x67payload"); // text string "payload"
        let payload = frame.get_payload();
        if payload.len() <= 23 {
            buf.push(0x40 + payload.len() as u8); // byte string, direct length
        } else if payload.len() <= 255 {
            buf.push(0x58); // byte string, uint8 length
            buf.push(payload.len() as u8);
        } else {
            buf.push(0x59); // byte string, uint16 length
            buf.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        }
        buf.extend_from_slice(payload);
        
        Ok(buf)
    }
    
    /// CBOR解码 - 简化实现
    fn decode_cbor(&self, data: &[u8]) -> Result<Frame> {
        if data.is_empty() {
            return Err(FlareError::deserialization_failed("空CBOR数据".to_string()));
        }
        
        // 验证CBOR map标记
        if data[0] != 0xA4 {
            return Err(FlareError::deserialization_failed("无效的CBOR格式".to_string()));
        }
        
        let mut pos = 1;
        let mut message_type = crate::common::protocol::MessageType::Data;
        let mut message_id = 0u64;
        let mut reliability = crate::common::protocol::Reliability::AtLeastOnce;
        let mut payload = Vec::new();
        
        // 简化的CBOR解析 - 按预期的字段顺序解析
        for _ in 0..4 {
            if pos >= data.len() {
                break;
            }
            
            // 跳过字段名称
            if pos < data.len() && data[pos] >= 0x60 && data[pos] <= 0x77 {
                let text_len = (data[pos] & 0x1F) as usize;
                pos += 1 + text_len; // 跳过字段名
            }
            
            if pos >= data.len() {
                break;
            }
            
            // 读取值
            let value_type = data[pos];
            pos += 1;
            
            if value_type <= 0x17 {
                // 无符号整数，直接值
                let val = value_type;
                if payload.is_empty() {
                    // 这可能是message_type, message_id或reliability
                    if message_type as u8 == 0 {
                        message_type = match val {
                            0 => crate::common::protocol::MessageType::Data,
                            1 => crate::common::protocol::MessageType::Heartbeat,
                            2 => crate::common::protocol::MessageType::Error, // 使用Error替代Ack
                            3 => crate::common::protocol::MessageType::Error,
                            _ => crate::common::protocol::MessageType::Data,
                        };
                    } else if message_id == 0 {
                        message_id = val as u64;
                    } else {
                        reliability = match val {
                            0 => crate::common::protocol::Reliability::BestEffort,
                            1 => crate::common::protocol::Reliability::AtLeastOnce,
                            2 => crate::common::protocol::Reliability::ExactlyOnce,
                            _ => crate::common::protocol::Reliability::AtLeastOnce,
                        };
                    }
                }
            } else if value_type == 0x18 {
                // uint8
                if pos < data.len() {
                    let val = data[pos];
                    pos += 1;
                    if message_id == 0 {
                        message_id = val as u64;
                    }
                }
            } else if value_type == 0x19 {
                // uint16
                if pos + 1 < data.len() {
                    let val = u16::from_be_bytes([data[pos], data[pos + 1]]);
                    pos += 2;
                    if message_id == 0 {
                        message_id = val as u64;
                    }
                }
            } else if value_type >= 0x40 && value_type <= 0x57 {
                // byte string, direct length
                let length = (value_type & 0x1F) as usize;
                if pos + length <= data.len() {
                    payload = data[pos..pos + length].to_vec();
                    pos += length;
                }
            } else if value_type == 0x58 {
                // byte string, uint8 length
                if pos < data.len() {
                    let length = data[pos] as usize;
                    pos += 1;
                    if pos + length <= data.len() {
                        payload = data[pos..pos + length].to_vec();
                        pos += length;
                    }
                }
            } else if value_type == 0x59 {
                // byte string, uint16 length
                if pos + 1 < data.len() {
                    let length = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
                    pos += 2;
                    if pos + length <= data.len() {
                        payload = data[pos..pos + length].to_vec();
                        pos += length;
                    }
                }
            }
        }
        
        Ok(Frame::new(message_type, message_id, reliability, payload))
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
            stats: Arc::new(RwLock::new(SerializationStats::default())),
        }
    }
}

#[async_trait]
impl FrameSerializer for CborSerializer {
    fn format(&self) -> SerializationFormat {
        SerializationFormat::Cbor
    }
    
    async fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
        let start_time = Instant::now();
        
        // CBOR序列化
        let result = self.encode_cbor(frame);
        
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
                Err(e)
            }
        }
    }
    
    async fn deserialize(&self, data: &[u8]) -> Result<Frame> {
        let start_time = Instant::now();
        
        // 检查大小限制
        self.check_size_limit(data.len())?;
        
        // CBOR反序列化
        let result = self.decode_cbor(data);
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
        // CBOR大小估算
        let base_size = 20; // 基础字段和结构开销
        let payload_size = frame.get_payload().len();
        Ok(base_size + payload_size)
    }
    
    async fn validate(&self, data: &[u8]) -> Result<bool> {
        // CBOR格式验证
        if data.is_empty() {
            return Ok(false);
        }
        
        // 检查CBOR map标记
        Ok(data[0] == 0xA4) // map with 4 items
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
    use crate::common::protocol::{Frame, MessageType, Reliability};
    
    #[tokio::test]
    async fn test_cbor_serializer_basic() {
        let serializer = CborSerializer::new();
        
        let frame = Frame::new(
            MessageType::Data,
            42,
            Reliability::AtLeastOnce,
            b"Hello CBOR!".to_vec(),
        );
        
        // 测试序列化
        let serialized = serializer.serialize(&frame).await.unwrap();
        assert!(!serialized.is_empty());
        assert_eq!(serialized[0], 0xA4); // CBOR map marker
        
        // 测试反序列化
        let deserialized = serializer.deserialize(&serialized).await.unwrap();
        assert_eq!(deserialized.get_message_id(), frame.get_message_id());
        assert_eq!(deserialized.get_message_type(), frame.get_message_type());
        assert_eq!(deserialized.get_reliability(), frame.get_reliability());
        assert_eq!(deserialized.get_payload(), frame.get_payload());
    }
    
    #[tokio::test]
    async fn test_cbor_validation() {
        let serializer = CborSerializer::new();
        
        // 有效的CBOR数据
        let valid_data = vec![0xA4, 0x64, 0x74, 0x79, 0x70, 0x65]; // map + "type"
        assert!(serializer.validate(&valid_data).await.unwrap());
        
        // 无效的数据
        let invalid_data = vec![0xFF, 0xFF];
        assert!(!serializer.validate(&invalid_data).await.unwrap());
        
        // 空数据
        assert!(!serializer.validate(&[]).await.unwrap());
    }
    
    #[tokio::test]
    async fn test_cbor_compact_size() {
        let serializer = CborSerializer::new();
        
        // 测试小消息的紧凑性
        let small_frame = Frame::new(
            MessageType::Heartbeat,
            1,
            Reliability::BestEffort,
            Vec::new(),
        );
        
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
        
        let payload_sizes = vec![0, 10, 23, 24, 100, 255, 256, 1000];
        
        for size in payload_sizes {
            let payload = vec![0x42u8; size]; // 填充字节
            let frame = Frame::new(
                MessageType::Data,
                size as u64,
                Reliability::AtLeastOnce,
                payload.clone(),
            );
            
            let serialized = serializer.serialize(&frame).await.unwrap();
            let deserialized = serializer.deserialize(&serialized).await.unwrap();
            
            assert_eq!(deserialized.get_payload().len(), size);
            assert_eq!(deserialized.get_payload(), &payload);
            
            println!("载荷{}字节 -> CBOR{}字节", size, serialized.len());
        }
    }
    
    #[tokio::test]
    async fn test_cbor_performance() {
        let serializer = CborSerializer::new();
        
        let frame = Frame::new(
            MessageType::Data,
            12345,
            Reliability::AtLeastOnce,
            vec![0u8; 256], // 256字节数据
        );
        
        // 性能测试 - 批量操作
        let iterations = 1000;
        let start = std::time::Instant::now();
        
        for _ in 0..iterations {
            let serialized = serializer.serialize(&frame).await.unwrap();
            let _ = serializer.deserialize(&serialized).await.unwrap();
        }
        
        let duration = start.elapsed();
        let avg_per_op = duration / iterations;
        
        println!("CBOR平均操作时间: {:?}", avg_per_op);
        
        // 每次操作应该远小于15ms
        assert!(avg_per_op.as_millis() < 1);
        
        let stats = serializer.stats();
        assert_eq!(stats.serialize_count, iterations as u64);
        assert_eq!(stats.deserialize_count, iterations as u64);
        assert!(stats.avg_serialize_time_us > 0);
        assert!(stats.avg_deserialize_time_us > 0);
    }
    
    #[tokio::test]
    async fn test_cbor_iot_scenario() {
        // 模拟IoT场景 - 小消息，频繁传输
        let serializer = CborSerializer::new();
        
        // IoT传感器数据
        let sensor_data = Frame::new(
            MessageType::Data,
            1001,
            Reliability::BestEffort,
            b"temp:23.5,hum:65.2".to_vec(), // 典型传感器数据
        );
        
        let cbor_data = serializer.serialize(&sensor_data).await.unwrap();
        
        // CBOR应该对此类数据非常高效
        assert!(cbor_data.len() < 50); // 应该小于50字节
        
        let restored = serializer.deserialize(&cbor_data).await.unwrap();
        assert_eq!(
            std::str::from_utf8(restored.get_payload()).unwrap(),
            "temp:23.5,hum:65.2"
        );
    }
}