//! Protocol Buffers序列化器实现
//!
//! 提供高效的Protobuf二进制序列化支持，适合跨语言、有版本要求的通信

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

/// Protocol Buffers序列化器实现
#[derive(Debug)]
pub struct ProtobufSerializer {
    /// 序列化配置
    config: Arc<RwLock<SerializationConfig>>,
    /// 统计信息
    stats: Arc<RwLock<SerializationStats>>,
}

impl ProtobufSerializer {
    /// 创建新的Protobuf序列化器
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(SerializationConfig::default())),
            stats: Arc::new(RwLock::new(SerializationStats::default())),
        }
    }
    
    /// 创建带配置的Protobuf序列化器
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
    
    /// Protobuf编码 - 模拟实现
    fn encode_protobuf(&self, frame: &Frame) -> Result<Vec<u8>> {
        // 这里是Protobuf编码的模拟实现
        // 实际项目中应该使用 prost 或 protobuf 库
        
        let mut buf = Vec::new();
        
        // Protobuf wire format模拟
        // 字段1: message_type (varint)
        buf.push(0x08); // field 1, varint
        buf.push(frame.get_message_type() as u8);
        
        // 字段2: message_id (varint) 
        buf.push(0x10); // field 2, varint
        let msg_id = frame.get_message_id();
        if msg_id < 128 {
            buf.push(msg_id as u8);
        } else {
            // 简化的varint编码
            buf.push((msg_id as u8) | 0x80);
            buf.push((msg_id >> 7) as u8);
        }
        
        // 字段3: reliability (varint)
        buf.push(0x18); // field 3, varint
        buf.push(frame.get_reliability() as u8);
        
        // 字段4: payload (length-delimited)
        let payload = frame.get_payload();
        if !payload.is_empty() {
            buf.push(0x22); // field 4, length-delimited
            buf.push(payload.len() as u8); // 简化长度编码
            buf.extend_from_slice(payload);
        }
        
        // 字段5: timestamp (varint) - 添加当前时间戳
        buf.push(0x28); // field 5, varint
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        buf.push(timestamp as u8);
        
        Ok(buf)
    }
    
    /// Protobuf解码 - 模拟实现
    fn decode_protobuf(&self, data: &[u8]) -> Result<Frame> {
        if data.is_empty() {
            return Err(FlareError::deserialization_failed("空Protobuf数据".to_string()));
        }
        
        let mut pos = 0;
        let mut message_type = crate::common::protocol::MessageType::Data;
        let mut message_id = 0u64;
        let mut reliability = crate::common::protocol::Reliability::AtLeastOnce;
        let mut payload = Vec::new();
        
        // 简化的Protobuf解析
        while pos < data.len() {
            if pos >= data.len() {
                break;
            }
            
            let field_tag = data[pos];
            pos += 1;
            
            match field_tag {
                0x08 => {
                    // message_type
                    if pos < data.len() {
                        let val = data[pos];
                        pos += 1;
                        message_type = match val {
                            1 => crate::common::protocol::MessageType::Heartbeat,
                            2 => crate::common::protocol::MessageType::HeartbeatAck,
                            3 => crate::common::protocol::MessageType::Connect,
                            4 => crate::common::protocol::MessageType::ConnectAck,
                            5 => crate::common::protocol::MessageType::Disconnect,
                            6 => crate::common::protocol::MessageType::DisconnectAck,
                            7 => crate::common::protocol::MessageType::Data,
                            8 => crate::common::protocol::MessageType::DataAck,
                            9 => crate::common::protocol::MessageType::Retransmit,
                            10 => crate::common::protocol::MessageType::ProtocolSwitch,
                            11 => crate::common::protocol::MessageType::ProtocolTest,
                            12 => crate::common::protocol::MessageType::Error,
                            13 => crate::common::protocol::MessageType::Notification,
                            17 => crate::common::protocol::MessageType::CustomEvent,
                            18 => crate::common::protocol::MessageType::CustomMessage,
                            _ => crate::common::protocol::MessageType::Data,
                        };
                    }
                }
                0x10 => {
                    // message_id
                    if pos < data.len() {
                        message_id = data[pos] as u64;
                        pos += 1;
                        // 处理多字节varint
                        if message_id >= 128 {
                            message_id = (message_id & 0x7F) | ((data[pos] as u64) << 7);
                            pos += 1;
                        }
                    }
                }
                0x18 => {
                    // reliability
                    if pos < data.len() {
                        let val = data[pos];
                        pos += 1;
                        reliability = match val {
                            0 => crate::common::protocol::Reliability::BestEffort,
                            1 => crate::common::protocol::Reliability::AtLeastOnce,
                            2 => crate::common::protocol::Reliability::ExactlyOnce,
                            _ => crate::common::protocol::Reliability::AtLeastOnce,
                        };
                    }
                }
                0x22 => {
                    // payload
                    if pos < data.len() {
                        let length = data[pos] as usize;
                        pos += 1;
                        if pos + length <= data.len() {
                            payload = data[pos..pos + length].to_vec();
                            pos += length;
                        }
                    }
                }
                0x28 => {
                    // timestamp - 跳过
                    if pos < data.len() {
                        pos += 1; // 跳过时间戳
                    }
                }
                _ => {
                    // 未知字段，跳过
                    break;
                }
            }
        }
        
        Ok(Frame::new(message_type, message_id, reliability, payload))
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
            stats: Arc::new(RwLock::new(SerializationStats::default())),
        }
    }
}

#[async_trait]
impl FrameSerializer for ProtobufSerializer {
    fn format(&self) -> SerializationFormat {
        SerializationFormat::Protobuf
    }
    
    async fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
        let start_time = Instant::now();
        
        // Protobuf序列化
        let result = self.encode_protobuf(frame);
        
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
        
        // Protobuf反序列化
        let result = self.decode_protobuf(data);
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
        // Protobuf大小估算
        let base_size = 10; // 基础字段大小
        let payload_size = frame.get_payload().len();
        Ok(base_size + payload_size)
    }
    
    async fn validate(&self, data: &[u8]) -> Result<bool> {
        // Protobuf格式验证
        if data.is_empty() {
            return Ok(false);
        }
        
        // 简单验证：检查是否包含有效的字段标签
        for &byte in data.iter().take(10) {
            if matches!(byte, 0x08 | 0x10 | 0x18 | 0x22 | 0x28) {
                return Ok(true);
            }
        }
        
        Ok(false)
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
    async fn test_protobuf_validation() {
        let serializer = ProtobufSerializer::new();
        
        // 有效的Protobuf数据
        let valid_data = vec![0x08, 0x01, 0x10, 0x5A]; // message_type=1, message_id=90
        assert!(serializer.validate(&valid_data).await.unwrap());
        
        // 无效的数据
        let invalid_data = vec![0xFF, 0xFF, 0xFF];
        assert!(!serializer.validate(&invalid_data).await.unwrap());
        
        // 空数据
        assert!(!serializer.validate(&[]).await.unwrap());
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
        
        // Protobuf对于小消息应该更紧凑
        assert!(protobuf_data.len() < json_data.len());
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
        let iterations = 1000;
        let start = std::time::Instant::now();
        
        for _ in 0..iterations {
            let serialized = serializer.serialize(&frame).await.unwrap();
            let _ = serializer.deserialize(&serialized).await.unwrap();
        }
        
        let duration = start.elapsed();
        let avg_per_op = duration / iterations;
        
        println!("Protobuf平均操作时间: {:?}", avg_per_op);
        
        // 每次序列化+反序列化应该远小于15ms
        assert!(avg_per_op.as_millis() < 1); // 小于1ms
    }
}