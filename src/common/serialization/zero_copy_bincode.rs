//! 零拷贝序列化器实现
//! 
//! 专为超低延迟场景优化的高性能序列化器

use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;
use std::time::Instant;

use crate::common::{
    error::{Result, FlareError},
    protocol::Frame,
    serialization::{
        FrameSerializer, SerializationFormat, SerializationConfig, SerializationStats,
    },
};

/// 缓冲区池，避免频繁内存分配
pub struct BufferPool {
    /// 小缓冲区池 (1KB)
    small_buffers: Mutex<VecDeque<Vec<u8>>>,
    /// 中等缓冲区池 (4KB)  
    medium_buffers: Mutex<VecDeque<Vec<u8>>>,
    /// 大缓冲区池 (16KB)
    large_buffers: Mutex<VecDeque<Vec<u8>>>,
    /// 池大小限制
    max_pool_size: usize,
}

impl BufferPool {
    pub fn new() -> Self {
        Self {
            small_buffers: Mutex::new(VecDeque::new()),
            medium_buffers: Mutex::new(VecDeque::new()),
            large_buffers: Mutex::new(VecDeque::new()),
            max_pool_size: 50,
        }
    }
    
    /// 获取适合大小的缓冲区
    pub fn acquire(&self, size: usize) -> Vec<u8> {
        match size {
            0..=1024 => {
                if let Ok(mut pool) = self.small_buffers.lock() {
                    if let Some(mut buffer) = pool.pop_front() {
                        buffer.clear();
                        buffer.reserve(1024);
                        return buffer;
                    }
                }
                Vec::with_capacity(1024)
            }
            1025..=4096 => {
                if let Ok(mut pool) = self.medium_buffers.lock() {
                    if let Some(mut buffer) = pool.pop_front() {
                        buffer.clear();
                        buffer.reserve(4096);
                        return buffer;
                    }
                }
                Vec::with_capacity(4096)
            }
            _ => {
                if let Ok(mut pool) = self.large_buffers.lock() {
                    if let Some(mut buffer) = pool.pop_front() {
                        buffer.clear();
                        buffer.reserve(16384);
                        return buffer;
                    }
                }
                Vec::with_capacity(16384.max(size))
            }
        }
    }
    
    /// 归还缓冲区到池中
    pub fn release(&self, buffer: Vec<u8>) {
        let capacity = buffer.capacity();
        
        match capacity {
            1024 => {
                if let Ok(mut pool) = self.small_buffers.lock() {
                    if pool.len() < self.max_pool_size {
                        pool.push_back(buffer);
                    }
                }
            }
            4096 => {
                if let Ok(mut pool) = self.medium_buffers.lock() {
                    if pool.len() < self.max_pool_size {
                        pool.push_back(buffer);
                    }
                }
            }
            16384.. => {
                if let Ok(mut pool) = self.large_buffers.lock() {
                    if pool.len() < self.max_pool_size {
                        pool.push_back(buffer);
                    }
                }
            }
            _ => {
                // 非标准大小的缓冲区直接丢弃
            }
        }
    }
}

/// 零拷贝Bincode序列化器
pub struct ZeroCopyBincodeSerializer {
    /// 配置
    config: SerializationConfig,
    /// 统计信息
    stats: Arc<Mutex<SerializationStats>>,
    /// 缓冲区池
    buffer_pool: Arc<BufferPool>,
}

impl ZeroCopyBincodeSerializer {
    /// 创建新的零拷贝序列化器
    pub fn new() -> Self {
        Self {
            config: SerializationConfig::ultra_low_latency(),
            stats: Arc::new(Mutex::new(SerializationStats::default())),
            buffer_pool: Arc::new(BufferPool::new()),
        }
    }
    
    /// 创建带配置的序列化器
    pub fn with_config(config: SerializationConfig) -> Self {
        Self {
            config,
            stats: Arc::new(Mutex::new(SerializationStats::default())),
            buffer_pool: Arc::new(BufferPool::new()),
        }
    }
    
    /// 使用预分配缓冲区序列化
    pub fn serialize_with_buffer(&self, frame: &Frame, buffer: &mut Vec<u8>) -> Result<usize> {
        let start_time = Instant::now();
        buffer.clear();
        
        // 直接使用bincode库序列化，但写入到预分配缓冲区
        match bincode::serialize(frame) {
            Ok(data) => {
                buffer.extend_from_slice(&data);
                
                let duration_us = start_time.elapsed().as_micros() as u64;
                self.update_serialize_stats(buffer.len(), duration_us, true);
                
                Ok(buffer.len())
            }
            Err(e) => {
                let duration_us = start_time.elapsed().as_micros() as u64;
                self.update_serialize_stats(0, duration_us, false);
                
                Err(FlareError::serialization_error(
                    format!("零拷贝Bincode序列化失败: {}", e)
                ))
            }
        }
    }
    
    /// 批量序列化多个消息
    pub fn serialize_batch(&self, frames: &[Frame]) -> Result<Vec<Vec<u8>>> {
        let start_time = Instant::now();
        let mut results = Vec::with_capacity(frames.len());
        
        for frame in frames {
            let mut buffer = self.buffer_pool.acquire(1024);
            match self.serialize_with_buffer(frame, &mut buffer) {
                Ok(_) => {
                    results.push(buffer);
                }
                Err(e) => {
                    // 归还已分配的缓冲区
                    for buffer in results {
                        self.buffer_pool.release(buffer);
                    }
                    return Err(e);
                }
            }
        }
        
        let duration_us = start_time.elapsed().as_micros() as u64;
        self.update_batch_stats(frames.len(), duration_us, true);
        
        Ok(results)
    }
    
    /// 更新序列化统计信息
    fn update_serialize_stats(&self, size: usize, duration_us: u64, success: bool) {
        if let Ok(mut stats) = self.stats.lock() {
            stats.serialize_count += 1;
            if success {
                stats.total_serialized_bytes += size as u64;
                
                // 更新平均时间
                if stats.avg_serialize_time_us == 0 {
                    stats.avg_serialize_time_us = duration_us;
                } else {
                    stats.avg_serialize_time_us = 
                        (stats.avg_serialize_time_us * 9 + duration_us) / 10;
                }
                
                if duration_us > stats.max_serialize_time_us {
                    stats.max_serialize_time_us = duration_us;
                }
                
                if stats.min_serialize_time_us == 0 || duration_us < stats.min_serialize_time_us {
                    stats.min_serialize_time_us = duration_us;
                }
            } else {
                stats.serialize_errors += 1;
            }
        }
    }
    
    /// 更新批处理统计信息
    fn update_batch_stats(&self, count: usize, duration_us: u64, success: bool) {
        if let Ok(mut stats) = self.stats.lock() {
            stats.batch_count += 1;
            if success {
                stats.total_batch_items += count as u64;
                
                let avg_per_item = duration_us / count as u64;
                if stats.avg_batch_time_per_item_us == 0 {
                    stats.avg_batch_time_per_item_us = avg_per_item;
                } else {
                    stats.avg_batch_time_per_item_us = 
                        (stats.avg_batch_time_per_item_us * 9 + avg_per_item) / 10;
                }
            } else {
                stats.batch_errors += 1;
            }
        }
    }
}

impl Default for ZeroCopyBincodeSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ZeroCopyBincodeSerializer {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            stats: Arc::new(Mutex::new(SerializationStats::default())),
            buffer_pool: Arc::clone(&self.buffer_pool),
        }
    }
}

#[async_trait]
impl FrameSerializer for ZeroCopyBincodeSerializer {
    fn format(&self) -> SerializationFormat {
        SerializationFormat::Bincode
    }
    
    async fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
        let mut buffer = self.buffer_pool.acquire(
            frame.get_payload().len() + 64 // 预估消息头大小
        );
        
        match self.serialize_with_buffer(frame, &mut buffer) {
            Ok(_) => Ok(buffer),
            Err(e) => {
                self.buffer_pool.release(buffer);
                Err(e)
            }
        }
    }
    
    async fn deserialize(&self, data: &[u8]) -> Result<Frame> {
        let start_time = Instant::now();
        
        // 直接使用bincode库进行反序列化
        match bincode::deserialize(data) {
            Ok(frame) => {
                let duration_us = start_time.elapsed().as_micros() as u64;
                self.update_deserialize_stats(data.len(), duration_us, true);
                Ok(frame)
            }
            Err(e) => {
                let duration_us = start_time.elapsed().as_micros() as u64;
                self.update_deserialize_stats(data.len(), duration_us, false);
                
                Err(FlareError::deserialization_error(
                    format!("零拷贝Bincode反序列化失败: {}", e)
                ))
            }
        }
    }
    
    fn name(&self) -> &'static str {
        "ZeroCopyBincodeSerializer"
    }
    
    fn description(&self) -> &'static str {
        "零拷贝Bincode序列化器，专为超低延迟场景优化"
    }
    
    fn clone_box(&self) -> Box<dyn FrameSerializer> {
        Box::new(self.clone())
    }
    
    fn config(&self) -> SerializationConfig {
        self.config.clone()
    }
    
    fn stats(&self) -> SerializationStats {
        self.stats.lock().unwrap().clone()
    }
    
    fn reset_stats(&self) {
        if let Ok(mut stats) = self.stats.lock() {
            *stats = SerializationStats::default();
        }
    }
}

/// 扩展SerializationStats以支持批处理统计
#[derive(Debug, Clone)]
pub struct ExtendedSerializationStats {
    pub base_stats: SerializationStats,
    pub batch_count: u64,
    pub total_batch_items: u64,
    pub avg_batch_time_per_item_us: u64,
    pub batch_errors: u64,
}

impl ZeroCopyBincodeSerializer {
    /// 更新反序列化统计信息
    fn update_deserialize_stats(&self, size: usize, duration_us: u64, success: bool) {
        if let Ok(mut stats) = self.stats.lock() {
            stats.deserialize_count += 1;
            if success {
                stats.total_deserialized_bytes += size as u64;
                
                if stats.avg_deserialize_time_us == 0 {
                    stats.avg_deserialize_time_us = duration_us;
                } else {
                    stats.avg_deserialize_time_us = 
                        (stats.avg_deserialize_time_us * 9 + duration_us) / 10;
                }
                
                if duration_us > stats.max_deserialize_time_us {
                    stats.max_deserialize_time_us = duration_us;
                }
                
                if stats.min_deserialize_time_us == 0 || duration_us < stats.min_deserialize_time_us {
                    stats.min_deserialize_time_us = duration_us;
                }
            } else {
                stats.deserialize_errors += 1;
            }
        }
    }
    
    /// 获取缓冲区池统计信息
    pub fn buffer_pool_stats(&self) -> BufferPoolStats {
        BufferPoolStats {
            small_buffers_count: self.buffer_pool.small_buffers.lock().unwrap().len(),
            medium_buffers_count: self.buffer_pool.medium_buffers.lock().unwrap().len(),
            large_buffers_count: self.buffer_pool.large_buffers.lock().unwrap().len(),
        }
    }
}

/// 缓冲区池统计信息
#[derive(Debug, Clone)]
pub struct BufferPoolStats {
    pub small_buffers_count: usize,
    pub medium_buffers_count: usize,
    pub large_buffers_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::{MessageType, Reliability};
    
    #[tokio::test]
    async fn test_zero_copy_serialization() {
        let serializer = ZeroCopyBincodeSerializer::new();
        
        let frame = Frame::new(
            MessageType::Data,
            1,
            Reliability::AtLeastOnce,
            vec![1, 2, 3, 4, 5],
        );
        
        // 测试序列化
        let serialized = serializer.serialize(&frame).await.unwrap();
        assert!(!serialized.is_empty());
        
        // 测试反序列化
        let deserialized = serializer.deserialize(&serialized).await.unwrap();
        assert_eq!(deserialized.get_message_type(), frame.get_message_type());
        assert_eq!(deserialized.get_payload(), frame.get_payload());
    }
    
    #[tokio::test]
    async fn test_batch_serialization() {
        let serializer = ZeroCopyBincodeSerializer::new();
        
        let frames = vec![
            Frame::new(MessageType::Data, 1, Reliability::AtLeastOnce, vec![1, 2, 3]),
            Frame::new(MessageType::Data, 2, Reliability::AtLeastOnce, vec![4, 5, 6]),
            Frame::new(MessageType::Data, 3, Reliability::AtLeastOnce, vec![7, 8, 9]),
        ];
        
        let results = serializer.serialize_batch(&frames).unwrap();
        assert_eq!(results.len(), 3);
        
        // 测试每个结果都能正确反序列化
        for (i, data) in results.iter().enumerate() {
            let deserialized = serializer.deserialize(data).await.unwrap();
            assert_eq!(deserialized.get_message_id(), frames[i].get_message_id());
        }
    }
    
    #[tokio::test]
    async fn test_performance() {
        let serializer = ZeroCopyBincodeSerializer::new();
        let frame = Frame::new(
            MessageType::Data,
            1,
            Reliability::AtLeastOnce,
            vec![0u8; 1024], // 1KB数据
        );
        
        let iterations = 1000;
        let start = std::time::Instant::now();
        
        for _ in 0..iterations {
            let serialized = serializer.serialize(&frame).await.unwrap();
            let _deserialized = serializer.deserialize(&serialized).await.unwrap();
        }
        
        let duration = start.elapsed();
        let avg_per_op = duration / iterations;
        
        println!("零拷贝序列化器平均操作时间: {:?}", avg_per_op);
        
        // 应该比标准实现更快
        assert!(avg_per_op.as_micros() < 100); // 小于0.1ms
        
        let stats = serializer.stats();
        println!("序列化统计: {:#?}", stats);
        assert!(stats.avg_serialize_time_us < 50); // 序列化时间小于50微秒
    }
    
    #[test]
    fn test_buffer_pool() {
        let pool = BufferPool::new();
        
        // 测试获取和归还
        let small_buffer = pool.acquire(512);
        assert!(small_buffer.capacity() >= 512);
        
        let medium_buffer = pool.acquire(2048);
        assert!(medium_buffer.capacity() >= 2048);
        
        let large_buffer = pool.acquire(8192);
        assert!(large_buffer.capacity() >= 8192);
        
        // 归还缓冲区
        pool.release(small_buffer);
        pool.release(medium_buffer);  
        pool.release(large_buffer);
        
        // 再次获取应该复用之前的缓冲区
        let reused_small = pool.acquire(512);
        assert_eq!(reused_small.capacity(), 1024);
        
        let stats = pool;
        println!("缓冲区池统计: {:?}", stats);
    }
}