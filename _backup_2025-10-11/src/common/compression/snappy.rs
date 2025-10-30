//! Snappy压缩器实现
//!
//! 提供平衡的Snappy压缩支持，Google开发的高性能压缩库

use async_trait::async_trait;
use std::sync::{Arc, RwLock};

use crate::common::{
    error::{Result, FlareError},
    compression::traits::{
        Compressor, CompressionFormat, CompressionConfig,
        CompressionResult, ConfigurableCompressor, CompressorFeature,
    },
};

/// Snappy压缩器实现
#[derive(Debug)]
pub struct SnappyCompressor {
    /// 压缩配置
    config: Arc<RwLock<CompressionConfig>>,
}

impl SnappyCompressor {
    /// 创建新的Snappy压缩器
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(CompressionConfig::balanced())),
        }
    }
    
    /// 创建带配置的Snappy压缩器
    pub fn with_config(config: CompressionConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
        }
    }
    
    /// Snappy压缩实现 - 使用snap库
    fn compress_snappy(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        
        // 使用snap库进行高性能Snappy压缩
        let mut encoder = snap::raw::Encoder::new();
        match encoder.compress_vec(data) {
            Ok(compressed) => Ok(compressed),
            Err(e) => Err(FlareError::general_error(
                format!("Snappy压缩失败: {}", e)
            ))
        }
    }
    
    /// Snappy解压实现 - 使用snap库
    fn decompress_snappy(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        
        // 使用snap库进行解压
        let mut decoder = snap::raw::Decoder::new();
        match decoder.decompress_vec(data) {
            Ok(decompressed) => Ok(decompressed),
            Err(e) => Err(FlareError::general_error(
                format!("Snappy解压失败: {}", e)
            ))
        }
    }
}

impl Default for SnappyCompressor {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for SnappyCompressor {
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
impl Compressor for SnappyCompressor {
    fn format(&self) -> CompressionFormat {
        CompressionFormat::Snappy
    }
    
    async fn compress(&self, data: &[u8]) -> Result<CompressionResult> {
        // 检查是否需要压缩
        if !self.should_compress(data) {
            return Ok(CompressionResult {
                data: data.to_vec(),
                original_size: data.len(),
                compressed_size: data.len(),
                was_compressed: false,
            });
        }
        
        // 执行压缩
        let result = self.compress_snappy(data);
        
        match result {
            Ok(compressed_data) => {
                let was_compressed = compressed_data.len() < data.len();
                let final_data = if was_compressed {
                    compressed_data
                } else {
                    data.to_vec()
                };
                let final_size = final_data.len();
                
                Ok(CompressionResult {
                    data: final_data,
                    original_size: data.len(),
                    compressed_size: final_size,
                    was_compressed,
                })
            }
            Err(e) => Err(e)
        }
    }
    
    async fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        // 尝试用Snappy解压，如果失败则认为是未压缩数据
        match self.decompress_snappy(data) {
            Ok(decompressed_data) => Ok(decompressed_data),
            Err(_) => {
                // 解压失败，可能是未压缩数据，直接返回
                Ok(data.to_vec())
            }
        }
    }
    
    fn name(&self) -> &'static str {
        "SnappyCompressor"
    }
    
    fn version(&self) -> &'static str {
        "1.0.0"
    }
    
    fn description(&self) -> &'static str {
        "Snappy快速压缩器，平衡压缩速度和压缩比，适合实时数据传输"
    }
    
    fn config(&self) -> CompressionConfig {
        self.config.read()
            .map(|c| c.clone())
            .unwrap_or_default()
    }
    
    fn set_config(&mut self, config: CompressionConfig) -> Result<()> {
        if let Ok(mut current_config) = self.config.write() {
            *current_config = config;
            Ok(())
        } else {
            Err(FlareError::general_error("无法获取配置写锁"))
        }
    }
    
    async fn estimate_compressed_size(&self, data: &[u8]) -> Result<usize> {
        // Snappy估算：假设压缩比为80%
        if self.should_compress(data) {
            Ok((data.len() as f64 * 0.8) as usize)
        } else {
            Ok(data.len())
        }
    }
    
    fn clone_box(&self) -> Box<dyn Compressor> {
        Box::new(self.clone())
    }
    
    fn supported_features(&self) -> Vec<CompressorFeature> {
        vec![]
    }
    
    fn mime_type(&self) -> &'static str {
        "application/x-snappy"
    }
}

#[async_trait]
impl ConfigurableCompressor for SnappyCompressor {
    fn update_config(&mut self, config: CompressionConfig) -> Result<()> {
        self.set_config(config)
    }
    
    fn configurable_params(&self) -> Vec<&'static str> {
        vec![
            "min_compress_size",
            "max_compress_size",
            "timeout_ms",
        ]
    }
    
    fn validate_config(&self, config: &CompressionConfig) -> Result<()> {
        if config.min_compress_size == 0 {
            return Err(FlareError::general_error(
                "最小压缩大小不能为0"
            ));
        }
        
        if config.timeout_ms == 0 {
            return Err(FlareError::general_error(
                "超时时间不能为0"
            ));
        }
        
        // Snappy不支持字典压缩
        if config.enable_dictionary {
            return Err(FlareError::general_error(
                "Snappy不支持字典压缩"
            ));
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_snappy_compressor_basic() {
        let compressor = SnappyCompressor::new();
        
        let data = b"Hello, Snappy compression! This is a test message for compression.".to_vec();
        
        // 测试压缩
        let result = compressor.compress(&data).await.unwrap();
        assert!(!result.data.is_empty());
        
        // 测试解压
        let decompressed = compressor.decompress(&result.data).await.unwrap();
        assert_eq!(decompressed, data);
    }
    
    #[tokio::test]
    async fn test_snappy_repetitive_data() {
        let compressor = SnappyCompressor::new();
        
        // 创建有重复模式的数据
        let mut data = Vec::new();
        for _ in 0..100 {
            data.extend_from_slice(b"ABCDEFGH");
        }
        
        let result = compressor.compress(&data).await.unwrap();
        assert!(result.was_compressed);
        assert!(result.compressed_size < result.original_size);
        
        let decompressed = compressor.decompress(&result.data).await.unwrap();
        assert_eq!(decompressed, data);
        
        println!("Snappy压缩：{}字节 -> {}字节 (压缩比: {:.2}%)", 
                 result.original_size, 
                 result.compressed_size,
                 result.ratio() * 100.0);
    }
    
    #[tokio::test]
    async fn test_snappy_performance() {
        let compressor = SnappyCompressor::new();
        
        let data = vec![0u8; 8192]; // 8KB数据
        
        // 性能测试
        let iterations = 50;
        let start = std::time::Instant::now();
        
        for _ in 0..iterations {
            let result = compressor.compress(&data).await.unwrap();
            let _ = compressor.decompress(&result.data).await.unwrap();
        }
        
        let duration = start.elapsed();
        let avg_per_op = duration / iterations;
        
        println!("Snappy平均操作时间: {:?}", avg_per_op);
        
        // Snappy应该比较快，满足延迟要求
        assert!(avg_per_op.as_millis() < 10); // 小于10ms
    }
}