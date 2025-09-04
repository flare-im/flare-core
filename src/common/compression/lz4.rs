//! LZ4压缩器实现
//!
//! 提供超快速的LZ4压缩支持，专为超低延迟场景优化

use async_trait::async_trait;
use std::sync::{Arc, RwLock};

use crate::common::{
    error::{Result, FlareError},
    compression::traits::{
        Compressor, CompressionFormat, CompressionConfig,
        CompressionResult, ConfigurableCompressor, CompressorFeature,
    },
};

/// LZ4压缩器实现
#[derive(Debug)]
pub struct Lz4Compressor {
    /// 压缩配置
    config: Arc<RwLock<CompressionConfig>>,
}

impl Lz4Compressor {
    /// 创建新的LZ4压缩器
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(CompressionConfig::default())),
        }
    }
    
    /// 创建带配置的LZ4压缩器
    pub fn with_config(config: CompressionConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
        }
    }
    
    /// 创建超快速配置的LZ4压缩器
    pub fn ultra_fast() -> Self {
        Self::with_config(CompressionConfig::ultra_low_latency())
    }
    
    /// 检查大小限制
    fn check_size_limit(&self, size: usize) -> Result<()> {
        if let Ok(config) = self.config.read() {
            if let Some(max_size) = config.max_compress_size {
                if size > max_size {
                    return Err(FlareError::general_error(
                        format!("数据大小({})超过压缩限制({})", size, max_size)
                    ));
                }
            }
        }
        Ok(())
    }
    
    /// LZ4压缩实现 - 使用lz4_flex库
    fn compress_lz4(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        
        // 使用lz4_flex进行高性能压缩
        let compressed = lz4_flex::compress_prepend_size(data);
        Ok(compressed)
    }
    
    /// LZ4解压实现 - 使用lz4_flex库
    fn decompress_lz4(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        
        // 使用lz4_flex进行解压
        match lz4_flex::decompress_size_prepended(data) {
            Ok(decompressed) => Ok(decompressed),
            Err(e) => Err(FlareError::general_error(
                format!("LZ4解压失败: {}", e)
            ))
        }
    }
    
    /// 获取支持的特性列表
    pub fn supported_features() -> Vec<CompressorFeature> {
        vec![
            CompressorFeature::UltraFast,
        ]
    }
}

impl Default for Lz4Compressor {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for Lz4Compressor {
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
impl Compressor for Lz4Compressor {
    fn format(&self) -> CompressionFormat {
        CompressionFormat::Lz4
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
        
        // 检查大小限制
        self.check_size_limit(data.len())?;
        
        // 执行压缩
        let result = self.compress_lz4(data);
        
        match result {
            Ok(compressed_data) => {
                // 检查压缩是否有效（压缩后更小）
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
        // 检查是否为LZ4压缩数据（更安全的检测）
        // 先尝试解压，如果失败说明可能不是LZ4格式
        if data.len() < 4 {
            // 太短的数据直接返回
            return Ok(data.to_vec());
        }
        
        // 尝试用LZ4解压，如果失败则认为是未压缩数据
        match self.decompress_lz4(data) {
            Ok(decompressed_data) => Ok(decompressed_data),
            Err(_) => {
                // 解压失败，可能是未压缩数据，直接返回
                Ok(data.to_vec())
            }
        }
    }
    
    fn name(&self) -> &'static str {
        "Lz4Compressor"
    }
    
    fn version(&self) -> &'static str {
        "1.0.0"
    }
    
    fn description(&self) -> &'static str {
        "LZ4超快速压缩器，专为超低延迟场景优化，压缩速度极快"
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
        // LZ4快速估算：假设压缩比为70%
        if self.should_compress(data) {
            Ok((data.len() as f64 * 0.7) as usize)
        } else {
            Ok(data.len())
        }
    }
    
    fn clone_box(&self) -> Box<dyn Compressor> {
        Box::new(self.clone())
    }
    
    fn supported_features(&self) -> Vec<CompressorFeature> {
        Self::supported_features()
    }
    
    fn mime_type(&self) -> &'static str {
        "application/x-lz4"
    }
}

#[async_trait]
impl ConfigurableCompressor for Lz4Compressor {
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
        
        if config.compression_level > 9 {
            return Err(FlareError::general_error(
                "LZ4压缩级别不能超过9"
            ));
        }
        
        // LZ4不支持字典压缩
        if config.enable_dictionary {
            return Err(FlareError::general_error(
                "LZ4不支持字典压缩"
            ));
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_lz4_compressor_basic() {
        let compressor = Lz4Compressor::new();
        
        let data = b"Hello, LZ4 compression! This is a test message.".to_vec();
        
        // 测试压缩
        let result = compressor.compress(&data).await.unwrap();
        assert!(!result.data.is_empty());
        
        // 测试解压
        let decompressed = compressor.decompress(&result.data).await.unwrap();
        assert_eq!(decompressed, data);
    }
    
    #[tokio::test]
    async fn test_lz4_ultra_fast() {
        let compressor = Lz4Compressor::ultra_fast();
        let config = compressor.config();
        
        assert_eq!(config.compression_level, 1);
        assert_eq!(config.timeout_ms, 2);
        assert_eq!(config.min_compress_size, 256);
        assert!(!config.enable_dictionary);
    }
    
    #[tokio::test]
    async fn test_lz4_small_data_no_compression() {
        let compressor = Lz4Compressor::new();
        
        // 小数据不应该被压缩
        let small_data = b"small".to_vec();
        let result = compressor.compress(&small_data).await.unwrap();
        
        assert!(!result.was_compressed);
        assert_eq!(result.data, small_data);
    }
    
    #[tokio::test]
    async fn test_lz4_repetitive_data() {
        let compressor = Lz4Compressor::new();
        
        // 重复数据应该压缩得很好
        let repetitive_data = vec![b'A'; 1000];
        let result = compressor.compress(&repetitive_data).await.unwrap();
        
        assert!(result.was_compressed);
        assert!(result.compressed_size < result.original_size);
        
        let decompressed = compressor.decompress(&result.data).await.unwrap();
        assert_eq!(decompressed, repetitive_data);
    }
    
    #[tokio::test]
    async fn test_lz4_performance() {
        let compressor = Lz4Compressor::ultra_fast();
        
        let data = vec![0u8; 10240]; // 10KB数据
        
        // 性能测试
        let iterations = 100;
        let start = std::time::Instant::now();
        
        for _ in 0..iterations {
            let result = compressor.compress(&data).await.unwrap();
            let _ = compressor.decompress(&result.data).await.unwrap();
        }
        
        let duration = start.elapsed();
        let avg_per_op = duration / iterations;
        
        println!("LZ4平均操作时间: {:?}", avg_per_op);
        
        // LZ4应该非常快，满足超低延迟要求
        assert!(avg_per_op.as_millis() < 5); // 小于5ms
    }
    
    #[tokio::test]
    async fn test_lz4_basic_functionality() {
        let compressor = Lz4Compressor::new();
        
        let data = vec![b'X'; 500];
        
        // 执行多次操作测试基本功能
        for _ in 0..10 {
            let result = compressor.compress(&data).await.unwrap();
            let decompressed = compressor.decompress(&result.data).await.unwrap();
            assert_eq!(decompressed, data);
        }
    }
}