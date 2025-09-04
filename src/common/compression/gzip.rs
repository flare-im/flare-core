//! Gzip压缩器实现
//!
//! 提供高压缩比的Gzip压缩支持，适合存储和非实时传输场景

use async_trait::async_trait;
use std::sync::{Arc, RwLock};

use crate::common::{
    error::{Result, FlareError},
    compression::traits::{
        Compressor, CompressionFormat, CompressionConfig,
        CompressionResult, ConfigurableCompressor, CompressorFeature,
    },
};

/// Gzip压缩器实现
#[derive(Debug)]
pub struct GzipCompressor {
    /// 压缩配置
    config: Arc<RwLock<CompressionConfig>>,
}

impl GzipCompressor {
    /// 创建新的Gzip压缩器
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(CompressionConfig::high_compression())),
        }
    }
    
    /// 创建带配置的Gzip压缩器
    pub fn with_config(config: CompressionConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
        }
    }
    
    /// Gzip压缩实现 - 使用flate2库
    fn compress_gzip(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        
        let compression_level = self.config.read()
            .map(|c| c.compression_level)
            .unwrap_or(6);
        
        // 使用flate2进行Gzip压缩
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        
        let mut encoder = GzEncoder::new(Vec::new(), Compression::new(compression_level as u32));
        match encoder.write_all(data) {
            Ok(_) => {
                match encoder.finish() {
                    Ok(compressed) => Ok(compressed),
                    Err(e) => Err(FlareError::general_error(
                        format!("Gzip压缩完成失败: {}", e)
                    ))
                }
            }
            Err(e) => Err(FlareError::general_error(
                format!("Gzip压缩失败: {}", e)
            ))
        }
    }
    
    /// Gzip解压实现 - 使用flate2库
    fn decompress_gzip(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        
        // 使用flate2进行Gzip解压
        use flate2::read::GzDecoder;
        use std::io::Read;
        
        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        match decoder.read_to_end(&mut decompressed) {
            Ok(_) => Ok(decompressed),
            Err(e) => Err(FlareError::general_error(
                format!("Gzip解压失败: {}", e)
            ))
        }
    }
}

impl Default for GzipCompressor {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for GzipCompressor {
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
impl Compressor for GzipCompressor {
    fn format(&self) -> CompressionFormat {
        CompressionFormat::Gzip
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
        let result = self.compress_gzip(data);
        
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
        // 尝试用Gzip解压，如果失败则认为是未压缩数据
        match self.decompress_gzip(data) {
            Ok(decompressed_data) => Ok(decompressed_data),
            Err(_) => {
                // 解压失败，可能是未压缩数据，直接返回
                Ok(data.to_vec())
            }
        }
    }
    
    fn name(&self) -> &'static str {
        "GzipCompressor"
    }
    
    fn version(&self) -> &'static str {
        "1.0.0"
    }
    
    fn description(&self) -> &'static str {
        "Gzip高压缩比压缩器，适合存储和非实时传输场景"
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
        // Gzip估算：假设压缩比为60%（更好的压缩比）
        if self.should_compress(data) {
            Ok((data.len() as f64 * 0.6) as usize)
        } else {
            Ok(data.len())
        }
    }
    
    fn clone_box(&self) -> Box<dyn Compressor> {
        Box::new(self.clone())
    }
    
    fn supported_features(&self) -> Vec<CompressorFeature> {
        vec![CompressorFeature::HighRatio]
    }
    
    fn mime_type(&self) -> &'static str {
        "application/gzip"
    }
}

#[async_trait]
impl ConfigurableCompressor for GzipCompressor {
    fn update_config(&mut self, config: CompressionConfig) -> Result<()> {
        self.set_config(config)
    }
    
    fn configurable_params(&self) -> Vec<&'static str> {
        vec![
            "min_compress_size",
            "max_compress_size",
            "compression_level",
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
        
        if config.compression_level == 0 || config.compression_level > 9 {
            return Err(FlareError::general_error(
                "Gzip压缩级别必须在1-9之间"
            ));
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_gzip_compressor_basic() {
        let compressor = GzipCompressor::new();
        
        let data = b"Hello, Gzip compression! This is a test message for high compression ratio testing.".to_vec();
        
        // 测试压缩
        let result = compressor.compress(&data).await.unwrap();
        assert!(!result.data.is_empty());
        
        // 测试解压
        let decompressed = compressor.decompress(&result.data).await.unwrap();
        assert_eq!(decompressed, data);
    }
    
    #[tokio::test]
    async fn test_gzip_high_compression() {
        let compressor = GzipCompressor::new();
        
        // 创建重复数据进行压缩测试
        let mut data = Vec::new();
        for _ in 0..100 {
            data.extend_from_slice(b"REPEATED_PATTERN_FOR_COMPRESSION_TEST");
        }
        
        let result = compressor.compress(&data).await.unwrap();
        
        assert!(result.was_compressed);
        assert!(result.compressed_size < result.original_size);
        
        // Gzip应该有很好的压缩比
        let compression_ratio = result.ratio();
        assert!(compression_ratio < 0.3); // 压缩比应该小于30%
        
        let decompressed = compressor.decompress(&result.data).await.unwrap();
        assert_eq!(decompressed, data);
    }
    
    #[tokio::test]
    async fn test_gzip_small_data_no_compression() {
        let compressor = GzipCompressor::new();
        
        // 小数据不应该被压缩
        let small_data = b"small".to_vec();
        let result = compressor.compress(&small_data).await.unwrap();
        
        assert!(!result.was_compressed);
        assert_eq!(result.data, small_data);
    }
}