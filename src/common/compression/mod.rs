//! 消息压缩模块
//!
//! 提供可配置的压缩功能，支持：
//! - 内置算法：LZ4、Snappy、Gzip、Zlib
//! - 自定义算法：用户可实现 `Compressor` trait
//! - 灵活配置：通过 `CompressionConfig` 选择算法

mod algorithms;
mod config;
mod compressor;

pub use algorithms::{Lz4Compressor, SnappyCompressor, GzipCompressor, ZlibCompressor};
pub use config::{CompressionConfig, CompressionAlgorithm, CompressionLevel};
pub use compressor::{Compressor, CompressorFactory, CustomCompressorRegistry};

use crate::common::error::FlareError;

/// 压缩数据
/// 
/// # 参数
/// - `data`: 原始数据
/// - `config`: 压缩配置
/// 
/// # 返回
/// - `Ok(Vec<u8>)`: 压缩后的数据
/// - `Err(FlareError)`: 压缩失败
pub fn compress(data: &[u8], config: &CompressionConfig) -> Result<Vec<u8>, FlareError> {
    let compressor = CompressorFactory::create(config)?;
    compressor.compress(data)
}

/// 解压数据
/// 
/// # 参数
/// - `data`: 压缩后的数据
/// - `config`: 压缩配置（用于确定算法）
/// 
/// # 返回
/// - `Ok(Vec<u8>)`: 解压后的数据
/// - `Err(FlareError)`: 解压失败
pub fn decompress(data: &[u8], config: &CompressionConfig) -> Result<Vec<u8>, FlareError> {
    let compressor = CompressorFactory::create(config)?;
    compressor.decompress(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_lz4() {
        let config = CompressionConfig::new(CompressionAlgorithm::Lz4);
        let data = b"Hello, World! This is a test message for compression.";
        
        let compressed = compress(data, &config).unwrap();
        // LZ4 包含元数据，短数据压缩后可能更大
        // assert!(compressed.len() < data.len());
        
        let decompressed = decompress(&compressed, &config).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_decompress_snappy() {
        let config = CompressionConfig::new(CompressionAlgorithm::Snappy);
        let data = b"Hello, World! This is a test message for compression.";
        
        let compressed = compress(data, &config).unwrap();
        let decompressed = decompress(&compressed, &config).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_decompress_gzip() {
        let config = CompressionConfig::new(CompressionAlgorithm::Gzip);
        let data = b"Hello, World! This is a test message for compression.";
        
        let compressed = compress(data, &config).unwrap();
        let decompressed = decompress(&compressed, &config).unwrap();
        assert_eq!(decompressed, data);
    }
}
