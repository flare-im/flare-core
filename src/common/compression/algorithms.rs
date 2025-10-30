//! 内置压缩算法实现

use crate::common::error::FlareError;
use super::compressor::Compressor;
use super::config::CompressionLevel;

/// LZ4 压缩器（速度快）
pub struct Lz4Compressor;

impl Lz4Compressor {
    pub fn new() -> Self {
        Self
    }
}

impl Compressor for Lz4Compressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError> {
        Ok(lz4_flex::compress_prepend_size(data))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError> {
        lz4_flex::decompress_size_prepended(data)
            .map_err(|e| FlareError::compression_error(format!("LZ4 解压失败: {}", e)))
    }

    fn name(&self) -> &str {
        "lz4"
    }

    fn algorithm_id(&self) -> u32 {
        1
    }
}

/// Snappy 压缩器（平衡速度和压缩率）
pub struct SnappyCompressor;

impl SnappyCompressor {
    pub fn new() -> Self {
        Self
    }
}

impl Compressor for SnappyCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError> {
        let mut encoder = snap::raw::Encoder::new();
        encoder.compress_vec(data)
            .map_err(|e| FlareError::compression_error(format!("Snappy 压缩失败: {}", e)))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError> {
        let mut decoder = snap::raw::Decoder::new();
        decoder.decompress_vec(data)
            .map_err(|e| FlareError::compression_error(format!("Snappy 解压失败: {}", e)))
    }

    fn name(&self) -> &str {
        "snappy"
    }

    fn algorithm_id(&self) -> u32 {
        2
    }
}

/// Gzip 压缩器（高压缩率）
pub struct GzipCompressor {
    level: CompressionLevel,
}

impl GzipCompressor {
    pub fn new(level: CompressionLevel) -> Self {
        Self { level }
    }
}

impl Compressor for GzipCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError> {
        use flate2::write::GzEncoder;
        use std::io::Write;

        let mut encoder = GzEncoder::new(Vec::new(), self.level.to_flate2_level());
        encoder.write_all(data)
            .map_err(|e| FlareError::compression_error(format!("Gzip 压缩失败: {}", e)))?;
        encoder.finish()
            .map_err(|e| FlareError::compression_error(format!("Gzip 压缩完成失败: {}", e)))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError> {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)
            .map_err(|e| FlareError::compression_error(format!("Gzip 解压失败: {}", e)))?;
        Ok(decompressed)
    }

    fn name(&self) -> &str {
        "gzip"
    }

    fn algorithm_id(&self) -> u32 {
        3
    }
}

/// Zlib 压缩器（兼容性好）
pub struct ZlibCompressor {
    level: CompressionLevel,
}

impl ZlibCompressor {
    pub fn new(level: CompressionLevel) -> Self {
        Self { level }
    }
}

impl Compressor for ZlibCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError> {
        use flate2::write::ZlibEncoder;
        use std::io::Write;

        let mut encoder = ZlibEncoder::new(Vec::new(), self.level.to_flate2_level());
        encoder.write_all(data)
            .map_err(|e| FlareError::compression_error(format!("Zlib 压缩失败: {}", e)))?;
        encoder.finish()
            .map_err(|e| FlareError::compression_error(format!("Zlib 压缩完成失败: {}", e)))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError> {
        use flate2::read::ZlibDecoder;
        use std::io::Read;

        let mut decoder = ZlibDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)
            .map_err(|e| FlareError::compression_error(format!("Zlib 解压失败: {}", e)))?;
        Ok(decompressed)
    }

    fn name(&self) -> &str {
        "zlib"
    }

    fn algorithm_id(&self) -> u32 {
        4
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_DATA: &[u8] = b"Hello, World! This is a test message for compression. It should be long enough to see compression benefits.";

    #[test]
    fn test_lz4_compressor() {
        let compressor = Lz4Compressor::new();
        
        let compressed = compressor.compress(TEST_DATA).unwrap();
        // LZ4 包含元数据，短数据压缩后可能更大，这是正常的
        // assert!(compressed.len() < TEST_DATA.len());
        
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, TEST_DATA);
    }

    #[test]
    fn test_snappy_compressor() {
        let compressor = SnappyCompressor::new();
        
        let compressed = compressor.compress(TEST_DATA).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, TEST_DATA);
    }

    #[test]
    fn test_gzip_compressor() {
        let compressor = GzipCompressor::new(CompressionLevel::Default);
        
        let compressed = compressor.compress(TEST_DATA).unwrap();
        assert!(compressed.len() < TEST_DATA.len());
        
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, TEST_DATA);
    }

    #[test]
    fn test_zlib_compressor() {
        let compressor = ZlibCompressor::new(CompressionLevel::Default);
        
        let compressed = compressor.compress(TEST_DATA).unwrap();
        assert!(compressed.len() < TEST_DATA.len());
        
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, TEST_DATA);
    }

    #[test]
    fn test_compression_levels() {
        let data = b"Test data for compression level comparison. " .repeat(10);
        let data_bytes = data.as_slice();
        
        let fastest = GzipCompressor::new(CompressionLevel::Fastest);
        let best = GzipCompressor::new(CompressionLevel::Best);
        
        let compressed_fastest = fastest.compress(data_bytes).unwrap();
        let compressed_best = best.compress(data_bytes).unwrap();
        
        // Best 压缩率应该更高（文件更小）
        assert!(compressed_best.len() <= compressed_fastest.len());
        
        // 但都应该能正确解压
        assert_eq!(fastest.decompress(&compressed_fastest).unwrap(), data_bytes);
        assert_eq!(best.decompress(&compressed_best).unwrap(), data_bytes);
    }
}
