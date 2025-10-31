//! 内置压缩算法实现
//! 
//! 提供常用的压缩算法实现

use crate::common::error::{FlareError, Result};
use super::traits::Compressor;

/// 压缩算法类型枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum CompressionAlgorithm {
    /// 不使用压缩
    None,
    /// Gzip 压缩
    Gzip,
    /// Zstd 压缩（待实现）
    Zstd,
}

impl CompressionAlgorithm {
    /// 从字符串转换为压缩算法
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "none" | "" => Some(CompressionAlgorithm::None),
            "gzip" => Some(CompressionAlgorithm::Gzip),
            "zstd" => Some(CompressionAlgorithm::Zstd),
            _ => None,
        }
    }

    /// 转换为字符串
    pub fn as_str(&self) -> &'static str {
        match self {
            CompressionAlgorithm::None => "none",
            CompressionAlgorithm::Gzip => "gzip",
            CompressionAlgorithm::Zstd => "zstd",
        }
    }
}

/// 无压缩器（直通）
pub struct NoCompressor;

impl Compressor for NoCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::None
    }

    fn name(&self) -> &'static str {
        "none"
    }

    fn can_detect(&self, _data: &[u8]) -> bool {
        // 无压缩不检测，总是作为后备选项
        false
    }
}

/// Gzip 压缩器
pub struct GzipCompressor;

impl Compressor for GzipCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        use std::io::Write;
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(data)
            .map_err(|e| FlareError::encoding_error(format!("Gzip compression failed: {}", e)))?;
        encoder
            .finish()
            .map_err(|e| FlareError::encoding_error(format!("Gzip compression finish failed: {}", e)))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        use std::io::Read;
        use flate2::read::GzDecoder;

        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| FlareError::encoding_error(format!("Gzip decompression failed: {}", e)))?;
        Ok(decompressed)
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Gzip
    }

    fn name(&self) -> &'static str {
        "gzip"
    }

    fn can_detect(&self, data: &[u8]) -> bool {
        // Gzip 魔数: 0x1f 0x8b
        data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b
    }
}

