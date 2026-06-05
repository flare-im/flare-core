//! 内置压缩算法实现
//!
//! 提供常用的压缩算法实现

use super::traits::Compressor;
use crate::common::error::{FlareError, Result};

/// 压缩算法类型枚举
///
/// 支持内置算法和自定义算法扩展
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum CompressionAlgorithm {
    /// 不使用压缩
    None,
    /// Gzip 压缩
    Gzip,
    /// Zstd 压缩（待实现）
    Zstd,
    /// 自定义压缩算法（通过字符串标识符）
    ///
    /// 使用此变体可以注册和使用自定义压缩算法
    /// 自定义算法必须通过 `CompressionUtil::register_custom` 注册
    ///
    /// # 示例
    /// ```rust
    /// use flare_core::common::compression::{CompressionAlgorithm, CompressionUtil, Compressor};
    /// use std::sync::Arc;
    ///
    /// // 注册自定义压缩器
    /// struct MyCustomCompressor;
    /// impl Compressor for MyCustomCompressor { /* ... */ }
    ///
    /// CompressionUtil::register_custom(Arc::new(MyCustomCompressor));
    ///
    /// // 使用自定义算法
    /// let algo = CompressionAlgorithm::Custom("my_custom".to_string());
    /// ```
    Custom(String),
}

impl CompressionAlgorithm {
    /// 从字符串转换为压缩算法
    ///
    /// 如果字符串匹配内置算法，返回对应的枚举值
    /// 否则返回 `Custom(String)` 变体
    ///
    /// # 示例
    /// ```rust
    /// use flare_core::common::compression::CompressionAlgorithm;
    ///
    /// assert_eq!(CompressionAlgorithm::from_str("none"), Some(CompressionAlgorithm::None));
    /// assert_eq!(CompressionAlgorithm::from_str("gzip"), Some(CompressionAlgorithm::Gzip));
    /// assert_eq!(
    ///     CompressionAlgorithm::from_str("my_custom"),
    ///     Some(CompressionAlgorithm::Custom("my_custom".to_string()))
    /// );
    /// ```
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "none" | "" => Some(CompressionAlgorithm::None),
            "gzip" => Some(CompressionAlgorithm::Gzip),
            "zstd" => Some(CompressionAlgorithm::Zstd),
            custom => Some(CompressionAlgorithm::Custom(custom.to_string())),
        }
    }

    /// 转换为字符串标识符
    ///
    /// 返回算法的字符串表示，可用于注册表查找
    ///
    /// # 示例
    /// ```rust
    /// use flare_core::common::compression::CompressionAlgorithm;
    ///
    /// assert_eq!(CompressionAlgorithm::None.as_str(), "none");
    /// assert_eq!(CompressionAlgorithm::Gzip.as_str(), "gzip");
    /// assert_eq!(CompressionAlgorithm::Custom("my_custom".to_string()).as_str(), "my_custom");
    /// ```
    pub fn as_str(&self) -> String {
        match self {
            CompressionAlgorithm::None => "none".to_string(),
            CompressionAlgorithm::Gzip => "gzip".to_string(),
            CompressionAlgorithm::Zstd => "zstd".to_string(),
            CompressionAlgorithm::Custom(name) => name.clone(),
        }
    }

    /// 检查是否是自定义算法
    pub fn is_custom(&self) -> bool {
        matches!(self, CompressionAlgorithm::Custom(_))
    }

    /// 获取自定义算法名称（如果是自定义算法）
    pub fn custom_name(&self) -> Option<&str> {
        match self {
            CompressionAlgorithm::Custom(name) => Some(name),
            _ => None,
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
        #[cfg(feature = "compression-gzip")]
        {
            use flate2::Compression;
            use flate2::write::GzEncoder;
            use std::io::Write;

            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(data).map_err(|e| {
                FlareError::encoding_error(format!("Gzip compression failed: {}", e))
            })?;
            encoder.finish().map_err(|e| {
                FlareError::encoding_error(format!("Gzip compression finish failed: {}", e))
            })
        }

        #[cfg(not(feature = "compression-gzip"))]
        {
            let _ = data;
            Err(FlareError::operation_not_supported(
                "gzip compression feature is disabled",
            ))
        }
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        #[cfg(feature = "compression-gzip")]
        {
            use flate2::read::GzDecoder;
            use std::io::Read;

            let mut decoder = GzDecoder::new(data);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed).map_err(|e| {
                FlareError::encoding_error(format!("Gzip decompression failed: {}", e))
            })?;
            Ok(decompressed)
        }

        #[cfg(not(feature = "compression-gzip"))]
        {
            let _ = data;
            Err(FlareError::operation_not_supported(
                "gzip compression feature is disabled",
            ))
        }
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
