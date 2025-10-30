//! 压缩配置

use serde::{Serialize, Deserialize};

/// 压缩算法类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// 无压缩
    None,
    /// LZ4 压缩（速度快）
    Lz4,
    /// Snappy 压缩（平衡）
    Snappy,
    /// Gzip 压缩（压缩率高）
    Gzip,
    /// Zlib 压缩（兼容性好）
    Zlib,
    /// 自定义压缩算法
    Custom(u32),
}

/// 压缩级别
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionLevel {
    /// 最快速度（最低压缩率）
    Fastest,
    /// 快速
    Fast,
    /// 默认（平衡）
    Default,
    /// 最佳压缩（最高压缩率）
    Best,
    /// 自定义级别 (0-9)
    Custom(u32),
}

impl CompressionLevel {
    /// 转换为 flate2 的压缩级别
    pub fn to_flate2_level(&self) -> flate2::Compression {
        match self {
            CompressionLevel::Fastest => flate2::Compression::fast(),
            CompressionLevel::Fast => flate2::Compression::new(3),
            CompressionLevel::Default => flate2::Compression::default(),
            CompressionLevel::Best => flate2::Compression::best(),
            CompressionLevel::Custom(level) => flate2::Compression::new(*level),
        }
    }
}

/// 压缩配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    /// 压缩算法
    pub algorithm: CompressionAlgorithm,
    /// 压缩级别
    pub level: CompressionLevel,
    /// 是否启用压缩
    pub enabled: bool,
    /// 最小压缩阈值（字节数，小于此值不压缩）
    pub min_size: usize,
}

impl CompressionConfig {
    /// 创建新的压缩配置
    pub fn new(algorithm: CompressionAlgorithm) -> Self {
        Self {
            algorithm,
            level: CompressionLevel::Default,
            enabled: true,
            min_size: 128, // 默认最小128字节才压缩
        }
    }

    /// 创建无压缩配置
    pub fn none() -> Self {
        Self {
            algorithm: CompressionAlgorithm::None,
            level: CompressionLevel::Default,
            enabled: false,
            min_size: 0,
        }
    }

    /// 设置压缩级别
    pub fn with_level(mut self, level: CompressionLevel) -> Self {
        self.level = level;
        self
    }

    /// 设置最小压缩阈值
    pub fn with_min_size(mut self, min_size: usize) -> Self {
        self.min_size = min_size;
        self
    }

    /// 是否应该压缩数据
    pub fn should_compress(&self, data_size: usize) -> bool {
        self.enabled && self.algorithm != CompressionAlgorithm::None && data_size >= self.min_size
    }
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self::new(CompressionAlgorithm::Lz4)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_compress() {
        let config = CompressionConfig::new(CompressionAlgorithm::Lz4);
        
        // 小于最小阈值，不压缩
        assert!(!config.should_compress(100));
        
        // 大于等于最小阈值，压缩
        assert!(config.should_compress(128));
        assert!(config.should_compress(1024));
    }

    #[test]
    fn test_none_config() {
        let config = CompressionConfig::none();
        assert!(!config.should_compress(1024));
    }

    #[test]
    fn test_builder_pattern() {
        let config = CompressionConfig::new(CompressionAlgorithm::Gzip)
            .with_level(CompressionLevel::Best)
            .with_min_size(256);
        
        assert_eq!(config.algorithm, CompressionAlgorithm::Gzip);
        assert_eq!(config.level, CompressionLevel::Best);
        assert_eq!(config.min_size, 256);
    }
}
