//! 压缩器 trait 和工厂

use crate::common::error::FlareError;
use super::config::{CompressionConfig, CompressionAlgorithm};
use super::algorithms::{Lz4Compressor, SnappyCompressor, GzipCompressor, ZlibCompressor};
use std::sync::Arc;

/// 压缩器 trait
/// 
/// 用户可以实现此 trait 来提供自定义压缩算法
pub trait Compressor: Send + Sync {
    /// 压缩数据
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError>;
    
    /// 解压数据
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError>;
    
    /// 获取压缩器名称
    fn name(&self) -> &str;
    
    /// 获取算法 ID
    fn algorithm_id(&self) -> u32;
}

/// 压缩器工厂
pub struct CompressorFactory;

impl CompressorFactory {
    /// 根据配置创建压缩器
    pub fn create(config: &CompressionConfig) -> Result<Box<dyn Compressor>, FlareError> {
        match config.algorithm {
            CompressionAlgorithm::None => {
                Err(FlareError::invalid_input("压缩算法为 None，无法创建压缩器".to_string()))
            }
            CompressionAlgorithm::Lz4 => {
                Ok(Box::new(Lz4Compressor::new()))
            }
            CompressionAlgorithm::Snappy => {
                Ok(Box::new(SnappyCompressor::new()))
            }
            CompressionAlgorithm::Gzip => {
                Ok(Box::new(GzipCompressor::new(config.level)))
            }
            CompressionAlgorithm::Zlib => {
                Ok(Box::new(ZlibCompressor::new(config.level)))
            }
            CompressionAlgorithm::Custom(id) => {
                Err(FlareError::invalid_input(format!(
                    "自定义压缩算法 {} 未注册，请使用 register_custom_compressor 注册",
                    id
                )))
            }
        }
    }

    /// 创建共享压缩器（Arc包装）
    pub fn create_shared(config: &CompressionConfig) -> Result<Arc<dyn Compressor>, FlareError> {
        Self::create(config).map(|c| Arc::from(c))
    }
}

/// 自定义压缩器注册表
/// 
/// 用于注册和管理自定义压缩器
pub struct CustomCompressorRegistry {
    compressors: std::collections::HashMap<u32, Arc<dyn Compressor>>,
}

impl CustomCompressorRegistry {
    /// 创建新的注册表
    pub fn new() -> Self {
        Self {
            compressors: std::collections::HashMap::new(),
        }
    }

    /// 注册自定义压缩器
    /// 
    /// # 参数
    /// - `id`: 算法 ID（建议从 1000 开始）
    /// - `compressor`: 压缩器实例
    pub fn register(&mut self, id: u32, compressor: Arc<dyn Compressor>) -> Result<(), FlareError> {
        if id < 1000 {
            return Err(FlareError::invalid_input(
                "自定义压缩器 ID 应该从 1000 开始，避免与内置算法冲突".to_string()
            ));
        }
        
        self.compressors.insert(id, compressor);
        Ok(())
    }

    /// 获取自定义压缩器
    pub fn get(&self, id: u32) -> Option<Arc<dyn Compressor>> {
        self.compressors.get(&id).cloned()
    }

    /// 移除自定义压缩器
    pub fn unregister(&mut self, id: u32) -> bool {
        self.compressors.remove(&id).is_some()
    }
}

impl Default for CustomCompressorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_lz4_compressor() {
        let config = CompressionConfig::new(CompressionAlgorithm::Lz4);
        let compressor = CompressorFactory::create(&config).unwrap();
        assert_eq!(compressor.name(), "lz4");
    }

    #[test]
    fn test_create_snappy_compressor() {
        let config = CompressionConfig::new(CompressionAlgorithm::Snappy);
        let compressor = CompressorFactory::create(&config).unwrap();
        assert_eq!(compressor.name(), "snappy");
    }

    #[test]
    fn test_create_gzip_compressor() {
        let config = CompressionConfig::new(CompressionAlgorithm::Gzip);
        let compressor = CompressorFactory::create(&config).unwrap();
        assert_eq!(compressor.name(), "gzip");
    }

    #[test]
    fn test_custom_registry() {
        let mut registry = CustomCompressorRegistry::new();
        
        // 尝试注册 ID < 1000 的压缩器，应该失败
        let lz4 = Arc::new(Lz4Compressor::new());
        assert!(registry.register(100, lz4.clone()).is_err());
        
        // 注册 ID >= 1000 的压缩器，应该成功
        assert!(registry.register(1000, lz4.clone()).is_ok());
        
        // 获取注册的压缩器
        let retrieved = registry.get(1000);
        assert!(retrieved.is_some());
        
        // 移除压缩器
        assert!(registry.unregister(1000));
        assert!(registry.get(1000).is_none());
    }
}
