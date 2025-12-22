//! 压缩器注册表
//!
//! 管理压缩器的注册和查找，支持用户注册自定义压缩器

use super::algorithms::{CompressionAlgorithm, GzipCompressor, NoCompressor};
use super::traits::Compressor;
use crate::common::error::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

lazy_static::lazy_static! {
    /// 全局压缩器注册表
    static ref COMPRESSION_REGISTRY: CompressionRegistry = {
        let registry = CompressionRegistry::new();
        registry.register_defaults();
        registry
    };
}

/// 压缩器注册表
///
/// 管理压缩器的注册和查找
pub struct CompressionRegistry {
    compressors: Arc<RwLock<HashMap<String, Arc<dyn Compressor>>>>,
}

impl CompressionRegistry {
    /// 创建新的注册表
    pub fn new() -> Self {
        Self {
            compressors: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 注册内置压缩器
    pub fn register_defaults(&self) {
        self.register("none", Arc::new(NoCompressor));
        self.register("gzip", Arc::new(GzipCompressor));
    }

    /// 注册压缩器
    ///
    /// # 参数
    /// - `name`: 压缩器名称（用于查找）
    /// - `compressor`: 压缩器实例
    ///
    /// # 示例
    ///
    /// ```rust
    /// use flare_core::common::compression::{CompressionRegistry, Compressor};
    /// use std::sync::Arc;
    ///
    /// struct MyCompressor;
    /// impl Compressor for MyCompressor { /* ... */ }
    ///
    /// let registry = CompressionRegistry::new();
    /// registry.register("my_custom", Arc::new(MyCompressor));
    /// ```
    pub fn register(&self, name: &str, compressor: Arc<dyn Compressor>) {
        if let Ok(mut compressors) = self.compressors.write() {
            compressors.insert(name.to_string(), compressor);
        }
    }

    /// 查找压缩器
    ///
    /// # 参数
    /// - `name`: 压缩器名称
    ///
    /// # 返回
    /// 找到的压缩器，如果不存在则返回 None
    pub fn find(&self, name: &str) -> Option<Arc<dyn Compressor>> {
        self.compressors
            .read()
            .ok()
            .and_then(|compressors| compressors.get(name).map(Arc::clone))
    }

    /// 根据算法类型查找压缩器
    pub fn find_by_algorithm(
        &self,
        algorithm: CompressionAlgorithm,
    ) -> Option<Arc<dyn Compressor>> {
        self.find(&algorithm.as_str())
    }

    /// 检查压缩器是否已注册
    ///
    /// # 参数
    /// - `name`: 压缩器名称
    ///
    /// # 返回
    /// 如果已注册返回 `true`，否则返回 `false`
    pub fn is_registered(&self, name: &str) -> bool {
        self.compressors
            .read()
            .map(|compressors| compressors.contains_key(name))
            .unwrap_or(false)
    }

    /// 尝试自动检测压缩算法
    ///
    /// 遍历所有注册的压缩器，使用 `can_detect` 方法检测
    pub fn auto_detect(&self, data: &[u8]) -> Option<Arc<dyn Compressor>> {
        self.compressors.read().ok().and_then(|compressors| {
            compressors
                .values()
                .find(|compressor| compressor.can_detect(data))
                .map(Arc::clone)
        })
    }

    /// 获取全局注册表实例
    pub fn global() -> &'static CompressionRegistry {
        &COMPRESSION_REGISTRY
    }
}

impl Default for CompressionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// 压缩工具类
///
/// 提供便捷的压缩/解压方法，使用全局注册表
pub struct CompressionUtil;

impl CompressionUtil {
    /// 根据算法类型获取压缩器
    pub fn get_compressor(algorithm: CompressionAlgorithm) -> Arc<dyn Compressor> {
        CompressionRegistry::global()
            .find_by_algorithm(algorithm)
            .unwrap_or_else(|| Arc::new(NoCompressor))
    }

    /// 根据名称获取压缩器
    pub fn get_compressor_by_name(name: &str) -> Option<Arc<dyn Compressor>> {
        CompressionRegistry::global().find(name)
    }

    /// 压缩数据（根据算法类型）
    pub fn compress(data: &[u8], algorithm: CompressionAlgorithm) -> Result<Vec<u8>> {
        let compressor = Self::get_compressor(algorithm);
        compressor.compress(data)
    }

    /// 解压数据（根据算法类型）
    pub fn decompress(data: &[u8], algorithm: CompressionAlgorithm) -> Result<Vec<u8>> {
        let compressor = Self::get_compressor(algorithm);
        compressor.decompress(data)
    }

    /// 检查压缩器是否已注册
    ///
    /// # 参数
    /// - `name`: 压缩器名称
    ///
    /// # 返回
    /// 如果已注册返回 `true`，否则返回 `false`
    pub fn is_registered(name: &str) -> bool {
        CompressionRegistry::global().is_registered(name)
    }

    /// 尝试自动检测压缩算法并解压
    ///
    /// 首先尝试自动检测压缩算法，如果检测不到则作为无压缩处理
    pub fn auto_decompress(data: &[u8]) -> Result<(Vec<u8>, CompressionAlgorithm)> {
        // 尝试自动检测
        if let Some(compressor) = CompressionRegistry::global().auto_detect(data) {
            let decompressed = compressor.decompress(data)?;
            return Ok((decompressed, compressor.algorithm()));
        }

        // 默认不压缩
        Ok((data.to_vec(), CompressionAlgorithm::None))
    }

    /// 注册自定义压缩器到全局注册表
    ///
    /// # 示例
    ///
    /// ```rust
    /// use flare_core::common::compression::{CompressionUtil, Compressor};
    /// use std::sync::Arc;
    ///
    /// struct MyCompressor;
    /// impl Compressor for MyCompressor { /* ... */ }
    ///
    /// CompressionUtil::register_custom(Arc::new(MyCompressor));
    /// ```
    pub fn register_custom(compressor: Arc<dyn Compressor>) {
        CompressionRegistry::global().register(compressor.name(), compressor);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_registry() {
        let registry = CompressionRegistry::new();
        registry.register_defaults();

        assert!(registry.find("gzip").is_some());
        assert!(registry.find("none").is_some());
        assert!(registry.find("unknown").is_none());
    }

    #[test]
    fn test_auto_detect() {
        let data = b"\x1f\x8b\x08test"; // Gzip 魔数
        let registry = CompressionRegistry::new();
        registry.register_defaults();

        let compressor = registry.auto_detect(data);
        assert!(compressor.is_some());
        assert_eq!(compressor.unwrap().algorithm(), CompressionAlgorithm::Gzip);
    }
}
