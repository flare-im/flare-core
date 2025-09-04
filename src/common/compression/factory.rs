//! 压缩器工厂
//!
//! 提供统一的压缩器创建和管理接口

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::common::{
    error::Result,
    compression::traits::{
        Compressor, CompressionFormat, CompressionConfig,
    },
    compression::{
        Lz4Compressor, SnappyCompressor, GzipCompressor,
    },
};

/// 压缩器工厂
pub struct CompressorFactory {
    /// 注册的压缩器构造函数
    constructors: Arc<RwLock<HashMap<CompressionFormat, Box<dyn Fn() -> Box<dyn Compressor> + Send + Sync>>>>,
}

impl CompressorFactory {
    /// 创建新的压缩器工厂
    pub fn new() -> Self {
        let factory = Self {
            constructors: Arc::new(RwLock::new(HashMap::new())),
        };
        
        // 注册默认压缩器
        factory.register_defaults();
        factory
    }
    
    /// 注册默认压缩器
    fn register_defaults(&self) {
        self.register(CompressionFormat::Lz4, || Box::new(Lz4Compressor::new()));
        self.register(CompressionFormat::Snappy, || Box::new(SnappyCompressor::new()));
        self.register(CompressionFormat::Gzip, || Box::new(GzipCompressor::new()));
        self.register(CompressionFormat::None, || Box::new(NoCompressor::new()));
    }
    
    /// 注册压缩器
    pub fn register<F>(&self, format: CompressionFormat, constructor: F)
    where
        F: Fn() -> Box<dyn Compressor> + Send + Sync + 'static,
    {
        if let Ok(mut constructors) = self.constructors.write() {
            constructors.insert(format, Box::new(constructor));
        }
    }
    
    /// 创建压缩器
    pub fn create(&self, format: CompressionFormat) -> Box<dyn Compressor> {
        if let Ok(constructors) = self.constructors.read() {
            if let Some(constructor) = constructors.get(&format) {
                return constructor();
            }
        }
        
        // 降级到默认实现
        match format {
            CompressionFormat::Lz4 => Box::new(Lz4Compressor::new()),
            CompressionFormat::Snappy => Box::new(SnappyCompressor::new()),
            CompressionFormat::Gzip => Box::new(GzipCompressor::new()),
            CompressionFormat::None => Box::new(NoCompressor::new()),
        }
    }
    
    /// 创建带配置的压缩器
    pub fn create_with_config(&self, format: CompressionFormat, config: CompressionConfig) -> Box<dyn Compressor> {
        let mut compressor = self.create(format);
        if let Err(e) = compressor.set_config(config) {
            eprintln!("设置压缩器配置失败: {}", e);
        }
        compressor
    }
    
    /// 获取所有支持的格式
    pub fn supported_formats(&self) -> Vec<CompressionFormat> {
        if let Ok(constructors) = self.constructors.read() {
            constructors.keys().cloned().collect()
        } else {
            vec![
                CompressionFormat::Lz4,
                CompressionFormat::Snappy,
                CompressionFormat::Gzip,
                CompressionFormat::None,
            ]
        }
    }
    
    /// 获取推荐的压缩器（根据使用场景）
    pub fn get_recommended(&self, scenario: &str) -> Box<dyn Compressor> {
        match scenario {
            "ultra_low_latency" | "gaming" | "trading" => {
                // 超低延迟场景：LZ4
                Box::new(Lz4Compressor::ultra_fast())
            }
            "real_time" | "streaming" => {
                // 实时场景：Snappy平衡
                Box::new(SnappyCompressor::new())
            }
            "storage" | "backup" | "archive" => {
                // 存储场景：Gzip高压缩比
                Box::new(GzipCompressor::new())
            }
            "balanced" | "general" => {
                // 平衡场景：Snappy
                Box::new(SnappyCompressor::new())
            }
            _ => {
                // 默认：LZ4快速压缩
                Box::new(Lz4Compressor::new())
            }
        }
    }
    
    /// 验证压缩器是否已注册
    pub fn is_registered(&self, format: CompressionFormat) -> bool {
        if let Ok(constructors) = self.constructors.read() {
            constructors.contains_key(&format)
        } else {
            false
        }
    }
}

impl Default for CompressorFactory {
    fn default() -> Self {
        Self::new()
    }
}

// 全局工厂实例
lazy_static::lazy_static! {
    static ref GLOBAL_FACTORY: CompressorFactory = CompressorFactory::new();
}

/// 静态工厂方法
impl CompressorFactory {
    /// 创建压缩器（静态方法）
    pub fn create_static(format: CompressionFormat) -> Box<dyn Compressor> {
        GLOBAL_FACTORY.create(format)
    }
    
    /// 创建带配置的压缩器（静态方法）
    pub fn create_with_config_static(format: CompressionFormat, config: CompressionConfig) -> Box<dyn Compressor> {
        GLOBAL_FACTORY.create_with_config(format, config)
    }
    
    /// 获取推荐的压缩器（静态方法）
    pub fn recommended_static(scenario: &str) -> Box<dyn Compressor> {
        GLOBAL_FACTORY.get_recommended(scenario)
    }
    
    /// 注册全局压缩器
    pub fn register_global<F>(format: CompressionFormat, constructor: F)
    where
        F: Fn() -> Box<dyn Compressor> + Send + Sync + 'static,
    {
        GLOBAL_FACTORY.register(format, constructor);
    }
}

/// 便捷函数

/// 创建LZ4压缩器
pub fn create_lz4() -> Box<dyn Compressor> {
    Box::new(Lz4Compressor::new())
}

/// 创建超快速LZ4压缩器
pub fn create_lz4_ultra_fast() -> Box<dyn Compressor> {
    Box::new(Lz4Compressor::ultra_fast())
}

/// 创建Snappy压缩器
pub fn create_snappy() -> Box<dyn Compressor> {
    Box::new(SnappyCompressor::new())
}

/// 创建Gzip压缩器
pub fn create_gzip() -> Box<dyn Compressor> {
    Box::new(GzipCompressor::new())
}

/// 创建无压缩器
pub fn create_none() -> Box<dyn Compressor> {
    Box::new(NoCompressor::new())
}

/// 无压缩器实现（直接传递数据）
#[derive(Debug, Clone)]
pub struct NoCompressor {
    config: CompressionConfig,
}

impl NoCompressor {
    pub fn new() -> Self {
        Self {
            config: CompressionConfig::default(),
        }
    }
}

impl Default for NoCompressor {
    fn default() -> Self {
        Self::new()
    }
}

use async_trait::async_trait;
use crate::common::compression::traits::CompressionResult;

#[async_trait]
impl Compressor for NoCompressor {
    fn format(&self) -> CompressionFormat {
        CompressionFormat::None
    }
    
    async fn compress(&self, data: &[u8]) -> Result<CompressionResult> {
        Ok(CompressionResult {
            data: data.to_vec(),
            original_size: data.len(),
            compressed_size: data.len(),
            was_compressed: false,
        })
    }
    
    async fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }
    
    fn name(&self) -> &'static str {
        "NoCompressor"
    }
    
    fn version(&self) -> &'static str {
        "1.0.0"
    }
    
    fn description(&self) -> &'static str {
        "无压缩器，直接传递数据，用于测试和对比"
    }
    
    fn config(&self) -> CompressionConfig {
        self.config.clone()
    }
    
    fn set_config(&mut self, config: CompressionConfig) -> Result<()> {
        self.config = config;
        Ok(())
    }
    

    

    
    fn clone_box(&self) -> Box<dyn Compressor> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_factory_create_all_formats() {
        let factory = CompressorFactory::new();
        
        let formats = vec![
            CompressionFormat::Lz4,
            CompressionFormat::Snappy,
            CompressionFormat::Gzip,
            CompressionFormat::None,
        ];
        
        for format in formats {
            let compressor = factory.create(format);
            assert_eq!(compressor.format(), format);
            
            // 测试基本功能
            let test_data = b"test data".to_vec();
            let result = compressor.compress(&test_data).await.unwrap();
            let decompressed = compressor.decompress(&result.data).await.unwrap();
            assert_eq!(decompressed, test_data);
        }
    }
    
    #[tokio::test]
    async fn test_factory_recommendations() {
        let scenarios = vec![
            ("ultra_low_latency", CompressionFormat::Lz4),
            ("gaming", CompressionFormat::Lz4),
            ("real_time", CompressionFormat::Snappy),
            ("storage", CompressionFormat::Gzip),
            ("balanced", CompressionFormat::Snappy),
        ];
        
        for (scenario, expected_format) in scenarios {
            let compressor = CompressorFactory::recommended_static(scenario);
            assert_eq!(compressor.format(), expected_format);
        }
    }
    
    #[tokio::test]
    async fn test_static_factory_methods() {
        // 测试静态方法
        let lz4 = CompressorFactory::create_static(CompressionFormat::Lz4);
        assert_eq!(lz4.format(), CompressionFormat::Lz4);
        
        let config = CompressionConfig::ultra_low_latency();
        let configured = CompressorFactory::create_with_config_static(CompressionFormat::Snappy, config);
        assert_eq!(configured.format(), CompressionFormat::Snappy);
    }
    
    #[test]
    fn test_factory_registration() {
        let factory = CompressorFactory::new();
        
        // 验证默认格式已注册
        assert!(factory.is_registered(CompressionFormat::Lz4));
        assert!(factory.is_registered(CompressionFormat::Snappy));
        assert!(factory.is_registered(CompressionFormat::Gzip));
        
        // 验证支持的格式
        let supported = factory.supported_formats();
        assert!(supported.contains(&CompressionFormat::Lz4));
        assert!(supported.len() >= 3);
    }
}