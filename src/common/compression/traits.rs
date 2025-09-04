//! 压缩器特征和配置定义
//!
//! 定义统一的压缩器接口，支持用户扩展

use async_trait::async_trait;
use std::fmt;
use crate::common::error::Result;

/// 压缩器特性枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressorFeature {
    /// 超快速压缩
    UltraFast,
    /// 高压缩比
    HighRatio,
    /// 流式压缩
    Streaming,
    /// 字典压缩
    Dictionary,
}

/// 压缩格式枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressionFormat {
    /// LZ4压缩 - 极速压缩，适合超低延迟场景
    Lz4,
    /// Snappy压缩 - 平衡压缩，Google开发
    Snappy,
    /// Gzip压缩 - 高压缩比，适合存储场景
    Gzip,
    /// 无压缩
    None,
}

impl fmt::Display for CompressionFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionFormat::Lz4 => write!(f, "LZ4"),
            CompressionFormat::Snappy => write!(f, "Snappy"), 
            CompressionFormat::Gzip => write!(f, "Gzip"),
            CompressionFormat::None => write!(f, "None"),
        }
    }
}

/// 压缩配置
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// 启用压缩的最小数据大小（字节）
    /// 小于此值的数据不会被压缩，避免负优化
    pub min_compress_size: usize,
    
    /// 最大压缩数据大小限制（字节）
    pub max_compress_size: Option<usize>,
    
    /// 压缩级别 (1-9，1最快，9最小)
    pub compression_level: u8,
    
    /// 启用字典压缩（适用于重复数据）
    pub enable_dictionary: bool,
    
    /// 字典大小
    pub dictionary_size: usize,
    
    /// 压缩超时时间（毫秒）
    pub timeout_ms: u64,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            min_compress_size: 128,      // 128字节以下不压缩
            max_compress_size: None,     // 无大小限制
            compression_level: 1,        // 最快压缩
            enable_dictionary: false,    // 默认不启用字典
            dictionary_size: 32768,      // 32KB字典
            timeout_ms: 5,               // 5ms超时，满足超低延迟要求
        }
    }
}

impl CompressionConfig {
    /// 创建超低延迟配置
    pub fn ultra_low_latency() -> Self {
        Self {
            min_compress_size: 256,      // 更高阈值
            compression_level: 1,        // 最快压缩
            timeout_ms: 2,               // 2ms超时
            enable_dictionary: false,    // 禁用字典降低延迟
            ..Default::default()
        }
    }
    
    /// 创建高压缩比配置
    pub fn high_compression() -> Self {
        Self {
            min_compress_size: 64,       // 较低阈值
            compression_level: 6,        // 平衡压缩
            timeout_ms: 50,              // 更长超时
            enable_dictionary: true,     // 启用字典
            ..Default::default()
        }
    }
    
    /// 创建平衡配置
    pub fn balanced() -> Self {
        Self {
            min_compress_size: 128,
            compression_level: 3,
            timeout_ms: 10,
            enable_dictionary: false,
            ..Default::default()
        }
    }
    
    /// 设置最小压缩大小
    pub fn with_min_size(mut self, size: usize) -> Self {
        self.min_compress_size = size;
        self
    }
    
    /// 设置压缩级别
    pub fn with_level(mut self, level: u8) -> Self {
        self.compression_level = level.min(9);
        self
    }
    
    /// 设置超时时间
    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
}

/// 压缩结果
#[derive(Debug)]
pub struct CompressionResult {
    /// 压缩后的数据
    pub data: Vec<u8>,
    /// 原始大小
    pub original_size: usize,
    /// 压缩后大小
    pub compressed_size: usize,
    /// 是否实际进行了压缩
    pub was_compressed: bool,
}

impl CompressionResult {
    /// 计算压缩比
    pub fn ratio(&self) -> f64 {
        if self.original_size == 0 {
            1.0
        } else {
            self.compressed_size as f64 / self.original_size as f64
        }
    }
    
    /// 计算节省的字节数
    pub fn bytes_saved(&self) -> usize {
        if self.was_compressed && self.original_size > self.compressed_size {
            self.original_size - self.compressed_size
        } else {
            0
        }
    }
}

/// 压缩器特征
#[async_trait]
pub trait Compressor: Send + Sync {
    /// 获取压缩格式
    fn format(&self) -> CompressionFormat;
    
    /// 压缩数据
    async fn compress(&self, data: &[u8]) -> Result<CompressionResult>;
    
    /// 解压数据
    async fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>;
    
    /// 获取压缩器名称
    fn name(&self) -> &'static str;
    
    /// 获取版本
    fn version(&self) -> &'static str;
    
    /// 获取描述
    fn description(&self) -> &'static str;
    
    /// 获取配置
    fn config(&self) -> CompressionConfig;
    
    /// 设置配置
    fn set_config(&mut self, config: CompressionConfig) -> Result<()>;
    
    /// 估算压缩后大小
    async fn estimate_compressed_size(&self, data: &[u8]) -> Result<usize> {
        // 默认实现：实际压缩获取大小
        let result = self.compress(data).await?;
        Ok(result.compressed_size)
    }
    
    /// 检查数据是否值得压缩
    fn should_compress(&self, data: &[u8]) -> bool {
        let config = self.config();
        data.len() >= config.min_compress_size
    }
    
    /// 批量压缩
    async fn compress_batch(&self, data_vec: &[Vec<u8>]) -> Result<Vec<CompressionResult>> {
        let mut results = Vec::with_capacity(data_vec.len());
        for data in data_vec {
            results.push(self.compress(data).await?);
        }
        Ok(results)
    }
    
    /// 批量解压
    async fn decompress_batch(&self, data_vec: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        let mut results = Vec::with_capacity(data_vec.len());
        for data in data_vec {
            results.push(self.decompress(data).await?);
        }
        Ok(results)
    }
    
    /// 克隆压缩器
    fn clone_box(&self) -> Box<dyn Compressor>;
    
    /// 支持的特性
    fn supported_features(&self) -> Vec<CompressorFeature> {
        vec![]
    }
    
    /// 获取MIME类型
    fn mime_type(&self) -> &'static str {
        "application/octet-stream"
    }
}

/// 可配置压缩器特征
#[async_trait]
pub trait ConfigurableCompressor: Compressor {
    /// 更新配置
    fn update_config(&mut self, config: CompressionConfig) -> Result<()>;
    
    /// 获取可配置参数列表
    fn configurable_params(&self) -> Vec<&'static str>;
    
    /// 验证配置
    fn validate_config(&self, config: &CompressionConfig) -> Result<()>;
    
    /// 获取推荐配置
    fn recommended_config(&self, scenario: &str) -> CompressionConfig {
        match scenario {
            "ultra_low_latency" => CompressionConfig::ultra_low_latency(),
            "high_compression" => CompressionConfig::high_compression(),
            "balanced" => CompressionConfig::balanced(),
            _ => CompressionConfig::default(),
        }
    }
}

/// 压缩器克隆帮助宏
#[macro_export]
macro_rules! impl_compressor_clone {
    ($compressor:ty) => {
        impl Clone for $compressor {
            fn clone(&self) -> Self {
                let config = self.config();
                Self::with_config(config)
            }
        }
    };
}