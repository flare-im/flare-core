//! 消息压缩模块
//!
//! 提供高性能的消息压缩支持，专为超低延迟场景优化
//! 支持多种压缩算法和用户扩展

pub mod traits;
pub mod lz4;
pub mod snappy;
pub mod gzip;
pub mod factory;

// 重新导出核心类型
pub use traits::{
    Compressor, CompressionFormat, CompressionConfig,
    ConfigurableCompressor, CompressorFeature,
};

pub use lz4::Lz4Compressor;
pub use snappy::SnappyCompressor;
pub use gzip::GzipCompressor;
pub use factory::CompressorFactory;

/// 便捷的压缩器创建函数
pub fn create_compressor(format: CompressionFormat) -> Box<dyn Compressor> {
    CompressorFactory::create_static(format)
}

/// 便捷的带配置压缩器创建函数
pub fn create_compressor_with_config(
    format: CompressionFormat, 
    config: CompressionConfig
) -> Box<dyn Compressor> {
    CompressorFactory::create_with_config_static(format, config)
}

/// 获取推荐的超低延迟压缩器（LZ4）
pub fn ultra_low_latency_compressor() -> Box<dyn Compressor> {
    Box::new(Lz4Compressor::ultra_fast())
}

/// 获取平衡性能压缩器（Snappy）
pub fn balanced_compressor() -> Box<dyn Compressor> {
    Box::new(SnappyCompressor::new())
}