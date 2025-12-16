//! 压缩模块
//!
//! 提供可扩展的压缩接口，支持用户自定义压缩算法实现

pub mod algorithms;
pub mod registry;
pub mod traits;

pub use algorithms::{CompressionAlgorithm, GzipCompressor, NoCompressor};
pub use registry::{CompressionRegistry, CompressionUtil};
pub use traits::Compressor;
