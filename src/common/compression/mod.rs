//! 压缩模块
//! 
//! 提供可扩展的压缩接口，支持用户自定义压缩算法实现

pub mod traits;
pub mod algorithms;
pub mod registry;

pub use traits::Compressor;
pub use algorithms::{CompressionAlgorithm, NoCompressor, GzipCompressor};
pub use registry::{CompressionRegistry, CompressionUtil};

