//! 压缩器 Trait 定义
//!
//! 定义标准压缩接口，方便用户实现自定义压缩算法

use super::algorithms::CompressionAlgorithm;
use crate::common::error::Result;

/// 压缩器标准接口
///
/// 实现此 trait 以支持自定义压缩算法
///
/// # 示例
///
/// ```rust
/// use flare_core::common::compression::{Compressor, CompressionAlgorithm};
/// use flare_core::common::error::Result;
///
/// struct MyCustomCompressor;
///
/// impl Compressor for MyCustomCompressor {
///     fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
///         // 实现压缩逻辑
///         Ok(data.to_vec())
///     }
///     
///     fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
///         // 实现解压逻辑
///         Ok(data.to_vec())
///     }
///     
///     fn algorithm(&self) -> CompressionAlgorithm {
///         CompressionAlgorithm::None
///     }
///     
///     fn name(&self) -> &'static str {
///         "my_custom"
///     }
///     
///     fn can_detect(&self, data: &[u8]) -> bool {
///         // 实现魔数检测逻辑
///         false
///     }
/// }
/// ```
pub trait Compressor: Send + Sync {
    /// 压缩数据
    ///
    /// # 参数
    /// - `data`: 要压缩的原始数据
    ///
    /// # 返回
    /// 压缩后的数据
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// 解压数据
    ///
    /// # 参数
    /// - `data`: 要解压的压缩数据
    ///
    /// # 返回
    /// 解压后的原始数据
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// 获取压缩算法类型
    fn algorithm(&self) -> CompressionAlgorithm;

    /// 获取压缩器名称（用于注册和查找）
    ///
    /// 名称应该是唯一的，用于在注册表中标识压缩器
    fn name(&self) -> &'static str {
        // 注意：由于 algorithm() 返回的 CompressionAlgorithm 可能包含 Custom(String)，
        // 这里需要特殊处理。对于内置算法，返回静态字符串；对于自定义算法，需要在实现中覆盖此方法
        match self.algorithm() {
            CompressionAlgorithm::None => "none",
            CompressionAlgorithm::Gzip => "gzip",
            CompressionAlgorithm::Zstd => "zstd",
            CompressionAlgorithm::Custom(_) => {
                // 自定义算法必须在实现中覆盖 name() 方法
                panic!("Custom compression algorithm must override name() method")
            }
        }
    }

    /// 检测数据是否使用此压缩算法（通过魔数等）
    ///
    /// # 参数
    /// - `data`: 待检测的数据（通常是数据的前几个字节）
    ///
    /// # 返回
    /// 如果数据可能是由此压缩器压缩的，返回 `true`
    fn can_detect(&self, _data: &[u8]) -> bool {
        false
    }
}
