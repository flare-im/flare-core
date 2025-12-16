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
        self.algorithm().as_str()
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
