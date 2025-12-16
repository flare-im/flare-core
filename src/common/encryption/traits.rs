//! 加密器 Trait 定义
//!
//! 定义标准加密接口，方便用户实现自定义加密算法

use super::algorithms::EncryptionAlgorithm;
use crate::common::error::Result;

/// 加密器标准接口
///
/// 实现此 trait 以支持自定义加密算法
///
/// # 示例
///
/// ```rust
/// use flare_core::common::encryption::{Encryptor, EncryptionAlgorithm};
/// use flare_core::common::error::Result;
///
/// struct MyCustomEncryptor;
///
/// impl Encryptor for MyCustomEncryptor {
///     fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
///         // 实现加密逻辑
///         Ok(data.to_vec())
///     }
///     
///     fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
///         // 实现解密逻辑
///         Ok(data.to_vec())
///     }
///     
///     fn algorithm(&self) -> EncryptionAlgorithm {
///         EncryptionAlgorithm::None
///     }
///     
///     fn name(&self) -> &'static str {
///         "my_custom"
///     }
/// }
/// ```
pub trait Encryptor: Send + Sync {
    /// 加密数据
    ///
    /// # 参数
    /// - `data`: 要加密的原始数据
    ///
    /// # 返回
    /// 加密后的数据
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// 解密数据
    ///
    /// # 参数
    /// - `data`: 要解密的加密数据
    ///
    /// # 返回
    /// 解密后的原始数据
    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>>;

    /// 获取加密算法类型
    fn algorithm(&self) -> EncryptionAlgorithm;

    /// 获取加密器名称（用于注册和查找）
    ///
    /// 名称应该是唯一的，用于在注册表中标识加密器
    fn name(&self) -> &'static str {
        self.algorithm().as_str()
    }
}
