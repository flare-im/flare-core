//! 内置加密算法实现
//!
//! 提供常用的加密算法实现

use super::traits::Encryptor;
use crate::common::error::{FlareError, Result};

/// 加密算法类型枚举
///
/// 支持内置算法和自定义算法扩展
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum EncryptionAlgorithm {
    /// 不使用加密
    None,
    /// AES-256-GCM 加密
    Aes256Gcm,
    /// 自定义加密算法（通过字符串标识符）
    ///
    /// 使用此变体可以注册和使用自定义加密算法
    /// 自定义算法必须通过 `EncryptionUtil::register_custom` 注册
    ///
    /// # 示例
    /// ```rust
    /// use flare_core::common::encryption::{EncryptionAlgorithm, EncryptionUtil, Encryptor};
    /// use std::sync::Arc;
    ///
    /// // 注册自定义加密器
    /// struct MyCustomEncryptor;
    /// impl Encryptor for MyCustomEncryptor { /* ... */ }
    ///
    /// EncryptionUtil::register_custom(Arc::new(MyCustomEncryptor));
    ///
    /// // 使用自定义算法
    /// let algo = EncryptionAlgorithm::Custom("my_custom".to_string());
    /// ```
    Custom(String),
}

impl EncryptionAlgorithm {
    /// 从字符串转换为加密算法
    ///
    /// 如果字符串匹配内置算法，返回对应的枚举值
    /// 否则返回 `Custom(String)` 变体
    ///
    /// # 示例
    /// ```rust
    /// use flare_core::common::encryption::EncryptionAlgorithm;
    ///
    /// assert_eq!(EncryptionAlgorithm::from_str("none"), Some(EncryptionAlgorithm::None));
    /// assert_eq!(EncryptionAlgorithm::from_str("aes256gcm"), Some(EncryptionAlgorithm::Aes256Gcm));
    /// assert_eq!(
    ///     EncryptionAlgorithm::from_str("my_custom"),
    ///     Some(EncryptionAlgorithm::Custom("my_custom".to_string()))
    /// );
    /// ```
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "none" | "" => Some(EncryptionAlgorithm::None),
            "aes256gcm" | "aes-256-gcm" => Some(EncryptionAlgorithm::Aes256Gcm),
            custom => Some(EncryptionAlgorithm::Custom(custom.to_string())),
        }
    }

    /// 转换为字符串标识符
    ///
    /// 返回算法的字符串表示，可用于注册表查找
    ///
    /// # 示例
    /// ```rust
    /// use flare_core::common::encryption::EncryptionAlgorithm;
    ///
    /// assert_eq!(EncryptionAlgorithm::None.as_str(), "none");
    /// assert_eq!(EncryptionAlgorithm::Aes256Gcm.as_str(), "aes256gcm");
    /// assert_eq!(EncryptionAlgorithm::Custom("my_custom".to_string()).as_str(), "my_custom");
    /// ```
    pub fn as_str(&self) -> String {
        match self {
            EncryptionAlgorithm::None => "none".to_string(),
            EncryptionAlgorithm::Aes256Gcm => "aes256gcm".to_string(),
            EncryptionAlgorithm::Custom(name) => name.clone(),
        }
    }

    /// 检查是否是自定义算法
    pub fn is_custom(&self) -> bool {
        matches!(self, EncryptionAlgorithm::Custom(_))
    }

    /// 获取自定义算法名称（如果是自定义算法）
    pub fn custom_name(&self) -> Option<&str> {
        match self {
            EncryptionAlgorithm::Custom(name) => Some(name),
            _ => None,
        }
    }
}

/// 无加密器（直通）
pub struct NoEncryptor;

impl Encryptor for NoEncryptor {
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn algorithm(&self) -> EncryptionAlgorithm {
        EncryptionAlgorithm::None
    }

    fn name(&self) -> &'static str {
        "none"
    }
}

/// AES-256-GCM 加密器
///
/// 使用 AES-256-GCM 算法进行加密，每次加密使用随机生成的 12 字节 nonce
/// 加密后的数据格式: \[nonce (12 bytes)\]\[ciphertext\]
///
/// # 安全说明
/// - 每次加密都会生成新的随机 nonce，确保相同明文产生不同密文
/// - 密钥必须严格保密，建议使用安全的密钥派生函数（如 PBKDF2）从密码生成
/// - nonce 会随密文一起存储，解密时需要提取
pub struct Aes256GcmEncryptor {
    #[cfg(feature = "encryption-aes-gcm")]
    key: [u8; 32], // AES-256 需要 32 字节密钥
}

impl Aes256GcmEncryptor {
    /// 创建新的 AES-256-GCM 加密器
    ///
    /// # 参数
    /// - `key`: 32 字节的密钥
    ///
    /// # 错误
    /// 如果密钥长度不是 32 字节，返回错误
    pub fn new(key: &[u8]) -> Result<Self> {
        #[cfg(not(feature = "encryption-aes-gcm"))]
        {
            let _ = key;
            return Err(FlareError::operation_not_supported(
                "aes-256-gcm encryption feature is disabled",
            ));
        }

        #[cfg(feature = "encryption-aes-gcm")]
        {
            if key.len() != 32 {
                return Err(FlareError::protocol_error(format!(
                    "AES-256-GCM requires a 32-byte key, got {} bytes",
                    key.len()
                )));
            }

            let mut key_array = [0u8; 32];
            key_array.copy_from_slice(key);

            Ok(Self { key: key_array })
        }
    }

    /// 从密码派生密钥（使用 PBKDF2）
    ///
    /// # 参数
    /// - `password`: 密码
    /// - `salt`: 盐值（可选，如果不提供则使用默认盐值）
    ///
    /// # 返回
    /// 加密器实例
    pub fn from_password(password: &[u8], salt: Option<&[u8]>) -> Result<Self> {
        #[cfg(not(feature = "encryption-aes-gcm"))]
        {
            let _ = (password, salt);
            return Err(FlareError::operation_not_supported(
                "aes-256-gcm encryption feature is disabled",
            ));
        }

        #[cfg(feature = "encryption-aes-gcm")]
        {
            use sha2::{Digest, Sha256};

            let mut hasher = Sha256::new();
            hasher.update(password);
            if let Some(s) = salt {
                hasher.update(s);
            }
            let key = hasher.finalize();

            Self::new(&key)
        }
    }
}

impl Encryptor for Aes256GcmEncryptor {
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        #[cfg(not(feature = "encryption-aes-gcm"))]
        {
            let _ = data;
            return Err(FlareError::operation_not_supported(
                "aes-256-gcm encryption feature is disabled",
            ));
        }

        #[cfg(feature = "encryption-aes-gcm")]
        {
            use aes_gcm::{
                Aes256Gcm as AesGcm,
                aead::{Aead, AeadCore, KeyInit, OsRng},
            };
            tracing::debug!("Encrypting data: {:?}", data);
            // 创建加密器
            let cipher = AesGcm::new_from_slice(&self.key).map_err(|e| {
                FlareError::encoding_error(format!("Failed to create AES-GCM cipher: {}", e))
            })?;

            // 生成随机 nonce（12 字节）
            let nonce = AesGcm::generate_nonce(&mut OsRng);

            // 加密数据
            let ciphertext = cipher.encrypt(&nonce, data).map_err(|e| {
                FlareError::encoding_error(format!("AES-GCM encryption failed: {}", e))
            })?;

            // 组合 nonce 和 ciphertext: [nonce (12 bytes)][ciphertext]
            // nonce 是 GenericArray<u8, U12>，转换为数组
            // 使用 into() 将 GenericArray 转换为数组
            let nonce_bytes: [u8; 12] = nonce.into();
            let mut result = Vec::with_capacity(12 + ciphertext.len());
            result.extend_from_slice(&nonce_bytes);
            result.extend_from_slice(&ciphertext);

            Ok(result)
        }
    }

    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        #[cfg(not(feature = "encryption-aes-gcm"))]
        {
            let _ = data;
            return Err(FlareError::operation_not_supported(
                "aes-256-gcm encryption feature is disabled",
            ));
        }

        #[cfg(feature = "encryption-aes-gcm")]
        {
            use aes_gcm::{
                Aes256Gcm as AesGcm, Nonce,
                aead::{Aead, KeyInit},
            };

            tracing::debug!("Decrypting data: {:?}", data);
            // 检查数据长度（至少需要 12 字节 nonce）
            if data.len() < 12 {
                return Err(FlareError::deserialization_error(format!(
                    "Encrypted data too short: expected at least 12 bytes, got {}",
                    data.len()
                )));
            }

            // 提取 nonce 和 ciphertext
            let (nonce_bytes, ciphertext) = data.split_at(12);

            // 将 nonce_bytes 转换为固定大小数组
            let nonce_array: [u8; 12] = nonce_bytes.try_into().map_err(|_| {
                FlareError::deserialization_error(
                    "Failed to convert nonce bytes to array".to_string(),
                )
            })?;

            // 创建 Nonce（使用 From trait）
            let nonce = Nonce::from(nonce_array);

            // 创建解密器
            let cipher = AesGcm::new_from_slice(&self.key).map_err(|e| {
                FlareError::encoding_error(format!("Failed to create AES-GCM cipher: {}", e))
            })?;

            // 解密数据
            let plaintext = cipher.decrypt(&nonce, ciphertext).map_err(|e| {
                FlareError::deserialization_error(format!("AES-GCM decryption failed: {}", e))
            })?;

            Ok(plaintext)
        }
    }

    fn algorithm(&self) -> EncryptionAlgorithm {
        EncryptionAlgorithm::Aes256Gcm
    }

    fn name(&self) -> &'static str {
        "aes256gcm"
    }
}
