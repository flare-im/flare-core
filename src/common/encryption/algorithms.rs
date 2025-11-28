//! 内置加密算法实现
//! 
//! 提供常用的加密算法实现

use crate::common::error::{FlareError, Result};
use super::traits::Encryptor;

/// 加密算法类型枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum EncryptionAlgorithm {
    /// 不使用加密
    None,
    /// AES-256-GCM 加密
    Aes256Gcm,
}

impl EncryptionAlgorithm {
    /// 从字符串转换为加密算法
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "none" | "" => Some(EncryptionAlgorithm::None),
            "aes256gcm" | "aes-256-gcm" => Some(EncryptionAlgorithm::Aes256Gcm),
            _ => None,
        }
    }

    /// 转换为字符串
    pub fn as_str(&self) -> &'static str {
        match self {
            EncryptionAlgorithm::None => "none",
            EncryptionAlgorithm::Aes256Gcm => "aes256gcm",
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
/// 加密后的数据格式: [nonce (12 bytes)][ciphertext]
/// 
/// # 安全说明
/// - 每次加密都会生成新的随机 nonce，确保相同明文产生不同密文
/// - 密钥必须严格保密，建议使用安全的密钥派生函数（如 PBKDF2）从密码生成
/// - nonce 会随密文一起存储，解密时需要提取
pub struct Aes256GcmEncryptor {
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
        if key.len() != 32 {
            return Err(FlareError::protocol_error(
                format!("AES-256-GCM requires a 32-byte key, got {} bytes", key.len())
            ));
        }
        
        let mut key_array = [0u8; 32];
        key_array.copy_from_slice(key);
        
        Ok(Self {
            key: key_array,
        })
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
        use sha2::{Sha256, Digest};
        
        let mut hasher = Sha256::new();
        hasher.update(password);
        if let Some(s) = salt {
            hasher.update(s);
        }
        let key = hasher.finalize();
        
        Self::new(&key)
    }
}

impl Encryptor for Aes256GcmEncryptor {
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        use aes_gcm::{
            aead::{Aead, AeadCore, KeyInit, OsRng},
            Aes256Gcm as AesGcm,
            Nonce,
        };
        
        // 创建加密器
        let cipher = AesGcm::new_from_slice(&self.key)
            .map_err(|e| FlareError::encoding_error(format!("Failed to create AES-GCM cipher: {}", e)))?;
        
        // 生成随机 nonce（12 字节）
        let nonce = AesGcm::generate_nonce(&mut OsRng);
        
        // 加密数据
        let ciphertext = cipher.encrypt(&nonce, data)
            .map_err(|e| FlareError::encoding_error(format!("AES-GCM encryption failed: {}", e)))?;
        
        // 组合 nonce 和 ciphertext: [nonce (12 bytes)][ciphertext]
        // nonce 是 GenericArray<u8, U12>，转换为数组
        // 使用 into() 将 GenericArray 转换为数组
        let nonce_bytes: [u8; 12] = nonce.into();
        let mut result = Vec::with_capacity(12 + ciphertext.len());
        result.extend_from_slice(&nonce_bytes);
        result.extend_from_slice(&ciphertext);
        
        Ok(result)
    }

    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        use aes_gcm::{
            aead::{Aead, KeyInit},
            Aes256Gcm as AesGcm,
            Nonce,
        };
        
        // 检查数据长度（至少需要 12 字节 nonce）
        if data.len() < 12 {
            return Err(FlareError::deserialization_error(
                format!("Encrypted data too short: expected at least 12 bytes, got {}", data.len())
            ));
        }
        
        // 提取 nonce 和 ciphertext
        let (nonce_bytes, ciphertext) = data.split_at(12);
        
        // 将 nonce_bytes 转换为固定大小数组
        let nonce_array: [u8; 12] = nonce_bytes.try_into()
            .map_err(|_| FlareError::deserialization_error(
                "Failed to convert nonce bytes to array".to_string()
            ))?;
        
        // 创建 Nonce（使用 From trait）
        let nonce = Nonce::from(nonce_array);
        
        // 创建解密器
        let cipher = AesGcm::new_from_slice(&self.key)
            .map_err(|e| FlareError::encoding_error(format!("Failed to create AES-GCM cipher: {}", e)))?;
        
        // 解密数据
        let plaintext = cipher.decrypt(&nonce, ciphertext)
            .map_err(|e| FlareError::deserialization_error(format!("AES-GCM decryption failed: {}", e)))?;
        
        Ok(plaintext)
    }

    fn algorithm(&self) -> EncryptionAlgorithm {
        EncryptionAlgorithm::Aes256Gcm
    }

    fn name(&self) -> &'static str {
        "aes256gcm"
    }
}

