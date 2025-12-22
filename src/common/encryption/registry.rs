//! 加密器注册表
//!
//! 管理加密器的注册和查找，支持用户注册自定义加密器

use super::algorithms::NoEncryptor;
use super::traits::Encryptor;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

lazy_static::lazy_static! {
    /// 全局加密器注册表
    static ref ENCRYPTION_REGISTRY: EncryptionRegistry = {
        let registry = EncryptionRegistry::new();
        // 注册内置加密器
        registry.register_defaults();
        registry
    };
}

/// 加密器注册表
///
/// 管理加密器的注册和查找
pub struct EncryptionRegistry {
    encryptors: Arc<RwLock<HashMap<String, Arc<dyn Encryptor>>>>,
}

impl Default for EncryptionRegistry {
    fn default() -> Self {
        Self {
            encryptors: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl EncryptionRegistry {
    /// 创建新的注册表
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册内置加密器
    pub fn register_defaults(&self) {
        self.register("none", Arc::new(NoEncryptor));
        // AES-256-GCM 需要密钥，不能直接注册，需要用户提供密钥后创建
    }

    /// 注册加密器
    ///
    /// # 参数
    /// - `name`: 加密器名称（用于查找）
    /// - `encryptor`: 加密器实例
    ///
    /// # 示例
    ///
    /// ```rust
    /// use flare_core::common::encryption::{EncryptionRegistry, Encryptor};
    /// use std::sync::Arc;
    ///
    /// struct MyEncryptor;
    /// impl Encryptor for MyEncryptor { /* ... */ }
    ///
    /// let registry = EncryptionRegistry::new();
    /// registry.register("my_custom", Arc::new(MyEncryptor));
    /// ```
    pub fn register(&self, name: &str, encryptor: Arc<dyn Encryptor>) {
        if let Ok(mut encryptors) = self.encryptors.write() {
            encryptors.insert(name.to_string(), encryptor);
        }
    }

    /// 查找加密器
    ///
    /// # 参数
    /// - `name`: 加密器名称
    ///
    /// # 返回
    /// 找到的加密器，如果不存在则返回 None
    pub fn find(&self, name: &str) -> Option<Arc<dyn Encryptor>> {
        if let Ok(encryptors) = self.encryptors.read() {
            encryptors.get(name).cloned()
        } else {
            None
        }
    }

    /// 检查加密器是否已注册
    ///
    /// # 参数
    /// - `name`: 加密器名称
    ///
    /// # 返回
    /// 如果已注册返回 `true`，否则返回 `false`
    pub fn is_registered(&self, name: &str) -> bool {
        if let Ok(encryptors) = self.encryptors.read() {
            encryptors.contains_key(name)
        } else {
            false
        }
    }

    /// 获取所有已注册的加密器名称
    ///
    /// # 返回
    /// 已注册的加密器名称列表
    pub fn list_registered(&self) -> Vec<String> {
        if let Ok(encryptors) = self.encryptors.read() {
            encryptors.keys().cloned().collect()
        } else {
            Vec::new()
        }
    }
}

/// 全局加密器注册表访问器
impl EncryptionRegistry {
    /// 获取全局注册表实例
    pub fn global() -> &'static EncryptionRegistry {
        &ENCRYPTION_REGISTRY
    }
}

/// 加密工具类
///
/// 提供便捷的全局加密器访问方法
pub struct EncryptionUtil;

impl EncryptionUtil {
    /// 查找加密器（从全局注册表）
    ///
    /// # 参数
    /// - `name`: 加密器名称
    ///
    /// # 返回
    /// 找到的加密器，如果不存在则返回 None
    ///
    /// # 示例
    ///
    /// ```rust
    /// use flare_core::common::encryption::EncryptionUtil;
    ///
    /// let encryptor = EncryptionUtil::find("none");
    /// if let Some(enc) = encryptor {
    ///     let encrypted = enc.encrypt(b"hello").unwrap();
    ///     let decrypted = enc.decrypt(&encrypted).unwrap();
    /// }
    /// ```
    pub fn find(name: &str) -> Option<Arc<dyn Encryptor>> {
        EncryptionRegistry::global().find(name)
    }

    /// 检查加密器是否已注册
    ///
    /// # 参数
    /// - `name`: 加密器名称
    ///
    /// # 返回
    /// 如果已注册返回 `true`，否则返回 `false`
    pub fn is_registered(name: &str) -> bool {
        EncryptionRegistry::global().is_registered(name)
    }

    /// 注册自定义加密器（到全局注册表）
    ///
    /// # 参数
    /// - `encryptor`: 加密器实例
    ///
    /// # 示例
    ///
    /// ```rust
    /// use flare_core::common::encryption::{EncryptionUtil, Encryptor};
    /// use std::sync::Arc;
    ///
    /// struct MyEncryptor;
    /// impl Encryptor for MyEncryptor { /* ... */ }
    ///
    /// EncryptionUtil::register_custom(Arc::new(MyEncryptor));
    /// ```
    pub fn register_custom(encryptor: Arc<dyn Encryptor>) {
        let name = encryptor.name();
        // 如果已注册同名加密器，记录警告但不覆盖（避免密钥不一致问题）
        if EncryptionUtil::is_registered(name) {
            tracing::warn!(
                "Encryptor '{}' is already registered. Skipping registration to avoid key mismatch.",
                name
            );
            return;
        }
        EncryptionRegistry::global().register(name, encryptor);
        tracing::debug!("Registered encryptor: {}", name);
    }

    /// 获取所有已注册的加密器名称列表
    ///
    /// # 返回
    /// 已注册的加密器名称列表
    pub fn list_registered() -> Vec<String> {
        EncryptionRegistry::global().list_registered()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encryption_registry() {
        let registry = EncryptionRegistry::new();
        registry.register_defaults();

        assert!(registry.find("none").is_some());
        assert!(registry.find("unknown").is_none());
    }

    #[test]
    fn test_no_encryptor() {
        let encryptor = NoEncryptor;
        let data = b"hello world";
        let encrypted = encryptor.encrypt(data).unwrap();
        let decrypted = encryptor.decrypt(&encrypted).unwrap();
        assert_eq!(data, decrypted.as_slice());
    }

    #[test]
    fn test_aes256gcm_encryptor() {
        use crate::common::encryption::Aes256GcmEncryptor;

        // 创建测试密钥（32 字节）
        let key = b"01234567890123456789012345678901"; // 32 bytes
        let encryptor = Aes256GcmEncryptor::new(key).unwrap();

        // 测试数据
        let plaintext = b"Hello, World! This is a test message for AES-256-GCM encryption.";

        // 加密
        let ciphertext = encryptor.encrypt(plaintext).unwrap();

        // 验证密文长度（应该包含 12 字节 nonce + 密文 + 16 字节认证标签）
        // GCM 模式是流式加密，密文长度 = 明文长度 + 16 字节认证标签
        assert!(ciphertext.len() >= plaintext.len() + 12 + 16); // nonce(12) + plaintext + tag(16)

        // 解密
        let decrypted = encryptor.decrypt(&ciphertext).unwrap();
        assert_eq!(plaintext, decrypted.as_slice());

        // 测试相同明文产生不同密文（由于随机 nonce）
        let ciphertext2 = encryptor.encrypt(plaintext).unwrap();
        assert_ne!(ciphertext, ciphertext2); // 应该不同

        // 但解密后应该相同
        let decrypted2 = encryptor.decrypt(&ciphertext2).unwrap();
        assert_eq!(plaintext, decrypted2.as_slice());
    }

    #[test]
    fn test_aes256gcm_from_password() {
        use crate::common::encryption::Aes256GcmEncryptor;

        // 从密码派生密钥
        let password = b"my_secret_password";
        let salt = b"some_salt";
        let encryptor = Aes256GcmEncryptor::from_password(password, Some(salt)).unwrap();

        let plaintext = b"Test message";
        let ciphertext = encryptor.encrypt(plaintext).unwrap();
        let decrypted = encryptor.decrypt(&ciphertext).unwrap();
        assert_eq!(plaintext, decrypted.as_slice());
    }

    #[test]
    fn test_aes256gcm_invalid_key_length() {
        use crate::common::encryption::Aes256GcmEncryptor;

        // 测试无效密钥长度
        let short_key = b"short"; // 只有 5 字节
        assert!(Aes256GcmEncryptor::new(short_key).is_err());

        let long_key = b"0123456789012345678901234567890123456789"; // 超过 32 字节
        assert!(Aes256GcmEncryptor::new(long_key).is_err());
    }

    #[test]
    fn test_aes256gcm_invalid_ciphertext() {
        use crate::common::encryption::Aes256GcmEncryptor;

        let key = b"01234567890123456789012345678901";
        let encryptor = Aes256GcmEncryptor::new(key).unwrap();

        // 测试太短的密文
        let short_data = b"short";
        assert!(encryptor.decrypt(short_data).is_err());

        // 测试无效的密文
        let invalid_data = vec![0u8; 20]; // 长度足够但内容无效
        assert!(encryptor.decrypt(&invalid_data).is_err());
    }
}
