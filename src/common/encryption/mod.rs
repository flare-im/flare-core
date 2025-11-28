//! 加密模块
//! 
//! 提供可扩展的加密接口，支持用户自定义加密算法实现

pub mod traits;
pub mod algorithms;
pub mod registry;

pub use traits::Encryptor;
pub use algorithms::{EncryptionAlgorithm, NoEncryptor, Aes256GcmEncryptor};
pub use registry::{EncryptionRegistry, EncryptionUtil};

