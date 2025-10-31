//! 序列化模块
//! 
//! 提供可扩展的序列化接口，支持用户自定义序列化格式

pub mod traits;
pub mod formats;
pub mod registry;

pub use traits::Serializer;
pub use formats::{ProtobufSerializer, JsonSerializer};
pub use registry::{SerializationRegistry, SerializationUtil};

