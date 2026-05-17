//! 序列化模块
//!
//! 提供可扩展的序列化接口，支持用户自定义序列化格式

pub mod formats;
pub mod registry;
pub mod traits;

pub use formats::{FramedProtobufSerializer, JsonSerializer, ProtobufSerializer};
pub use registry::{SerializationRegistry, SerializationUtil};
pub use traits::Serializer;
