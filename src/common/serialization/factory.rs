//! 序列化器工厂模块
//!
//! **注意：此模块已废弃**
//!
//! 请使用 `crate::common::parsing::PayloadCodec` 替代。
//! 新的设计采用枚举模式，避免 trait object 的复杂性。
//!
//! # 迁移指南
//!
//! ```rust
//! // 旧代码
//! // let serializer = SerializerFactory::create(&config);
//!
//! // 新代码
//! use crate::common::parsing::PayloadCodec;
//! let codec = PayloadCodec::from_format(config.format);
//! ```

use super::{SerializationConfig, SerializationFormat};
use crate::common::serialization::json::JsonSerializer;
use crate::common::serialization::protobuf::ProtobufSerializer;

/// 序列化器枚举（已废弃）
///
/// 请使用 `parsing::PayloadCodec` 替代
#[deprecated(since = "0.1.0", note = "请使用 parsing::PayloadCodec 替代")]
pub enum AnySerializer {
    Json(JsonSerializer),
    Protobuf(ProtobufSerializer),
}

/// 序列化器工厂（已废弃）
///
/// 请使用 `parsing::PayloadCodec::from_format()` 替代
#[deprecated(since = "0.1.0", note = "请使用 parsing::PayloadCodec::from_format() 替代")]
pub struct SerializerFactory;

#[allow(deprecated)]
impl SerializerFactory {
    /// 创建序列化器（已废弃）
    #[deprecated(since = "0.1.0", note = "请使用 parsing::PayloadCodec::from_format() 替代")]
    pub fn create(config: &SerializationConfig) -> AnySerializer {
        match config.format {
            SerializationFormat::Json => AnySerializer::Json(JsonSerializer),
            SerializationFormat::Protobuf => AnySerializer::Protobuf(ProtobufSerializer),
        }
    }
}
