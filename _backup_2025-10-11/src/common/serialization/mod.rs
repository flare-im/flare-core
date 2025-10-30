//! 消息帧序列化和反序列化模块
//!
//! 提供统一的消息帧序列化接口，支持扩展不同的序列化格式

pub mod traits;
pub mod json;
pub mod msgpack;
pub mod bincode;
pub mod protobuf;
pub mod cbor;
pub mod factory;

// 重新导出主要类型
pub use traits::{FrameSerializer, SerializationFormat, SerializationConfig};
pub use json::JsonSerializer;
pub use msgpack::MessagePackSerializer;
pub use bincode::BincodeSerializer;
pub use protobuf::ProtobufSerializer;
pub use cbor::CborSerializer;
pub use factory::{
    SerializerFactory, json_serializer, json_pretty_serializer, default_serializer,
    msgpack_serializer, bincode_serializer, protobuf_serializer, cbor_serializer,
    high_performance_serializer
};