//! 内置序列化格式实现
//!
//! 提供常用的序列化格式实现

use super::traits::Serializer;
use crate::common::error::{FlareError, Result};
use crate::common::protocol::{Frame, SerializationFormat};

/// Protobuf 序列化器
pub struct ProtobufSerializer;

impl Serializer for ProtobufSerializer {
    fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        prost::Message::encode(frame, &mut buf)
            .map_err(|e| FlareError::encoding_error(format!("Protobuf encode error: {}", e)))?;
        Ok(buf)
    }

    fn deserialize(&self, data: &[u8]) -> Result<Frame> {
        prost::Message::decode(data)
            .map_err(|e| FlareError::deserialization_error(format!("Protobuf decode error: {}", e)))
    }

    fn format(&self) -> SerializationFormat {
        SerializationFormat::Protobuf
    }

    fn name(&self) -> &'static str {
        "protobuf"
    }

    fn can_detect(&self, data: &[u8]) -> bool {
        // Protobuf 没有标准的魔数，但可以尝试解析
        // 这里简化处理，总是返回 true，让解析器尝试
        !data.is_empty()
    }
}

/// JSON 序列化器
pub struct JsonSerializer;

impl Serializer for JsonSerializer {
    fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
        serde_json::to_vec(frame)
            .map_err(|e| FlareError::serialization_error(format!("JSON encode error: {}", e)))
    }

    fn deserialize(&self, data: &[u8]) -> Result<Frame> {
        serde_json::from_slice(data)
            .map_err(|e| FlareError::deserialization_error(format!("JSON decode error: {}", e)))
    }

    fn format(&self) -> SerializationFormat {
        SerializationFormat::Json
    }

    fn name(&self) -> &'static str {
        "json"
    }

    fn can_detect(&self, data: &[u8]) -> bool {
        // JSON 通常以 { 或 [ 开头
        if data.is_empty() {
            return false;
        }
        let first = data[0];
        first == b'{' || first == b'['
    }
}
