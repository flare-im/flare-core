//! 内置序列化格式实现
//!
//! 提供常用的序列化格式实现

use super::traits::Serializer;
use crate::common::error::{FlareError, Result};
use crate::common::protocol::{Frame, SerializationFormat};
use crate::common::protobuf_decoder::{safe_protobuf_decode, ProtobufDecoder};


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
        // 使用安全的protobuf解码函数
        safe_protobuf_decode::<Frame>(data)
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

/// 带粘包处理的 Protobuf 序列化器
/// 
/// 用于处理带长度前缀的 Protobuf 消息，防止将 varint 长度前缀误认为字符串内容
/// 这解决了 protobuf string 字段前出现 "\x0c" 的问题，该问题是由于 length varint 
/// 被当成字符串解码导致的
pub struct FramedProtobufSerializer {
    /// 用于处理粘包的解码器
    _decoder: Option<ProtobufDecoder<Frame>>, 
}

impl FramedProtobufSerializer {
    pub fn new() -> Self {
        Self { _decoder: None }
    }

    /// 为当前线程/连接创建独立的解码器实例
    #[allow(dead_code)]
    fn get_or_create_decoder(&mut self) -> &mut ProtobufDecoder<Frame> {
        if self._decoder.is_none() {
            self._decoder = Some(ProtobufDecoder::new());
        }
        self._decoder.as_mut().unwrap()
    }
}

impl Serializer for FramedProtobufSerializer {
    fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        prost::Message::encode(frame, &mut buf)
            .map_err(|e| FlareError::encoding_error(format!("Framed Protobuf encode error: {}", e)))?;
        
        // 为消息添加长度前缀
        let mut prefixed_buf = Vec::new();
        prost::encoding::encode_varint(buf.len() as u64, &mut prefixed_buf);
        prefixed_buf.extend_from_slice(&buf);
        
        Ok(prefixed_buf)
    }

    fn deserialize(&self, data: &[u8]) -> Result<Frame> {
        // 对于带前缀的protobuf消息，需要使用专用解码器
        // 这里我们直接使用安全解码函数，因为单次解码不需要维护状态
        safe_protobuf_decode::<Frame>(data)
            .map_err(|e| FlareError::deserialization_error(format!("Framed Protobuf decode error: {}", e)))
    }

    fn format(&self) -> SerializationFormat {
        SerializationFormat::Protobuf
    }

    fn name(&self) -> &'static str {
        "framed_protobuf"
    }

    fn can_detect(&self, data: &[u8]) -> bool {
        // 检查是否可能是带前缀的protobuf消息
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