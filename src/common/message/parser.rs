//! 消息解析模块
//! 
//! 负责将原始字节数据解析为 Frame 消息
//! 使用压缩器和序列化器模块的标准接口，支持自动检测和扩展

use crate::common::error::Result;
use crate::common::protocol::{Frame, SerializationFormat};
use crate::common::compression::{CompressionUtil, CompressionAlgorithm};
use crate::common::serializer::SerializationUtil;

/// 消息解析器
#[derive(Clone)]
pub struct MessageParser {
    default_format: SerializationFormat,
    default_compression: CompressionAlgorithm,
}

impl MessageParser {
    /// 创建新的消息解析器
    pub fn new(format: SerializationFormat, compression: CompressionAlgorithm) -> Self {
        Self {
            default_format: format,
            default_compression: compression,
        }
    }

    /// 创建使用 Protobuf 格式的解析器
    pub fn protobuf() -> Self {
        Self::new(SerializationFormat::Protobuf, CompressionAlgorithm::None)
    }

    /// 创建使用 JSON 格式的解析器
    pub fn json() -> Self {
        Self::new(SerializationFormat::Json, CompressionAlgorithm::None)
    }

    /// 获取默认序列化格式
    pub fn default_format(&self) -> SerializationFormat {
        self.default_format
    }

    /// 获取默认压缩算法
    pub fn default_compression(&self) -> CompressionAlgorithm {
        self.default_compression
    }

    /// 解析消息（自动检测格式和压缩）
    /// 
    /// 首先尝试自动检测压缩，然后自动检测序列化格式并解析
    pub fn parse(&self, data: &[u8]) -> Result<Frame> {
        // 解压缩数据
        let decompressed = self.decompress_data(data)?;
        
        // 尝试自动检测并解析
        self.parse_decompressed(&decompressed)
    }

    /// 根据指定格式解析消息
    pub fn parse_with_format(&self, data: &[u8], format: SerializationFormat) -> Result<Frame> {
        // 解压缩数据
        let decompressed = self.decompress_data(data)?;

        // 使用指定的序列化器
        let serializer = SerializationUtil::get_serializer(format)
            .ok_or_else(|| crate::common::error::FlareError::deserialization_error(
                format!("Serializer not found for format: {:?}", format)
            ))?;

        serializer.deserialize(&decompressed)
    }

    /// 序列化消息（使用默认格式）
    pub fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
        self.serialize_with_format(frame, self.default_format, self.default_compression)
    }

    /// 序列化消息（指定格式和压缩）
    pub fn serialize_with_format(
        &self,
        frame: &Frame,
        format: SerializationFormat,
        compression: CompressionAlgorithm,
    ) -> Result<Vec<u8>> {
        // 使用指定的序列化器
        let serializer = SerializationUtil::get_serializer(format)
            .ok_or_else(|| crate::common::error::FlareError::encoding_error(
                format!("Serializer not found for format: {:?}", format)
            ))?;

        // 序列化
        let data = serializer.serialize(frame)?;

        // 应用压缩
        CompressionUtil::compress(&data, compression)
    }

    /// 从 Frame 的 metadata 中读取压缩算法
    pub fn get_compression_from_frame(frame: &Frame) -> CompressionAlgorithm {
        frame.metadata
            .get("compression")
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .and_then(|s| CompressionAlgorithm::from_str(s))
            .unwrap_or(CompressionAlgorithm::None)
    }

    /// 从 Frame 的 metadata 中读取序列化格式
    pub fn get_format_from_frame(frame: &Frame) -> Option<SerializationFormat> {
        frame.metadata
            .get("format")
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .and_then(|s| {
                match s.to_lowercase().as_str() {
                    "protobuf" => Some(SerializationFormat::Protobuf),
                    "json" => Some(SerializationFormat::Json),
                    _ => None,
                }
            })
    }
    
    // ============================================================================
    // 内部辅助方法
    // ============================================================================
    
    /// 解压缩数据（内部辅助方法）
    fn decompress_data(&self, data: &[u8]) -> Result<Vec<u8>> {
        let (decompressed, _algorithm) = CompressionUtil::auto_decompress(data)?;
        Ok(decompressed)
    }
    
    /// 解析已解压缩的数据（内部辅助方法）
    fn parse_decompressed(&self, decompressed: &[u8]) -> Result<Frame> {
        // 尝试自动检测序列化格式
        let detected_serializers = SerializationUtil::auto_detect(decompressed);
        
        // 尝试每个检测到的序列化器
        for serializer in detected_serializers {
            if let Ok(frame) = serializer.deserialize(decompressed) {
                return Ok(frame);
            }
        }

        // 如果自动检测失败，尝试所有已注册的序列化器
        self.try_all_serializers(decompressed)
    }
    
    /// 尝试所有已注册的序列化器（内部辅助方法）
    fn try_all_serializers(&self, data: &[u8]) -> Result<Frame> {
        for format in [SerializationFormat::Protobuf, SerializationFormat::Json] {
            if let Some(serializer) = SerializationUtil::get_serializer(format) {
                if let Ok(frame) = serializer.deserialize(data) {
                    return Ok(frame);
                }
            }
        }

        Err(crate::common::error::FlareError::deserialization_error(
            "Failed to parse message: no compatible serializer found".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::{FrameBuilder, ping};

    #[test]
    fn test_parse_protobuf() {
        let parser = MessageParser::protobuf();
        let frame = FrameBuilder::new()
            .with_command(crate::common::protocol::Command {
                r#type: Some(crate::common::protocol::flare::core::commands::command::Type::System(ping())),
            })
            .build();
        
        let data = parser.serialize(&frame).unwrap();
        let parsed = parser.parse(&data).unwrap();
        assert_eq!(parsed.message_id, frame.message_id);
    }

    #[test]
    fn test_parse_json() {
        let parser = MessageParser::json();
        let frame = FrameBuilder::new()
            .with_command(crate::common::protocol::Command {
                r#type: Some(crate::common::protocol::flare::core::commands::command::Type::System(ping())),
            })
            .build();
        
        let data = parser.serialize(&frame).unwrap();
        let parsed = parser.parse(&data).unwrap();
        assert_eq!(parsed.message_id, frame.message_id);
    }
}
