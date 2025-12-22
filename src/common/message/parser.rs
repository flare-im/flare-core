//! 消息解析模块
//!
//! 负责将原始字节数据解析为 Frame 消息
//! 使用压缩器、序列化器和加密器模块的标准接口，支持自动检测和扩展

use crate::common::compression::{CompressionAlgorithm, CompressionUtil};
use crate::common::encryption::{EncryptionAlgorithm, EncryptionUtil};
use crate::common::error::Result;
use crate::common::protocol::{Frame, SerializationFormat};
use crate::common::serializer::SerializationUtil;
use lazy_static::lazy_static;

// 协商前的消息解析器（全局共享）
// 所有连接在协商完成前都使用相同的配置：JSON、不压缩、不加密
// 使用场景：CONNECT、CONNECT_ACK、NEGOTIATION_READY 消息的解析和序列化
// 使用 lazy_static 实现全局单例，避免每次消息处理都创建新的 parser
lazy_static! {
    pub static ref PRE_NEGOTIATION_PARSER: MessageParser = MessageParser::new(
        SerializationFormat::Json,
        CompressionAlgorithm::None,
        EncryptionAlgorithm::None,
    );
}

/// 消息解析器
#[derive(Debug, Clone)]
pub struct MessageParser {
    default_format: SerializationFormat,
    default_compression: CompressionAlgorithm,
    default_encryption: EncryptionAlgorithm,
    /// 自定义序列化格式名称（可选）
    ///
    /// 当使用自定义序列化格式时，通过此字段指定格式名称
    /// 如果设置了此字段，序列化/反序列化时会优先使用名称查找序列化器
    /// 否则使用 `default_format` 对应的内置序列化器
    custom_format_name: Option<String>,
}

impl MessageParser {
    /// 创建新的消息解析器
    pub fn new(
        format: SerializationFormat,
        compression: CompressionAlgorithm,
        encryption: EncryptionAlgorithm,
    ) -> Self {
        Self {
            default_format: format,
            default_compression: compression,
            default_encryption: encryption,
            custom_format_name: None,
        }
    }

    /// 创建使用自定义序列化格式的解析器
    ///
    /// # 参数
    /// - `format_name`: 自定义序列化格式名称（必须在注册表中注册）
    /// - `compression`: 压缩算法
    /// - `encryption`: 加密算法
    ///
    /// # 示例
    /// ```rust
    /// use flare_core::common::message::MessageParser;
    /// use flare_core::common::compression::CompressionAlgorithm;
    /// use flare_core::common::encryption::EncryptionAlgorithm;
    ///
    /// // 创建使用自定义格式的解析器
    /// let parser = MessageParser::with_custom_format(
    ///     "messagepack",
    ///     CompressionAlgorithm::None,
    ///     EncryptionAlgorithm::None,
    /// );
    /// ```
    pub fn with_custom_format(
        format_name: &str,
        compression: CompressionAlgorithm,
        encryption: EncryptionAlgorithm,
    ) -> Self {
        Self {
            default_format: SerializationFormat::Json, // 占位符，实际使用 custom_format_name
            default_compression: compression,
            default_encryption: encryption,
            custom_format_name: Some(format_name.to_string()),
        }
    }

    /// 创建使用指定格式和压缩的解析器
    pub fn new_with_format_compression(
        format: SerializationFormat,
        compression: CompressionAlgorithm,
    ) -> Self {
        Self::new(format, compression, EncryptionAlgorithm::None)
    }

    /// 创建使用 Protobuf 格式的解析器
    pub fn protobuf() -> Self {
        Self::new(
            SerializationFormat::Protobuf,
            CompressionAlgorithm::None,
            EncryptionAlgorithm::None,
        )
    }

    /// 创建使用 JSON 格式的解析器
    pub fn json() -> Self {
        Self::new(
            SerializationFormat::Json,
            CompressionAlgorithm::None,
            EncryptionAlgorithm::None,
        )
    }

    /// 获取默认序列化格式
    pub fn default_format(&self) -> SerializationFormat {
        self.default_format
    }

    /// 获取默认压缩算法
    pub fn default_compression(&self) -> CompressionAlgorithm {
        self.default_compression.clone()
    }

    /// 获取默认加密算法
    pub fn default_encryption(&self) -> EncryptionAlgorithm {
        self.default_encryption.clone()
    }

    /// 解析消息（自动检测格式、压缩和加密）
    ///
    /// 处理流程：解密 -> 解压缩 -> 反序列化
    /// 默认使用容错模式（解密失败时尝试作为未加密数据处理）
    pub fn parse(&self, data: &[u8]) -> Result<Frame> {
        self.parse_with_fallback(data, true)
    }

    /// 解析消息（支持容错标记）
    ///
    /// # 参数
    /// - `data`: 要解析的原始数据
    /// - `allow_fallback`: 如果为 true，启用容错模式：
    ///   - 解密失败时尝试作为未加密数据处理
    ///   - 解压缩失败时尝试作为未压缩数据处理
    ///   - 反序列化失败时尝试所有序列化格式
    ///   - 如果为 false，严格模式：任何步骤失败都直接返回错误
    ///
    /// # 处理流程
    /// 解密（根据 allow_fallback 决定是否容错） -> 解压缩（根据 allow_fallback 决定是否容错） -> 反序列化（根据 allow_fallback 决定是否容错）
    pub fn parse_with_fallback(&self, data: &[u8], allow_fallback: bool) -> Result<Frame> {
        // 1. 解密数据（根据 allow_fallback 决定是否容错）
        let decrypted = self.decrypt_data_with_fallback(data, allow_fallback)?;

        // 2. 解压缩数据（根据 allow_fallback 决定是否容错）
        let decompressed = self.decompress_data_with_fallback(&decrypted, allow_fallback)?;

        // 3. 反序列化（根据 allow_fallback 决定是否容错）
        self.parse_decompressed_with_fallback(&decompressed, allow_fallback)
    }

    /// 根据指定格式解析消息
    pub fn parse_with_format(&self, data: &[u8], format: SerializationFormat) -> Result<Frame> {
        // 1. 解密数据
        let decrypted = self.decrypt_data(data)?;

        // 2. 解压缩数据
        let decompressed = self.decompress_data(&decrypted)?;

        // 3. 使用指定的序列化器（优先使用自定义格式名称）
        let serializer = if let Some(custom_name) = &self.custom_format_name {
            // 如果指定了自定义格式名称，优先使用名称查找
            SerializationUtil::get_serializer_by_name(custom_name)
        } else {
            // 否则使用格式枚举查找
            SerializationUtil::get_serializer(format)
        }
        .ok_or_else(|| {
            let format_info = if let Some(name) = &self.custom_format_name {
                format!("custom format '{}'", name)
            } else {
                format!("{:?}", format)
            };
            crate::common::error::FlareError::deserialization_error(format!(
                "Serializer not found: {}",
                format_info
            ))
        })?;

        serializer.deserialize(&decompressed)
    }

    /// 序列化消息（使用默认格式、压缩和加密）
    pub fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
        self.serialize_with_format(
            frame,
            self.default_format,
            self.default_compression.clone(),
            self.default_encryption.clone(),
        )
    }

    /// 序列化消息（指定格式、压缩和加密）
    ///
    /// 处理流程：序列化 -> 压缩 -> 加密
    pub fn serialize_with_format(
        &self,
        frame: &Frame,
        format: SerializationFormat,
        compression: CompressionAlgorithm,
        encryption: EncryptionAlgorithm,
    ) -> Result<Vec<u8>> {
        // 1. 使用指定的序列化器序列化（优先使用自定义格式名称）
        let serializer = if let Some(custom_name) = &self.custom_format_name {
            // 如果指定了自定义格式名称，优先使用名称查找
            SerializationUtil::get_serializer_by_name(custom_name)
        } else {
            // 否则使用格式枚举查找
            SerializationUtil::get_serializer(format)
        }
        .ok_or_else(|| {
            let format_info = if let Some(name) = &self.custom_format_name {
                format!("custom format '{}'", name)
            } else {
                format!("{:?}", format)
            };
            crate::common::error::FlareError::encoding_error(format!(
                "Serializer not found: {}",
                format_info
            ))
        })?;

        let data = serializer.serialize(frame)?;

        // 2. 应用压缩
        let compressed = CompressionUtil::compress(&data, compression)?;

        // 3. 应用加密
        self.encrypt_data(&compressed, encryption)
    }

    /// 序列化消息（指定格式和压缩，使用默认加密）
    pub fn serialize_with_format_compression(
        &self,
        frame: &Frame,
        format: SerializationFormat,
        compression: CompressionAlgorithm,
    ) -> Result<Vec<u8>> {
        self.serialize_with_format(frame, format, compression, self.default_encryption.clone())
    }

    /// 从 Frame 的 metadata 中读取压缩算法
    pub fn get_compression_from_frame(frame: &Frame) -> CompressionAlgorithm {
        frame
            .metadata
            .get("compression")
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .and_then(CompressionAlgorithm::from_str)
            .unwrap_or(CompressionAlgorithm::None)
    }

    /// 从 Frame 的 metadata 中读取序列化格式
    pub fn get_format_from_frame(frame: &Frame) -> Option<SerializationFormat> {
        frame
            .metadata
            .get("format")
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .and_then(|s| {
                if s.eq_ignore_ascii_case("protobuf") {
                    Some(SerializationFormat::Protobuf)
                } else if s.eq_ignore_ascii_case("json") {
                    Some(SerializationFormat::Json)
                } else {
                    None
                }
            })
    }

    /// 从 Frame 的 metadata 中读取加密算法
    pub fn get_encryption_from_frame(frame: &Frame) -> EncryptionAlgorithm {
        frame
            .metadata
            .get("encryption")
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .and_then(EncryptionAlgorithm::from_str)
            .unwrap_or(EncryptionAlgorithm::None)
    }

    // ============================================================================
    // 内部辅助方法
    // ============================================================================

    /// 解密数据（内部辅助方法）
    ///
    /// # 参数
    /// - `data`: 要解密的数据
    /// - `allow_fallback`: 如果为 true，解密失败时尝试作为未加密数据处理（容错）
    ///   如果为 false，解密失败直接返回错误（严格模式）
    fn decrypt_data_with_fallback(&self, data: &[u8], allow_fallback: bool) -> Result<Vec<u8>> {
        // 如果加密算法是 None，直接返回数据
        if self.default_encryption == EncryptionAlgorithm::None {
            return Ok(data.to_vec());
        }

        // 从全局注册表中查找加密器
        let encryptor_name = self.default_encryption.as_str();
        let encryptor = EncryptionUtil::find(&encryptor_name).ok_or_else(|| {
            // 提供更详细的错误信息，包括已注册的加密器列表
            let registered = EncryptionUtil::list_registered();
            let error_msg = format!(
                "Encryptor '{}' not found. Registered: {:?}",
                encryptor_name, registered
            );
            tracing::error!("{}", error_msg);
            crate::common::error::FlareError::deserialization_error(error_msg)
        })?;

        match encryptor.decrypt(data) {
            Ok(decrypted) => Ok(decrypted),
            Err(e) => {
                if allow_fallback {
                    tracing::trace!(
                        "解密失败，尝试作为未加密数据处理: encryption={:?}, data_len={}",
                        self.default_encryption,
                        data.len()
                    );
                    Ok(data.to_vec())
                } else {
                    Err(crate::common::error::FlareError::deserialization_error(
                        format!(
                            "解密失败: encryption={:?}, error={}, data_len={}",
                            self.default_encryption,
                            e,
                            data.len()
                        ),
                    ))
                }
            }
        }
    }

    /// 解密数据（内部辅助方法，默认容错模式）
    ///
    /// 如果解密失败，尝试将数据作为未加密数据返回（容错处理）
    /// 这样可以兼容客户端在收到 CONNECT_ACK 之前发送的未加密消息
    fn decrypt_data(&self, data: &[u8]) -> Result<Vec<u8>> {
        self.decrypt_data_with_fallback(data, true)
    }

    /// 加密数据（内部辅助方法）
    fn encrypt_data(&self, data: &[u8], encryption: EncryptionAlgorithm) -> Result<Vec<u8>> {
        // 如果加密算法是 None，直接返回数据
        if encryption == EncryptionAlgorithm::None {
            return Ok(data.to_vec());
        }

        // 从全局注册表中查找加密器
        let encryptor_name = encryption.as_str();
        let encryptor = EncryptionUtil::find(&encryptor_name).ok_or_else(|| {
            // 提供更详细的错误信息，包括已注册的加密器列表
            let registered = EncryptionUtil::list_registered();
            let error_msg = format!(
                "Encryptor '{}' not found. Registered: {:?}",
                encryptor_name, registered
            );
            tracing::error!("{}", error_msg);
            crate::common::error::FlareError::encoding_error(error_msg)
        })?;

        // 加密数据
        encryptor.encrypt(data)
    }

    /// 解压缩数据（内部辅助方法，默认容错模式）
    fn decompress_data(&self, data: &[u8]) -> Result<Vec<u8>> {
        self.decompress_data_with_fallback(data, true)
    }

    /// 解压缩数据（支持容错标记）
    ///
    /// # 参数
    /// - `data`: 要解压缩的数据
    /// - `allow_fallback`: 如果为 true，解压缩失败时尝试作为未压缩数据处理（容错模式）
    ///   如果为 false，解压缩失败直接返回错误（严格模式）
    fn decompress_data_with_fallback(&self, data: &[u8], allow_fallback: bool) -> Result<Vec<u8>> {
        // 如果压缩算法是 None，直接返回数据
        if self.default_compression == CompressionAlgorithm::None {
            return Ok(data.to_vec());
        }

        // 如果配置了压缩，先尝试自动检测并解压缩
        // 这样可以处理即使配置了压缩，但数据可能未压缩的情况（容错）
        match CompressionUtil::auto_decompress(data) {
            Ok((decompressed, detected_algorithm)) => {
                // 如果检测到压缩算法，说明数据确实是压缩的，返回解压缩后的数据
                if detected_algorithm != CompressionAlgorithm::None {
                    Ok(decompressed)
                } else {
                    // 如果自动检测没有检测到压缩，但配置了压缩
                    if allow_fallback {
                        tracing::trace!(
                            "自动检测未发现压缩，尝试作为未压缩数据处理: compression={:?}, data_len={}",
                            self.default_compression,
                            data.len()
                        );
                        Ok(data.to_vec())
                    } else {
                        // 严格模式：配置了压缩但数据未压缩，返回错误
                        Err(crate::common::error::FlareError::deserialization_error(
                            format!(
                                "解压缩失败（严格模式）: 配置了压缩 {:?} 但数据未压缩",
                                self.default_compression
                            ),
                        ))
                    }
                }
            }
            Err(e) => {
                if allow_fallback {
                    tracing::trace!(
                        "解压缩失败，尝试作为未压缩数据处理: compression={:?}, data_len={}",
                        self.default_compression,
                        data.len()
                    );
                    Ok(data.to_vec())
                } else {
                    // 严格模式：解压缩失败直接返回错误
                    Err(crate::common::error::FlareError::deserialization_error(
                        format!(
                            "解压缩失败（严格模式）: compression={:?}, error={}",
                            self.default_compression, e
                        ),
                    ))
                }
            }
        }
    }

    /// 解析已解压缩的数据（内部辅助方法，默认容错模式）
    #[allow(dead_code)]
    fn parse_decompressed(&self, decompressed: &[u8]) -> Result<Frame> {
        self.parse_decompressed_with_fallback(decompressed, true)
    }

    /// 解析已解压缩的数据（支持容错标记）
    ///
    /// # 参数
    /// - `decompressed`: 已解压缩的数据
    /// - `allow_fallback`: 如果为 true，反序列化失败时尝试所有序列化格式（容错模式）
    ///   如果为 false，只尝试默认格式，失败直接返回错误（严格模式）
    fn parse_decompressed_with_fallback(
        &self,
        decompressed: &[u8],
        allow_fallback: bool,
    ) -> Result<Frame> {
        // 尝试自动检测序列化格式
        let detected_serializers = SerializationUtil::auto_detect(decompressed);

        // 尝试每个检测到的序列化器
        for serializer in detected_serializers {
            if let Ok(frame) = serializer.deserialize(decompressed) {
                return Ok(frame);
            }
        }

        if allow_fallback {
            // 容错模式：如果自动检测失败，尝试所有已注册的序列化器
            self.try_all_serializers(decompressed)
        } else {
            // 严格模式：只尝试默认格式（优先使用自定义格式名称）
            let serializer = if let Some(custom_name) = &self.custom_format_name {
                // 如果指定了自定义格式名称，优先使用名称查找
                SerializationUtil::get_serializer_by_name(custom_name)
            } else {
                // 否则使用格式枚举查找
                SerializationUtil::get_serializer(self.default_format)
            }
            .ok_or_else(|| {
                let format_info = if let Some(name) = &self.custom_format_name {
                    format!("custom format '{}'", name)
                } else {
                    format!("format {:?}", self.default_format)
                };
                crate::common::error::FlareError::deserialization_error(format!(
                    "Serializer not found for {}",
                    format_info
                ))
            })?;
            serializer.deserialize(decompressed).map_err(|e| {
                let format_info = if let Some(name) = &self.custom_format_name {
                    format!("custom format '{}'", name)
                } else {
                    format!("format {:?}", self.default_format)
                };
                crate::common::error::FlareError::deserialization_error(format!(
                    "反序列化失败（严格模式）: {}, error={}",
                    format_info, e
                ))
            })
        }
    }

    /// 尝试所有已注册的序列化器（内部辅助方法）
    fn try_all_serializers(&self, data: &[u8]) -> Result<Frame> {
        [SerializationFormat::Protobuf, SerializationFormat::Json]
            .iter()
            .find_map(|&format| {
                SerializationUtil::get_serializer(format)
                    .and_then(|serializer| serializer.deserialize(data).ok())
            })
            .ok_or_else(|| {
                crate::common::error::FlareError::deserialization_error(
                    "Failed to parse message: no compatible serializer found".to_string(),
                )
            })
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
                r#type: Some(
                    crate::common::protocol::flare::core::commands::command::Type::System(ping()),
                ),
            })
            .build();

        let data = parser.serialize(&frame).unwrap();
        let parsed = parser.parse(&data).unwrap();
        assert_eq!(parsed.message_id, frame.message_id);
    }

    #[test]
    fn test_parse_json() {
        let parser = &PRE_NEGOTIATION_PARSER;
        let frame = FrameBuilder::new()
            .with_command(crate::common::protocol::Command {
                r#type: Some(
                    crate::common::protocol::flare::core::commands::command::Type::System(ping()),
                ),
            })
            .build();

        let data = parser.serialize(&frame).unwrap();
        let parsed = parser.parse(&data).unwrap();
        assert_eq!(parsed.message_id, frame.message_id);
    }

    #[test]
    fn test_get_encryption_from_frame() {
        use crate::common::protocol::FrameBuilder;
        use std::collections::HashMap;

        // 测试无加密 metadata
        let frame = FrameBuilder::new().build();
        assert_eq!(
            MessageParser::get_encryption_from_frame(&frame),
            EncryptionAlgorithm::None
        );

        // 测试有加密 metadata
        let mut metadata = HashMap::new();
        metadata.insert("encryption".to_string(), b"aes256gcm".to_vec());
        let frame = FrameBuilder::new()
            .with_metadata("encryption".to_string(), b"aes256gcm".to_vec())
            .build();
        assert_eq!(
            MessageParser::get_encryption_from_frame(&frame),
            EncryptionAlgorithm::Aes256Gcm
        );

        // 测试无效加密 metadata
        let frame = FrameBuilder::new()
            .with_metadata("encryption".to_string(), b"invalid".to_vec())
            .build();
        assert_eq!(
            MessageParser::get_encryption_from_frame(&frame),
            EncryptionAlgorithm::None
        );
    }
}
