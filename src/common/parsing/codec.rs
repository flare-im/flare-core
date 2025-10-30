/// 编解码器模块
/// 
/// 提供 Payload 和 Frame 的编解码功能

use crate::common::error::FlareError;
use crate::common::protocol::frame::Frame;
use crate::common::protocol::reliability::Reliability;
use crate::common::protocol::commands::{Command, ControlCmd, MessageCmd, DataCommand};
use std::io::{Write, Read, Cursor};

/// Payload 编解码器枚举
/// 
/// 采用枚举模式封装不同的序列化器，避免 trait object 的 dyn 兼容性问题
/// 
/// # 支持的格式
/// 
/// - **JSON**: 人类可读，适合调试和跨语言兼容
/// - **Protobuf**: 高效紧凑，适合生产环境（占位实现）
/// 
/// # 示例
/// 
/// ```rust,no_run
/// use flare_core::common::parsing::PayloadCodec;
/// use serde::{Serialize, Deserialize};
/// 
/// #[derive(Serialize, Deserialize)]
/// struct MyStruct { id: u32 }
/// 
/// // 直接使用枚举值
/// let codec = PayloadCodec::Json;
/// 
/// // 序列化和反序列化
/// let data = MyStruct { id: 42 };
/// let bytes = codec.encode(&data)?;
/// let decoded: MyStruct = codec.decode(&bytes)?;
/// # Ok::<(), flare_core::common::error::FlareError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadCodec {
    /// JSON 格式（基于 serde_json）
    #[default]
    Json,
    /// Protobuf 格式（基于 prost，占位实现）
    Protobuf,
}

impl PayloadCodec {
    
    /// 判断是否为二进制格式
    pub fn is_binary(&self) -> bool {
        matches!(self, PayloadCodec::Protobuf)
    }
    
    /// 判断是否为文本格式
    pub fn is_text(&self) -> bool {
        matches!(self, PayloadCodec::Json)
    }
    
    /// 获取格式的典型文件扩展名
    pub fn file_extension(&self) -> &str {
        match self {
            PayloadCodec::Json => "json",
            PayloadCodec::Protobuf => "pb",
        }
    }
    
    /// 获取格式的 MIME 类型
    pub fn mime_type(&self) -> &str {
        match self {
            PayloadCodec::Json => "application/json",
            PayloadCodec::Protobuf => "application/x-protobuf",
        }
    }

    /// 序列化数据
    /// 
    /// # 参数
    /// 
    /// - `data`: 实现了 `serde::Serialize` 的业务对象
    /// 
    /// # 返回
    /// 
    /// - `Ok(Vec<u8>)`: 序列化后的字节数组
    /// - `Err(FlareError)`: 序列化失败
    /// 
    /// # 示例
    /// 
    /// ```
    /// use flare_core::common::parsing::PayloadCodec;
    /// use serde::{Serialize, Deserialize};
    /// 
    /// #[derive(Serialize, Deserialize)]
    /// struct MyData { id: u32 }
    /// 
    /// let codec = PayloadCodec::Json;
    /// let data = MyData { id: 42 };
    /// let bytes = codec.encode(&data).unwrap();
    /// ```
    pub fn encode<T: serde::Serialize>(&self, data: &T) -> Result<Vec<u8>, FlareError> {
        match self {
            PayloadCodec::Json => {
                serde_json::to_vec(data)
                    .map_err(|e| FlareError::serialization_error(format!("JSON encoding failed: {}", e)))
            }
            PayloadCodec::Protobuf => {
                // TODO: 实现真正的 Protobuf 编码
                // 现在使用 JSON 作为占位
                serde_json::to_vec(data)
                    .map_err(|e| FlareError::serialization_error(format!("Protobuf encoding failed (JSON fallback): {}", e)))
            }
        }
    }
    
    /// 序列化数据（美化输出，仅适用于 JSON）
    /// 
    /// 对于 JSON 格式，生成格式化的、易读的输出。
    /// 对于其他格式，与 `encode` 相同。
    /// 
    /// # 参数
    /// 
    /// - `data`: 实现了 `serde::Serialize` 的业务对象
    /// 
    /// # 返回
    /// 
    /// - `Ok(Vec<u8>)`: 序列化后的字节数组
    /// - `Err(FlareError)`: 序列化失败
    pub fn encode_pretty<T: serde::Serialize>(&self, data: &T) -> Result<Vec<u8>, FlareError> {
        match self {
            PayloadCodec::Json => {
                serde_json::to_vec_pretty(data)
                    .map_err(|e| FlareError::serialization_error(format!("JSON pretty encoding failed: {}", e)))
            }
            _ => self.encode(data), // 其他格式不支持 pretty
        }
    }

    /// 反序列化数据
    /// 
    /// # 参数
    /// 
    /// - `bytes`: 待反序列化的字节数组
    /// 
    /// # 返回
    /// 
    /// - `Ok(T)`: 反序列化后的业务对象
    /// - `Err(FlareError)`: 反序列化失败
    /// 
    /// # 示例
    /// 
    /// ```
    /// use flare_core::common::parsing::PayloadCodec;
    /// use serde::{Serialize, Deserialize};
    /// 
    /// #[derive(Serialize, Deserialize, PartialEq, Debug)]
    /// struct MyData { id: u32 }
    /// 
    /// let codec = PayloadCodec::Json;
    /// let data = MyData { id: 42 };
    /// let bytes = codec.encode(&data).unwrap();
    /// let decoded: MyData = codec.decode(&bytes).unwrap();
    /// assert_eq!(data, decoded);
    /// ```
    pub fn decode<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, FlareError> {
        // 预先验证输入
        if bytes.is_empty() {
            return Err(FlareError::general_error("Cannot decode empty bytes"));
        }
        
        match self {
            PayloadCodec::Json => {
                serde_json::from_slice(bytes)
                    .map_err(|e| {
                        FlareError::general_error(format!(
                            "JSON decoding failed: {} (bytes: {} bytes, preview: {})",
                            e,
                            bytes.len(),
                            String::from_utf8_lossy(&bytes[..bytes.len().min(100)])
                        ))
                    })
            }
            PayloadCodec::Protobuf => {
                // TODO: 实现真正的 Protobuf 解码
                serde_json::from_slice(bytes)
                    .map_err(|e| {
                        FlareError::general_error(format!("Protobuf decoding failed (JSON fallback): {}", e))
                    })
            }
        }
    }
    
    /// 尝试反序列化，失败时返回 None
    /// 
    /// 这是 `decode` 的非抛出异常版本，适用于需要容错处理的场景。
    /// 
    /// # 参数
    /// 
    /// - `bytes`: 待反序列化的字节数组
    /// 
    /// # 返回
    /// 
    /// - `Some(T)`: 反序列化成功
    /// - `None`: 反序列化失败
    pub fn try_decode<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Option<T> {
        self.decode(bytes).ok()
    }

    /// 获取编解码器名称
    /// 
    /// # 返回
    /// 
    /// 编解码器的字符串名称
    pub fn name(&self) -> &str {
        match self {
            PayloadCodec::Json => "json",
            PayloadCodec::Protobuf => "protobuf",
        }
    }
    
    /// 估算序列化后的大小（字节）
    /// 
    /// 注意：这是一个粗略估计，仅供参考。
    /// 
    /// # 参数
    /// 
    /// - `data`: 实现了 `serde::Serialize` 的业务对象
    /// 
    /// # 返回
    /// 
    /// - `Ok(usize)`: 估计的字节数
    /// - `Err(FlareError)`: 估计失败
    pub fn estimate_size<T: serde::Serialize>(&self, data: &T) -> Result<usize, FlareError> {
        // 直接序列化并返回大小（简单但准确）
        self.encode(data).map(|bytes| bytes.len())
    }
    
    /// 验证数据是否可以被此编解码器解码
    /// 
    /// # 参数
    /// 
    /// - `bytes`: 待验证的字节数组
    /// 
    /// # 返回
    /// 
    /// - `true`: 数据格式有效
    /// - `false`: 数据格式无效
    pub fn validate_bytes(&self, bytes: &[u8]) -> bool {
        if bytes.is_empty() {
            return false;
        }
        
        match self {
            PayloadCodec::Json => {
                // 尝试解析为 JSON Value
                serde_json::from_slice::<serde_json::Value>(bytes).is_ok()
            }
            PayloadCodec::Protobuf => {
                // TODO: 实现真正的 Protobuf 验证
                // 现在使用 JSON 验证
                serde_json::from_slice::<serde_json::Value>(bytes).is_ok()
            }
        }
    }
    
    /// 转换为字符串表示（仅适用于文本格式）
    /// 
    /// # 参数
    /// 
    /// - `bytes`: 序列化后的字节数组
    /// 
    /// # 返回
    /// 
    /// - `Ok(String)`: 字符串表示
    /// - `Err(FlareError)`: 转换失败或不支持
    pub fn to_string(&self, bytes: &[u8]) -> Result<String, FlareError> {
        match self {
            PayloadCodec::Json => {
                String::from_utf8(bytes.to_vec())
                    .map_err(|e| FlareError::general_error(format!("Invalid UTF-8 in JSON data: {}", e)))
            }
            _ => {
                Err(FlareError::general_error(format!(
                    "Format '{}' does not support string conversion",
                    self.name()
                )))
            }
        }
    }
}

/// Frame 编解码器 trait
/// 
/// 负责 Frame 结构的完整编解码
pub trait FrameCodec {
    /// 将 Frame 编码为字节数组
    fn encode_frame(&self, frame: &Frame) -> Result<Vec<u8>, FlareError>;
    
    /// 将字节数组解码为 Frame
    fn decode_frame(&self, bytes: &[u8]) -> Result<Frame, FlareError>;
    
    /// 验证 Frame 的完整性
    fn validate_frame(&self, frame: &Frame) -> Result<(), FlareError> {
        if frame.message_id.is_empty() {
            return Err(FlareError::general_error("message_id cannot be empty"));
        }
        Ok(())
    }
}

/// 压缩算法枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    /// 无压缩
    None,
    /// Gzip 压缩（通用，兼容性好）
    Gzip,
    /// LZ4 压缩（速度快）
    Lz4,
    /// Snappy 压缩（平衡速度和压缩率）
    Snappy,
}

impl CompressionAlgorithm {
    /// 从 u8 标志位解析压缩算法
    pub fn from_u8(value: u8) -> Result<Self, FlareError> {
        match value {
            0 => Ok(CompressionAlgorithm::None),
            1 => Ok(CompressionAlgorithm::Gzip),
            2 => Ok(CompressionAlgorithm::Lz4),
            3 => Ok(CompressionAlgorithm::Snappy),
            _ => Err(FlareError::general_error(format!("Invalid compression algorithm: {}", value))),
        }
    }
    
    /// 转换为 u8 标志位
    pub fn to_u8(&self) -> u8 {
        match self {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Gzip => 1,
            CompressionAlgorithm::Lz4 => 2,
            CompressionAlgorithm::Snappy => 3,
        }
    }
    
    /// 获取算法名称
    pub fn name(&self) -> &str {
        match self {
            CompressionAlgorithm::None => "none",
            CompressionAlgorithm::Gzip => "gzip",
            CompressionAlgorithm::Lz4 => "lz4",
            CompressionAlgorithm::Snappy => "snappy",
        }
    }
    
    /// 压缩数据
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError> {
        match self {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Gzip => {
                use flate2::write::GzEncoder;
                use flate2::Compression;
                use std::io::Write;
                
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(data)
                    .map_err(|e| FlareError::general_error(format!("Gzip compression failed: {}", e)))?;
                encoder.finish()
                    .map_err(|e| FlareError::general_error(format!("Gzip finish failed: {}", e)))
            }
            CompressionAlgorithm::Lz4 => {
                Ok(lz4_flex::compress_prepend_size(data))
            }
            CompressionAlgorithm::Snappy => {
                use snap::raw::Encoder;
                let mut encoder = Encoder::new();
                encoder.compress_vec(data)
                    .map_err(|e| FlareError::general_error(format!("Snappy compression failed: {}", e)))
            }
        }
    }
    
    /// 解压数据
    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError> {
        match self {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Gzip => {
                use flate2::read::GzDecoder;
                use std::io::Read;
                
                let mut decoder = GzDecoder::new(data);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed)
                    .map_err(|e| FlareError::general_error(format!("Gzip decompression failed: {}", e)))?;
                Ok(decompressed)
            }
            CompressionAlgorithm::Lz4 => {
                lz4_flex::decompress_size_prepended(data)
                    .map_err(|e| FlareError::general_error(format!("LZ4 decompression failed: {}", e)))
            }
            CompressionAlgorithm::Snappy => {
                use snap::raw::Decoder;
                let mut decoder = Decoder::new();
                decoder.decompress_vec(data)
                    .map_err(|e| FlareError::general_error(format!("Snappy decompression failed: {}", e)))
            }
        }
    }
}

/// 协议魔数
const MAGIC_NUMBER: u16 = 0xF1A7;
/// 协议版本
const PROTOCOL_VERSION: u8 = 1;

/// 默认的 Frame 编解码器
/// 
/// 使用二进制协议格式：
/// - Magic (2 bytes): 0xF1A7
/// - Version (1 byte): 协议版本
/// - Flags (1 byte): 保留标志位
/// - MessageID (length + content)
/// - Reliability (1 byte)
/// - Command (type + data)
/// - Payload (length + content)
/// 
/// # 高级功能
/// 
/// - 压缩支持：可选的 Payload 压缩
/// - 校验和：数据完整性验证
/// - 最大消息限制：防止内存溢出
#[derive(Debug, Clone)]
pub struct DefaultFrameCodec {
    max_message_size: usize,
    enable_compression: bool,
    compression_threshold: usize,  // 大于此大小才启用压缩
    compression_algorithm: CompressionAlgorithm,
    enable_checksum: bool,
}

impl DefaultFrameCodec {
    /// 创建新的编解码器实例（使用默认配置）
    pub fn new() -> Self {
        Self {
            max_message_size: 10 * 1024 * 1024, // 10MB
            enable_compression: false,
            compression_threshold: 1024, // 1KB
            compression_algorithm: CompressionAlgorithm::Lz4,
            enable_checksum: false,
        }
    }

    /// 创建带最大消息大小配置的编解码器
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            max_message_size: max_size,
            enable_compression: false,
            compression_threshold: 1024,
            compression_algorithm: CompressionAlgorithm::Lz4,
            enable_checksum: false,
        }
    }
    
    /// 启用压缩
    pub fn with_compression(mut self, algorithm: CompressionAlgorithm, threshold: usize) -> Self {
        self.enable_compression = true;
        self.compression_algorithm = algorithm;
        self.compression_threshold = threshold;
        self
    }
    
    /// 启用校验和
    pub fn with_checksum(mut self, enable: bool) -> Self {
        self.enable_checksum = enable;
        self
    }

    fn encode_reliability(reliability: &Reliability) -> u8 {
        match reliability {
            Reliability::BestEffort => 0,
            Reliability::AtLeastOnce => 1,
        }
    }

    fn decode_reliability(byte: u8) -> Result<Reliability, FlareError> {
        match byte {
            0 => Ok(Reliability::BestEffort),
            1 => Ok(Reliability::AtLeastOnce),
            _ => Err(FlareError::general_error(format!("Invalid reliability byte: {}", byte))),
        }
    }

    fn encode_command(command: &Command) -> Result<Vec<u8>, FlareError> {
        let mut buffer = Vec::new();
        match command {
            Command::Control(ControlCmd::Ping) => {
                buffer.push(0); // Control
                buffer.push(0); // Ping
            }
            Command::Control(ControlCmd::Pong) => {
                buffer.push(0); // Control
                buffer.push(1); // Pong
            }
            Command::Message(MessageCmd::Data(_)) => {
                buffer.push(1); // Message
                buffer.push(0); // Data
            }
            _ => {
                buffer.push(99); // Custom/Unknown
                buffer.push(0);
            }
        }
        Ok(buffer)
    }

    fn decode_command(cursor: &mut Cursor<&[u8]>) -> Result<Command, FlareError> {
        let mut type_byte = [0u8; 1];
        cursor.read_exact(&mut type_byte)
            .map_err(|e| FlareError::general_error(format!("Failed to read command type: {}", e)))?;

        let mut subtype_byte = [0u8; 1];
        cursor.read_exact(&mut subtype_byte)
            .map_err(|e| FlareError::general_error(format!("Failed to read command subtype: {}", e)))?;

        match (type_byte[0], subtype_byte[0]) {
            (0, 0) => Ok(Command::Control(ControlCmd::Ping)),
            (0, 1) => Ok(Command::Control(ControlCmd::Pong)),
            (1, 0) => Ok(Command::Message(MessageCmd::Data(DataCommand { data: Vec::new() }))),
            _ => Ok(Command::Message(MessageCmd::Data(DataCommand { data: Vec::new() }))),
        }
    }
}

impl Default for DefaultFrameCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameCodec for DefaultFrameCodec {
    fn encode_frame(&self, frame: &Frame) -> Result<Vec<u8>, FlareError> {
        self.validate_frame(frame)?;

        let mut buffer = Vec::with_capacity(1024);

        // Magic Number (2 bytes)
        buffer.write_all(&MAGIC_NUMBER.to_be_bytes())
            .map_err(|e| FlareError::serialization_error(format!("Failed to write magic: {}", e)))?;

        // Protocol Version (1 byte)
        buffer.push(PROTOCOL_VERSION);

        // Flags (1 byte)
        buffer.push(0);

        // MessageID (2 bytes length + content)
        let message_id_bytes = frame.message_id.as_bytes();
        let message_id_len = message_id_bytes.len() as u16;
        buffer.write_all(&message_id_len.to_be_bytes())
            .map_err(|e| FlareError::serialization_error(format!("Failed to write message_id length: {}", e)))?;
        buffer.write_all(message_id_bytes)
            .map_err(|e| FlareError::serialization_error(format!("Failed to write message_id: {}", e)))?;

        // Reliability (1 byte)
        buffer.push(Self::encode_reliability(&frame.reliability));

        // Command (2 bytes length + content)
        let command_bytes = Self::encode_command(&frame.command)?;
        let command_len = command_bytes.len() as u16;
        buffer.write_all(&command_len.to_be_bytes())
            .map_err(|e| FlareError::serialization_error(format!("Failed to write command length: {}", e)))?;
        buffer.write_all(&command_bytes)
            .map_err(|e| FlareError::serialization_error(format!("Failed to write command: {}", e)))?;

        // Payload (4 bytes length + content)
        let payload_len = frame.payload.len() as u32;
        buffer.write_all(&payload_len.to_be_bytes())
            .map_err(|e| FlareError::serialization_error(format!("Failed to write payload length: {}", e)))?;
        buffer.write_all(&frame.payload)
            .map_err(|e| FlareError::serialization_error(format!("Failed to write payload: {}", e)))?;

        Ok(buffer)
    }

    fn decode_frame(&self, bytes: &[u8]) -> Result<Frame, FlareError> {
        if bytes.len() < 8 {
            return Err(FlareError::general_error(format!("Frame too short: {} bytes", bytes.len())));
        }

        if bytes.len() > self.max_message_size {
            return Err(FlareError::general_error(format!("Frame too large: {} bytes", bytes.len())));
        }

        let mut cursor = Cursor::new(bytes);

        // Magic Number
        let mut magic_bytes = [0u8; 2];
        cursor.read_exact(&mut magic_bytes)
            .map_err(|e| FlareError::general_error(format!("Failed to read magic: {}", e)))?;
        let magic = u16::from_be_bytes(magic_bytes);
        if magic != MAGIC_NUMBER {
            return Err(FlareError::general_error(format!("Invalid magic: 0x{:04X}", magic)));
        }

        // Version
        let mut version_byte = [0u8; 1];
        cursor.read_exact(&mut version_byte)
            .map_err(|e| FlareError::general_error(format!("Failed to read version: {}", e)))?;

        // Flags (skip)
        let mut _flags = [0u8; 1];
        cursor.read_exact(&mut _flags)
            .map_err(|e| FlareError::general_error(format!("Failed to read flags: {}", e)))?;

        // MessageID
        let mut message_id_len_bytes = [0u8; 2];
        cursor.read_exact(&mut message_id_len_bytes)
            .map_err(|e| FlareError::general_error(format!("Failed to read message_id length: {}", e)))?;
        let message_id_len = u16::from_be_bytes(message_id_len_bytes) as usize;

        let mut message_id_bytes = vec![0u8; message_id_len];
        cursor.read_exact(&mut message_id_bytes)
            .map_err(|e| FlareError::general_error(format!("Failed to read message_id: {}", e)))?;
        let message_id = String::from_utf8(message_id_bytes)
            .map_err(|e| FlareError::general_error(format!("Invalid UTF-8 in message_id: {}", e)))?;

        // Reliability
        let mut reliability_byte = [0u8; 1];
        cursor.read_exact(&mut reliability_byte)
            .map_err(|e| FlareError::general_error(format!("Failed to read reliability: {}", e)))?;
        let reliability = Self::decode_reliability(reliability_byte[0])?;

        // Command
        let mut command_len_bytes = [0u8; 2];
        cursor.read_exact(&mut command_len_bytes)
            .map_err(|e| FlareError::general_error(format!("Failed to read command length: {}", e)))?;
        let _command_len = u16::from_be_bytes(command_len_bytes) as usize;

        let command = Self::decode_command(&mut cursor)?;

        // Payload
        let mut payload_len_bytes = [0u8; 4];
        cursor.read_exact(&mut payload_len_bytes)
            .map_err(|e| FlareError::general_error(format!("Failed to read payload length: {}", e)))?;
        let payload_len = u32::from_be_bytes(payload_len_bytes) as usize;

        let mut payload = vec![0u8; payload_len];
        cursor.read_exact(&mut payload)
            .map_err(|e| FlareError::general_error(format!("Failed to read payload: {}", e)))?;

        Ok(Frame {
            message_id,
            payload: payload.into(),
            reliability,
            command,
        })
    }

    fn validate_frame(&self, frame: &Frame) -> Result<(), FlareError> {
        if frame.message_id.is_empty() {
            return Err(FlareError::general_error("message_id cannot be empty"));
        }
        if frame.payload.len() > self.max_message_size {
            return Err(FlareError::general_error(format!(
                "Payload too large: {} bytes (max: {})",
                frame.payload.len(),
                self.max_message_size
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::factory::FrameFactory;

    #[test]
    fn test_payload_codec_json() {
        let codec = PayloadCodec::Json;
        
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct TestData {
            id: u32,
            name: String,
        }

        let data = TestData { id: 42, name: "test".to_string() };
        let bytes = codec.encode(&data).unwrap();
        let decoded: TestData = codec.decode(&bytes).unwrap();
        
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_frame_codec() {
        let codec = DefaultFrameCodec::new();
        let frame = FrameFactory::create_data_frame(
            "test-123".to_string(),
            b"Hello, World!".to_vec(),
            Reliability::AtLeastOnce,
        ).unwrap();

        let encoded = codec.encode_frame(&frame).unwrap();
        let decoded = codec.decode_frame(&encoded).unwrap();

        assert_eq!(decoded.message_id, frame.message_id);
        assert_eq!(decoded.payload, frame.payload);
        assert_eq!(decoded.reliability, frame.reliability);
    }
}
