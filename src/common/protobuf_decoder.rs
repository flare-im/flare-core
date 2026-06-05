//! # 安全的 Protobuf 解码器（含粘包处理）
//!
//! 用于处理带长度前缀的 Protobuf 消息，防止将 varint 长度前缀误认为字符串内容
//! 这解决了 protobuf string 字段前出现 "\x0c" 的问题，该问题是由于 length varint
//! 被当成字符串解码导致的
//!
//! 此模块提供了专门的 Protobuf 序列化器实现，以替代基础的 ProtobufSerializer

use bytes::BytesMut;
use prost::Message;
use std::io::{self, Cursor};

/// Protobuf 消息解码器，支持粘包处理
pub struct ProtobufDecoder<T> {
    /// 消息类型
    _phantom: std::marker::PhantomData<T>,
    /// 缓冲区，用于处理粘包
    buffer: BytesMut,
}

impl<T> ProtobufDecoder<T>
where
    T: Message + Default,
{
    /// 创建新的解码器
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
            buffer: BytesMut::new(),
        }
    }

    /// 向解码器添加数据（处理粘包）
    pub fn add_data(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// 解码下一个完整的消息
    ///
    /// 返回 Ok(Some(message)) 如果有足够的数据解码一个完整的消息
    /// 返回 Ok(None) 如果数据不足（需要更多数据）
    /// 返回 Err 如果解码失败
    pub fn decode_next(&mut self) -> Result<Option<T>, Box<dyn std::error::Error + Send + Sync>> {
        // 尝试解析长度前缀（varint 编码）
        if let Some((message_len, offset)) = self.read_varint()? {
            // 检查是否有足够的数据来解码完整的消息
            if self.buffer.len() < offset + message_len {
                // 数据不足，需要等待更多数据
                return Ok(None);
            }

            // 提取消息数据
            let message_data = self.buffer.split_to(offset + message_len).freeze();
            let message_bytes = &message_data[offset..];

            // 解码 protobuf 消息
            let message = T::decode(message_bytes)?;
            Ok(Some(message))
        } else {
            // 数据不足，无法读取完整的 varint
            Ok(None)
        }
    }

    /// 读取 varint 编码的长度
    /// 返回 (length, bytes_consumed)
    fn read_varint(&self) -> Result<Option<(usize, usize)>, io::Error> {
        let mut cursor = Cursor::new(&self.buffer);
        let mut result: u64 = 0;
        let mut shift = 0;

        for _i in 0..10 {
            // varint 最多 10 个字节
            if cursor.position() as usize >= self.buffer.len() {
                // 数据不足，无法读取完整的 varint
                return Ok(None);
            }

            let byte = cursor.get_ref()[cursor.position() as usize];
            cursor.set_position(cursor.position() + 1);

            // 提取低 7 位
            let value = (byte & 0x7F) as u64;
            result |= value << shift;

            // 检查最高位是否为 1（表示还有后续字节）
            if (byte & 0x80) == 0 {
                // 完成了 varint 解码
                return Ok(Some((result as usize, (cursor.position()) as usize)));
            }

            shift += 7;
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Varint too long",
        ))
    }

    /// 检查是否还有未处理的数据
    pub fn has_remaining(&self) -> bool {
        !self.buffer.is_empty()
    }

    /// 获取剩余数据长度
    pub fn remaining_len(&self) -> usize {
        self.buffer.len()
    }

    /// 清空缓冲区
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

impl<T> Default for ProtobufDecoder<T>
where
    T: Message + Default,
{
    fn default() -> Self {
        Self::new()
    }
}

/// 安全的 protobuf 消息内容解码函数
///
/// 此函数安全地尝试解码 protobuf 数据，如果解码失败则返回错误而不是崩溃
pub fn safe_protobuf_decode<T>(data: &[u8]) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    T: Message + Default,
{
    match T::decode(data) {
        Ok(message) => Ok(message),
        Err(e) => Err(Box::new(e)),
    }
}

/// 安全的字符串解码函数（用于调试目的）
///
/// 此函数仅在确认数据是有效 UTF-8 时才进行转换
pub fn safe_string_decode(data: &[u8]) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    match std::str::from_utf8(data) {
        Ok(s) => Ok(s.to_string()),
        Err(e) => Err(Box::new(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    // 定义一个简单的测试消息
    #[derive(Clone, PartialEq, Message)]
    struct TestMessage {
        #[prost(string, tag = "1")]
        content: String,
        #[prost(int32, tag = "2")]
        id: i32,
    }

    #[test]
    fn test_protobuf_decoder() {
        let mut decoder = ProtobufDecoder::<TestMessage>::new();

        // 创建测试消息
        let test_msg = TestMessage {
            content: "Hello, World!".to_string(),
            id: 42,
        };

        // 编码消息（带长度前缀）
        let encoded_msg = test_msg.encode_to_vec();
        let mut prefixed_data = Vec::new();
        prost::encoding::encode_varint(encoded_msg.len() as u64, &mut prefixed_data);
        prefixed_data.extend_from_slice(&encoded_msg);

        // 添加数据到解码器
        decoder.add_data(&prefixed_data);

        // 解码消息
        let decoded_msg = decoder.decode_next().unwrap().unwrap();

        assert_eq!(decoded_msg.content, "Hello, World!");
        assert_eq!(decoded_msg.id, 42);
    }

    #[test]
    fn test_protobuf_decoder_partial_data() {
        let mut decoder = ProtobufDecoder::<TestMessage>::new();

        // 创建测试消息
        let test_msg = TestMessage {
            content: "Hello, Partial Data Test!".to_string(),
            id: 123,
        };

        // 编码消息（带长度前缀）
        let encoded_msg = test_msg.encode_to_vec();
        let mut prefixed_data = Vec::new();
        prost::encoding::encode_varint(encoded_msg.len() as u64, &mut prefixed_data);
        prefixed_data.extend_from_slice(&encoded_msg);

        // 只添加部分数据
        let half_point = prefixed_data.len() / 2;
        decoder.add_data(&prefixed_data[..half_point]);

        // 尝试解码 - 应该返回 None（数据不足）
        let result = decoder.decode_next().unwrap();
        assert!(result.is_none());

        // 添加剩余数据
        decoder.add_data(&prefixed_data[half_point..]);

        // 现在应该能够解码
        let decoded_msg = decoder.decode_next().unwrap().unwrap();
        assert_eq!(decoded_msg.content, "Hello, Partial Data Test!");
        assert_eq!(decoded_msg.id, 123);
    }

    #[test]
    fn test_safe_protobuf_decode() {
        let test_msg = TestMessage {
            content: "Safe decode test".to_string(),
            id: 999,
        };

        let encoded = test_msg.encode_to_vec();
        let decoded = safe_protobuf_decode::<TestMessage>(&encoded).unwrap();

        assert_eq!(decoded.content, "Safe decode test");
        assert_eq!(decoded.id, 999);
    }

    #[test]
    fn test_safe_string_decode() {
        let valid_utf8 = b"Valid UTF-8 string";
        let result = safe_string_decode(valid_utf8).unwrap();
        assert_eq!(result, "Valid UTF-8 string");

        // 测试无效的 UTF-8 数据
        let invalid_utf8 = &[0xFF, 0xFE, 0xFD]; // 无效的 UTF-8 序列
        let result = safe_string_decode(invalid_utf8);
        assert!(result.is_err());
    }
}
