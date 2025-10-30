/// 统一的消息解析器
/// 
/// 整合 PayloadCodec 和 FrameCodec，提供完整的消息解析能力

use crate::common::parsing::codec::{PayloadCodec, FrameCodec, DefaultFrameCodec};
use crate::common::protocol::frame::Frame;
use crate::common::protocol::commands::{Command, MessageCmd, DataCommand};
use crate::common::protocol::reliability::Reliability;
use crate::common::error::FlareError;
use std::sync::atomic::{AtomicU64, Ordering};
use bytes::Bytes;

/// 解析器统计信息
#[derive(Debug, Clone, Default)]
pub struct ParserStats {
    /// 成功解析的消息数
    pub parsed_count: u64,
    /// 解析失败的消息数
    pub failed_count: u64,
    /// 总处理字节数
    pub total_bytes: u64,
}

/// 统一的消息解析器
/// 
/// 提供完整的消息解析能力：
/// 1. 字节流 ↔ Frame
/// 2. Frame ↔ 业务对象
/// 3. 统计信息收集
/// 
/// # 使用示例
/// 
/// ```rust
/// use flare_core::common::parsing::{MessageParser, PayloadCodec};
/// use serde::{Serialize, Deserialize};
/// 
/// #[derive(Serialize, Deserialize)]
/// struct MyMessage {
///     id: u32,
///     content: String,
/// }
/// 
/// #[tokio::main]
/// async fn main() {
///     // 创建解析器（直接使用 PayloadCodec）
///     let parser = MessageParser::new(PayloadCodec::Json);
///     
///     // 构建并编码消息
///     let msg = MyMessage { id: 1, content: "Hello".to_string() };
///     let frame = parser.build_frame(&msg, "msg-1".to_string()).await.unwrap();
///     let bytes = parser.encode_frame(&frame).await.unwrap();
///     
///     // 解析并反序列化消息
///     let received_frame = parser.parse_bytes(&bytes).await.unwrap();
///     let received_msg: MyMessage = parser.parse_payload(&received_frame).await.unwrap();
/// }
/// ```
pub struct MessageParser {
    /// Payload 编解码器
    payload_codec: PayloadCodec,
    /// Frame 编解码器
    frame_codec: Box<dyn FrameCodec + Send + Sync>,
    /// 统计信息
    parsed_count: AtomicU64,
    failed_count: AtomicU64,
    total_bytes: AtomicU64,
}

impl MessageParser {
    /// 创建新的消息解析器
    /// 
    /// # 参数
    /// - `codec`: Payload 编解码器
    /// 
    /// # 返回
    /// - `MessageParser`: 配置好的解析器
    /// 
    /// # 示例
    /// 
    /// ```rust
    /// use flare_core::common::parsing::{MessageParser, PayloadCodec};
    /// 
    /// // 推荐方式：直接使用 PayloadCodec
    /// let parser = MessageParser::new(PayloadCodec::Json);
    /// ```
    pub fn new(codec: PayloadCodec) -> Self {
        Self {
            payload_codec: codec,
            frame_codec: Box::new(DefaultFrameCodec::new()),
            parsed_count: AtomicU64::new(0),
            failed_count: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
        }
    }
    
    /// 创建带自定义 Frame 编解码器的解析器
    /// 
    /// # 参数
    /// - `codec`: Payload 编解码器
    /// - `frame_codec`: 自定义的 Frame 编解码器
    /// 
    /// # 返回
    /// - `MessageParser`: 配置好的解析器
    pub fn with_frame_codec(
        codec: PayloadCodec,
        frame_codec: Box<dyn FrameCodec + Send + Sync>,
    ) -> Self {
        Self {
            payload_codec: codec,
            frame_codec,
            parsed_count: AtomicU64::new(0),
            failed_count: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
        }
    }

    /// 解析原始字节为 Frame
    /// 
    /// # 参数
    /// - `bytes`: 原始字节数组
    /// 
    /// # 返回
    /// - `Ok(Frame)`: 解析成功
    /// - `Err(FlareError)`: 解析失败
    pub async fn parse_bytes(&self, bytes: &[u8]) -> Result<Frame, FlareError> {
        // 更新统计
        self.total_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);

        // 解码 Frame
        match self.frame_codec.decode_frame(bytes) {
            Ok(frame) => {
                self.parsed_count.fetch_add(1, Ordering::Relaxed);
                Ok(frame)
            }
            Err(e) => {
                self.failed_count.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// 解析 Frame 中的 Payload 为业务对象
    /// 
    /// # 参数
    /// - `frame`: 已解析的 Frame 对象
    /// 
    /// # 返回
    /// - `Ok(T)`: 业务对象
    /// - `Err(FlareError)`: 反序列化失败
    pub async fn parse_payload<T: serde::de::DeserializeOwned>(&self, frame: &Frame) -> Result<T, FlareError> {
        self.payload_codec.decode(&frame.payload)
    }

    /// 构建包含业务数据的 Frame
    /// 
    /// # 参数
    /// - `data`: 业务对象
    /// - `message_id`: 消息唯一标识
    /// 
    /// # 返回
    /// - `Ok(Frame)`: 构建的 Frame 对象
    /// - `Err(FlareError)`: 序列化失败
    pub async fn build_frame<T: serde::Serialize>(&self, data: &T, message_id: String) -> Result<Frame, FlareError> {
        // 序列化业务数据
        let payload_vec = self.payload_codec.encode(data)?;
        let payload = Bytes::from(payload_vec);

        // 创建 Data Frame
        let data_cmd = DataCommand { data: payload.to_vec() };
        let command = Command::Message(MessageCmd::Data(data_cmd));

        Ok(Frame {
            message_id,
            payload,
            reliability: Reliability::BestEffort,
            command,
        })
    }

    /// 构建带可靠性级别的 Frame
    /// 
    /// # 参数
    /// - `data`: 业务对象
    /// - `message_id`: 消息唯一标识
    /// - `reliability`: 可靠性级别
    /// 
    /// # 返回
    /// - `Ok(Frame)`: 构建的 Frame 对象
    /// - `Err(FlareError)`: 序列化失败
    pub async fn build_frame_with_reliability<T: serde::Serialize>(
        &self,
        data: &T,
        message_id: String,
        reliability: Reliability,
    ) -> Result<Frame, FlareError> {
        let payload_vec = self.payload_codec.encode(data)?;
        let payload = Bytes::from(payload_vec);

        let data_cmd = DataCommand { data: payload.to_vec() };
        let command = Command::Message(MessageCmd::Data(data_cmd));

        Ok(Frame {
            message_id,
            payload,
            reliability,
            command,
        })
    }

    /// 将 Frame 编码为可传输的字节数组
    /// 
    /// # 参数
    /// - `frame`: 待编码的 Frame 对象
    /// 
    /// # 返回
    /// - `Ok(Vec<u8>)`: 编码后的字节数组
    /// - `Err(FlareError)`: 编码失败
    pub async fn encode_frame(&self, frame: &Frame) -> Result<Vec<u8>, FlareError> {
        self.frame_codec.encode_frame(frame)
    }

    /// 完整的解析和处理流程：字节 → Frame
    /// 
    /// # 参数
    /// - `bytes`: 原始字节数组
    /// 
    /// # 返回
    /// - `Ok(Frame)`: 解析后的 Frame
    /// - `Err(FlareError)`: 解析失败
    pub async fn parse_and_handle(&self, bytes: &[u8]) -> Result<Frame, FlareError> {
        self.parse_bytes(bytes).await
    }

    /// 获取统计信息
    pub fn get_stats(&self) -> ParserStats {
        ParserStats {
            parsed_count: self.parsed_count.load(Ordering::Relaxed),
            failed_count: self.failed_count.load(Ordering::Relaxed),
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
        }
    }

    /// 重置统计信息
    pub fn reset_stats(&self) {
        self.parsed_count.store(0, Ordering::Relaxed);
        self.failed_count.store(0, Ordering::Relaxed);
        self.total_bytes.store(0, Ordering::Relaxed);
    }

    /// 获取当前使用的序列化格式名称
    pub fn codec_name(&self) -> &str {
        self.payload_codec.name()
    }
    
    /// 获取 Payload 编解码器的引用
    pub fn codec(&self) -> PayloadCodec {
        self.payload_codec
    }
    
    // ==================== 高级功能 ====================
    
    /// 批量解析字节流（适用于批量接收场景）
    /// 
    /// # 参数
    /// - `batches`: 多个待解析的字节数组
    /// 
    /// # 返回
    /// - `Vec<Result<Frame, FlareError>>`: 每个字节数组的解析结果
    /// 
    /// # 示例
    /// 
    /// ```rust
    /// use flare_core::common::parsing::{MessageParser, PayloadCodec};
    /// 
    /// #[tokio::main]
    /// async fn main() {
    ///     let parser = MessageParser::new(PayloadCodec::Json);
    ///     
    ///     let batches = vec![
    ///         vec![1, 2, 3],
    ///         vec![4, 5, 6],
    ///         vec![7, 8, 9],
    ///     ];
    ///     
    ///     let results = parser.parse_batch(&batches).await;
    ///     for (i, result) in results.iter().enumerate() {
    ///         match result {
    ///             Ok(frame) => println!("Frame {}: {:?}", i, frame.message_id),
    ///             Err(e) => println!("Error {}: {:?}", i, e),
    ///         }
    ///     }
    /// }
    /// ```
    pub async fn parse_batch(&self, batches: &[Vec<u8>]) -> Vec<Result<Frame, FlareError>> {
        let mut results = Vec::with_capacity(batches.len());
        
        for bytes in batches {
            let result = self.parse_bytes(bytes).await;
            results.push(result);
        }
        
        results
    }
    
    /// 批量编码 Frame（适用于批量发送场景）
    /// 
    /// # 参数
    /// - `frames`: 多个待编码的 Frame
    /// 
    /// # 返回
    /// - `Vec<Result<Vec<u8>, FlareError>>`: 每个 Frame 的编码结果
    pub async fn encode_batch(&self, frames: &[Frame]) -> Vec<Result<Vec<u8>, FlareError>> {
        let mut results = Vec::with_capacity(frames.len());
        
        for frame in frames {
            let result = self.encode_frame(frame).await;
            results.push(result);
        }
        
        results
    }
    
    /// 流式解析：从缓冲区中尽可能解析多个完整的 Frame
    /// 
    /// # 参数
    /// - `buffer`: 缓冲区（可能包含多个 Frame 或不完整的 Frame）
    /// 
    /// # 返回
    /// - `Ok((Vec<Frame>, usize))`: 解析的 Frame 列表和已消耗的字节数
    /// - `Err(FlareError)`: 解析错误
    /// 
    /// # 注意
    /// 
    /// 返回的 `consumed_bytes` 表示已成功解析的字节数，
    /// 调用者应该从缓冲区中移除这些字节。
    /// 
    /// # 示例
    /// 
    /// ```rust
    /// use flare_core::common::parsing::{MessageParser, PayloadCodec};
    /// 
    /// #[tokio::main]
    /// async fn main() {
    ///     let parser = MessageParser::new(PayloadCodec::Json);
    ///     let mut buffer = vec![/* 缓冲区数据 */];
    ///     
    ///     match parser.parse_stream(&buffer).await {
    ///         Ok((frames, consumed)) => {
    ///             println!("解析了 {} 个 Frame，消耗 {} 字节", frames.len(), consumed);
    ///             // 从缓冲区中移除已消耗的字节
    ///             buffer.drain(..consumed);
    ///         }
    ///         Err(e) => eprintln!("解析错误: {:?}", e),
    ///     }
    /// }
    /// ```
    pub async fn parse_stream(&self, buffer: &[u8]) -> Result<(Vec<Frame>, usize), FlareError> {
        // 简单实现：尝试把整个缓冲区作为一个 Frame 解析
        // TODO: 实现真正的流式解析，支持分帧逻辑
        
        if buffer.is_empty() {
            return Ok((Vec::new(), 0));
        }
        
        match self.parse_bytes(buffer).await {
            Ok(frame) => Ok((vec![frame], buffer.len())),
            Err(_) => {
                // 数据不完整，等待更多数据
                Ok((Vec::new(), 0))
            }
        }
    }
}

/// 克隆 MessageParser（复制统计信息）
impl Clone for MessageParser {
    fn clone(&self) -> Self {
        Self {
            payload_codec: self.payload_codec.clone(),
            frame_codec: Box::new(DefaultFrameCodec::new()), // 使用新的 frame_codec
            parsed_count: AtomicU64::new(self.parsed_count.load(Ordering::Relaxed)),
            failed_count: AtomicU64::new(self.failed_count.load(Ordering::Relaxed)),
            total_bytes: AtomicU64::new(self.total_bytes.load(Ordering::Relaxed)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Serialize, Deserialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestMessage {
        id: u32,
        content: String,
    }

    #[tokio::test]
    async fn test_build_and_parse_frame() {
        let parser = MessageParser::new(PayloadCodec::Json);

        let msg = TestMessage {
            id: 42,
            content: "Hello, Flare!".to_string(),
        };

        // 构建 Frame
        let frame = parser.build_frame(&msg, "test-123".to_string()).await.unwrap();
        assert_eq!(frame.message_id, "test-123");

        // 编码 Frame
        let bytes = parser.encode_frame(&frame).await.unwrap();

        // 解码 Frame
        let decoded_frame = parser.parse_bytes(&bytes).await.unwrap();
        assert_eq!(decoded_frame.message_id, frame.message_id);

        // 解析 Payload
        let decoded_msg: TestMessage = parser.parse_payload(&decoded_frame).await.unwrap();
        assert_eq!(decoded_msg, msg);
    }

    #[tokio::test]
    async fn test_stats() {
        let parser = MessageParser::new(PayloadCodec::Json);

        let msg = TestMessage {
            id: 1,
            content: "Test".to_string(),
        };

        let frame = parser.build_frame(&msg, "msg-1".to_string()).await.unwrap();
        let bytes = parser.encode_frame(&frame).await.unwrap();

        // 解析几次
        for _ in 0..5 {
            let _ = parser.parse_bytes(&bytes).await.unwrap();
        }

        let stats = parser.get_stats();
        assert_eq!(stats.parsed_count, 5);
        assert!(stats.total_bytes > 0);
    }

    #[tokio::test]
    async fn test_different_formats() {
        // JSON
        let parser_json = MessageParser::new(PayloadCodec::Json);
        assert_eq!(parser_json.codec_name(), "json");

        // Protobuf（占位实现，使用 JSON fallback）
        let parser_protobuf = MessageParser::new(PayloadCodec::Protobuf);
        assert_eq!(parser_protobuf.codec_name(), "protobuf");

        let msg = TestMessage {
            id: 99,
            content: "Format test".to_string(),
        };

        // 两种格式都能正常工作
        let frame_json = parser_json.build_frame(&msg, "json-1".to_string()).await.unwrap();
        let bytes_json = parser_json.encode_frame(&frame_json).await.unwrap();
        let decoded_json = parser_json.parse_bytes(&bytes_json).await.unwrap();
        let msg_json: TestMessage = parser_json.parse_payload(&decoded_json).await.unwrap();
        assert_eq!(msg_json, msg);

        let frame_protobuf = parser_protobuf.build_frame(&msg, "protobuf-1".to_string()).await.unwrap();
        let bytes_protobuf = parser_protobuf.encode_frame(&frame_protobuf).await.unwrap();
        let decoded_protobuf = parser_protobuf.parse_bytes(&bytes_protobuf).await.unwrap();
        let msg_protobuf: TestMessage = parser_protobuf.parse_payload(&decoded_protobuf).await.unwrap();
        assert_eq!(msg_protobuf, msg);
    }
    
    #[tokio::test]
    async fn test_batch_parsing() {
        let parser = MessageParser::new(PayloadCodec::Json);
        
        // 准备多个消息
        let messages = vec![
            TestMessage { id: 1, content: "First".to_string() },
            TestMessage { id: 2, content: "Second".to_string() },
            TestMessage { id: 3, content: "Third".to_string() },
        ];
        
        // 构建并编码
        let mut frames = Vec::new();
        for (i, msg) in messages.iter().enumerate() {
            let frame = parser.build_frame(msg, format!("msg-{}", i)).await.unwrap();
            frames.push(frame);
        }
        
        // 批量编码
        let encoded_results = parser.encode_batch(&frames).await;
        assert_eq!(encoded_results.len(), 3);
        
        let batches: Vec<Vec<u8>> = encoded_results.into_iter()
            .map(|r| r.unwrap())
            .collect();
        
        // 批量解析
        let decoded_results = parser.parse_batch(&batches).await;
        assert_eq!(decoded_results.len(), 3);
        
        // 验证解析结果
        for (i, result) in decoded_results.iter().enumerate() {
            let frame = result.as_ref().unwrap();
            assert_eq!(frame.message_id, format!("msg-{}", i));
            
            let msg: TestMessage = parser.parse_payload(frame).await.unwrap();
            assert_eq!(msg, messages[i]);
        }
    }
    
    #[tokio::test]
    async fn test_stream_parsing() {
        let parser = MessageParser::new(PayloadCodec::Json);
        
        let msg = TestMessage {
            id: 42,
            content: "Stream test".to_string(),
        };
        
        // 构建并编码
        let frame = parser.build_frame(&msg, "stream-1".to_string()).await.unwrap();
        let bytes = parser.encode_frame(&frame).await.unwrap();
        
        // 流式解析
        let (frames, consumed) = parser.parse_stream(&bytes).await.unwrap();
        
        assert_eq!(frames.len(), 1);
        assert_eq!(consumed, bytes.len());
        assert_eq!(frames[0].message_id, "stream-1");
        
        let decoded_msg: TestMessage = parser.parse_payload(&frames[0]).await.unwrap();
        assert_eq!(decoded_msg, msg);
    }
    
    #[tokio::test]
    async fn test_stream_parsing_incomplete() {
        let parser = MessageParser::new(PayloadCodec::Json);
        
        // 不完整的数据（只有几个字节）
        let incomplete_data = vec![1, 2, 3];
        
        // 应该返回空列表，不消耗任何字节
        let (frames, consumed) = parser.parse_stream(&incomplete_data).await.unwrap();
        
        assert_eq!(frames.len(), 0);
        assert_eq!(consumed, 0);
    }
}
