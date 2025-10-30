//! 消息处理器
//!
//! 统一处理消息的构建、编码、压缩、发送和接收、解压、解析流程
//! 让连接层只负责二进制数据的传输

use crate::common::protocol::frame::Frame;
use crate::common::parsing::{MessageParser, PayloadCodec};
use crate::common::compression::{CompressionConfig, CompressionAlgorithm, compress, decompress};
use crate::common::error::FlareError;

/// 消息处理器配置
#[derive(Clone)]
pub struct MessageProcessorConfig {
    /// Payload 编解码器
    pub codec: PayloadCodec,
    /// 压缩配置
    pub compression_config: Option<CompressionConfig>,
    /// 最小压缩大小（字节），小于此大小不压缩
    pub compression_min_size: usize,
}

impl Default for MessageProcessorConfig {
    fn default() -> Self {
        Self {
            codec: PayloadCodec::Json,
            compression_config: Some(CompressionConfig::new(CompressionAlgorithm::Lz4).with_min_size(128)),
            compression_min_size: 128,
        }
    }
}

/// 统一的消息处理器
///
/// 负责完整的消息处理流程：
/// - 发送：Frame → 编码 → 压缩 → 二进制数据
/// - 接收：二进制数据 → 解压 → 解析 → Frame
pub struct MessageProcessor {
    /// 消息解析器（负责编码/解码 Frame）
    parser: MessageParser,
    /// 压缩配置
    compression_config: Option<CompressionConfig>,
    /// 最小压缩大小
    compression_min_size: usize,
}

impl MessageProcessor {
    /// 从配置创建消息处理器
    pub fn from_config(config: MessageProcessorConfig) -> Self {
        Self {
            parser: MessageParser::new(config.codec),
            compression_config: config.compression_config,
            compression_min_size: config.compression_min_size,
        }
    }

    /// 创建默认的消息处理器（JSON + LZ4压缩）
    pub fn default() -> Self {
        Self::from_config(MessageProcessorConfig::default())
    }

    /// 创建不压缩的消息处理器
    pub fn without_compression(codec: PayloadCodec) -> Self {
        Self {
            parser: MessageParser::new(codec),
            compression_config: None,
            compression_min_size: usize::MAX,
        }
    }

    /// 处理发送：将 Frame 转换为二进制数据
    ///
    /// # 流程
    /// 1. 编码 Frame 为字节数组
    /// 2. 根据配置决定是否压缩
    /// 3. 返回最终二进制数据
    ///
    /// # 参数
    /// - `frame`: 要发送的 Frame
    ///
    /// # 返回
    /// - `Ok(Vec<u8>)`: 处理后的二进制数据
    /// - `Err(FlareError)`: 处理失败
    pub async fn process_send(&self, frame: &Frame) -> Result<Vec<u8>, FlareError> {
        // 1. 编码 Frame 为字节数组
        let encoded = self.parser.encode_frame(frame).await?;
        
        // 2. 决定是否压缩
        let result = if encoded.len() >= self.compression_min_size {
            if let Some(ref comp_config) = self.compression_config {
                // 压缩
                compress(&encoded, comp_config).map_err(|e| {
                    FlareError::general_error(format!("压缩失败: {}", e))
                })?
            } else {
                encoded
            }
        } else {
            encoded
        };
        
        Ok(result)
    }

    /// 处理接收：将二进制数据转换为 Frame
    ///
    /// # 流程
    /// 1. 尝试解压（如果需要）
    /// 2. 解析为 Frame
    /// 3. 返回 Frame
    ///
    /// # 参数
    /// - `data`: 接收到的二进制数据
    /// - `try_decompress`: 是否尝试解压（通常从 Frame 元数据获取，这里简化处理）
    ///
    /// # 返回
    /// - `Ok(Frame)`: 解析后的 Frame
    /// - `Err(FlareError)`: 解析失败
    pub async fn process_receive(&self, data: &[u8], try_decompress: bool) -> Result<Frame, FlareError> {
        // 1. 如果需要且配置了压缩，尝试解压
        let decoded_data = if try_decompress && self.compression_config.is_some() {
            if let Some(ref comp_config) = self.compression_config {
                // 尝试解压，如果失败说明未压缩，直接使用原数据
                decompress(data, comp_config).unwrap_or_else(|_| data.to_vec())
            } else {
                data.to_vec()
            }
        } else {
            data.to_vec()
        };
        
        // 2. 解析为 Frame
        self.parser.parse_bytes(&decoded_data).await
    }

    /// 处理接收（自动检测压缩）：尝试多种方式解析
    ///
    /// 先尝试不解压解析，失败则尝试解压后解析
    pub async fn process_receive_auto(&self, data: &[u8]) -> Result<Frame, FlareError> {
        // 先尝试不解压直接解析
        match self.parser.parse_bytes(data).await {
            Ok(frame) => Ok(frame),
            Err(_) => {
                // 如果失败，尝试解压后解析
                self.process_receive(data, true).await
            }
        }
    }

    /// 获取当前使用的编解码器
    pub fn codec(&self) -> PayloadCodec {
        self.parser.codec()
    }
}

/// 为 MessageProcessor 实现 Clone（共享解析器配置）
impl Clone for MessageProcessor {
    fn clone(&self) -> Self {
        Self {
            parser: self.parser.clone(),
            compression_config: self.compression_config.clone(),
            compression_min_size: self.compression_min_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::commands::{Command, MessageCmd, DataCommand};
    use bytes::Bytes;

    #[tokio::test]
    async fn test_processor_send_receive() {
        let processor = MessageProcessor::default();
        
        // 构建测试 Frame
        let payload = Bytes::from(b"Hello, World!".to_vec());
        let frame = Frame {
            message_id: "test-001".to_string(),
            payload: payload.clone(),
            reliability: crate::common::protocol::reliability::Reliability::BestEffort,
            command: Command::Message(MessageCmd::Data(DataCommand { data: payload.to_vec() })),
        };
        
        // 处理发送
        let encoded = processor.process_send(&frame).await.unwrap();
        assert!(!encoded.is_empty());
        
        // 处理接收（小数据不压缩）
        let decoded = processor.process_receive_auto(&encoded).await.unwrap();
        assert_eq!(decoded.message_id, frame.message_id);
        assert_eq!(decoded.payload, frame.payload);
    }

    #[tokio::test]
    async fn test_processor_without_compression() {
        let processor = MessageProcessor::without_compression(PayloadCodec::Json);
        
        let payload = Bytes::from(b"Test".to_vec());
        let frame = Frame {
            message_id: "test-002".to_string(),
            payload: payload.clone(),
            reliability: crate::common::protocol::reliability::Reliability::BestEffort,
            command: Command::Message(MessageCmd::Data(DataCommand { data: payload.to_vec() })),
        };
        
        let encoded = processor.process_send(&frame).await.unwrap();
        let decoded = processor.process_receive_auto(&encoded).await.unwrap();
        assert_eq!(decoded.message_id, frame.message_id);
    }
}

