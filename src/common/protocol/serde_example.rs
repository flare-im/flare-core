//! 序列化示例模块
//! 展示如何使用 JSON 和 Protobuf 序列化 Frame 消息

use super::{Frame, FrameBuilder, ping, pong, connect, SerializationFormat};
use super::flare::core::{Reliability, Command};
use super::flare::core::commands::command::Type as CommandType;
use prost::Message;

/// 示例：创建并序列化为 Protobuf
pub fn example_protobuf_serialization() -> Vec<u8> {
    // 创建一个简单的 PING Frame
    let frame = FrameBuilder::new()
        .with_command(Command {
            r#type: Some(CommandType::System(ping())),
        })
        .with_reliability(Reliability::BestEffort)
        .build();
    
    // 序列化为 Protobuf
    let mut buf = Vec::new();
    frame.encode(&mut buf).unwrap();
    buf
}

/// 示例：创建并序列化为 JSON
pub fn example_json_serialization() -> String {
    // 创建一个 CONNECT Frame
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("client_id".to_string(), "test-client".as_bytes().to_vec());
    
    let frame = FrameBuilder::new()
        .with_command(Command {
            r#type: Some(CommandType::System(connect(
                SerializationFormat::Json,
                metadata,
            ))),
        })
        .with_reliability(Reliability::AtLeastOnce)
        .build();
    
    // 序列化为 JSON
    serde_json::to_string(&frame).unwrap()
}

/// 示例：从 Protobuf 反序列化
pub fn example_protobuf_deserialization(data: &[u8]) -> Result<Frame, prost::DecodeError> {
    Frame::decode(data)
}

/// 示例：从 JSON 反序列化
pub fn example_json_deserialization(data: &str) -> Result<Frame, serde_json::Error> {
    serde_json::from_str(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protobuf_serialization() {
        let data = example_protobuf_serialization();
        assert!(!data.is_empty());
        
        // 反序列化验证
        let frame = example_protobuf_deserialization(&data).unwrap();
        assert!(!frame.message_id.is_empty());
    }

    #[test]
    fn test_json_serialization() {
        let json = example_json_serialization();
        assert!(!json.is_empty());
        assert!(json.contains("message_id"));
        
        // 反序列化验证
        let frame = example_json_deserialization(&json).unwrap();
        assert!(!frame.message_id.is_empty());
    }

    #[test]
    fn test_round_trip_protobuf() {
        let frame1 = FrameBuilder::new()
            .with_command(Command {
                r#type: Some(CommandType::System(pong())),
            })
            .with_reliability(Reliability::ExactlyOnce)
            .build();
        
        let data = frame1.encode_to_vec();
        let frame2 = Frame::decode(data.as_slice()).unwrap();
        
        assert_eq!(frame1.message_id, frame2.message_id);
        assert_eq!(frame1.reliability, frame2.reliability);
    }

    #[test]
    fn test_round_trip_json() {
        let frame1 = FrameBuilder::new()
            .with_command(Command {
                r#type: Some(CommandType::System(ping())),
            })
            .with_reliability(Reliability::Ordered)
            .build();
        
        let json = serde_json::to_string(&frame1).unwrap();
        let frame2: Frame = serde_json::from_str(&json).unwrap();
        
        assert_eq!(frame1.message_id, frame2.message_id);
        assert_eq!(frame1.reliability, frame2.reliability);
    }
}
