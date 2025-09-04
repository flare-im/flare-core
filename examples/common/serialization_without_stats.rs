//! 序列化器测试示例（无统计功能）
//!
//! 演示移除统计功能后的序列化器使用

use flare_core::{
    common::{
        protocol::{Frame, MessageType, Reliability},
        serialization::{
            bincode::BincodeSerializer,
            cbor::CborSerializer,
            msgpack::MessagePackSerializer,
            protobuf::ProtobufSerializer,
            traits::FrameSerializer,
        },
    },
    FlareError,
};

type Result<T> = std::result::Result<T, FlareError>;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== 序列化器测试（无统计功能） ===");
    
    // 创建测试消息帧
    let frame = Frame::new(
        MessageType::Data,
        12345,
        Reliability::AtLeastOnce,
        b"Hello, Flare Core!".to_vec(),
    );
    
    println!("原始消息帧: {:?}", frame);
    
    // 测试Bincode序列化器
    test_bincode_serializer(&frame).await?;
    
    // 测试CBOR序列化器
    test_cbor_serializer(&frame).await?;
    
    // 测试MessagePack序列化器
    test_msgpack_serializer(&frame).await?;
    
    // 测试Protobuf序列化器
    test_protobuf_serializer(&frame).await?;
    
    println!("\n=== 所有测试完成 ===");
    Ok(())
}

async fn test_bincode_serializer(frame: &Frame) -> Result<()> {
    println!("\n--- Bincode序列化器测试 ---");
    
    let serializer = BincodeSerializer::new();
    
    // 序列化
    let serialized = serializer.serialize(frame).await?;
    println!("序列化后大小: {} 字节", serialized.len());
    
    // 反序列化
    let deserialized = serializer.deserialize(&serialized).await?;
    println!("反序列化成功: {:?}", deserialized.get_message_id());
    
    assert_eq!(frame.get_message_id(), deserialized.get_message_id());
    assert_eq!(frame.get_message_type(), deserialized.get_message_type());
    assert_eq!(frame.get_payload(), deserialized.get_payload());
    
    println!("✓ Bincode序列化器测试通过");
    Ok(())
}

async fn test_cbor_serializer(frame: &Frame) -> Result<()> {
    println!("\n--- CBOR序列化器测试 ---");
    
    let serializer = CborSerializer::new();
    
    // 序列化
    let serialized = serializer.serialize(frame).await?;
    println!("序列化后大小: {} 字节", serialized.len());
    
    // 反序列化
    let deserialized = serializer.deserialize(&serialized).await?;
    println!("反序列化成功: {:?}", deserialized.get_message_id());
    
    assert_eq!(frame.get_message_id(), deserialized.get_message_id());
    assert_eq!(frame.get_message_type(), deserialized.get_message_type());
    assert_eq!(frame.get_payload(), deserialized.get_payload());
    
    println!("✓ CBOR序列化器测试通过");
    Ok(())
}

async fn test_msgpack_serializer(frame: &Frame) -> Result<()> {
    println!("\n--- MessagePack序列化器测试 ---");
    
    let serializer = MessagePackSerializer::new();
    
    // 序列化
    let serialized = serializer.serialize(frame).await?;
    println!("序列化后大小: {} 字节", serialized.len());
    
    // 反序列化
    let deserialized = serializer.deserialize(&serialized).await?;
    println!("反序列化成功: {:?}", deserialized.get_message_id());
    
    assert_eq!(frame.get_message_id(), deserialized.get_message_id());
    assert_eq!(frame.get_message_type(), deserialized.get_message_type());
    assert_eq!(frame.get_payload(), deserialized.get_payload());
    
    println!("✓ MessagePack序列化器测试通过");
    Ok(())
}

async fn test_protobuf_serializer(frame: &Frame) -> Result<()> {
    println!("\n--- Protobuf序列化器测试 ---");
    
    let serializer = ProtobufSerializer::new();
    
    // 序列化
    let serialized = serializer.serialize(frame).await?;
    println!("序列化后大小: {} 字节", serialized.len());
    
    // 反序列化
    let deserialized = serializer.deserialize(&serialized).await?;
    println!("反序列化成功: {:?}", deserialized.get_message_id());
    
    assert_eq!(frame.get_message_id(), deserialized.get_message_id());
    assert_eq!(frame.get_message_type(), deserialized.get_message_type());
    assert_eq!(frame.get_payload(), deserialized.get_payload());
    
    println!("✓ Protobuf序列化器测试通过");
    Ok(())
}