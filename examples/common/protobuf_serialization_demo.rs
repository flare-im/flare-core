//! Protobuf序列化演示
//! 
//! 演示如何使用Protobuf序列化器进行消息序列化和反序列化

use flare_core::common::{
    protocol::{Frame, MessageType, Reliability},
    serialization::{ProtobufSerializer, FrameSerializer},
};
use tracing::{info, error};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("🚀 启动Protobuf序列化演示");
    
    // 创建Protobuf序列化器
    let serializer = ProtobufSerializer::new();
    
    info!("✅ Protobuf序列化器创建成功");
    info!("📊 序列化器信息:");
    info!("  - 名称: {}", serializer.name());
    info!("  - 版本: {}", serializer.version());
    info!("  - 描述: {}", serializer.description());
    info!("  - MIME类型: {}", serializer.mime_type());
    info!("  - 文件扩展名: {}", serializer.file_extension());
    
    // 创建测试消息
    let test_messages = vec![
        Frame::new(
            MessageType::Data,
            1,
            Reliability::AtLeastOnce,
            b"Hello, Protobuf!".to_vec(),
        ),
        Frame::new(
            MessageType::Heartbeat,
            2,
            Reliability::BestEffort,
            Vec::new(),
        ),
        Frame::new(
            MessageType::Connect,
            3,
            Reliability::ExactlyOnce,
            b"Connection request with Protobuf".to_vec(),
        ),
        Frame::new(
            MessageType::CustomMessage,
            4,
            Reliability::Ordered,
            serde_json::to_vec(&serde_json::json!({
                "user_id": 12345,
                "action": "login",
                "timestamp": 1634567890
            }))?,
        ),
    ];
    
    info!("📊 准备测试 {} 条不同类型的消息", test_messages.len());
    
    // 测试序列化和反序列化
    for (i, original_frame) in test_messages.iter().enumerate() {
        info!("--- 测试消息 {} ---", i + 1);
        info!("消息类型: {:?}", original_frame.get_message_type());
        info!("可靠性级别: {:?}", original_frame.get_reliability());
        info!("消息ID: {}", original_frame.get_message_id());
        info!("载荷大小: {} 字节", original_frame.get_payload().len());
        
        // 序列化
        let start_serialize = std::time::Instant::now();
        let serialized_data = match serializer.serialize(original_frame).await {
            Ok(data) => data,
            Err(e) => {
                error!("❌ 序列化失败: {}", e);
                continue;
            }
        };
        let serialize_duration = start_serialize.elapsed();
        
        info!("序列化完成，数据大小: {} 字节，耗时: {:?}", serialized_data.len(), serialize_duration);
        
        // 反序列化
        let start_deserialize = std::time::Instant::now();
        let deserialized_frame = match serializer.deserialize(&serialized_data).await {
            Ok(frame) => frame,
            Err(e) => {
                error!("❌ 反序列化失败: {}", e);
                continue;
            }
        };
        let deserialize_duration = start_deserialize.elapsed();
        
        info!("反序列化完成，耗时: {:?}", deserialize_duration);
        
        // 验证数据一致性
        if original_frame.get_message_id() == deserialized_frame.get_message_id() &&
           original_frame.get_message_type() == deserialized_frame.get_message_type() &&
           original_frame.get_reliability() == deserialized_frame.get_reliability() &&
           original_frame.get_payload() == deserialized_frame.get_payload() {
            info!("✅ 数据一致性验证通过");
        } else {
            error!("❌ 数据一致性验证失败");
            info!("原始消息ID: {}, 反序列化消息ID: {}", 
                  original_frame.get_message_id(), deserialized_frame.get_message_id());
            info!("原始消息类型: {:?}, 反序列化消息类型: {:?}", 
                  original_frame.get_message_type(), deserialized_frame.get_message_type());
            info!("原始可靠性: {:?}, 反序列化可靠性: {:?}", 
                  original_frame.get_reliability(), deserialized_frame.get_reliability());
            info!("原始载荷长度: {}, 反序列化载荷长度: {}", 
                  original_frame.get_payload().len(), deserialized_frame.get_payload().len());
        }
        
        // 比较不同序列化格式的大小
        let json_serializer = flare_core::common::serialization::JsonSerializer::new();
        let json_data = json_serializer.serialize(original_frame).await.unwrap_or_default();
        let msgpack_serializer = flare_core::common::serialization::MessagePackSerializer::new();
        let msgpack_data = msgpack_serializer.serialize(original_frame).await.unwrap_or_default();
        let bincode_serializer = flare_core::common::serialization::BincodeSerializer::new();
        let bincode_data = bincode_serializer.serialize(original_frame).await.unwrap_or_default();
        
        info!("📏 不同序列化格式大小比较:");
        info!("  - Protobuf: {} 字节", serialized_data.len());
        info!("  - JSON: {} 字节", json_data.len());
        info!("  - MessagePack: {} 字节", msgpack_data.len());
        info!("  - Bincode: {} 字节", bincode_data.len());
        
        let size_savings = if serialized_data.len() < json_data.len() {
            Some(((json_data.len() - serialized_data.len()) as f64 / json_data.len() as f64) * 100.0)
        } else {
            None
        };
        
        if let Some(savings) = size_savings {
            info!("🎉 Protobuf相比JSON节省了 {:.2}% 的空间", savings);
        }
    }
    
    // 性能测试
    info!("--- 性能测试 ---");
    let test_frame = Frame::new(
        MessageType::Data,
        999,
        Reliability::AtLeastOnce,
        vec![0u8; 1024], // 1KB数据
    );
    
    const ITERATIONS: usize = 1000;
    info!("进行 {} 次序列化/反序列化循环测试", ITERATIONS);
    
    let start_perf_test = std::time::Instant::now();
    for _ in 0..ITERATIONS {
        let serialized = serializer.serialize(&test_frame).await?;
        let _deserialized = serializer.deserialize(&serialized).await?;
    }
    let perf_duration = start_perf_test.elapsed();
    
    let avg_duration = perf_duration / ITERATIONS as u32;
    info!("性能测试完成:");
    info!("  - 总耗时: {:?}", perf_duration);
    info!("  - 平均每次操作: {:?}", avg_duration);
    info!("  - 每秒操作数: {:.0}", ITERATIONS as f64 / perf_duration.as_secs_f64());
    
    if avg_duration.as_millis() < 1 {
        info!("✅ 性能优秀，平均操作时间小于1毫秒");
    } else {
        info!("⚠️ 平均操作时间: {:.2} 毫秒", avg_duration.as_millis() as f64);
    }
    
    info!("✅ Protobuf序列化演示完成");
    Ok(())
}