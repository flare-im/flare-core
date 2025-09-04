//! 序列化模块使用示例
//!
//! 展示如何使用统一消息帧的序列化和反序列化功能

use flare_core::common::{
    protocol::{Frame, MessageType, Reliability},
    serialization::{
        FrameSerializer, SerializationFormat, SerializationConfig,
        JsonSerializer, SerializerFactory,
        json_serializer, json_pretty_serializer,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    // tracing_subscriber::init(); // 暂时注释掉，因为没有这个依赖
    
    // 创建测试消息帧
    let frame = Frame::new(
        MessageType::Data,
        12345,
        Reliability::AtLeastOnce,
        "这是一条测试消息".as_bytes().to_vec(),
    );
    
    println!("原始消息帧:");
    println!("  消息ID: {}", frame.get_message_id());
    println!("  消息类型: {:?}", frame.get_message_type());
    println!("  可靠性: {:?}", frame.get_reliability());
    println!("  负载大小: {} 字节", frame.get_payload().len());
    println!();
    
    // 1. 使用便捷函数创建序列化器
    println!("=== 使用便捷函数 ===");
    
    // JSON序列化器
    let json_ser = json_serializer();
    let json_data = json_ser.serialize(&frame).await?;
    println!("JSON序列化结果: {} 字节", json_data.len());
    println!("JSON内容: {}", String::from_utf8_lossy(&json_data));
    
    // 美化JSON序列化器
    let pretty_ser = json_pretty_serializer();
    let pretty_data = pretty_ser.serialize(&frame).await?;
    println!("美化JSON序列化结果: {} 字节", pretty_data.len());
    println!("美化JSON内容:\n{}", String::from_utf8_lossy(&pretty_data));
    
    // 反序列化测试
    let deserialized = json_ser.deserialize(&json_data).await?;
    println!("反序列化成功，消息ID: {}", deserialized.get_message_id());
    println!();
    
    // 2. 使用工厂创建序列化器
    println!("=== 使用序列化器工厂 ===");
    
    let factory = SerializerFactory::new();
    
    // 列出支持的格式
    let formats = factory.supported_formats();
    println!("支持的序列化格式:");
    for format in &formats {
        println!("  - {}", format);
    }
    println!();
    
    // 创建JSON序列化器
    let factory_json_ser = factory.create(SerializationFormat::Json)?;
    let factory_data = factory_json_ser.serialize(&frame).await?;
    println!("工厂创建的JSON序列化器结果: {} 字节", factory_data.len());
    
    // 根据MIME类型创建序列化器
    let mime_ser = factory.create_by_mime_type("application/json")?;
    let mime_data = mime_ser.serialize(&frame).await?;
    println!("根据MIME类型创建的序列化器结果: {} 字节", mime_data.len());
    
    // 根据文件扩展名创建序列化器
    let ext_ser = factory.create_by_extension("json")?;
    let ext_data = ext_ser.serialize(&frame).await?;
    println!("根据文件扩展名创建的序列化器结果: {} 字节", ext_data.len());
    println!();
    
    // 3. 使用配置创建序列化器
    println!("=== 使用配置创建序列化器 ===");
    
    let config = SerializationConfig::new()
        .with_pretty_format()
        .with_max_size(1024 * 1024); // 1MB限制
    
    let configured_ser = JsonSerializer::with_config(config);
    let configured_data = configured_ser.serialize(&frame).await?;
    println!("配置化序列化器结果: {} 字节", configured_data.len());
    
    // 获取序列化器信息
    let info = factory.get_serializer_info(SerializationFormat::Json)?;
    println!("序列化器信息:");
    println!("  名称: {}", info.name);
    println!("  版本: {}", info.version);
    println!("  描述: {}", info.description);
    println!("  MIME类型: {}", info.mime_type);
    println!("  文件扩展名: {}", info.file_extension);
    println!();
    
    // 4. 测试Frame的便捷方法
    println!("=== Frame便捷方法测试 ===");
    
    // 使用Frame的JSON方法
    let frame_json = frame.to_json_bytes()?;
    println!("Frame.to_json_bytes(): {} 字节", frame_json.len());
    
    let frame_pretty_json = frame.to_pretty_json_bytes()?;
    println!("Frame.to_pretty_json_bytes(): {} 字节", frame_pretty_json.len());
    
    // 反序列化测试
    let frame_from_json = Frame::from_json_bytes(&frame_json)?;
    println!("Frame.from_json_bytes() 成功，消息ID: {}", frame_from_json.get_message_id());
    
    // 保持向后兼容性测试
    let bincode_data = frame.to_bytes()?;
    println!("Bincode序列化结果: {} 字节", bincode_data.len());
    
    let frame_from_bincode = Frame::from_bytes(&bincode_data)?;
    println!("Bincode反序列化成功，消息ID: {}", frame_from_bincode.get_message_id());
    println!();
    
    // 5. 批量序列化测试
    println!("=== 批量序列化测试 ===");
    
    let frames = vec![
        Frame::heartbeat(),
        Frame::heartbeat_ack(),
        Frame::data(1, "消息1".as_bytes().to_vec()),
        Frame::data(2, "消息2".as_bytes().to_vec()),
    ];
    
    let json_ser = json_serializer();
    let batch_results = json_ser.serialize_batch(&frames).await?;
    
    println!("批量序列化 {} 个消息帧:", frames.len());
    for (i, data) in batch_results.iter().enumerate() {
        println!("  消息 {}: {} 字节", i + 1, data.len());
    }
    
    // 批量反序列化测试
    let batch_frames = json_ser.deserialize_batch(&batch_results).await?;
    println!("批量反序列化成功，恢复 {} 个消息帧", batch_frames.len());
    println!();
    
    // 6. 统计信息测试
    println!("=== 统计信息测试 ===");
    
    let mut json_ser_with_stats = JsonSerializer::new();
    
    // 执行多次序列化操作
    for i in 0..10 {
        let test_frame = Frame::data(i, format!("测试消息 {}", i).as_bytes().to_vec());
        let _ = json_ser_with_stats.serialize(&test_frame).await?;
    }
    
    let stats = json_ser_with_stats.stats();
    println!("序列化统计信息:");
    println!("  序列化次数: {}", stats.serialize_count);
    println!("  序列化字节数: {}", stats.serialized_bytes);
    println!("  平均序列化时间: {} 微秒", stats.avg_serialize_time_us);
    println!("  序列化成功率: {:.2}%", stats.serialize_success_rate() * 100.0);
    println!("  平均序列化大小: {:.2} 字节", stats.avg_serialized_size());
    
    println!("\n序列化模块演示完成！");
    
    Ok(())
}