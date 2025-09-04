//! 压缩+序列化组合演示
//!
//! 演示压缩如何与序列化器结合，进一步降低消息延迟

use std::time::Instant;
use flare_core::common::{
    Frame, MessageType, Reliability,
    FrameSerializer,
    Compressor, CompressionFormat, CompressorFactory,
    JsonSerializer,
    serialization::BincodeSerializer,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 压缩+序列化组合性能演示");
    println!("===============================================");
    
    // 创建测试数据
    let test_frame = Frame::new(
        MessageType::Data,
        12345,
        Reliability::AtLeastOnce,
        create_test_payload(1024), // 1KB测试数据
    );
    
    println!("📊 测试数据: {} 字节载荷", test_frame.get_payload().len());
    println!();
    
    // 测试序列化器
    let serializers: Vec<(String, Box<dyn FrameSerializer>)> = vec![
        ("JSON".to_string(), Box::new(JsonSerializer::new())),
        ("Bincode".to_string(), Box::new(BincodeSerializer::new())),
    ];
    
    // 测试压缩器
    let compressors: Vec<(String, Box<dyn Compressor>)> = vec![
        ("LZ4".to_string(), CompressorFactory::create_static(CompressionFormat::Lz4)),
        ("Snappy".to_string(), CompressorFactory::create_static(CompressionFormat::Snappy)),
        ("Gzip".to_string(), CompressorFactory::create_static(CompressionFormat::Gzip)),
        ("无压缩".to_string(), CompressorFactory::create_static(CompressionFormat::None)),
    ];
    
    for (ser_name, serializer) in &serializers {
        println!("🔧 {} 序列化器测试:", ser_name);
        
        // 先序列化
        let serialized = serializer.serialize(&test_frame).await?;
        println!("  序列化后: {} 字节", serialized.len());
        
        for (comp_name, compressor) in &compressors {
            let start = Instant::now();
            
            // 压缩序列化数据
            let compressed = compressor.compress(&serialized).await?;
            let compress_time = start.elapsed();
            
            let start = Instant::now();
            // 解压
            let decompressed = compressor.decompress(&compressed.data).await?;
            let decompress_time = start.elapsed();
            
            // 反序列化
            let start = Instant::now();
            let restored = serializer.deserialize(&decompressed).await?;
            let deserialize_time = start.elapsed();
            
            let total_time = compress_time + decompress_time + deserialize_time;
            
            // 验证数据完整性
            assert_eq!(restored.get_message_id(), test_frame.get_message_id());
            assert_eq!(restored.get_payload(), test_frame.get_payload());
            
            // 计算效果
            let compression_ratio = if compressed.was_compressed {
                (compressed.compressed_size as f64 / serialized.len() as f64) * 100.0
            } else {
                100.0
            };
            
            let savings = if compressed.was_compressed {
                serialized.len() - compressed.compressed_size
            } else {
                0
            };
            
            println!("    {} 压缩:", comp_name);
            println!("      压缩后: {} 字节 ({:.1}%, 节省 {} 字节)", 
                     compressed.compressed_size, compression_ratio, savings);
            println!("      总耗时: {:.2}ms (压缩: {:.2}ms, 解压: {:.2}ms)", 
                     total_time.as_secs_f64() * 1000.0,
                     compress_time.as_secs_f64() * 1000.0,
                     decompress_time.as_secs_f64() * 1000.0);
            
            // 检查是否满足超低延迟要求
            let meets_requirement = total_time.as_millis() < 15;
            println!("      超低延迟要求(<15ms): {}", if meets_requirement { "✅" } else { "❌" });
        }
        println!();
    }
    
    // 场景推荐测试
    println!("🎯 不同场景推荐组合:");
    println!("===============================================");
    
    test_scenario("游戏/交易 (超低延迟)", 
                  &BincodeSerializer::new(), 
                  CompressorFactory::recommended_static("ultra_low_latency").as_ref()).await?;
    
    test_scenario("实时通信 (平衡)", 
                  &JsonSerializer::new(), 
                  CompressorFactory::recommended_static("real_time").as_ref()).await?;
    
    test_scenario("存储/备份 (高压缩)", 
                  &BincodeSerializer::new(), 
                  CompressorFactory::recommended_static("storage").as_ref()).await?;
    
    println!("✅ 演示完成！");
    
    Ok(())
}

/// 创建测试载荷
fn create_test_payload(size: usize) -> Vec<u8> {
    // 创建具有一定重复性的数据，模拟真实场景
    let mut payload = Vec::with_capacity(size);
    let patterns = [
        "user_message_data".as_bytes(),
        b"timestamp:",
        b"event_type:",
        b"user_id:",
        b"session_id:",
        b"metadata:",
    ];
    
    let mut pattern_idx = 0;
    while payload.len() < size {
        let pattern = patterns[pattern_idx % patterns.len()];
        payload.extend_from_slice(pattern);
        pattern_idx += 1;
        
        // 添加一些变化数据
        let num = (payload.len() % 1000).to_string();
        payload.extend_from_slice(num.as_bytes());
        payload.push(b' ');
    }
    
    payload.truncate(size);
    payload
}

/// 测试特定场景的组合
async fn test_scenario(
    name: &str, 
    serializer: &dyn FrameSerializer, 
    compressor: &dyn Compressor
) -> Result<(), Box<dyn std::error::Error>> {
    let test_frame = Frame::new(
        MessageType::Data,
        1,
        Reliability::AtLeastOnce,
        create_test_payload(2048), // 2KB数据
    );
    
    let start = Instant::now();
    
    // 序列化
    let serialized = serializer.serialize(&test_frame).await?;
    
    // 压缩
    let compressed = compressor.compress(&serialized).await?;
    
    // 解压
    let decompressed = compressor.decompress(&compressed.data).await?;
    
    // 反序列化
    let _restored = serializer.deserialize(&decompressed).await?;
    
    let total_time = start.elapsed();
    
    let compression_ratio = if compressed.was_compressed {
        (compressed.compressed_size as f64 / serialized.len() as f64) * 100.0
    } else {
        100.0
    };
    
    println!("{}: {} + {}", name, serializer.name(), compressor.name());
    println!("  数据流: {}字节 → {}字节 → {}字节 ({:.1}%)", 
             test_frame.get_payload().len(), serialized.len(), compressed.compressed_size, compression_ratio);
    println!("  总耗时: {:.2}ms {}", 
             total_time.as_secs_f64() * 1000.0,
             if total_time.as_millis() < 15 { "✅" } else { "⚠️" });
    println!();
    
    Ok(())
}