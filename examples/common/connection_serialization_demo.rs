//! WebSocket和QUIC连接中使用FrameSerializer的示例
//!
//! 展示如何为连接配置不同的序列化格式以满足不同的性能要求

use flare_core::common::{
    protocol::{Frame, MessageType, Reliability},
    connections::{
        ConnectionConfig, ConnectionType, ConnectionRole,
    },
    serialization::{SerializationFormat, SerializationConfig},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== WebSocket和QUIC连接序列化器集成演示 ===\n");

    // 1. 创建不同序列化配置的连接
    println!("1. 创建不同序列化配置的连接");
    
    // 高性能配置（使用紧凑JSON，适合生产环境）
    let high_perf_config = ConnectionConfig::low_latency(
        "high_perf_client".to_string(),
        "ws://localhost:8080".to_string(),
    )
    .with_json_serialization(); // 使用紧凑JSON
    
    // 调试配置（使用美化JSON，便于调试）
    let debug_config = ConnectionConfig::client(
        "debug_client".to_string(),
        "ws://localhost:8081".to_string(),
    )
    .with_pretty_json_serialization(); // 使用美化JSON
    
    // QUIC高性能配置
    let quic_config = ConnectionConfig::low_latency(
        "quic_client".to_string(),
        "127.0.0.1:4433".to_string(),
    )
    .with_type(ConnectionType::Quic)
    .with_serialization_format(SerializationFormat::Json)
    .with_serialization_config(
        SerializationConfig::new()
            .with_max_size(64 * 1024) // 64KB限制，适合低延迟
    );
    
    println!("  - 高性能WebSocket配置: {:?}", high_perf_config.serialization_format);
    println!("  - 调试WebSocket配置: {:?}", debug_config.serialization_format);
    println!("  - QUIC配置: {:?}", quic_config.serialization_format);
    println!();

    // 2. 展示序列化器性能对比
    println!("2. 序列化器性能对比");
    
    // 创建测试消息
    let test_message = Frame::new(
        MessageType::Data,
        12345,
        Reliability::AtLeastOnce,
        "这是一条测试消息，用于比较不同序列化器的性能表现".as_bytes().to_vec(),
    );
    
    // JSON序列化器（紧凑）
    let json_serializer = flare_core::common::serialization::factory::json_serializer();
    let json_start = std::time::Instant::now();
    let json_data = json_serializer.serialize(&test_message).await?;
    let json_duration = json_start.elapsed();
    
    // JSON序列化器（美化）
    let pretty_serializer = flare_core::common::serialization::factory::json_pretty_serializer();
    let pretty_start = std::time::Instant::now();
    let pretty_data = pretty_serializer.serialize(&test_message).await?;
    let pretty_duration = pretty_start.elapsed();
    
    println!("  序列化性能对比:");
    println!("    紧凑JSON: {} 字节, 耗时 {:?}", json_data.len(), json_duration);
    println!("    美化JSON: {} 字节, 耗时 {:?}", pretty_data.len(), pretty_duration);
    println!("    压缩比: {:.1}%", (pretty_data.len() as f64 / json_data.len() as f64 - 1.0) * 100.0);
    println!();

    // 3. 不同应用场景的配置建议
    println!("3. 不同应用场景的配置建议");
    
    // 超低延迟场景（游戏、实时交易）
    let ultra_low_latency = ConnectionConfig::low_latency(
        "game_client".to_string(),
        "quic://game-server:4433".to_string(),
    )
    .with_type(ConnectionType::Quic)
    .with_json_serialization()
    .with_serialization_config(
        SerializationConfig::new()
            .with_max_size(32 * 1024) // 32KB限制
    );
    
    println!("  超低延迟场景 (平均延迟 < 15ms):");
    println!("    协议: QUIC (UDP-based, 0-RTT)");
    println!("    序列化: 紧凑JSON");
    println!("    配置: 小缓冲区, 严格大小限制");
    println!("    适用: 游戏, 实时交易, 视频流");
    println!();
    
    // 高吞吐量场景（批量数据处理）
    let high_throughput = ConnectionConfig::high_performance(
        "batch_client".to_string(),
        "ws://data-server:8080".to_string(),
    )
    .with_json_serialization();
    
    println!("  高吞吐量场景:");
    println!("    协议: WebSocket (TCP-based, 可靠传输)");
    println!("    序列化: 紧凑JSON");
    println!("    配置: 大缓冲区, 高消息大小限制");
    println!("    适用: 批量数据处理, 文件传输");
    println!();
    
    // 调试开发场景
    let development = ConnectionConfig::client(
        "dev_client".to_string(),
        "ws://localhost:3000".to_string(),
    )
    .with_pretty_json_serialization();
    
    println!("  调试开发场景:");
    println!("    协议: WebSocket");
    println!("    序列化: 美化JSON (便于调试)");
    println!("    配置: 适中的缓冲区和限制");
    println!("    适用: 开发调试, API测试");
    println!();

    // 4. 连接配置序列化性能测试
    println!("4. 实际连接配置性能测试");
    
    // 模拟不同配置下的序列化性能
    let configs = vec![
        ("超低延迟QUIC", ultra_low_latency),
        ("高吞吐量WebSocket", high_throughput),
        ("调试开发", development),
    ];
    
    for (name, config) in configs {
        println!("  测试配置: {}", name);
        
        // 创建对应的序列化器
        let factory = flare_core::common::serialization::SerializerFactory::new();
        let serializer = if let (Some(format), Some(config)) = (&config.serialization_format, &config.serialization_config) {
            factory.create_with_config(*format, config.clone())?
        } else {
            // 如果没有指定配置，使用默认的JSON序列化器
            flare_core::common::serialization::SerializerFactory::json()
        };
        
        // 批量序列化测试
        let mut total_size = 0;
        let mut total_time = std::time::Duration::default();
        let test_count = 100;
        
        for i in 0..test_count {
            let msg = Frame::data(i, format!("测试消息 #{}", i).as_bytes().to_vec());
            let start = std::time::Instant::now();
            let data = serializer.serialize(&msg).await?;
            total_time += start.elapsed();
            total_size += data.len();
        }
        
        let avg_time = total_time / test_count as u32;
        let avg_size = total_size / test_count as usize;
        
        println!("    批量序列化 {} 条消息:", test_count);
        println!("      平均时间: {:?}", avg_time);
        println!("      平均大小: {} 字节", avg_size);
        println!("      总耗时: {:?}", total_time);
        println!();
    }

    // 5. 序列化器选择建议
    println!("5. 序列化器选择建议");
    println!();
    println!("  📊 性能优先 (延迟 < 15ms):");
    println!("     ✓ JSON紧凑格式");
    println!("     ✓ 小消息大小限制 (< 32KB)");
    println!("     ✓ QUIC协议");
    println!();
    println!("  🔧 调试优先:");
    println!("     ✓ JSON美化格式");
    println!("     ✓ WebSocket协议 (便于抓包分析)");
    println!("     ✓ 详细的错误信息");
    println!();
    println!("  ⚖️ 平衡选择:");
    println!("     ✓ JSON紧凑格式");
    println!("     ✓ 适中的消息大小限制");
    println!("     ✓ WebSocket协议 (兼容性好)");
    println!();
    println!("  🚀 未来扩展:");
    println!("     ✓ 预留Bincode支持 (更高性能)");
    println!("     ✓ 预留MessagePack支持 (跨语言)");
    println!("     ✓ 自定义序列化器支持");

    println!("\n=== 序列化器集成演示完成 ===");
    Ok(())
}