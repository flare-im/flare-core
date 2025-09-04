//! 灵活序列化器集成演示
//!
//! 展示不同的序列化器集成方式：配置驱动 vs 直接传入

use flare_core::common::{
    protocol::{Frame, MessageType, Reliability},
    connections::{ConnectionBuilder, ConnectionFactory, ConnectionConfig},
    serialization::{JsonSerializer, SerializationFormat, SerializationConfig, FrameSerializer},
};
use std::sync::Arc;
use async_trait::async_trait;

/// 自定义序列化器示例 - 简单的消息计数器
#[derive(Debug)]
struct CountingSerializer {
    inner: JsonSerializer,
    serialize_count: std::sync::atomic::AtomicU64,
    deserialize_count: std::sync::atomic::AtomicU64,
}

impl CountingSerializer {
    fn new() -> Self {
        Self {
            inner: JsonSerializer::new(),
            serialize_count: std::sync::atomic::AtomicU64::new(0),
            deserialize_count: std::sync::atomic::AtomicU64::new(0),
        }
    }
    
    fn get_counts(&self) -> (u64, u64) {
        (
            self.serialize_count.load(std::sync::atomic::Ordering::SeqCst),
            self.deserialize_count.load(std::sync::atomic::Ordering::SeqCst),
        )
    }
}

#[async_trait]
impl FrameSerializer for CountingSerializer {
    fn format(&self) -> SerializationFormat {
        SerializationFormat::Json
    }
    
    async fn serialize(&self, frame: &Frame) -> flare_core::common::error::Result<Vec<u8>> {
        self.serialize_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut data = self.inner.serialize(frame).await?;
        
        // 添加计数前缀
        let count = self.serialize_count.load(std::sync::atomic::Ordering::SeqCst);
        let prefix = format!("[COUNT:{}]", count).into_bytes();
        data.splice(0..0, prefix);
        Ok(data)
    }
    
    async fn deserialize(&self, data: &[u8]) -> flare_core::common::error::Result<Frame> {
        self.deserialize_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        // 移除计数前缀（如果存在）
        let data = if data.starts_with(b"[COUNT:") {
            if let Some(end) = data.iter().position(|&b| b == b']') {
                &data[end + 1..]
            } else {
                data
            }
        } else {
            data
        };
        
        self.inner.deserialize(data).await
    }
    
    fn name(&self) -> &'static str {
        "CountingSerializer"
    }
    
    fn description(&self) -> &'static str {
        "JSON序列化器，带消息计数功能"
    }
    
    fn clone_box(&self) -> Box<dyn FrameSerializer> {
        Box::new(Self::new())
    }
    
    fn mime_type(&self) -> &'static str {
        "application/json"
    }
    
    fn file_extension(&self) -> &'static str {
        "json"
    }
}

/// 压缩序列化器示例（模拟）
#[derive(Debug)]
struct CompressedSerializer {
    inner: JsonSerializer,
}

impl CompressedSerializer {
    fn new() -> Self {
        Self {
            inner: JsonSerializer::new(),
        }
    }
    
    // 模拟压缩（实际中可以使用 flate2 等库）
    fn compress(&self, data: &[u8]) -> Vec<u8> {
        let mut compressed = b"[COMPRESSED]".to_vec();
        compressed.extend_from_slice(data);
        compressed
    }
    
    // 模拟解压缩
    fn decompress(&self, data: &[u8]) -> Vec<u8> {
        if data.starts_with(b"[COMPRESSED]") {
            data[12..].to_vec()
        } else {
            data.to_vec()
        }
    }
}

#[async_trait]
impl FrameSerializer for CompressedSerializer {
    fn format(&self) -> SerializationFormat {
        SerializationFormat::Json
    }
    
    async fn serialize(&self, frame: &Frame) -> flare_core::common::error::Result<Vec<u8>> {
        let data = self.inner.serialize(frame).await?;
        Ok(self.compress(&data))
    }
    
    async fn deserialize(&self, data: &[u8]) -> flare_core::common::error::Result<Frame> {
        let decompressed = self.decompress(data);
        self.inner.deserialize(&decompressed).await
    }
    
    fn name(&self) -> &'static str {
        "CompressedSerializer"
    }
    
    fn description(&self) -> &'static str {
        "带压缩功能的JSON序列化器（模拟）"
    }
    
    fn clone_box(&self) -> Box<dyn FrameSerializer> {
        Box::new(Self::new())
    }
    
    fn mime_type(&self) -> &'static str {
        "application/json+compressed"
    }
    
    fn file_extension(&self) -> &'static str {
        "json.gz"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 灵活序列化器集成方式对比演示 ===\n");

    // 1. 方案一：基于配置的序列化器（原有方式）
    println!("1. 方案一：基于配置的序列化器创建");
    
    let config_based = ConnectionBuilder::client("config_client".to_string(), "ws://localhost:8080".to_string())
        .with_json_serialization()
        .build_config();
    
    println!("  - 配置方式创建连接");
    println!("  - 序列化格式: {:?}", config_based.get_serialization_format());
    println!("  - 优点: 配置可序列化，工厂统一管理，类型安全");
    println!("  - 缺点: 扩展需要修改枚举，无法运行时动态选择");
    println!();

    // 2. 方案二：直接传入序列化器实例（新方式）
    println!("2. 方案二：直接传入序列化器实例");
    
    let custom_serializer1 = Arc::new(Box::new(CountingSerializer::new()) as Box<dyn FrameSerializer>);
    let custom_serializer2 = Arc::new(Box::new(CompressedSerializer::new()) as Box<dyn FrameSerializer>);
    
    let builder_with_counting = ConnectionBuilder::client("counting_client".to_string(), "ws://localhost:8080".to_string())
        .with_serializer(custom_serializer1.clone());
    
    let builder_with_compression = ConnectionBuilder::client("compressed_client".to_string(), "ws://localhost:8080".to_string())
        .with_serializer(custom_serializer2.clone());
    
    println!("  - 计数序列化器: {}", custom_serializer1.name());
    println!("  - 压缩序列化器: {}", custom_serializer2.name());
    println!("  - 优点: 极大灵活性，无需修改核心代码，运行时动态");
    println!("  - 缺点: 配置无法序列化，类型擦除");
    println!();

    // 3. 方案三：混合方式（推荐的最优方案）
    println!("3. 方案三：混合方式（推荐）");
    
    // 3.1 预设配置 + 自定义序列化器
    let ultra_low_latency = ConnectionBuilder::client("game_client".to_string(), "quic://game-server:4433".to_string())
        .ultra_low_latency()
        .with_serializer(custom_serializer1.clone()); // 覆盖预设的序列化器
    
    // 3.2 基础配置 + 配置序列化器
    let debug_config = ConnectionBuilder::client("debug_client".to_string(), "ws://localhost:3000".to_string())
        .debug_friendly(); // 使用预设的美化JSON
    
    println!("  - 超低延迟 + 计数序列化器: {}", ultra_low_latency.serializer_description());
    println!("  - 调试配置 + 美化JSON: {}", debug_config.serializer_description());
    println!("  - 优点: 灵活性 + 便捷性，兼顾两种方式的优点");
    println!("  - 缺点: API稍微复杂，需要理解两种模式");
    println!();

    // 4. 实际性能测试
    println!("4. 不同序列化器性能测试");
    
    let test_message = Frame::new(
        MessageType::Data,
        12345,
        Reliability::AtLeastOnce,
        "这是一条用于测试序列化性能的消息".as_bytes().to_vec(),
    );
    
    // 4.1 标准JSON序列化器
    let json_serializer = flare_core::common::serialization::factory::json_serializer();
    let json_start = std::time::Instant::now();
    let json_data = json_serializer.serialize(&test_message).await?;
    let json_duration = json_start.elapsed();
    
    // 4.2 计数序列化器
    let counting_serializer = CountingSerializer::new();
    let counting_start = std::time::Instant::now();
    let counting_data = counting_serializer.serialize(&test_message).await?;
    let counting_duration = counting_start.elapsed();
    
    // 4.3 压缩序列化器
    let compressed_serializer = CompressedSerializer::new();
    let compressed_start = std::time::Instant::now();
    let compressed_data = compressed_serializer.serialize(&test_message).await?;
    let compressed_duration = compressed_start.elapsed();
    
    println!("  序列化性能对比:");
    println!("    标准JSON: {} 字节, 耗时 {:?}", json_data.len(), json_duration);
    println!("    计数JSON: {} 字节, 耗时 {:?}", counting_data.len(), counting_duration);
    println!("    压缩JSON: {} 字节, 耗时 {:?}", compressed_data.len(), compressed_duration);
    println!();
    
    // 验证反序列化
    let _restored_json = json_serializer.deserialize(&json_data).await?;
    let _restored_counting = counting_serializer.deserialize(&counting_data).await?;
    let _restored_compressed = compressed_serializer.deserialize(&compressed_data).await?;
    
    let (serialize_count, deserialize_count) = counting_serializer.get_counts();
    println!("  计数序列化器统计: 序列化{}次, 反序列化{}次", serialize_count, deserialize_count);
    println!();

    // 5. 工厂支持演示
    println!("5. 工厂模式支持");
    
    let factory = ConnectionFactory::new();
    
    // 使用构建器创建连接（通过工厂）
    let _config_connection = factory.create_client_from_builder(
        ConnectionBuilder::client("factory_client".to_string(), "ws://localhost:8080".to_string())
            .with_json_serialization()
    ).await;
    
    let _custom_connection = factory.create_client_from_builder(
        ConnectionBuilder::client("factory_custom".to_string(), "ws://localhost:8080".to_string())
            .with_custom_serializer(CountingSerializer::new())
    ).await;
    
    println!("  - 工厂支持配置驱动的连接创建");
    println!("  - 工厂支持自定义序列化器的连接创建");
    println!("  - 保持统一的创建接口");
    println!();

    // 6. 最优方案建议
    println!("6. 最优方案建议");
    println!();
    println!("  🎯 推荐使用混合方案:");
    println!("     ✓ 默认情况：使用配置驱动方式（简单、类型安全）");
    println!("     ✓ 特殊需求：使用直接传入方式（灵活、可扩展）");
    println!("     ✓ 预设场景：使用构建器预设（便捷、优化过的）");
    println!();
    println!("  📊 选择指南:");
    println!("     - 标准应用场景 → 配置驱动 (with_json_serialization)");
    println!("     - 性能优化场景 → 预设配置 (ultra_low_latency)");
    println!("     - 特殊序列化需求 → 自定义序列化器 (with_serializer)");
    println!("     - 调试开发场景 → 预设配置 (debug_friendly)");
    println!();
    println!("  🔧 实现建议:");
    println!("     - 保持现有配置方式的兼容性");
    println!("     - 添加构建器模式提供更多灵活性");
    println!("     - 预设常用场景配置");
    println!("     - 支持运行时序列化器切换");

    println!("\n=== 灵活序列化器演示完成 ===");
    Ok(())
}