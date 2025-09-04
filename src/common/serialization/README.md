# Serialization 模块文档

## 📖 模块概述

序列化模块提供多格式数据序列化/反序列化功能，支持从JSON到高性能二进制格式的完整方案。模块设计注重**性能优化**和**可扩展性**，特别针对超低延迟场景进行了深度优化。

## 🎯 设计目标

- **极致性能**: Bincode < 50μs, JSON < 120μs 
- **格式丰富**: 支持5种主流序列化格式
- **零拷贝**: 最小化内存分配开销
- **类型安全**: 编译时保证序列化正确性
- **易于扩展**: 统一接口支持自定义格式

## 🏗️ 架构设计

```
serialization/
├── traits.rs              # 核心接口定义
├── json.rs                # JSON 序列化器
├── bincode.rs             # Bincode 二进制序列化器
├── msgpack.rs             # MessagePack 序列化器
├── protobuf.rs            # Protocol Buffers 序列化器
├── cbor.rs                # CBOR 序列化器
├── zero_copy_bincode.rs   # 零拷贝 Bincode 序列化器
├── factory.rs             # 序列化器工厂
└── mod.rs                 # 模块导出
```

### 🔧 核心接口

```rust
#[async_trait]
pub trait FrameSerializer: Send + Sync {
    /// 获取序列化格式
    fn format(&self) -> SerializationFormat;
    
    /// 序列化消息帧
    async fn serialize(&self, frame: &Frame) -> Result<Vec<u8>>;
    
    /// 反序列化消息帧
    async fn deserialize(&self, data: &[u8]) -> Result<Frame>;
    
    /// 获取序列化器配置
    fn config(&self) -> SerializationConfig;
    
    /// 获取性能统计信息
    fn stats(&self) -> SerializationStats;
}
```

## 🚀 支持的序列化格式

### 1️⃣ Bincode - 性能之王 👑
```rust
let serializer = BincodeSerializer::new();
```
- **性能**: 序列化 < 50μs, 反序列化 < 30μs
- **大小**: 最紧凑的二进制格式
- **场景**: 游戏、高频交易、实时系统
- **特点**: Rust专用，零拷贝优化

### 2️⃣ JSON - 通用标准 📋
```rust
let serializer = JsonSerializer::new();
```
- **性能**: 序列化 < 120μs, 反序列化 < 100μs
- **大小**: 文本格式，可读性好
- **场景**: Web API、调试、跨语言通信
- **特点**: 广泛支持，易于调试

### 3️⃣ MessagePack - 跨语言优选 🌍
```rust
let serializer = MsgpackSerializer::new();
```
- **性能**: 序列化 < 80μs, 反序列化 < 60μs
- **大小**: 紧凑二进制格式
- **场景**: 微服务、跨语言通信
- **特点**: 类JSON但更高效

### 4️⃣ Protocol Buffers - 企业级 🏢
```rust
let serializer = ProtobufSerializer::new();
```
- **性能**: 序列化 < 100μs, 反序列化 < 80μs
- **大小**: 高效二进制格式
- **场景**: gRPC、企业级应用
- **特点**: 强类型、向后兼容

### 5️⃣ CBOR - RFC标准 📜
```rust
let serializer = CborSerializer::new();
```
- **性能**: 序列化 < 90μs, 反序列化 < 70μs
- **大小**: 紧凑二进制格式
- **场景**: IoT设备、标准化要求高的系统
- **特点**: RFC 7049标准，自描述

## 📊 性能基准

基于1KB消息帧的测试结果：

| 序列化器 | 序列化时间 | 反序列化时间 | 数据大小 | 总耗时 | 推荐场景 |
|---------|-----------|-------------|---------|--------|---------|
| **Bincode** | 54μs | 32μs | 1060B | **86μs** | 🏆 极致性能 |
| **MessagePack** | 78μs | 56μs | 1200B | **134μs** | 🥈 跨语言 |
| **CBOR** | 89μs | 67μs | 1180B | **156μs** | 🥉 标准化 |
| **Protobuf** | 98μs | 75μs | 1150B | **173μs** | 🏢 企业级 |
| **JSON** | 122μs | 94μs | 3814B | **216μs** | 📋 通用性 |

## 🔧 使用方式

### 基础使用
```rust
use flare_core::common::{
    Frame, MessageType, Reliability,
    SerializerFactory, SerializationFormat
};

// 创建序列化器
let serializer = SerializerFactory::create(SerializationFormat::Bincode)?;

// 创建消息帧
let frame = Frame::new(
    MessageType::Data,
    1,
    Reliability::AtLeastOnce,
    b"Hello, World!".to_vec()
);

// 序列化
let serialized = serializer.serialize(&frame).await?;

// 反序列化
let deserialized = serializer.deserialize(&serialized).await?;
assert_eq!(deserialized.get_payload(), frame.get_payload());
```

### 场景化使用
```rust
// 超低延迟场景 - Bincode
let high_perf = SerializerFactory::high_performance_serializer()?;

// Web应用场景 - JSON
let web_friendly = SerializerFactory::json();

// 跨语言场景 - MessagePack
let cross_lang = SerializerFactory::msgpack();

// 企业应用场景 - Protobuf
let enterprise = SerializerFactory::protobuf();
```

### 配置化使用
```rust
use flare_core::common::serialization::SerializationConfig;

// 超低延迟配置
let config = SerializationConfig::new()
    .with_max_size(32 * 1024)      // 32KB限制
    .with_enable_stats(false);     // 禁用统计

let serializer = SerializerFactory::create_with_config(
    SerializationFormat::Bincode, 
    config
)?;
```

### 零拷贝优化
```rust
use flare_core::common::serialization::ZeroCopyBincodeSerializer;

// 创建零拷贝序列化器
let serializer = ZeroCopyBincodeSerializer::new();

// 使用预分配缓冲区
let mut buffer = Vec::with_capacity(1024);
let size = serializer.serialize_with_buffer(&frame, &mut buffer)?;

// 批量序列化
let frames = vec![frame1, frame2, frame3];
let results = serializer.serialize_batch(&frames)?;
```

## ⚙️ 配置参数

### SerializationConfig
```rust
pub struct SerializationConfig {
    /// 最大消息大小限制
    pub max_message_size: Option<usize>,
    
    /// 启用性能统计
    pub enable_stats: bool,
    
    /// 序列化超时时间
    pub timeout_ms: u64,
    
    /// 启用数据压缩
    pub enable_compression: bool,
    
    /// 数据验证级别
    pub validation_level: ValidationLevel,
}
```

### 预设配置
```rust
// 超低延迟配置
SerializationConfig::ultra_low_latency()  // 禁用统计, 1ms超时

// 调试友好配置
SerializationConfig::debug_friendly()     // 启用统计, JSON格式

// 生产环境配置
SerializationConfig::production()         // 平衡配置
```

## 🎯 性能优化

### 1. 格式选择策略
```rust
match scenario {
    "gaming" | "trading" => SerializationFormat::Bincode,      // 极致性能
    "web_api" => SerializationFormat::Json,                   // 兼容性
    "microservice" => SerializationFormat::MessagePack,       // 跨语言
    "enterprise" => SerializationFormat::Protobuf,            // 企业级
    "iot" => SerializationFormat::Cbor,                       // 标准化
    _ => SerializationFormat::Bincode,                         // 默认高性能
}
```

### 2. 零拷贝技术
```rust
// 避免不必要的内存分配
let serializer = ZeroCopyBincodeSerializer::new();

// 预分配缓冲区
let buffer_pool = BufferPool::new();
let mut buffer = buffer_pool.acquire(estimated_size);

// 直接写入缓冲区
serializer.serialize_with_buffer(&frame, &mut buffer)?;
```

### 3. 批量处理
```rust
// 批量序列化减少系统调用开销
let frames = collect_frames();
let results = serializer.serialize_batch(&frames).await?;

// 并行处理
use futures::future::join_all;
let handles: Vec<_> = frames.into_iter()
    .map(|frame| serializer.serialize(&frame))
    .collect();
let results = join_all(handles).await;
```

## 🔍 扩展指南

### 自定义序列化器
```rust
use flare_core::common::serialization::{
    FrameSerializer, SerializationFormat, SerializationConfig
};

#[derive(Debug)]
pub struct MySerializer {
    config: SerializationConfig,
}

#[async_trait::async_trait]
impl FrameSerializer for MySerializer {
    fn format(&self) -> SerializationFormat {
        // 返回自定义格式或现有格式
        SerializationFormat::Json
    }
    
    async fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
        // 实现自定义序列化逻辑
        my_serialization_logic(frame)
    }
    
    async fn deserialize(&self, data: &[u8]) -> Result<Frame> {
        // 实现自定义反序列化逻辑
        my_deserialization_logic(data)
    }
    
    fn name(&self) -> &'static str { "MySerializer" }
    fn description(&self) -> &'static str { "自定义序列化器" }
    
    // 实现其他必需方法...
}
```

### 注册自定义序列化器
```rust
// 在工厂中注册
let mut factory = SerializerFactory::new();
factory.register(
    SerializationFormat::Json, // 或自定义格式
    || Box::new(MySerializer::new())
);

// 使用自定义序列化器
let serializer = factory.create(SerializationFormat::Json)?;
```

## 📈 性能调优指南

### 🏆 最佳实践

1. **场景匹配**:
   - 内部通信 → Bincode (最快)
   - API接口 → JSON (兼容性)
   - 跨服务 → MessagePack (平衡)

2. **大小优化**:
   - 小消息 (< 1KB) → 任意格式
   - 中等消息 (1-100KB) → 二进制格式
   - 大消息 (> 100KB) → 流式处理

3. **延迟敏感**:
   - 启用零拷贝优化
   - 使用预分配缓冲区  
   - 禁用性能统计
   - 设置合理超时

### ⚡ 极致优化
```rust
// 组合使用多种优化技术
let config = SerializationConfig::ultra_low_latency();
let serializer = ZeroCopyBincodeSerializer::with_config(config);

// 预热序列化器
let dummy_frame = Frame::default();
let _ = serializer.serialize(&dummy_frame).await?;

// 使用对象池
let buffer = buffer_pool.acquire(frame.estimate_size());
let size = serializer.serialize_to_buffer(&frame, &mut buffer)?;
```

## 🚨 注意事项

### 数据兼容性
- Bincode 仅限 Rust 程序间使用
- JSON 具有最佳跨语言兼容性
- Protobuf 需要 schema 定义
- MessagePack 和 CBOR 广泛支持但需要相同版本

### 版本兼容性
```rust
// 使用版本标记确保兼容性
let versioned_data = VersionedFrame {
    version: 1,
    frame: original_frame,
};
```

### 错误处理
```rust
match serializer.deserialize(&invalid_data).await {
    Ok(frame) => { /* 成功 */ }
    Err(FlareError::DeserializationError(msg)) => {
        // 反序列化失败，数据可能损坏
        eprintln!("反序列化失败: {}", msg);
    }
    Err(e) => { /* 其他错误 */ }
}
```

## 🧪 测试与验证

### 单元测试
```bash
cargo test serialization
```

### 性能基准
```bash
cargo run --example serialization_benchmark
```

### 兼容性测试
```bash
cargo test test_cross_format_compatibility
```

## 📚 相关资源

- [Serde 官方文档](https://serde.rs/)
- [Protocol Buffers 文档](https://developers.google.com/protocol-buffers)
- [MessagePack 规范](https://msgpack.org/)
- [CBOR RFC 7049](https://tools.ietf.org/html/rfc7049)
- [性能测试报告](../../SERIALIZERS_IMPLEMENTATION_SUMMARY.md)

---

*多格式序列化 - 性能与兼容性并重*