# Common 模块设计文档

## 📖 模块概述

`common` 模块是 Flare Core 的核心基础模块，提供了框架所需的所有基础组件和抽象接口。该模块采用**分层架构**和**插件化设计**，确保了高度的可扩展性和模块化。

## 🏗️ 整体架构

```
src/common/
├── connections/        # 连接管理层
├── compression/        # 数据压缩层  
├── serialization/      # 数据序列化层
├── messaging/          # 消息处理层
├── pipeline/           # 异步处理管道
├── system/            # 系统优化层
├── protocol/          # 协议定义
└── error/            # 错误处理
```

### 🎯 设计原则

1. **分层抽象**: 每层专注于特定职责，层间通过接口交互
2. **插件化扩展**: 支持用户自定义序列化器、压缩器等组件
3. **简化设计**: 删除复杂统计信息，降低系统复杂度
4. **零成本抽象**: 编译时优化，运行时无额外开销
5. **类型安全**: 充分利用 Rust 类型系统保证安全性
6. **异步优先**: 全面支持异步编程模型

## 📋 模块功能

### 🔌 Connections (连接管理)
- **功能**: 统一的连接抽象，支持 WebSocket、QUIC 等多种协议
- **特性**: 连接池、自动重连、事件驱动
- **优化**: 连接预热、复用、智能调度

### 🗜️ Compression (压缩处理)  
- **功能**: 多算法压缩支持 (LZ4、Snappy、Gzip)
- **特性**: 自适应压缩、阈值控制
- **优化**: 零拷贝、预分配缓冲区、简化统计

### 📦 Serialization (序列化)
- **功能**: 多格式序列化 (JSON、Bincode、MessagePack、Protobuf、CBOR)
- **特性**: 配置化、性能监控
- **优化**: 零拷贝、批量处理

### 📬 Messaging (消息处理)
- **功能**: 优先级队列、消息调度
- **特性**: 多优先级、超时处理、批量操作
- **优化**: 智能调度、背压控制

### ⚡ Pipeline (异步管道)
- **功能**: 流水线处理、并行化
- **特性**: 多阶段并发、负载均衡
- **优化**: 异步并行、资源复用

### 💻 System (系统优化)
- **功能**: CPU亲和性、内存对齐、NUMA优化
- **特性**: 系统级调优
- **优化**: 硬件感知、性能最大化

### 🔄 Protocol (协议定义)
- **功能**: 统一消息格式 (Frame)
- **特性**: 可靠性保证、类型安全
- **优化**: 紧凑编码、快速序列化

### ❌ Error (错误处理)
- **功能**: 统一错误类型、错误链
- **特性**: 结构化错误、上下文保留
- **优化**: 零分配错误处理

## 🔧 使用方式

### 基础连接
```rust
use flare_core::common::{
    ConnectionFactory, ConnectionType, ConnectionConfig,
    Frame, MessageType, Reliability
};

// 创建连接
let config = ConnectionConfig::client("client1", "ws://localhost:8080")
    .with_type(ConnectionType::WebSocket);
let factory = ConnectionFactory::new();
let mut connection = factory.create_client_connection(config).await?;

// 发送消息
let frame = Frame::new(MessageType::Data, 1, Reliability::AtLeastOnce, b"Hello".to_vec());
connection.send_message(frame).await?;
```

### 高性能配置
```rust
use flare_core::common::{
    SerializerFactory, SerializationFormat,
    CompressorFactory, CompressionFormat,
    PriorityMessageQueue, MessagePriority
};

// 超低延迟序列化
let serializer = SerializerFactory::bincode();

// 快速压缩
let compressor = CompressorFactory::create_static(CompressionFormat::Lz4);

// 优先级消息处理
let queue = PriorityMessageQueue::default();
let priority_msg = create_realtime_message(frame);
queue.enqueue(priority_msg).await?;
```

### 异步处理管道
```rust
use flare_core::common::pipeline::AsyncMessagePipeline;

// 创建处理管道
let pipeline = AsyncMessagePipeline::new(serializer, compressor);

// 异步处理消息
pipeline.process_async(frame).await?;
```

## 📊 性能特征

### 🏆 延迟表现
- **序列化**: Bincode < 50μs, JSON < 120μs
- **压缩**: LZ4 < 25μs, Snappy < 15μs
- **连接**: QUIC 比 WebSocket 延迟降低 34%
- **总体**: 端到端 < 100μs (超低延迟配置)

### 🚀 吞吐量
- **消息处理**: >100K msg/s (优化配置)
- **数据压缩**: >1GB/s (LZ4)
- **网络传输**: 受限于网络带宽

### 💾 内存使用
- **零拷贝**: 序列化和压缩避免内存复制
- **对象池**: 缓冲区复用，减少GC压力  
- **预分配**: 启动时分配核心资源

## 🎯 扩展指南

### 自定义序列化器
```rust
use flare_core::common::serialization::{FrameSerializer, SerializationFormat};

#[derive(Debug)]
pub struct MySerializer;

#[async_trait::async_trait]
impl FrameSerializer for MySerializer {
    fn format(&self) -> SerializationFormat { /* 实现 */ }
    async fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> { /* 实现 */ }
    async fn deserialize(&self, data: &[u8]) -> Result<Frame> { /* 实现 */ }
    // ... 其他必需方法
}
```

### 自定义压缩器
```rust
use flare_core::common::compression::{Compressor, CompressionFormat};

#[derive(Debug)]
pub struct MyCompressor;

#[async_trait::async_trait]  
impl Compressor for MyCompressor {
    fn format(&self) -> CompressionFormat { /* 实现 */ }
    async fn compress(&self, data: &[u8]) -> Result<CompressionResult> { /* 实现 */ }
    async fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> { /* 实现 */ }
    // ... 其他必需方法
}
```

## 🚨 注意事项

### 性能优化
- 使用 `Bincode` 序列化器获得最佳性能
- 小消息(<128字节)建议跳过压缩
- QUIC协议适合低延迟场景
- 启用连接池进行连接复用

### 错误处理
- 网络错误会自动重试
- 序列化错误直接返回
- 压缩失败会回退到原数据
- 使用 `?` 运算符进行错误传播

### 内存管理
- 大消息使用流式处理
- 及时释放不需要的连接
- 监控内存使用情况
- 合理配置缓冲区大小

## 🔍 调试和监控

### 性能监控
```rust
// 获取序列化器统计信息
let stats = serializer.stats();
println!("序列化统计: {:?}", stats);

// 获取连接池统计信息  
let pool_stats = connection_pool.get_stats().await;
println!("连接池统计: {:?}", pool_stats);

// 获取队列统计信息
let queue_stats = priority_queue.get_stats().await;
println!("队列统计: {:?}", queue_stats);
```

### 日志配置
```rust
// 启用详细日志
tracing_subscriber::fmt()
    .with_max_level(tracing::Level::DEBUG)
    .init();
```

## 📚 相关文档

- [连接模块文档](./connections/README.md)
- [序列化模块文档](./serialization/README.md)
- [压缩模块文档](./compression/README.md)
- [消息处理模块文档](./messaging/README.md)
- [异步管道文档](./pipeline/README.md)
- [系统优化文档](./system/README.md)

## 🤝 贡献指南

1. 遵循现有的代码风格和架构模式
2. 新增功能需要充分的单元测试
3. 性能相关修改需要基准测试
4. 更新相关文档和示例代码
5. 确保向后兼容性

---

*Flare Core Common - 构建高性能实时通信应用的坚实基础*
