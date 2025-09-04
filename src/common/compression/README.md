# Compression 模块文档

## 📖 模块概述

压缩模块提供高性能的数据压缩功能，支持多种压缩算法，专为**超低延迟**场景优化。模块采用统一接口设计，支持用户自定义压缩器扩展。

## 🎯 设计目标

- **超低延迟**: 所有压缩操作 < 15ms，最佳配置 < 1ms
- **高压缩比**: 典型数据压缩至原大小的 20-50%
- **零拷贝**: 最小化内存分配和复制开销
- **可扩展性**: 支持用户自定义压缩算法
- **智能优化**: 根据数据特征自动选择策略

## 🏗️ 架构设计

```
compression/
├── traits.rs          # 核心接口定义
├── lz4.rs            # LZ4 超快压缩器
├── snappy.rs         # Snappy 平衡压缩器
├── gzip.rs           # Gzip 高压缩比压缩器
├── factory.rs        # 压缩器工厂
└── mod.rs            # 模块导出
```

### 🔧 核心接口

```rust
#[async_trait]
pub trait Compressor: Send + Sync {
    /// 获取压缩格式
    fn format(&self) -> CompressionFormat;
    
    /// 压缩数据
    async fn compress(&self, data: &[u8]) -> Result<CompressionResult>;
    
    /// 解压数据  
    async fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>;
    
    /// 检查是否值得压缩
    fn should_compress(&self, data: &[u8]) -> bool;
}
```

## 🚀 支持的压缩算法

### 1️⃣ LZ4 - 超低延迟之王 👑
```rust
let lz4 = Lz4Compressor::new();
```
- **性能**: 压缩 < 25μs, 解压 < 10μs
- **压缩比**: 通常 40-60%
- **场景**: 游戏、交易、实时应用
- **库**: `lz4_flex` 高性能实现

### 2️⃣ Snappy - 平衡性能之选 ⚖️
```rust
let snappy = SnappyCompressor::new();
```
- **性能**: 压缩 < 15μs, 解压 < 5μs  
- **压缩比**: 通常 20-50%
- **场景**: 实时通信、流媒体
- **库**: Google `snap` 官方库

### 3️⃣ Gzip - 高压缩比专家 🗜️
```rust
let gzip = GzipCompressor::new();
```
- **性能**: 压缩 < 2ms, 解压 < 200μs
- **压缩比**: 通常 15-30% (最佳)
- **场景**: 存储、备份、批量传输
- **库**: `flate2` 标准库

## 📊 性能基准

基于实际测试数据（1KB消息）：

| 压缩器 | 压缩时间 | 解压时间 | 压缩比 | 总耗时 | 推荐场景 |
|-------|---------|---------|--------|--------|---------|
| **Snappy** | 12μs | 5μs | 20.3% | **17μs** | 🏆 通用最佳 |
| **LZ4** | 23μs | 8μs | 17.7% | **31μs** | ⚡ 超低延迟 |
| **Gzip** | 655μs | 150μs | 18.6% | **805μs** | 💾 高压缩比 |

## 🔧 使用方式

### 基础使用
```rust
use flare_core::common::compression::{
    CompressorFactory, CompressionFormat
};

// 创建压缩器
let compressor = CompressorFactory::create_static(CompressionFormat::Snappy);

// 压缩数据
let result = compressor.compress(&data).await?;
println!("压缩: {}B -> {}B (节省 {}B)", 
         result.original_size, 
         result.compressed_size,
         result.bytes_saved());

// 解压数据
let decompressed = compressor.decompress(&result.data).await?;
```

### 场景化使用
```rust
// 超低延迟场景
let ultra_fast = CompressorFactory::recommended_static("ultra_low_latency");

// 实时通信场景
let balanced = CompressorFactory::recommended_static("real_time");

// 存储场景
let high_compression = CompressorFactory::recommended_static("storage");
```

### 配置化使用
```rust
use flare_core::common::compression::CompressionConfig;

// 超低延迟配置
let config = CompressionConfig::ultra_low_latency();
let compressor = Lz4Compressor::with_config(config);

// 高压缩比配置
let config = CompressionConfig::high_compression();
let compressor = GzipCompressor::with_config(config);
```

## ⚙️ 配置参数

### CompressionConfig
```rust
pub struct CompressionConfig {
    /// 最小压缩阈值 (默认: 128字节)
    pub min_compress_size: usize,
    
    /// 最大数据大小限制
    pub max_compress_size: Option<usize>,
    
    /// 压缩级别 1-9 (默认: 1)
    pub compression_level: u8,
    
    /// 启用字典压缩
    pub enable_dictionary: bool,
    
    /// 压缩超时 (默认: 5ms)
    pub timeout_ms: u64,
}
```

### 预设配置
```rust
// 超低延迟 - 优先速度
CompressionConfig::ultra_low_latency()  // 2ms超时, 级别1

// 平衡模式 - 速度与压缩比平衡
CompressionConfig::balanced()           // 10ms超时, 级别3

// 高压缩比 - 优先压缩效果
CompressionConfig::high_compression()   // 50ms超时, 级别6
```

## 🎯 优化策略

### 1. 智能阈值
```rust
// 小数据不压缩，避免负优化
if data.len() < config.min_compress_size {
    return original_data;
}
```

### 2. 压缩效果检查
```rust
// 压缩效果差时返回原数据
if compressed_size >= original_size {
    return original_data;
}
```

### 3. 超时保护
```rust
// 防止压缩时间过长
tokio::time::timeout(timeout, compress_operation).await?
```

## 🔍 扩展指南

### 自定义压缩器
```rust
use flare_core::common::compression::{Compressor, CompressionFormat, CompressionResult};

#[derive(Debug)]
pub struct MyCompressor {
    config: CompressionConfig,
}

#[async_trait::async_trait]
impl Compressor for MyCompressor {
    fn format(&self) -> CompressionFormat {
        CompressionFormat::None // 或自定义格式
    }
    
    async fn compress(&self, data: &[u8]) -> Result<CompressionResult> {
        // 实现压缩逻辑
        let compressed = my_compress_algorithm(data)?;
        
        Ok(CompressionResult {
            data: compressed,
            original_size: data.len(),
            compressed_size: compressed.len(),
            was_compressed: compressed.len() < data.len(),
        })
    }
    
    async fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        // 实现解压逻辑
        my_decompress_algorithm(data)
    }
    
    // 实现其他必需方法...
}
```

### 注册自定义压缩器
```rust
// 全局注册
CompressorFactory::register_global(
    CompressionFormat::None, // 或自定义格式
    || Box::new(MyCompressor::new())
);

// 使用自定义压缩器
let compressor = CompressorFactory::create_static(CompressionFormat::None);
```

## 📈 性能优化建议

### 🏆 最佳实践
1. **场景选择**:
   - 游戏/交易 → LZ4
   - Web应用 → Snappy  
   - 存储/备份 → Gzip

2. **阈值调优**:
   - 小消息 (< 256B) 跳过压缩
   - 大消息启用流式处理
   - 监控压缩效果调整阈值

3. **配置优化**:
   - 延迟敏感 → `ultra_low_latency()`
   - 带宽受限 → `high_compression()`
   - 一般场景 → `balanced()`

### ⚡ 极致优化
```rust
// 零拷贝 + 预分配
let mut buffer = BufferPool::acquire(estimated_size);
compressor.compress_to_buffer(data, &mut buffer).await?;

// 批量处理
let results = compressor.compress_batch(&data_vec).await?;

// 异步并行
let handles: Vec<_> = data_chunks.into_iter()
    .map(|chunk| compressor.compress(chunk))
    .collect();
let results = futures::future::join_all(handles).await;
```

## 🚨 注意事项

### 内存管理
- 大数据压缩可能消耗大量内存
- 使用合适的`max_compress_size`限制
- 监控内存使用情况

### 错误处理
```rust
match compressor.compress(&data).await {
    Ok(result) => {
        // 处理压缩成功
    }
    Err(FlareError::Timeout(_)) => {
        // 压缩超时，使用原数据
        return Ok(data.to_vec());
    }
    Err(e) => {
        // 其他错误
        return Err(e);
    }
}
```

### 线程安全
- 所有压缩器都实现了 `Send + Sync`
- 支持多线程并发使用
- 内部状态通过 `Arc<RwLock<>>` 保护

## 🧪 测试验证

### 单元测试
```bash
cargo test compression
```

### 基准测试  
```bash
cargo run --example compression_demo
```

### 性能测试
```bash
cargo run --example ultra_low_latency_demo
```

## 📚 相关资源

- [LZ4 算法文档](https://github.com/lz4/lz4)
- [Snappy 算法文档](https://github.com/google/snappy)
- [Gzip 规范](https://tools.ietf.org/html/rfc1952)
- [性能测试报告](../../COMPRESSION_IMPLEMENTATION_SUMMARY.md)

---

*高性能压缩 - 为超低延迟而生*