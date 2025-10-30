# 压缩模块使用指南

## 概述

`compression` 模块提供灵活的消息压缩功能，支持：
- ✅ **内置算法**: LZ4、Snappy、Gzip、Zlib
- ✅ **可配置**: 压缩级别、最小阈值
- ✅ **可扩展**: 支持自定义压缩算法
- ✅ **高性能**: 优化的压缩和解压性能

## 快速开始

### 基本使用

```rust
use flare_core::common::compression::{compress, decompress, CompressionConfig, CompressionAlgorithm};

// 创建配置
let config = CompressionConfig::new(CompressionAlgorithm::Lz4);

// 压缩数据
let data = b"Hello, World!";
let compressed = compress(data, &config).unwrap();

// 解压数据
let decompressed = decompress(&compressed, &config).unwrap();
assert_eq!(decompressed, data);
```

## 内置压缩算法

### 1. LZ4 - 速度优先

**特点**:
- ⚡ 最快的压缩和解压速度
- 📦 中等压缩率
- 🎯 适合实时通信场景

**使用**:
```rust
let config = CompressionConfig::new(CompressionAlgorithm::Lz4);
```

**性能**（100KB数据）:
- 压缩时间: ~0.67ms
- 解压时间: ~0.34ms
- 压缩率: 0.4% (高度重复数据)

### 2. Snappy - 平衡选择

**特点**:
- ⚡ 快速的压缩和解压
- 📦 良好的压缩率
- 🎯 Google 开发，稳定可靠

**使用**:
```rust
let config = CompressionConfig::new(CompressionAlgorithm::Snappy);
```

**性能**（100KB数据）:
- 压缩时间: ~0.46ms
- 解压时间: ~0.18ms
- 压缩率: 4.7%

### 3. Gzip - 压缩率优先

**特点**:
- 📦 高压缩率
- ⏱️ 较慢的压缩速度
- 🌐 广泛兼容性

**使用**:
```rust
let config = CompressionConfig::new(CompressionAlgorithm::Gzip);
```

**性能**（100KB数据）:
- 压缩时间: ~2.63ms
- 解压时间: ~0.52ms
- 压缩率: 0.1% (最佳)

### 4. Zlib - 兼容性好

**特点**:
- 📦 高压缩率
- 🌐 最广泛的兼容性
- ⚙️ 可调压缩级别

**使用**:
```rust
let config = CompressionConfig::new(CompressionAlgorithm::Zlib);
```

## 高级配置

### 压缩级别

```rust
use flare_core::common::compression::CompressionLevel;

// 最快速度（最低压缩率）
let config = CompressionConfig::new(CompressionAlgorithm::Gzip)
    .with_level(CompressionLevel::Fastest);

// 最佳压缩（最高压缩率）
let config = CompressionConfig::new(CompressionAlgorithm::Gzip)
    .with_level(CompressionLevel::Best);

// 自定义级别 (0-9)
let config = CompressionConfig::new(CompressionAlgorithm::Gzip)
    .with_level(CompressionLevel::Custom(5));
```

**压缩级别对比**（580字节数据，Gzip）:

| 级别 | 压缩后大小 | 压缩率 | 耗时 |
|------|-----------|--------|------|
| 最快 | 68 字节 | 11.7% | 1.43ms |
| 快速 | 64 字节 | 11.0% | 1.23ms |
| 默认 | 64 字节 | 11.0% | 1.25ms |
| 最佳 | 64 字节 | 11.0% | 1.24ms |

### 最小压缩阈值

```rust
// 只压缩大于 256 字节的数据
let config = CompressionConfig::new(CompressionAlgorithm::Lz4)
    .with_min_size(256);

let small_data = b"Small";
let large_data = "Large ".repeat(50);

// 小数据不压缩
if !config.should_compress(small_data.len()) {
    println!("数据太小，不压缩");
}

// 大数据压缩
if config.should_compress(large_data.len()) {
    let compressed = compress(large_data.as_bytes(), &config).unwrap();
}
```

### Builder 模式

```rust
let config = CompressionConfig::new(CompressionAlgorithm::Gzip)
    .with_level(CompressionLevel::Best)
    .with_min_size(512);

println!("算法: {:?}", config.algorithm);
println!("级别: {:?}", config.level);
println!("最小阈值: {} 字节", config.min_size);
```

## 自定义压缩器

### 实现 Compressor trait

```rust
use flare_core::common::compression::Compressor;
use flare_core::common::error::FlareError;

struct MyCompressor;

impl Compressor for MyCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError> {
        // 实现压缩逻辑
        Ok(data.to_vec())
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError> {
        // 实现解压逻辑
        Ok(data.to_vec())
    }

    fn name(&self) -> &str {
        "my-compressor"
    }

    fn algorithm_id(&self) -> u32 {
        1000 // 自定义 ID >= 1000
    }
}
```

### 注册自定义压缩器

```rust
use flare_core::common::compression::CustomCompressorRegistry;
use std::sync::Arc;

let mut registry = CustomCompressorRegistry::new();

// 注册自定义压缩器
let compressor = Arc::new(MyCompressor);
registry.register(1000, compressor).unwrap();

// 获取压缩器
let compressor = registry.get(1000).unwrap();
let compressed = compressor.compress(b"test data").unwrap();
```

### 自定义压缩器示例：XOR

```rust
struct XorCompressor {
    key: u8,
}

impl Compressor for XorCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError> {
        Ok(data.iter().map(|b| b ^ self.key).collect())
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError> {
        Ok(data.iter().map(|b| b ^ self.key).collect())
    }

    fn name(&self) -> &str {
        "xor"
    }

    fn algorithm_id(&self) -> u32 {
        1000
    }
}

// 使用
let compressor = XorCompressor { key: 0x42 };
let compressed = compressor.compress(b"Hello").unwrap();
let decompressed = compressor.decompress(&compressed).unwrap();
```

## 性能对比

### 不同数据大小的性能

#### 100 字节
| 算法 | 压缩率 | 压缩时间 | 解压时间 |
|------|--------|----------|----------|
| LZ4 | 16.0% | 14.13µs | 3.29µs |
| Snappy | 9.0% | 6.08µs | 2.50µs |
| Gzip | 33.0% | 1.21ms | 86.67µs |

#### 1 KB
| 算法 | 压缩率 | 压缩时间 | 解压时间 |
|------|--------|----------|----------|
| LZ4 | 1.9% | 17.75µs | 8.42µs |
| Snappy | 5.2% | 15.25µs | 5.33µs |
| Gzip | 3.5% | 1.22ms | 92.92µs |

#### 10 KB
| 算法 | 压缩率 | 压缩时间 | 解压时间 |
|------|--------|----------|----------|
| LZ4 | 0.5% | 145.67µs | 71.71µs |
| Snappy | 4.8% | 179.75µs | 36.13µs |
| Gzip | 0.4% | 899.00µs | 107.17µs |

#### 100 KB
| 算法 | 压缩率 | 压缩时间 | 解压时间 |
|------|--------|----------|----------|
| LZ4 | 0.4% | 674.58µs | 343.88µs |
| Snappy | 4.7% | 455.71µs | 175.83µs |
| Gzip | 0.1% | 2.63ms | 516.25µs |

### 选择建议

| 场景 | 推荐算法 | 原因 |
|------|---------|------|
| 实时通信 | LZ4 或 Snappy | 低延迟，快速压缩/解压 |
| 大文件传输 | Gzip | 节省带宽，高压缩率 |
| 日志压缩 | Zlib | 兼容性好，压缩率高 |
| 短消息 | 无压缩 | 压缩开销大于收益 |

## 与 MessageParser 集成

### 在消息解析中使用压缩

```rust
use flare_core::common::parsing::MessageParser;
use flare_core::common::compression::{compress, CompressionConfig, CompressionAlgorithm};

// 创建解析器和压缩配置
let parser = MessageParser::new(PayloadCodec::Json);
let compression = CompressionConfig::new(CompressionAlgorithm::Lz4);

// 编码并压缩
let frame = parser.build_frame(&my_message, "msg-1".to_string()).await?;
let bytes = parser.encode_frame(&frame).await?;
let compressed = compress(&bytes, &compression)?;

// 解压并解析
let decompressed = decompress(&compressed, &compression)?;
let frame = parser.parse_bytes(&decompressed).await?;
```

## 最佳实践

### 1. 选择合适的算法

```rust
// 根据场景选择
let config = match use_case {
    UseCase::RealTime => CompressionConfig::new(CompressionAlgorithm::Lz4),
    UseCase::Storage => CompressionConfig::new(CompressionAlgorithm::Gzip)
        .with_level(CompressionLevel::Best),
    UseCase::Balanced => CompressionConfig::new(CompressionAlgorithm::Snappy),
};
```

### 2. 设置合理的阈值

```rust
// 避免压缩小数据
let config = CompressionConfig::new(CompressionAlgorithm::Lz4)
    .with_min_size(128); // 至少 128 字节才压缩

if config.should_compress(data.len()) {
    compressed_data = compress(data, &config)?;
} else {
    // 直接发送原始数据
    send_raw(data)?;
}
```

### 3. 预估压缩效果

```rust
// 测试压缩效果
let sample_data = get_sample_data();
let compressed = compress(&sample_data, &config)?;
let ratio = compressed.len() as f64 / sample_data.len() as f64;

if ratio > 0.9 {
    println!("压缩效果不明显，考虑不使用压缩");
}
```

### 4. 错误处理

```rust
match compress(data, &config) {
    Ok(compressed) => send_compressed(compressed),
    Err(e) => {
        eprintln!("压缩失败: {:?}", e);
        // 降级：发送未压缩数据
        send_raw(data)?;
    }
}
```

## API 参考

### 核心函数

```rust
// 压缩数据
pub fn compress(data: &[u8], config: &CompressionConfig) -> Result<Vec<u8>, FlareError>

// 解压数据
pub fn decompress(data: &[u8], config: &CompressionConfig) -> Result<Vec<u8>, FlareError>
```

### CompressionConfig

```rust
// 创建配置
pub fn new(algorithm: CompressionAlgorithm) -> Self
pub fn none() -> Self

// 链式配置
pub fn with_level(self, level: CompressionLevel) -> Self
pub fn with_min_size(self, min_size: usize) -> Self

// 辅助方法
pub fn should_compress(&self, data_size: usize) -> bool
```

### Compressor trait

```rust
pub trait Compressor: Send + Sync {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError>;
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, FlareError>;
    fn name(&self) -> &str;
    fn algorithm_id(&self) -> u32;
}
```

## 测试

### 运行测试

```bash
# 运行所有压缩测试
cargo test --lib common::compression

# 运行演示
cargo run --example compression_demo
```

### 测试覆盖

- ✅ 所有内置算法的压缩/解压
- ✅ 压缩级别配置
- ✅ 最小阈值判断
- ✅ 自定义压缩器注册
- ✅ 性能基准测试

## 故障排除

### 问题：压缩后反而变大

**原因**: 数据太小或已经压缩过
**解决**: 设置合理的 `min_size` 阈值

```rust
let config = CompressionConfig::new(CompressionAlgorithm::Lz4)
    .with_min_size(256); // 至少 256 字节
```

### 问题：压缩太慢

**原因**: 使用了高压缩级别
**解决**: 降低压缩级别或切换算法

```rust
// 使用快速级别
let config = CompressionConfig::new(CompressionAlgorithm::Gzip)
    .with_level(CompressionLevel::Fastest);

// 或切换到更快的算法
let config = CompressionConfig::new(CompressionAlgorithm::Lz4);
```

### 问题：自定义压缩器无法注册

**原因**: ID < 1000
**解决**: 使用 >= 1000 的 ID

```rust
// ❌ 错误
registry.register(100, compressor)?; // ID 太小

// ✅ 正确
registry.register(1000, compressor)?;
```

## 总结

压缩模块提供了灵活且高性能的压缩功能：

- ✅ **4种内置算法**，覆盖不同场景
- ✅ **可配置级别**，平衡速度和压缩率
- ✅ **智能阈值**，避免无效压缩
- ✅ **可扩展设计**，支持自定义算法
- ✅ **完整测试**，15个测试全部通过

根据实际场景选择合适的算法和配置，即可获得最佳性能！
