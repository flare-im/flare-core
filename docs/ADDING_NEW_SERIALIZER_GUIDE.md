# 添加新序列化格式指南

**项目**: flare-core  
**版本**: 0.1.0  
**最后更新**: 2025-10-16

---

## 📋 概述

本指南说明如何在 flare-core 项目中添加新的序列化格式支持。当前架构使用枚举模式，添加新格式需要修改多处代码。

**前提条件**：
- 熟悉 Rust 编程
- 了解 serde 序列化框架
- 理解项目的 parsing 模块架构

**预计时间**：1-2 小时

---

## 🎯 添加新格式清单

### 准备阶段

- [ ] 确认新格式的必要性（是否真的需要？）
- [ ] 选择合适的 Rust crate（如 MsgPack 用 `rmp-serde`）
- [ ] 了解新格式的特性（二进制/文本、压缩率、性能等）
- [ ] 评估对现有代码的影响

### 实施阶段

#### 步骤 1：修改枚举定义

**文件**: `src/common/parsing/codec.rs`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadCodec {
    Json,
    Protobuf,
    MsgPack,  // ➕ 添加新变体
}
```

- [ ] 添加新的枚举变体
- [ ] 确保命名清晰（使用 PascalCase）

#### 步骤 2：更新 `is_binary()` 方法

```rust
pub fn is_binary(&self) -> bool {
    matches!(self, PayloadCodec::Protobuf | PayloadCodec::MsgPack)  // ➕ 添加 MsgPack
}
```

- [ ] 判断新格式是二进制还是文本
- [ ] 更新 match 模式

#### 步骤 3：更新 `file_extension()` 方法

```rust
pub fn file_extension(&self) -> &str {
    match self {
        PayloadCodec::Json => "json",
        PayloadCodec::Protobuf => "pb",
        PayloadCodec::MsgPack => "msgpack",  // ➕ 添加扩展名
    }
}
```

- [ ] 提供合适的文件扩展名
- [ ] 参考格式的官方规范

#### 步骤 4：更新 `mime_type()` 方法

```rust
pub fn mime_type(&self) -> &str {
    match self {
        PayloadCodec::Json => "application/json",
        PayloadCodec::Protobuf => "application/x-protobuf",
        PayloadCodec::MsgPack => "application/msgpack",  // ➕ 添加 MIME 类型
    }
}
```

- [ ] 提供正确的 MIME 类型
- [ ] 查阅 IANA 注册表或格式文档

#### 步骤 5：实现 `encode()` 方法

```rust
pub fn encode<T: serde::Serialize>(&self, data: &T) -> Result<Vec<u8>, FlareError> {
    match self {
        PayloadCodec::Json => {
            serde_json::to_vec(data)
                .map_err(|e| FlareError::serialization_error(format!("JSON encoding failed: {}", e)))
        }
        PayloadCodec::Protobuf => {
            // ... 现有代码
        }
        PayloadCodec::MsgPack => {  // ➕ 添加序列化逻辑
            rmp_serde::to_vec(data)
                .map_err(|e| FlareError::serialization_error(format!("MsgPack encoding failed: {}", e)))
        }
    }
}
```

- [ ] 实现序列化逻辑
- [ ] 使用合适的错误处理
- [ ] 遵循现有代码风格

#### 步骤 6：实现 `decode()` 方法

```rust
pub fn decode<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, FlareError> {
    if bytes.is_empty() {
        return Err(FlareError::general_error("Cannot decode empty bytes"));
    }
    
    match self {
        PayloadCodec::Json => {
            // ... 现有代码
        }
        PayloadCodec::Protobuf => {
            // ... 现有代码
        }
        PayloadCodec::MsgPack => {  // ➕ 添加反序列化逻辑
            rmp_serde::from_slice(bytes)
                .map_err(|e| {
                    FlareError::general_error(format!(
                        "MsgPack decoding failed: {} (bytes: {} bytes)",
                        e,
                        bytes.len()
                    ))
                })
        }
    }
}
```

- [ ] 实现反序列化逻辑
- [ ] 提供详细的错误信息
- [ ] 保持错误格式一致

#### 步骤 7：更新 `name()` 方法

```rust
pub fn name(&self) -> &str {
    match self {
        PayloadCodec::Json => "json",
        PayloadCodec::Protobuf => "protobuf",
        PayloadCodec::MsgPack => "msgpack",  // ➕ 添加名称
    }
}
```

- [ ] 提供简洁的名称（小写）
- [ ] 用于日志和调试

#### 步骤 8：更新 `validate_bytes()` 方法

```rust
pub fn validate_bytes(&self, bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return false;
    }
    
    match self {
        PayloadCodec::Json => {
            serde_json::from_slice::<serde_json::Value>(bytes).is_ok()
        }
        PayloadCodec::Protobuf => {
            // ... 现有代码
        }
        PayloadCodec::MsgPack => {  // ➕ 添加验证逻辑
            rmp_serde::from_slice::<rmpv::Value>(bytes).is_ok()
        }
    }
}
```

- [ ] 实现字节验证逻辑
- [ ] 返回布尔值（不抛出异常）

#### 步骤 9：（可选）更新 `encode_pretty()` 方法

```rust
pub fn encode_pretty<T: serde::Serialize>(&self, data: &T) -> Result<Vec<u8>, FlareError> {
    match self {
        PayloadCodec::Json => {
            serde_json::to_vec_pretty(data)
                .map_err(|e| FlareError::serialization_error(format!("JSON pretty encoding failed: {}", e)))
        }
        _ => self.encode(data), // 其他格式不支持 pretty（包括新格式）
    }
}
```

- [ ] 如果新格式支持美化输出，添加实现
- [ ] 否则使用默认的 `encode()` 方法

#### 步骤 10：添加依赖

**文件**: `Cargo.toml`

```toml
[dependencies]
# ... 现有依赖
rmp-serde = "1.1"  # ➕ 添加 MsgPack 序列化支持
rmpv = "1.0"       # ➕ 用于验证
```

- [ ] 添加所需的 crate 依赖
- [ ] 选择稳定的版本
- [ ] 运行 `cargo update` 更新依赖

#### 步骤 11：编写单元测试

**文件**: `src/common/parsing/codec.rs`（在 `#[cfg(test)] mod tests` 中）

```rust
#[test]
fn test_payload_codec_msgpack() {
    let codec = PayloadCodec::MsgPack;
    
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct TestData {
        id: u32,
        name: String,
    }

    let data = TestData { id: 42, name: "test".to_string() };
    let bytes = codec.encode(&data).unwrap();
    let decoded: TestData = codec.decode(&bytes).unwrap();
    
    assert_eq!(data, decoded);
    assert!(codec.is_binary());
    assert_eq!(codec.name(), "msgpack");
    assert_eq!(codec.file_extension(), "msgpack");
    assert_eq!(codec.mime_type(), "application/msgpack");
}
```

- [ ] 测试编码和解码往返
- [ ] 测试元数据方法（name, extension, mime_type）
- [ ] 测试边界情况（空数据、大数据等）

#### 步骤 12：更新集成测试

**文件**: `src/common/parsing/parser.rs`（在测试中）

```rust
#[tokio::test]
async fn test_msgpack_parser() {
    let parser = MessageParser::new(PayloadCodec::MsgPack);
    assert_eq!(parser.codec_name(), "msgpack");
    
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct TestMessage {
        id: u32,
        content: String,
    }
    
    let msg = TestMessage {
        id: 99,
        content: "MsgPack test".to_string(),
    };
    
    let frame = parser.build_frame(&msg, "msgpack-1".to_string()).await.unwrap();
    let bytes = parser.encode_frame(&frame).await.unwrap();
    let decoded = parser.parse_bytes(&bytes).await.unwrap();
    let msg_decoded: TestMessage = parser.parse_payload(&decoded).await.unwrap();
    
    assert_eq!(msg, msg_decoded);
}
```

- [ ] 测试 MessageParser 集成
- [ ] 测试完整的编解码流程
- [ ] 验证与其他组件的交互

### 验证阶段

#### 步骤 13：运行测试

```bash
# 运行所有测试
cargo test --lib common::parsing

# 运行特定测试
cargo test --lib test_payload_codec_msgpack

# 查看测试输出
cargo test --lib common::parsing -- --nocapture
```

- [ ] 所有测试通过
- [ ] 无编译警告
- [ ] 无 clippy 警告

#### 步骤 14：性能测试（可选）

```rust
#[test]
fn benchmark_msgpack_encoding() {
    let codec = PayloadCodec::MsgPack;
    let data = vec![1u32; 1000];
    
    let start = std::time::Instant::now();
    for _ in 0..1000 {
        let _ = codec.encode(&data).unwrap();
    }
    let duration = start.elapsed();
    
    println!("MsgPack encoding 1000 iterations: {:?}", duration);
    // 与 JSON 对比
}
```

- [ ] 测试序列化性能
- [ ] 与其他格式对比
- [ ] 记录性能数据

#### 步骤 15：更新文档

**文件**: `src/common/parsing/codec.rs`（文档注释）

```rust
/// Payload 编解码器枚举
/// 
/// # 支持的格式
/// 
/// - **JSON**: 人类可读，适合调试和跨语言兼容
/// - **Protobuf**: 高效紧凑，适合生产环境（占位实现）
/// - **MsgPack**: 二进制格式，性能和大小平衡  // ➕ 添加说明
```

- [ ] 更新代码注释
- [ ] 更新 README 文档
- [ ] 添加使用示例

---

## 🔍 检查清单总结

### 代码修改

- [ ] ✅ 修改 `PayloadCodec` 枚举
- [ ] ✅ 更新 `is_binary()` 方法
- [ ] ✅ 更新 `file_extension()` 方法
- [ ] ✅ 更新 `mime_type()` 方法
- [ ] ✅ 实现 `encode()` 方法
- [ ] ✅ 实现 `decode()` 方法
- [ ] ✅ 更新 `name()` 方法
- [ ] ✅ 更新 `validate_bytes()` 方法
- [ ] ✅ （可选）更新 `encode_pretty()` 方法

### 依赖和配置

- [ ] ✅ 添加 Cargo 依赖

### 测试

- [ ] ✅ 编写单元测试
- [ ] ✅ 编写集成测试
- [ ] ✅ 运行全量测试
- [ ] ✅ 验证无编译警告
- [ ] ✅ （可选）性能基准测试

### 文档

- [ ] ✅ 更新代码注释
- [ ] ✅ 更新使用文档
- [ ] ✅ 添加示例代码

---

## 📝 示例：添加 MsgPack 支持

### 完整代码示例

参考文件：`docs/serialization_improvement_examples.rs`

### Cargo.toml 修改

```toml
[dependencies]
rmp-serde = "1.1"
rmpv = "1.0"
```

### 主要代码修改点

1. **枚举定义**：添加 `MsgPack` 变体
2. **编码逻辑**：使用 `rmp_serde::to_vec()`
3. **解码逻辑**：使用 `rmp_serde::from_slice()`
4. **元数据**：设置正确的 name、extension、mime_type

---

## ⚠️ 常见陷阱

### 1. 忘记更新某个 match 分支

**症状**：编译错误 "non-exhaustive patterns"

**解决**：仔细检查所有使用 `PayloadCodec` 的 match 语句

### 2. 错误处理不一致

**症状**：某些方法返回不同类型的错误

**解决**：统一使用 `FlareError`，保持错误消息格式一致

### 3. 依赖版本冲突

**症状**：cargo build 失败

**解决**：检查依赖版本兼容性，必要时更新其他依赖

### 4. 测试覆盖不足

**症状**：运行时发现 bug

**解决**：编写全面的测试，包括边界情况

---

## 🎓 最佳实践

1. **先实现，后优化**：首先保证功能正确，然后再考虑性能
2. **参考现有代码**：保持代码风格一致
3. **详细的错误信息**：帮助调试问题
4. **全面的测试**：覆盖正常和异常情况
5. **清晰的文档**：帮助其他开发者理解

---

## 🔗 相关资源

- **架构分析报告**：`docs/SERIALIZATION_ARCHITECTURE_ANALYSIS.md`
- **代码示例**：`docs/serialization_improvement_examples.rs`
- **Serde 文档**：https://serde.rs/
- **项目记忆**：序列化工厂简化要求

---

## 💬 获取帮助

如有问题，请：
1. 查阅相关文档
2. 查看现有代码实现
3. 运行测试验证
4. 联系项目维护者

---

**文档状态**: ✅ 完成  
**最后更新**: 2025-10-16  
**维护者**: flare-core 团队
