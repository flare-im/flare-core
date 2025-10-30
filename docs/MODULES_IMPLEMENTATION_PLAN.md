# Common 模块实施计划

**计划日期**: 2025-10-17  
**优先级**: 高  
**预计时间**: 4-6周

---

## 📋 执行摘要

### 核心决策

| 模块 | 优先级 | 行动 | 理由 |
|------|--------|------|------|
| **messaging** | 🔴 高 | ✅ 立即实现 | 核心功能缺失，阻塞可靠性保证 |
| **compression** | 🟡 中 | ⏳ 第二阶段 | 性能优化需要，非阻塞 |
| **pipeline** | 🟢 低 | ⏸️ 暂缓 | 架构预留，当前无明确需求 |
| **system** | ⚪ 无 | ❌ 删除 | 无用途，避免模块膨胀 |

### Protobuf 集成策略

**问题**：当前存在手写和 Protobuf 生成代码双轨并行，导致：
- 🔴 类型不兼容（Frame/Command/Reliability 重复定义）
- 🔴 功能不一致（手写版缺少 timestamp、priority、metadata）
- 🔴 维护困难（修改需要同步两处）

**解决方案**：✅ **完全迁移到 Protobuf**
- 删除手写实现
- 统一使用 prost 生成的代码
- 提供兼容层保持 API 稳定

---

## 🎯 阶段1：Protobuf 统一（第1-2周）

### 目标
完全迁移到 Protobuf 定义，消除重复代码。

### 任务清单

#### 1.1 修复 build.rs 配置 ✅

**当前问题**：
```rust
// 路径映射可能不正确
config.extern_path(".flare.core.commands", "crate::common::protocol::flare_proto::commands");
```

**修复方案**：
```rust
// build.rs
fn main() -> Result<()> {
    std::fs::create_dir_all("src/common/protocol")?;
    
    let mut config = prost_build::Config::new();
    config.out_dir("src/common/protocol");
    
    // Serde 支持
    config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    config.type_attribute(".", "#[serde(rename_all = \"snake_case\")]");
    
    // 简化路径映射
    config.extern_path(".flare.core", "crate::common::protocol::flare_core");
    
    // 编译
    config.compile_protos(
        &["proto/frame.proto", "proto/commands.proto"],
        &["proto/"]
    )?;
    
    // 生成模块导出文件
    generate_mod_file()?;
    
    Ok(())
}

fn generate_mod_file() -> Result<()> {
    let mod_content = r#"
// 自动生成的模块导出文件

// Protobuf 生成的定义
pub mod flare_core;
pub mod flare_proto {
    pub mod commands {
        include!("commands.rs");  // 如果 prost 生成单独的 commands 文件
    }
}

// 重新导出常用类型
pub use flare_core::{Frame, Reliability};
pub use flare_proto::commands::{
    Command, ControlCmd, MessageCmd, NotificationCmd, EventCmd,
};

// 工厂和辅助
pub mod factory;
"#;
    
    std::fs::write("src/common/protocol/generated.rs", mod_content)?;
    Ok(())
}
```

#### 1.2 删除手写定义 ✅

```bash
# 重命名旧文件（保留备份）
cd src/common/protocol/
mv frame.rs frame_deprecated.rs
mv commands.rs commands_deprecated.rs
mv reliability.rs reliability_deprecated.rs
```

#### 1.3 更新模块导出 ✅

```rust
// src/common/protocol/mod.rs
// 导出 Protobuf 生成的类型
mod flare_core;

pub use flare_core::{Frame, Reliability};

// 如果 commands 在单独文件
pub mod commands {
    // 导出所有 command 类型
}

// 保留的辅助模块
pub mod factory;
```

#### 1.4 更新所有引用 ✅

查找并替换所有引用：
```bash
# 查找所有使用旧定义的文件
rg "use.*protocol::(frame|commands|reliability)" src/

# 批量更新（小心操作）
# Frame -> flare_core::Frame
# Command -> flare_proto::commands::Command
```

**关键文件**：
- `src/common/protocol/factory.rs` - 更新 Frame 创建逻辑
- `src/common/parsing/parser.rs` - 更新 Frame 解析
- `src/common/connections/*.rs` - 更新连接层使用
- `examples/*.rs` - 更新示例代码

#### 1.5 测试验证 ✅

```bash
# 编译检查
cargo build --lib

# 运行测试
cargo test --lib

# 运行示例
cargo run --example websocket_demo
cargo run --example quic_demo
```

---

## 🎯 阶段2：messaging 核心实现（第3-4周）

### 目标
实现完整的消息解析、构建、队列和可靠性机制。

### 目录结构

```
src/common/messaging/
├── mod.rs              // 模块导出
├── parser.rs           // 消息解析器（Byte ↔ Frame）
├── builder.rs          // Frame 构建器
├── priority_queue.rs   // 优先级队列
└── reliability.rs      // 可靠性管理器
```

### 任务清单

#### 2.1 创建 parser.rs ✅

**功能**：Byte ↔ Frame 转换

```rust
// src/common/messaging/parser.rs
use crate::common::protocol::flare_core::Frame;
use crate::common::parsing::PayloadCodec;
use crate::common::error::FlareError;

pub struct MessageParser {
    codec: PayloadCodec,
}

impl MessageParser {
    pub fn new(codec: PayloadCodec) -> Self {
        Self { codec }
    }
    
    /// 解析字节流为 Frame（使用 Protobuf）
    pub fn parse_bytes(&self, bytes: &[u8]) -> Result<Frame, FlareError> {
        match self.codec {
            PayloadCodec::Protobuf => {
                use prost::Message;
                Frame::decode(bytes).map_err(|e| {
                    FlareError::serialization_error(format!("Protobuf decode failed: {}", e))
                })
            }
            PayloadCodec::Json => {
                serde_json::from_slice(bytes).map_err(|e| {
                    FlareError::serialization_error(format!("JSON decode failed: {}", e))
                })
            }
        }
    }
    
    /// 将 Frame 编码为字节流
    pub fn encode_frame(&self, frame: &Frame) -> Result<Vec<u8>, FlareError> {
        match self.codec {
            PayloadCodec::Protobuf => {
                use prost::Message;
                let mut buf = Vec::new();
                frame.encode(&mut buf).map_err(|e| {
                    FlareError::serialization_error(format!("Protobuf encode failed: {}", e))
                })?;
                Ok(buf)
            }
            PayloadCodec::Json => {
                serde_json::to_vec(frame).map_err(|e| {
                    FlareError::serialization_error(format!("JSON encode failed: {}", e))
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_protobuf_roundtrip() {
        let parser = MessageParser::new(PayloadCodec::Protobuf);
        
        let frame = Frame {
            message_id: "test-001".to_string(),
            reliability: 1, // AtLeastOnce
            timestamp: 1234567890,
            ..Default::default()
        };
        
        let bytes = parser.encode_frame(&frame).unwrap();
        let decoded = parser.parse_bytes(&bytes).unwrap();
        
        assert_eq!(decoded.message_id, "test-001");
    }
}
```

**测试**：
```bash
cargo test --lib common::messaging::parser
```

#### 2.2 创建 builder.rs ✅

**功能**：辅助构建 Frame

```rust
// src/common/messaging/builder.rs
use crate::common::protocol::flare_core::{Frame, Reliability};
use crate::common::protocol::commands::Command;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct FrameBuilder {
    frame: Frame,
}

impl FrameBuilder {
    pub fn new(message_id: String) -> Self {
        Self {
            frame: Frame {
                message_id,
                reliability: Reliability::BestEffort as i32,
                timestamp: Self::current_timestamp_ms(),
                command: None,
                session_id: None,
                priority: 0,
                compression: None,
                encrypted: false,
                metadata: std::collections::HashMap::new(),
            },
        }
    }
    
    pub fn with_command(mut self, command: Command) -> Self {
        self.frame.command = Some(command);
        self
    }
    
    pub fn with_reliability(mut self, reliability: Reliability) -> Self {
        self.frame.reliability = reliability as i32;
        self
    }
    
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.frame.priority = priority;
        self
    }
    
    pub fn with_session(mut self, session_id: String) -> Self {
        self.frame.session_id = Some(session_id);
        self
    }
    
    pub fn with_metadata(mut self, key: String, value: Vec<u8>) -> Self {
        self.frame.metadata.insert(key, value);
        self
    }
    
    pub fn build(self) -> Frame {
        self.frame
    }
    
    fn current_timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::commands::{Command, ControlCmd};
    
    #[test]
    fn test_builder() {
        let frame = FrameBuilder::new("msg-001".to_string())
            .with_command(Command::Control(ControlCmd::Ping))
            .with_reliability(Reliability::AtLeastOnce)
            .with_priority(10)
            .build();
        
        assert_eq!(frame.message_id, "msg-001");
        assert_eq!(frame.priority, 10);
    }
}
```

#### 2.3 创建 priority_queue.rs ✅

**功能**：按优先级排序的消息队列

```rust
// src/common/messaging/priority_queue.rs
use crate::common::protocol::flare_core::Frame;
use std::collections::BinaryHeap;
use std::cmp::{Ordering, Reverse};

#[derive(Debug)]
struct PriorityFrame {
    priority: u32,
    timestamp: u64,
    frame: Frame,
}

impl Ord for PriorityFrame {
    fn cmp(&self, other: &Self) -> Ordering {
        // 高优先级在前
        match self.priority.cmp(&other.priority) {
            Ordering::Equal => {
                // 优先级相同，早的在前
                other.timestamp.cmp(&self.timestamp)
            }
            other => other,
        }
    }
}

impl PartialOrd for PriorityFrame {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for PriorityFrame {}
impl PartialEq for PriorityFrame {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.timestamp == other.timestamp
    }
}

pub struct PriorityMessageQueue {
    queue: BinaryHeap<PriorityFrame>,
}

impl PriorityMessageQueue {
    pub fn new() -> Self {
        Self {
            queue: BinaryHeap::new(),
        }
    }
    
    pub fn push(&mut self, frame: Frame) {
        let priority = frame.priority;
        let timestamp = frame.timestamp;
        
        self.queue.push(PriorityFrame {
            priority,
            timestamp,
            frame,
        });
    }
    
    pub fn pop(&mut self) -> Option<Frame> {
        self.queue.pop().map(|pf| pf.frame)
    }
    
    pub fn peek(&self) -> Option<&Frame> {
        self.queue.peek().map(|pf| &pf.frame)
    }
    
    pub fn len(&self) -> usize {
        self.queue.len()
    }
    
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_priority_order() {
        let mut queue = PriorityMessageQueue::new();
        
        // 插入不同优先级的消息
        let frame1 = Frame {
            message_id: "low".to_string(),
            priority: 1,
            timestamp: 100,
            ..Default::default()
        };
        
        let frame2 = Frame {
            message_id: "high".to_string(),
            priority: 10,
            timestamp: 200,
            ..Default::default()
        };
        
        queue.push(frame1);
        queue.push(frame2);
        
        // 高优先级先出队
        let first = queue.pop().unwrap();
        assert_eq!(first.message_id, "high");
        
        let second = queue.pop().unwrap();
        assert_eq!(second.message_id, "low");
    }
}
```

#### 2.4 创建 reliability.rs ✅

**功能**：确认、重传、去重

```rust
// src/common/messaging/reliability.rs
use crate::common::protocol::flare_core::Frame;
use crate::common::error::FlareError;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

struct PendingMessage {
    frame: Frame,
    send_time: Instant,
    retry_count: u32,
}

pub struct ReliabilityManager {
    pending_acks: HashMap<String, PendingMessage>,
    received_ids: HashSet<String>,
    timeout: Duration,
    max_retries: u32,
}

impl ReliabilityManager {
    pub fn new() -> Self {
        Self {
            pending_acks: HashMap::new(),
            received_ids: HashSet::new(),
            timeout: Duration::from_secs(5),
            max_retries: 3,
        }
    }
    
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }
    
    /// 发送消息并等待确认
    pub fn send_with_ack(&mut self, frame: Frame) -> Result<(), FlareError> {
        let message_id = frame.message_id.clone();
        
        self.pending_acks.insert(
            message_id,
            PendingMessage {
                frame,
                send_time: Instant::now(),
                retry_count: 0,
            },
        );
        
        Ok(())
    }
    
    /// 处理确认消息
    pub fn handle_ack(&mut self, message_id: &str) -> bool {
        self.pending_acks.remove(message_id).is_some()
    }
    
    /// 检查超时并返回需要重传的消息
    pub fn check_timeout(&mut self) -> Vec<Frame> {
        let mut retry_frames = Vec::new();
        let now = Instant::now();
        
        self.pending_acks.retain(|_, pending| {
            if now.duration_since(pending.send_time) > self.timeout {
                if pending.retry_count < self.max_retries {
                    // 重传
                    let mut frame = pending.frame.clone();
                    retry_frames.push(frame);
                    
                    pending.send_time = now;
                    pending.retry_count += 1;
                    
                    true // 保留
                } else {
                    // 超过最大重试次数，放弃
                    false
                }
            } else {
                true // 未超时，保留
            }
        });
        
        retry_frames
    }
    
    /// 去重检查
    pub fn is_duplicate(&mut self, message_id: &str) -> bool {
        if self.received_ids.contains(message_id) {
            true
        } else {
            self.received_ids.insert(message_id.to_string());
            false
        }
    }
    
    /// 清理旧的接收记录（避免内存泄漏）
    pub fn cleanup_received(&mut self, max_size: usize) {
        if self.received_ids.len() > max_size {
            self.received_ids.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_ack_handling() {
        let mut manager = ReliabilityManager::new();
        
        let frame = Frame {
            message_id: "msg-001".to_string(),
            ..Default::default()
        };
        
        manager.send_with_ack(frame).unwrap();
        assert_eq!(manager.pending_acks.len(), 1);
        
        assert!(manager.handle_ack("msg-001"));
        assert_eq!(manager.pending_acks.len(), 0);
    }
    
    #[test]
    fn test_duplicate_detection() {
        let mut manager = ReliabilityManager::new();
        
        assert!(!manager.is_duplicate("msg-001"));
        assert!(manager.is_duplicate("msg-001"));
    }
}
```

#### 2.5 创建 mod.rs ✅

```rust
// src/common/messaging/mod.rs
//! 消息处理模块
//!
//! 提供消息解析、构建、队列和可靠性保证

pub mod parser;
pub mod builder;
pub mod priority_queue;
pub mod reliability;

pub use parser::MessageParser;
pub use builder::FrameBuilder;
pub use priority_queue::PriorityMessageQueue;
pub use reliability::ReliabilityManager;
```

#### 2.6 集成到 common/mod.rs ✅

```rust
// src/common/mod.rs
pub mod error;
pub mod connections;
pub mod protocol;
pub mod serialization;
pub mod parsing;
pub mod messaging;  // ✅ 新增
```

---

## 🎯 阶段3：compression 实现（第5-6周）

### 目标
实现 LZ4 压缩算法支持，优化大消息传输。

### 任务清单

#### 3.1 添加依赖 ✅

```toml
# Cargo.toml
[dependencies]
lz4 = "1.24"
```

#### 3.2 实现压缩模块 ✅

```
src/common/compression/
├── mod.rs
├── traits.rs
└── lz4.rs
```

详见 [`COMMON_MODULES_ANALYSIS.md`](COMMON_MODULES_ANALYSIS.md) 第 2.2 节。

---

## 📊 测试策略

### 单元测试
```bash
# 测试 messaging 模块
cargo test --lib common::messaging

# 测试 parser
cargo test --lib common::messaging::parser

# 测试 reliability
cargo test --lib common::messaging::reliability
```

### 集成测试
```bash
# 端到端测试
cargo test --test integration_messaging

# 示例测试
cargo run --example websocket_demo
cargo run --example quic_demo
```

### 性能测试
```bash
# 基准测试
cargo bench --bench frame_encoding
cargo bench --bench compression
```

---

## 🎯 成功标准

### 阶段1（Protobuf 统一）
- [ ] ✅ 编译通过，无警告
- [ ] ✅ 所有测试通过（31/31）
- [ ] ✅ 示例运行成功
- [ ] ✅ 无手写重复定义

### 阶段2（messaging 实现）
- [ ] ✅ MessageParser 可用
- [ ] ✅ FrameBuilder 简化创建
- [ ] ✅ PriorityQueue 正确排序
- [ ] ✅ ReliabilityManager 确认/重传工作
- [ ] ✅ 测试覆盖率 > 80%

### 阶段3（compression 实现）
- [ ] ✅ LZ4 压缩可用
- [ ] ✅ 自动压缩策略工作
- [ ] ✅ 性能无明显下降
- [ ] ✅ 压缩率 > 50%

---

## 📝 检查清单

### 开始前
- [ ] 备份当前代码
- [ ] 创建功能分支
- [ ] 了解 Protobuf 基础
- [ ] 阅读设计文档

### 开发中
- [ ] 遵循代码规范
- [ ] 编写单元测试
- [ ] 更新文档
- [ ] 提交小批次

### 完成后
- [ ] 所有测试通过
- [ ] 文档完整
- [ ] 代码审查
- [ ] 合并主分支

---

**负责人**: Flare Core Team  
**审核人**: TBD  
**最后更新**: 2025-10-17

