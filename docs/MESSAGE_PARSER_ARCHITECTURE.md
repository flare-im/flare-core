# Flare-Core 统一消息解析器架构设计与实现

## 📋 概述

本文档介绍了 flare-core 项目中统一消息解析器（Unified Message Parser）的架构设计与使用方法。该解析器为 WebSocket 和 QUIC 连接提供了一致的消息处理接口，支持多种序列化格式，具有良好的可扩展性。

## 🎯 设计目标

1. **统一接口**：为 QUIC 和 WebSocket 连接提供一致的消息解析接口
2. **可扩展性**：支持用户自定义序列化格式（JSON、Protobuf、MsgPack、Bincode 等）
3. **配置统一**：将序列化相关的配置集中管理，避免重复配置
4. **解耦设计**：确保序列化逻辑与连接协议无关

## 🏗️ 架构设计

### 核心组件

```
┌─────────────────────────────────────────────────────────┐
│                   MessageParser                         │
│  (统一的消息解析器 - 用户主要接口)                       │
└────────────────┬────────────────────────────────────────┘
                 │
                 ├─────────────────────────────────────┐
                 │                                     │
        ┌────────▼────────┐                  ┌────────▼────────┐
        │  PayloadCodec   │                  │   FrameCodec    │
        │  (序列化器)      │                  │  (协议编解码器)  │
        └────────┬────────┘                  └────────┬────────┘
                 │                                     │
      ┌──────────┴──────────┐                        │
      │                     │                         │
┌─────▼─────┐   ┌─────▼─────┐              ┌────────▼────────┐
│   JSON    │   │  MsgPack  │              │ DefaultFrameCodec│
└───────────┘   └───────────┘              │ (二进制协议)      │
┌───────────┐   ┌───────────┐              └─────────────────┘
│ Protobuf  │   │  Bincode  │
└───────────┘   └───────────┘
```

### 设计模式

1. **枚举封装模式**：`PayloadCodec` 使用枚举封装不同序列化器，避免 trait object 的 dyn 兼容性问题
2. **策略模式**：可在运行时切换不同的序列化策略
3. **门面模式**：`MessageParser` 提供统一简洁的API，隐藏内部复杂性

## 💡 核心 API

### 1. PayloadCodec（序列化器）

负责业务数据的序列化和反序列化。

```rust
pub enum PayloadCodec {
    Json,       // JSON 格式 (serde_json)
    Protobuf,   // Protobuf 格式 (prost)
    MsgPack,    // MessagePack 格式 (rmp-serde)
    Bincode,    // Bincode 格式 (bincode)
}

impl PayloadCodec {
    /// 从 SerializationFormat 创建
    pub fn from_format(format: SerializationFormat) -> Self;
    
    /// 序列化数据
    pub fn encode<T: serde::Serialize>(&self, data: &T) -> Result<Vec<u8>, FlareError>;
    
    /// 反序列化数据
    pub fn decode<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, FlareError>;
    
    /// 获取编解码器名称
    pub fn name(&self) -> &str;
}
```

### 2. FrameCodec（协议编解码器）

负责 Frame 结构的完整编解码。

```rust
pub trait FrameCodec {
    /// 将 Frame 编码为字节数组
    fn encode_frame(&self, frame: &Frame) -> Result<Vec<u8>, FlareError>;
    
    /// 将字节数组解码为 Frame
    fn decode_frame(&self, bytes: &[u8]) -> Result<Frame, FlareError>;
    
    /// 验证 Frame 的完整性
    fn validate_frame(&self, frame: &Frame) -> Result<(), FlareError>;
}
```

**DefaultFrameCodec** 实现了二进制协议：

```
+--------+--------+--------+-------------+-------------+-------------+--------+
| Magic  | Ver    | Flags  | MessageID   | Reliability | Command     | Payload|
| 2bytes | 1byte  | 1byte  | Len+Str     | 1byte       | Type+Data   | Data   |
+--------+--------+--------+-------------+-------------+-------------+--------+
```

- **Magic** (2 bytes): `0xF1A7` 魔数，用于快速识别
- **Version** (1 byte): 协议版本号，当前为 `1`
- **Flags** (1 byte): 保留标志位（压缩、加密等）
- **MessageID**: 长度（2 bytes）+ UTF-8 字符串
- **Reliability** (1 byte): 0 = BestEffort, 1 = AtLeastOnce
- **Command**: 类型（1 byte）+ 数据
- **Payload**: 长度（4 bytes）+ 实际数据

### 3. MessageParser（统一消息解析器）

整合 PayloadCodec 和 FrameCodec，提供完整的消息解析能力。

```rust
pub struct MessageParser {
    // 内部字段省略
}

impl MessageParser {
    /// 创建新的消息解析器
    pub fn new(format: SerializationFormat) -> Self;
    
    /// 创建带自定义 Frame 编解码器的解析器
    pub fn with_frame_codec(
        format: SerializationFormat,
        frame_codec: Box<dyn FrameCodec + Send + Sync>,
    ) -> Self;
    
    /// 解析原始字节为 Frame
    pub async fn parse_bytes(&self, bytes: &[u8]) -> Result<Frame, FlareError>;
    
    /// 解析 Frame 中的 Payload 为业务对象
    pub async fn parse_payload<T: serde::de::DeserializeOwned>(&self, frame: &Frame) -> Result<T, FlareError>;
    
    /// 构建包含业务数据的 Frame
    pub async fn build_frame<T: serde::Serialize>(&self, data: &T, message_id: String) -> Result<Frame, FlareError>;
    
    /// 构建带可靠性级别的 Frame
    pub async fn build_frame_with_reliability<T: serde::Serialize>(
        &self,
        data: &T,
        message_id: String,
        reliability: Reliability,
    ) -> Result<Frame, FlareError>;
    
    /// 将 Frame 编码为字节数组
    pub async fn encode_frame(&self, frame: &Frame) -> Result<Vec<u8>, FlareError>;
    
    /// 完整的解析流程
    pub async fn parse_and_handle(&self, bytes: &[u8]) -> Result<Frame, FlareError>;
    
    /// 获取统计信息
    pub fn get_stats(&self) -> ParserStats;
    
    /// 重置统计信息
    pub fn reset_stats(&self);
    
    /// 获取当前序列化格式名称
    pub fn codec_name(&self) -> &str;
}
```

## 📚 使用示例

### 基础使用

```rust
use flare_core::common::parsing::MessageParser;
use flare_core::common::serialization::SerializationFormat;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct ChatMessage {
    user_id: String,
    content: String,
    timestamp: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建 JSON 格式的解析器
    let parser = MessageParser::new(SerializationFormat::Json);
    
    // 构建消息
    let msg = ChatMessage {
        user_id: "user_123".to_string(),
        content: "Hello, Flare!".to_string(),
        timestamp: 1234567890,
    };
    
    // 序列化：业务对象 → Frame → 字节数组
    let frame = parser.build_frame(&msg, "msg-001".to_string()).await?;
    let bytes = parser.encode_frame(&frame).await?;
    
    println!("Encoded {} bytes", bytes.len());
    
    // 反序列化：字节数组 → Frame → 业务对象
    let received_frame = parser.parse_bytes(&bytes).await?;
    let received_msg: ChatMessage = parser.parse_payload(&received_frame).await?;
    
    assert_eq!(received_msg, msg);
    println!("Message verified: {:?}", received_msg);
    
    Ok(())
}
```

### 使用不同的序列化格式

```rust
use flare_core::common::parsing::MessageParser;
use flare_core::common::serialization::SerializationFormat;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // JSON 格式（适合调试和可读性）
    let json_parser = MessageParser::new(SerializationFormat::Json);
    
    // MsgPack 格式（紧凑、高效）
    let msgpack_parser = MessageParser::new(SerializationFormat::MsgPack);
    
    // Protobuf 格式（跨语言、高性能）
    let protobuf_parser = MessageParser::new(SerializationFormat::Protobuf);
    
    #[derive(Serialize, Deserialize)]
    struct Data {
        id: u32,
        value: String,
    }
    
    let data = Data { id: 42, value: "test".to_string() };
    
    // 使用 JSON
    let frame_json = json_parser.build_frame(&data, "json-1".to_string()).await?;
    let bytes_json = json_parser.encode_frame(&frame_json).await?;
    println!("JSON size: {} bytes", bytes_json.len());
    
    // 使用 MsgPack（通常更小）
    let frame_msgpack = msgpack_parser.build_frame(&data, "msgpack-1".to_string()).await?;
    let bytes_msgpack = msgpack_parser.encode_frame(&frame_msgpack).await?;
    println!("MsgPack size: {} bytes", bytes_msgpack.len());
    
    Ok(())
}
```

### 带可靠性级别的消息

```rust
use flare_core::common::parsing::MessageParser;
use flare_core::common::protocol::reliability::Reliability;
use flare_core::common::serialization::SerializationFormat;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let parser = MessageParser::new(SerializationFormat::Json);
    
    #[derive(Serialize, Deserialize)]
    struct ImportantMessage {
        content: String,
    }
    
    let msg = ImportantMessage {
        content: "This is important!".to_string(),
    };
    
    // 创建需要至少一次投递的消息
    let frame = parser.build_frame_with_reliability(
        &msg,
        "important-001".to_string(),
        Reliability::AtLeastOnce,  // 保证可靠投递
    ).await?;
    
    let bytes = parser.encode_frame(&frame).await?;
    
    // 验证可靠性级别
    let received_frame = parser.parse_bytes(&bytes).await?;
    assert_eq!(received_frame.reliability, Reliability::AtLeastOnce);
    
    Ok(())
}
```

### 统计信息监控

```rust
use flare_core::common::parsing::MessageParser;
use flare_core::common::serialization::SerializationFormat;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let parser = MessageParser::new(SerializationFormat::Json);
    
    #[derive(Serialize, Deserialize)]
    struct TestData { value: i32 }
    
    // 处理多条消息
    for i in 0..100 {
        let data = TestData { value: i };
        let frame = parser.build_frame(&data, format!("msg-{}", i)).await?;
        let bytes = parser.encode_frame(&frame).await?;
        let _ = parser.parse_bytes(&bytes).await?;
    }
    
    // 查看统计信息
    let stats = parser.get_stats();
    println!("Parsed: {} messages", stats.parsed_count);
    println!("Failed: {} messages", stats.failed_count);
    println!("Total bytes: {} bytes", stats.total_bytes);
    
    // 重置统计
    parser.reset_stats();
    
    Ok(())
}
```

### 自定义 Frame 编解码器

```rust
use flare_core::common::parsing::{MessageParser, FrameCodec};
use flare_core::common::protocol::frame::Frame;
use flare_core::common::error::FlareError;
use flare_core::common::serialization::SerializationFormat;

// 自定义的 Frame 编解码器
struct CustomFrameCodec {
    // 自定义字段
}

impl FrameCodec for CustomFrameCodec {
    fn encode_frame(&self, frame: &Frame) -> Result<Vec<u8>, FlareError> {
        // 自定义编码逻辑
        todo!()
    }
    
    fn decode_frame(&self, bytes: &[u8]) -> Result<Frame, FlareError> {
        // 自定义解码逻辑
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 使用自定义 Frame 编解码器
    let parser = MessageParser::with_frame_codec(
        SerializationFormat::Json,
        Box::new(CustomFrameCodec { /* ... */ }),
    );
    
    // 正常使用 parser...
    
    Ok(())
}
```

## 🔌 在连接中集成

### WebSocket 集成示例

```rust
use flare_core::common::parsing::MessageParser;
use flare_core::common::serialization::SerializationFormat;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建解析器
    let parser = MessageParser::new(SerializationFormat::Json);
    
    // 连接 WebSocket
    let (ws_stream, _) = connect_async("ws://localhost:8080").await?;
    let (mut write, mut read) = ws_stream.split();
    
    #[derive(Serialize, Deserialize)]
    struct MyMessage { text: String }
    
    // 发送消息
    let msg = MyMessage { text: "Hello".to_string() };
    let frame = parser.build_frame(&msg, "ws-001".to_string()).await?;
    let bytes = parser.encode_frame(&frame).await?;
    write.send(Message::Binary(bytes)).await?;
    
    // 接收消息
    while let Some(message) = read.next().await {
        let message = message?;
        if let Message::Binary(bytes) = message {
            let received_frame = parser.parse_bytes(&bytes).await?;
            let received_msg: MyMessage = parser.parse_payload(&received_frame).await?;
            println!("Received: {}", received_msg.text);
        }
    }
    
    Ok(())
}
```

### QUIC 集成示例

```rust
use flare_core::common::parsing::MessageParser;
use flare_core::common::serialization::SerializationFormat;
use quinn::{Endpoint, Connection};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建解析器
    let parser = MessageParser::new(SerializationFormat::MsgPack);
    
    // 连接 QUIC（省略配置细节）
    let endpoint = Endpoint::client(/* ... */)?;
    let connection = endpoint.connect(/* ... */)?.await?;
    
    #[derive(Serialize, Deserialize)]
    struct MyMessage { text: String }
    
    // 发送消息
    let msg = MyMessage { text: "Hello QUIC".to_string() };
    let frame = parser.build_frame(&msg, "quic-001".to_string()).await?;
    let bytes = parser.encode_frame(&frame).await?;
    
    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(&bytes).await?;
    send.finish().await?;
    
    // 接收消息
    let response_bytes = recv.read_to_end(65536).await?;
    let received_frame = parser.parse_bytes(&response_bytes).await?;
    let received_msg: MyMessage = parser.parse_payload(&received_frame).await?;
    println!("Received: {}", received_msg.text);
    
    Ok(())
}
```

## 🎨 高级特性

### 1. 性能优化

```rust
// 使用 MsgPack 获得更好的性能和更小的消息体积
let parser = MessageParser::new(SerializationFormat::MsgPack);

// 使用 Bincode 获得最快的序列化速度
let parser = MessageParser::new(SerializationFormat::Bincode);
```

### 2. 错误处理

```rust
match parser.parse_bytes(&bytes).await {
    Ok(frame) => {
        println!("Parsed frame: {}", frame.message_id);
    }
    Err(FlareError::SerializationError { message, .. }) => {
        eprintln!("Serialization error: {}", message);
    }
    Err(e) => {
        eprintln!("Other error: {}", e);
    }
}
```

### 3. 批量处理

```rust
let parser = MessageParser::new(SerializationFormat::Json);

// 批量构建消息
let messages: Vec<_> = (0..1000)
    .map(|i| MyData { id: i })
    .collect();

for (i, msg) in messages.iter().enumerate() {
    let frame = parser.build_frame(msg, format!("batch-{}", i)).await?;
    let bytes = parser.encode_frame(&frame).await?;
    // 发送 bytes...
}
```

## 📊 性能基准

| 序列化格式 | 编码速度 | 解码速度 | 消息大小 | 使用场景 |
|-----------|---------|---------|---------|---------|
| **JSON** | 中等 | 中等 | 较大 | 调试、日志、兼容性 |
| **MsgPack** | 快 | 快 | 小 | 生产环境、高性能 |
| **Protobuf** | 快 | 快 | 最小 | 跨语言、微服务 |
| **Bincode** | 最快 | 最快 | 小 | Rust 专用、最高性能 |

## 🔐 安全考虑

1. **消息大小限制**：DefaultFrameCodec 默认限制消息最大 10MB
2. **魔数验证**：使用 `0xF1A7` 魔数快速识别有效消息
3. **协议版本检查**：确保协议兼容性
4. **UTF-8 验证**：message_id 必须是有效的 UTF-8 字符串

## 🧪 测试

项目包含完整的单元测试：

```bash
# 测试所有解析器功能
cargo test --lib parsing

# 测试特定模块
cargo test --lib parsing::codec
cargo test --lib parsing::parser
```

## 📝 总结

flare-core 的统一消息解析器提供了：

✅ **统一接口**：WebSocket 和 QUIC 使用相同的 API  
✅ **灵活扩展**：支持多种序列化格式，易于添加新格式  
✅ **高性能**：零拷贝设计，高效的二进制协议  
✅ **类型安全**：完全利用 Rust 的类型系统  
✅ **易于测试**：清晰的接口，完善的单元测试  
✅ **生产就绪**：错误处理、统计监控、文档完备  

该架构为构建高性能、可扩展的即时通讯系统奠定了坚实的基础。
