# 序列化示例应用文档

## 概述

本文档说明了如何在 WebSocket 和 QUIC 示例中应用不同的序列化格式。

## 序列化格式对比

### JSON 序列化
- **优势**：人类可读，便于调试和开发
- **适用场景**：开发阶段、调试、日志记录
- **示例**：WebSocket Demo

### Protobuf 序列化
- **优势**：高效紧凑，适合生产环境
- **适用场景**：生产环境、高性能要求、网络带宽受限
- **示例**：QUIC Demo
- **注意**：当前使用 JSON 作为 Protobuf 的 fallback 实现

---

## WebSocket Demo - JSON 序列化

### 文件位置
`examples/websocket_demo.rs`

### 序列化配置
```rust
// 创建 JSON 消息解析器
let parser = MessageParser::new(PayloadCodec::Json);
```

### 消息结构
```rust
/// 示例消息结构 - 使用 JSON 序列化
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChatMessage {
    /// 消息 ID
    id: u32,
    /// 发送者
    sender: String,
    /// 消息内容
    content: String,
    /// 时间戳
    timestamp: u64,
}
```

### 发送消息
```rust
let codec = parser.codec();
for i in 1..=5 {
    let chat_msg = ChatMessage::new(
        i,
        "Client".to_string(),
        format!("Hello from WebSocket #{}", i),
    );
    
    // 使用 JSON 编码
    let payload = codec.encode(&chat_msg)?;
    
    let frame = FrameFactory::create_data_frame(
        FrameFactory::generate_message_id(),
        payload,
        Reliability::BestEffort,
    )?;

    conn.send_message(frame)?;
}
```

### 接收消息
```rust
fn on_message_received(&self, frame: Frame) {
    // 使用 JSON 反序列化
    let codec = self.parser.codec();
    match codec.decode::<ChatMessage>(&frame.payload) {
        Ok(msg) => {
            println!(
                "[{}] 📥 收到消息 [JSON]: #{} from {} - {}",
                self.name, msg.id, msg.sender, msg.content
            );
        }
        Err(_) => {
            // Fallback 到普通字符串
            let text = String::from_utf8_lossy(&frame.payload);
            println!("[{}] 📥 收到消息: {}", self.name, text);
        }
    }
}
```

### 运行示例
```bash
cargo run --example websocket_demo
```

### 输出示例
```
╔════════════════════════════════════════╗
║  Flare WebSocket 演示 (JSON 序列化)    ║
╚════════════════════════════════════════╝

📝 使用 JSON 序列化格式 - 人类可读，便于调试

🚀 WebSocket 服务端启动在 127.0.0.1:9001

[Client] 📥 收到消息 [JSON]: #0 from Server - Welcome to Flare WebSocket with JSON!
[Client] 📤 发送消息 [JSON]: #1 - Hello from WebSocket #1
[Client] 📤 发送消息 [JSON]: #2 - Hello from WebSocket #2
...
```

---

## QUIC Demo - Protobuf 序列化

### 文件位置
`examples/quic_demo.rs`

### 序列化配置
```rust
// 创建 Protobuf 消息解析器
let parser = MessageParser::new(PayloadCodec::Protobuf);
```

### 消息结构
```rust
/// 示例消息结构 - 使用 Protobuf 序列化（当前为 JSON fallback）
#[derive(Debug, Clone, Serialize, Deserialize)]
struct QuicMessage {
    /// 消息 ID
    id: u32,
    /// 消息类型
    msg_type: String,
    /// 消息内容
    content: String,
    /// 时间戳
    timestamp: u64,
    /// 序列号
    sequence: u32,
}
```

### 发送消息
```rust
for i in 1..=5 {
    let quic_msg = QuicMessage::new(
        i,
        "data".to_string(),
        format!("QUIC message #{}", i),
        i,
    );
    
    // 使用 Protobuf 编码
    let message_bytes = parser.codec().encode(&quic_msg)?;
    
    // 打开双向流
    let (mut send, mut recv) = connection.open_bi().await?;

    // 发送消息
    send.write_all(&message_bytes).await?;
    send.finish()?;
    
    println!("📤 [客户端] 发送 [Protobuf]: #{} - {}", quic_msg.id, quic_msg.content);

    // 接收响应
    let response = recv.read_to_end(65536).await?;
    
    // 解析响应
    match parser.codec().decode::<QuicMessage>(&response) {
        Ok(response_msg) => {
            println!(
                "📥 [客户端] 收到 [Protobuf]: #{} - {}",
                response_msg.id, response_msg.content
            );
        }
        Err(_) => {
            let response_str = String::from_utf8_lossy(&response);
            println!("📥 [客户端] 收到: {}", response_str);
        }
    }
}
```

### 服务端处理
```rust
match parser.codec().decode::<QuicMessage>(&data) {
    Ok(msg) => {
        println!(
            "📥 [服务端] 收到 [Protobuf]: #{} - {}",
            msg.id, msg.content
        );

        // 构造响应消息
        let response_msg = QuicMessage::new(
            msg.id + 1000,
            "response".to_string(),
            format!("Echo: {}", msg.content),
            msg.sequence,
        );
        
        // 使用 Protobuf 编码
        match parser.codec().encode(&response_msg) {
            Ok(response_bytes) => {
                send.write_all(&response_bytes).await?;
                println!(
                    "📤 [服务端] 回复 [Protobuf]: {}",
                    response_msg.content
                );
            }
            Err(e) => {
                eprintln!("Protobuf 编码失败: {:?}", e);
            }
        }
    }
    Err(_) => {
        // Fallback 到普通字符串
        let msg = String::from_utf8_lossy(&data);
        println!("📥 [服务端] 收到: {}", msg);
    }
}
```

### 运行示例
```bash
cargo run --example quic_demo
```

### 输出示例
```
╔════════════════════════════════════════╗
║  Flare QUIC 演示 (Protobuf 序列化)   ║
╚════════════════════════════════════════╝

📝 使用 Protobuf 序列化格式 - 高效紧凑，适合生产环境
⚠️  注：当前使用 JSON 作为 Protobuf 的 fallback 实现

✅ QUIC 连接建立成功

📤 [客户端] 发送 [Protobuf]: #1 - QUIC message #1
📥 [服务端] 收到 [Protobuf]: #1 - QUIC message #1
📤 [服务端] 回复 [Protobuf]: Echo: QUIC message #1
📥 [客户端] 收到 [Protobuf]: #1001 - Echo: QUIC message #1
...
📊 [客户端] 发送了 5 条 Protobuf 格式消息，全部收到响应
```

---

## 核心 API 使用

### 1. 创建消息解析器

```rust
// JSON 格式
let parser = MessageParser::new(PayloadCodec::Json);

// Protobuf 格式
let parser = MessageParser::new(PayloadCodec::Protobuf);
```

### 2. 获取编解码器

```rust
let codec = parser.codec();
```

### 3. 编码消息

```rust
let payload = codec.encode(&message)?;
```

### 4. 解码消息

```rust
let message: MyMessage = codec.decode(&payload)?;
```

---

## 技术要点

### 1. PayloadCodec 枚举

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadCodec {
    #[default]
    Json,
    Protobuf,
}
```

- **零成本抽象**：编译期内联，无运行时开销
- **类型安全**：编译期检查，避免运行时错误
- **可扩展**：容易添加新的序列化格式

### 2. MessageParser.codec() 方法

```rust
/// 获取 Payload 编解码器的引用
pub fn codec(&self) -> PayloadCodec {
    self.payload_codec
}
```

- **公共访问**：提供安全的内部字段访问
- **复制语义**：PayloadCodec 是 Copy 类型，调用无开销
- **线程安全**：可以在多个线程间共享

### 3. Serde 集成

所有消息结构必须实现 Serde 的 `Serialize` 和 `Deserialize` trait：

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MyMessage {
    // 字段定义
}
```

---

## 最佳实践

### 1. 选择合适的序列化格式

- **开发环境**：使用 JSON，便于调试
- **生产环境**：使用 Protobuf，提升性能
- **日志记录**：使用 JSON，便于查看

### 2. 错误处理

始终为序列化/反序列化操作提供 fallback：

```rust
match codec.decode::<MyMessage>(&payload) {
    Ok(msg) => {
        // 处理结构化消息
    }
    Err(_) => {
        // Fallback 到普通字符串或其他处理
        let text = String::from_utf8_lossy(&payload);
    }
}
```

### 3. 性能优化

- 复用 `MessageParser` 实例（它是 `Clone` 的）
- 批量处理消息以减少序列化开销
- 对于大消息，考虑流式处理

---

## 下一步

### 1. 实现真正的 Protobuf 支持

当前 Protobuf 使用 JSON fallback，未来可以集成：
- `prost` crate
- Protocol Buffers schema 定义
- 编译时代码生成

### 2. 添加更多序列化格式

```rust
pub enum PayloadCodec {
    Json,
    Protobuf,
    MessagePack,  // 待实现
    CBOR,         // 待实现
    Bincode,      // 待实现
}
```

### 3. 性能基准测试

对比不同序列化格式的：
- 编码/解码速度
- 序列化大小
- 内存使用

---

## 参考资料

- [Serde 官方文档](https://serde.rs/)
- [Protocol Buffers](https://developers.google.com/protocol-buffers)
- [JSON vs Protobuf 性能对比](https://auth0.com/blog/beating-json-performance-with-protobuf/)

---

**最后更新**: 2025-10-16
**作者**: Flare Core Team
