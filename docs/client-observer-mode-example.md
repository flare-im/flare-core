# 客户端观察者模式示例

观察者模式提供了基本的功能实现，适合需要自定义消息处理逻辑但不需要完整功能集的场景。

## 特点

- ✅ **实现 `ConnectionObserver` trait**
- ✅ **支持自定义事件处理**
- ✅ **支持协议竞速**：自动选择最快的协议（WebSocket 或 QUIC）
- ✅ **支持消息路由**
- ✅ **灵活扩展**

## 适用场景

- 需要多协议支持的应用
- 需要协议竞速优化连接速度
- 需要自定义消息处理逻辑但不需要完整功能集
- 需要更灵活的事件处理

## 完整示例

```rust
use flare_core::client::ObserverClientBuilder;
use flare_core::common::config_types::{HeartbeatConfig, TransportProtocol};
use flare_core::common::protocol::flare::core::commands::command::Type;
use flare_core::common::protocol::{
    Reliability, frame_with_message_command, generate_message_id, send_message,
};
use flare_core::transport::events::{ConnectionEvent, ConnectionObserver};
use std::io::{self, Write};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};

// ============================================================
// 消息观察者，用于接收和显示聊天消息
// ============================================================
struct ChatObserver {
    username: String,
    message_count: Arc<std::sync::atomic::AtomicU64>,
}

impl ChatObserver {
    fn new(username: String) -> Self {
        Self {
            username,
            message_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    fn get_message_count(&self) -> u64 {
        self.message_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl ConnectionObserver for ChatObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Connected => {
                println!("✅ 已连接到聊天室服务器");
            }
            ConnectionEvent::Disconnected(reason) => {
                println!("❌ 连接断开: {}", reason);
            }
            ConnectionEvent::Error(error) => {
                eprintln!("❌ 连接错误: {}", error);
            }
            ConnectionEvent::Message(data) => {
                // 解析接收到的消息（默认使用JSON，parse()会自动检测实际格式）
                if let Ok(frame) = flare_core::common::MessageParser::json().parse(data) {
                    if let Some(cmd) = &frame.command {
                        if let Some(Type::Message(msg_cmd)) = &cmd.r#type {
                            // 提取消息内容
                            let message_text = String::from_utf8_lossy(&msg_cmd.payload);

                            // 提取用户名（如果有）
                            let username = msg_cmd
                                .metadata
                                .get("username")
                                .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
                                .unwrap_or_else(|| "未知用户".to_string());

                            println!("[{}] {}", username, message_text);

                            // 更新消息计数
                            self.message_count
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("=== 观察者模式聊天室客户端示例 ===");
    println!("使用 ObserverClientBuilder 构建，支持协议竞速");

    // 获取用户名（从命令行参数或使用默认值）
    let username = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "用户".to_string());

    println!("\n正在连接到聊天室服务器（协议竞速：WebSocket 和 QUIC）...");
    println!("提示: 将自动选择最快的可用协议");

    // ============================================================
    // 创建观察者
    // ============================================================
    let observer = Arc::new(ChatObserver::new(username.clone()));
    let observer_clone = Arc::clone(&observer);

    // ============================================================
    // 配置心跳
    // ============================================================
    let heartbeat_config = HeartbeatConfig {
        enabled: true,
        interval: Duration::from_secs(30),
        timeout: Duration::from_secs(60),
    };

    // ============================================================
    // 使用 ObserverClientBuilder 创建客户端（协议竞速）
    // ============================================================
    let mut client = match ObserverClientBuilder::new("127.0.0.1:8080")
        .with_observer(observer as Arc<dyn ConnectionObserver>)
        // 协议竞速：同时尝试 WebSocket 和 QUIC，选择最快的
        .with_protocol_race(vec![TransportProtocol::QUIC, TransportProtocol::WebSocket])
        .with_protocol_url(
            TransportProtocol::WebSocket,
            "ws://127.0.0.1:8080".to_string(),
        )
        .with_protocol_url(TransportProtocol::QUIC, "quic://127.0.0.1:8081".to_string())
        .with_heartbeat(heartbeat_config)
        .with_connect_timeout(Duration::from_secs(5))
        .build_with_race()
        .await
    {
        Ok(client) => {
            println!("✅ 连接成功");
            println!("📡 使用的协议: {:?}", client.active_protocol());
            client
        }
        Err(e) => {
            eprintln!("❌ 连接失败: {}", e);
            return Err(e.into());
        }
    };

    println!("\n💬 开始聊天（输入消息并按回车发送，输入 'quit' 退出）");

    // ============================================================
    // 消息发送循环
    // ============================================================
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    loop {
        // 读取用户输入
        print!("> ");
        io::stdout().flush()?;
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            break;
        }

        let message = line.trim();
        if message.is_empty() {
            continue;
        }

        // 检查退出命令
        if message == "quit" || message == "exit" {
            println!("正在断开连接...");
            break;
        }

        // 构建消息元数据（包含用户名）
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("username".to_string(), username.as_bytes().to_vec());

        // 创建消息命令
        let msg_cmd = send_message(
            generate_message_id(),
            message.as_bytes().to_vec(),
            Some(metadata),
            None,
        );

        let frame = frame_with_message_command(msg_cmd, Reliability::BestEffort);

        // 发送消息
        if let Err(e) = client.send_frame(&frame).await {
            eprintln!("❌ 发送消息失败: {}", e);
            break;
        }
    }

    // ============================================================
    // 断开连接
    // ============================================================
    client.disconnect().await?;
    println!("\n✅ 已断开连接");
    println!("📊 总共接收了 {} 条消息", observer_clone.get_message_count());

    Ok(())
}
```

## 关键说明

### 1. ConnectionObserver trait

必须实现 `ConnectionObserver` trait，包含以下方法：

```rust
impl ConnectionObserver for MyObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Connected => {
                // 连接成功建立
            }
            ConnectionEvent::Disconnected(reason) => {
                // 连接断开（包含原因）
            }
            ConnectionEvent::Error(error) => {
                // 连接错误
            }
            ConnectionEvent::Message(data) => {
                // 收到消息数据（Vec<u8>，需要手动解析）
                if let Ok(frame) = flare_core::common::MessageParser::json().parse(data) {
                    // 处理 frame
                }
            }
        }
    }
}
```

### 2. 协议竞速

支持同时尝试多个协议，自动选择最快的：

```rust
.with_protocol_race(vec![TransportProtocol::QUIC, TransportProtocol::WebSocket])
.with_protocol_url(TransportProtocol::WebSocket, "ws://127.0.0.1:8080".to_string())
.with_protocol_url(TransportProtocol::QUIC, "quic://127.0.0.1:8081".to_string())
```

协议列表的顺序就是优先级顺序，QUIC 在前表示 QUIC 优先级更高。

### 3. 消息解析

`ConnectionEvent::Message` 提供的是原始字节数据，需要手动解析：

```rust
ConnectionEvent::Message(data) => {
    // 使用 MessageParser 解析消息
    if let Ok(frame) = flare_core::common::MessageParser::json().parse(data) {
        // 处理 frame
        if let Some(cmd) = &frame.command {
            if let Some(Type::Message(msg_cmd)) = &cmd.r#type {
                let message_text = String::from_utf8_lossy(&msg_cmd.payload);
                // 处理消息
            }
        }
    }
}
```

### 4. 配置选项

```rust
.with_observer(observer) // 设置观察者（必需）
.with_protocol_race(protocols) // 协议竞速
.with_protocol_url(protocol, url) // 协议URL
.with_heartbeat(heartbeat_config) // 心跳配置
.with_connect_timeout(timeout) // 连接超时
.with_reconnect_interval(interval) // 重连间隔
.with_max_reconnect_attempts(max) // 最大重连次数
```

### 5. 运行示例

```bash
# 使用默认日志级别（info）
cargo run --example observer_client

# 使用 debug 级别查看详细信息
RUST_LOG=debug cargo run --example observer_client

# 指定用户名
cargo run --example observer_client -- user123
```
