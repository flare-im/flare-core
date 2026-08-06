# 客户端简单模式示例

简单模式提供最小实现，使用闭包定义消息处理逻辑，适合快速原型开发和学习。

## 特点

- ✅ **零配置**：使用默认配置即可运行
- ✅ **轻量级**：不包含中间件、管道等高级功能
- ✅ **快速上手**：几行代码即可启动客户端
- ✅ **灵活**：使用闭包定义处理逻辑，无需实现 trait
- ✅ **仅支持 WebSocket**：默认使用 WebSocket 协议（ws://）

## 适用场景

- 快速原型开发
- 小型应用
- 学习和测试
- 需要完全控制消息处理流程的场景

## 完整示例

```rust
use flare_core::client::ClientBuilder;
use flare_core::common::config_types::HeartbeatConfig;
use flare_core::common::protocol::flare::core::commands::command::Type;
use flare_core::common::protocol::{
    Reliability, frame_with_message_command, generate_message_id, send_message,
};
use flare_core::transport::events::ConnectionEvent;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

// 全局消息计数器（用于测试和统计）
static MESSAGE_COUNT: AtomicU64 = AtomicU64::new(0);

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("=== 简单模式客户端示例 ===");

    // 获取用户名
    print!("请输入您的用户名: ");
    io::stdout().flush()?;
    let mut username = String::new();
    io::stdin().read_line(&mut username)?;
    let username = username.trim().to_string();

    if username.is_empty() {
        println!("用户名不能为空");
        return Err("用户名不能为空".into());
    }

    println!("\n正在连接到服务器...");

    // 配置心跳（30秒间隔，60秒超时）
    let heartbeat_config = HeartbeatConfig {
        enabled: true,
        interval: Duration::from_secs(30),
        timeout: Duration::from_secs(60),
    };

    // ============================================================
    // 使用 ClientBuilder 创建客户端，使用闭包定义消息处理逻辑
    // ============================================================
    let mut client = ClientBuilder::new("ws://127.0.0.1:8080")
        // ============================================================
        // 处理接收到的消息
        // ============================================================
        .on_message({
            let username = username.clone();
            move |frame| {
                // 更新消息计数
                MESSAGE_COUNT.fetch_add(1, Ordering::Relaxed);

                // 检查是否是消息命令
                if let Some(cmd) = &frame.command {
                    if let Some(Type::Message(msg_cmd)) = &cmd.r#type {
                        let message = String::from_utf8_lossy(&msg_cmd.payload);

                        // 检查是否是系统通知
                        if let Some(type_bytes) = msg_cmd.metadata.get("type") {
                            let msg_type = String::from_utf8_lossy(type_bytes);
                            if msg_type == "join" || msg_type == "leave" {
                                println!("\n[系统] {}", message);
                            } else {
                                println!("\n{}", message);
                            }
                        } else {
                            println!("\n{}", message);
                        }

                        // 显示输入提示
                        print!("{}> ", username);
                        let _ = io::stdout().flush();
                    }
                }
                Ok(())
            }
        })
        // ============================================================
        // 处理连接事件
        // ============================================================
        .on_event({
            let username = username.clone();
            move |event| {
                match event {
                    ConnectionEvent::Connected => {
                        println!("\n[系统] ✅ 已连接到服务器！");
                        print!("{}> ", username);
                        let _ = io::stdout().flush();
                    }
                    ConnectionEvent::Disconnected(reason) => {
                        println!("\n[系统] ❌ 连接已断开: {}", reason);
                    }
                    ConnectionEvent::Error(e) => {
                        eprintln!("\n[错误] {:?}", e);
                    }
                    _ => {}
                }
            }
        })
        // ============================================================
        // 配置选项
        // ============================================================
        .with_heartbeat(heartbeat_config) // 心跳配置
        .with_reconnect_interval(Duration::from_secs(3)) // 重连间隔
        .with_max_reconnect_attempts(Some(5)) // 最大重连次数
        .with_connect_timeout(Duration::from_secs(10)) // 连接超时
        .build()?;

    // ============================================================
    // 连接服务器
    // ============================================================
    println!("正在连接...");
    match client.connect().await {
        Ok(_) => {
            println!("✅ 连接成功！");
            println!("使用的协议: {:?}", client.active_protocol());
            println!("连接 ID: {:?}", client.connection_id());
            println!();
        }
        Err(e) => {
            eprintln!("❌ 连接失败: {}", e);
            return Err(format!("连接失败: {}", e).into());
        }
    }

    // ============================================================
    // 发送消息
    // ============================================================
    println!("💬 开始聊天（输入消息并按回车发送，输入 'quit' 退出）");

    let stdin = tokio::io::stdin();
    let mut reader = tokio::io::BufReader::new(stdin);
    let mut line = String::new();

    loop {
        print!("{}> ", username);
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
    println!("📊 总共接收了 {} 条消息", MESSAGE_COUNT.load(Ordering::Relaxed));

    Ok(())
}
```

## 关键说明

### 1. 闭包参数说明

- **`on_message`**：
  - `frame: &Frame` - 接收到的消息 Frame
  - 返回值：`Result<()>`

- **`on_event`**：
  - `event: &ConnectionEvent` - 连接事件
    - `Connected` - 连接成功建立
    - `Disconnected(reason: String)` - 连接断开（包含原因）
    - `Message(data: Vec<u8>)` - 收到消息数据
    - `Error(error: FlareError)` - 连接错误

### 2. 配置选项

```rust
.with_heartbeat(heartbeat_config) // 心跳配置
.with_reconnect_interval(Duration::from_secs(3)) // 重连间隔
.with_max_reconnect_attempts(Some(5)) // 最大重连次数（None 表示无限重连）
.with_connect_timeout(Duration::from_secs(10)) // 连接超时
```

### 3. 发送消息

```rust
// 构建消息命令
let msg_cmd = send_message(
    generate_message_id(),
    payload.as_bytes().to_vec(),
    Some(metadata), // 可选元数据
    None,           // 可选扩展字段
);

// 创建 Frame
let frame = frame_with_message_command(msg_cmd, Reliability::BestEffort);

// 发送消息
client.send_frame(&frame).await?;
```

### 4. 连接管理

```rust
// 连接服务器
client.connect().await?;

// 检查连接状态
if client.is_connected() {
    // 已连接
}

// 获取连接ID
let conn_id = client.connection_id();

// 获取使用的协议
let protocol = client.active_protocol();

// 断开连接
client.disconnect().await?;
```

### 5. 运行示例

```bash
# 使用默认日志级别（info）
cargo run --example simple_client

# 使用 debug 级别查看详细信息
RUST_LOG=debug cargo run --example simple_client
```
