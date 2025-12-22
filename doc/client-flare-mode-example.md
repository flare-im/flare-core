# 客户端 Flare 模式示例

Flare 模式提供完整功能，包含所有 `common` 和 `client` 模块的能力，是最强大的构建模式。

## 特点

- ✅ **必须实现 `MessageListener` trait**（核心接口）
- ✅ **消息管道**：自动处理序列化、压缩、加密
- ✅ **中间件支持**：日志、性能监控、验证等
- ✅ **自动重连**：支持断线重连
- ✅ **协议竞速**：自动选择最快的协议
- ✅ **序列化协商**：自动协商最佳序列化格式和压缩算法
- ✅ **加密支持**：支持 AES-256-GCM 等加密算法
- ✅ **设备管理**：支持多平台设备信息，自动处理设备冲突

## 适用场景

- 生产环境
- 需要完整功能的企业应用
- 需要高性能和可扩展性的场景
- 需要统一消息处理流程的场景
- 需要加密通信的安全应用
- 需要设备管理的多端应用

## 完整示例

```rust
use async_trait::async_trait;
use flare_core::client::*;
use flare_core::client::builder::flare::MessageListener;
use flare_core::common::compression::CompressionAlgorithm;
use flare_core::common::config_types::{HeartbeatConfig, TransportProtocol};
use flare_core::common::device::{DeviceInfo, DevicePlatform};
use flare_core::common::encryption::{Aes256GcmEncryptor, EncryptionAlgorithm, EncryptionUtil};
use flare_core::common::error::Result;
use flare_core::common::message::{
    ArcMessageMiddleware, LogLevel, LoggingMiddleware, MetricsMiddleware,
};
use flare_core::common::protocol::flare::core::commands::command::Type;
use flare_core::common::protocol::{
    Frame, Reliability, SerializationFormat, frame_with_message_command, generate_message_id,
    send_message,
};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};

// ============================================================
// 消息监听器，实现 MessageListener trait
// ============================================================
struct ChatMessageListener;

#[async_trait]
impl MessageListener for ChatMessageListener {
    async fn on_message(
        &self,
        frame: &Frame,
        _context: &MessageContext,
    ) -> Result<Option<Frame>> {
        // 检查是否是消息命令
        if let Some(cmd) = &frame.command {
            if let Some(Type::Message(msg_cmd)) = &cmd.r#type {
                let message_text = String::from_utf8_lossy(&msg_cmd.payload);

                // 提取用户名（如果有）
                let username = msg_cmd
                    .metadata
                    .get("username")
                    .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
                    .unwrap_or_else(|| "未知用户".to_string());

                println!("[{}] {}", username, message_text);
            }
        }

        // 返回 None 表示不需要响应，或返回 Some(Frame) 发送响应
        Ok(None)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("🚀 启动 Flare 聊天室客户端（完整功能演示）");

    // ============================================================
    // 1. 注册加密器（可选，用于加密通信）
    // ============================================================
    // 注意：在生产环境中，密钥应该从安全配置中读取，不要硬编码
    // 这里使用与服务端相同的示例密钥（32 字节）
    let encryption_key = b"01234567890123456789012345678901"; // 32 bytes for AES-256
    let encryptor = Aes256GcmEncryptor::new(encryption_key)?;
    EncryptionUtil::register_custom(Arc::new(encryptor));
    println!("🔐 已注册 AES-256-GCM 加密器");

    // ============================================================
    // 2. 获取用户ID（用于测试多设备互斥）
    // ============================================================
    let user_id = std::env::args()
        .nth(1)
        .unwrap_or_else(|| format!("user-{}", std::process::id()));

    println!("✅ 用户ID: {}", user_id);

    // ============================================================
    // 3. 创建设备信息（用于设备管理）
    // ============================================================
    let device_platform = std::env::var("DEVICE_PLATFORM")
        .unwrap_or_else(|_| "pc".to_string())
        .parse::<DevicePlatform>()
        .unwrap_or(DevicePlatform::Pc);

    let device_info = DeviceInfo {
        device_id: format!("device-{}", std::process::id()),
        platform: device_platform,
        model: Some("Example Device".to_string()),
        app_version: Some("1.0.0".to_string()),
        system_version: Some("Linux 5.0".to_string()),
    };

    println!("📱 设备信息: {:?}", device_info);

    // ============================================================
    // 4. 创建消息监听器
    // ============================================================
    let listener = Arc::new(ChatMessageListener);

    // ============================================================
    // 5. 配置心跳
    // ============================================================
    let heartbeat_config = HeartbeatConfig {
        enabled: true,
        interval: std::time::Duration::from_secs(30),
        timeout: std::time::Duration::from_secs(60),
    };

    // ============================================================
    // 6. 创建中间件（可选）
    // ============================================================
    let middleware: Vec<ArcMessageMiddleware> = vec![
        // 日志中间件
        Arc::new(LoggingMiddleware::new(LogLevel::Info)),
        // 性能监控中间件
        Arc::new(MetricsMiddleware::new()),
    ];

    // ============================================================
    // 7. 使用 FlareClientBuilder 构建客户端
    // ============================================================
    let mut client = FlareClientBuilder::new("127.0.0.1:8080")
        .with_listener(listener)
        // 协议竞速：自动选择最快的协议
        .with_protocol_race(vec![TransportProtocol::QUIC, TransportProtocol::WebSocket])
        .with_protocol_url(
            TransportProtocol::WebSocket,
            "ws://127.0.0.1:8080".to_string(),
        )
        .with_protocol_url(TransportProtocol::QUIC, "quic://127.0.0.1:8081".to_string())
        // 协商配置（客户端可以指定格式，服务端会协商使用）
        .with_format(SerializationFormat::Protobuf) // 可选：指定格式（或使用服务端默认）
        .with_compression(CompressionAlgorithm::Gzip) // 可选：指定压缩（或使用服务端默认）
        .with_encryption(EncryptionAlgorithm::Aes256Gcm) // 可选：指定加密（或使用服务端默认）
        // 设备信息
        .with_device_info(device_info)
        // 心跳配置
        .with_heartbeat(heartbeat_config)
        // 重连配置
        .with_reconnect_interval(std::time::Duration::from_secs(3))
        .with_max_reconnect_attempts(Some(5))
        // 连接超时
        .with_connect_timeout(std::time::Duration::from_secs(10))
        // 中间件
        .with_middleware(middleware)
        .build_with_race()
        .await?;

    println!("✅ 连接成功！");
    println!("📡 使用的协议: {:?}", client.active_protocol());
    println!("🔗 连接 ID: {:?}", client.connection_id());
    println!();

    // ============================================================
    // 8. 发送消息
    // ============================================================
    println!("💬 开始聊天（输入消息并按回车发送，输入 'quit' 退出）");

    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    loop {
        print!("> ");
        use std::io::Write;
        std::io::stdout().flush().unwrap();
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
        metadata.insert("username".to_string(), user_id.as_bytes().to_vec());

        // 创建消息命令
        let msg_cmd = send_message(
            generate_message_id(),
            message.as_bytes().to_vec(),
            Some(metadata),
            None,
        );

        let frame = frame_with_message_command(msg_cmd, Reliability::BestEffort);

        // 发送消息（消息管道会自动处理序列化、压缩、加密）
        if let Err(e) = client.send_frame(&frame).await {
            eprintln!("❌ 发送消息失败: {}", e);
            break;
        }
    }

    // ============================================================
    // 9. 断开连接
    // ============================================================
    client.disconnect().await?;
    println!("\n✅ 已断开连接");

    Ok(())
}
```

## 关键说明

### 1. MessageListener trait

必须实现 `MessageListener` trait，包含以下方法：

```rust
#[async_trait]
impl MessageListener for MyListener {
    async fn on_message(
        &self,
        frame: &Frame,
        context: &MessageContext,
    ) -> Result<Option<Frame>> {
        // 处理消息
        // frame: 接收到的消息 Frame（已经过消息管道处理：解密、解压、反序列化）
        // context: 消息上下文，包含连接信息等
        
        // 返回 None 表示不需要响应，或返回 Some(Frame) 发送响应
        Ok(None)
    }
}
```

### 2. 消息管道

Flare 模式的消息管道会自动处理：

1. **序列化**：自动将 Frame 序列化为字节（JSON 或 Protobuf）
2. **压缩**：自动压缩数据（Gzip、Zstd 或 None）
3. **加密**：自动加密数据（AES-256-GCM 或 None）
4. **中间件**：按顺序执行中间件（日志、性能监控等）

接收消息时，管道会按相反顺序处理：
1. **解密**
2. **解压**
3. **反序列化**
4. **中间件**

### 3. 中间件支持

可以添加多个中间件，按顺序执行：

```rust
let middleware: Vec<ArcMessageMiddleware> = vec![
    // 日志中间件
    Arc::new(LoggingMiddleware::new(LogLevel::Info)),
    // 性能监控中间件
    Arc::new(MetricsMiddleware::new()),
    // 自定义中间件
    // Arc::new(MyCustomMiddleware::new()),
];

.with_middleware(middleware)
```

### 4. 设备管理

可以指定设备信息，用于设备冲突管理：

```rust
let device_info = DeviceInfo {
    device_id: "device-001".to_string(),
    platform: DevicePlatform::Pc, // 或 Android, Ios, Web, HarmonyOs
    model: Some("Example Device".to_string()),
    app_version: Some("1.0.0".to_string()),
    system_version: Some("Linux 5.0".to_string()),
};

.with_device_info(device_info)
```

### 5. 加密支持

需要先注册加密器，然后在配置中启用：

```rust
// 注册加密器（必须与服务端使用相同的密钥）
let encryption_key = b"01234567890123456789012345678901"; // 32 bytes for AES-256
let encryptor = Aes256GcmEncryptor::new(encryption_key)?;
EncryptionUtil::register_custom(Arc::new(encryptor));

// 在构建器中启用加密
.with_encryption(EncryptionAlgorithm::Aes256Gcm)
```

### 6. 序列化协商

可以指定客户端偏好的格式，服务端会根据客户端能力协商使用：

```rust
.with_format(SerializationFormat::Protobuf) // 或 Json
.with_compression(CompressionAlgorithm::Gzip) // 或 None, Zstd
.with_encryption(EncryptionAlgorithm::Aes256Gcm) // 或 None
```

如果不指定，将使用服务端默认的格式。

### 7. 运行示例

```bash
# 使用默认日志级别（info）
cargo run --example flare_chat_client

# 指定用户ID
cargo run --example flare_chat_client -- user123

# 指定设备平台（用于测试设备冲突）
DEVICE_PLATFORM=android cargo run --example flare_chat_client -- user123
```
