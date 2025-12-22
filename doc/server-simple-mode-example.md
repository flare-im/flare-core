# 服务端简单模式示例

简单模式提供最小实现，使用闭包定义消息处理逻辑，适合快速原型开发和学习。

## 特点

- ✅ **零配置**：使用默认配置即可运行
- ✅ **轻量级**：不包含中间件、管道等高级功能
- ✅ **快速上手**：几行代码即可启动服务器
- ✅ **灵活**：使用闭包定义处理逻辑，无需实现 trait
- ✅ **仅支持 WebSocket**：默认使用 WebSocket 协议（ws://），不支持 TLS/SSL

## 适用场景

- 快速原型开发
- 小型应用
- 学习和测试
- 需要完全控制消息处理流程的场景

## 完整示例

```rust
use flare_core::server::ServerBuilder;
use flare_core::common::protocol::flare::core::commands::command::Type;
use flare_core::common::protocol::{
    Reliability, frame_with_message_command, generate_message_id, send_message,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, error};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("=== 简单模式服务器示例 ===");

    // 存储连接ID到用户名的映射（示例中使用共享状态）
    let usernames: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

    // 使用 ServerBuilder 创建服务端，使用闭包定义消息处理逻辑
    let usernames_for_message = Arc::clone(&usernames);
    let usernames_for_connect = Arc::clone(&usernames);
    let usernames_for_disconnect = Arc::clone(&usernames);

    let mut server = ServerBuilder::new("0.0.0.0:8080")
        // ============================================================
        // 处理接收到的消息
        // ============================================================
        .on_message(move |frame, ctx| {
            let usernames = Arc::clone(&usernames_for_message);
            Box::pin(async move {
                // 检查是否是消息命令
                if let Some(cmd) = &frame.command {
                    if let Some(Type::Message(msg_cmd)) = &cmd.r#type {
                        // 提取消息内容
                        let message_text = String::from_utf8_lossy(&msg_cmd.payload);

                        // 获取或创建用户名
                        let username = {
                            let mut usernames = usernames.lock().await;
                            usernames
                                .entry(ctx.connection_id.clone())
                                .or_insert_with(|| {
                                    // 如果消息包含用户名信息，提取用户名
                                    if let Some(username_bytes) = msg_cmd.metadata.get("username") {
                                        String::from_utf8_lossy(username_bytes).to_string()
                                    } else {
                                        format!("用户_{}", &ctx.connection_id[..8.min(ctx.connection_id.len())])
                                    }
                                })
                                .clone()
                        };

                        info!("[聊天室] {} 说: {}", username, message_text);

                        // 构建广播消息（包含用户名）
                        let mut broadcast_metadata = HashMap::new();
                        broadcast_metadata.insert("username".to_string(), username.as_bytes().to_vec());
                        broadcast_metadata.insert(
                            "connection_id".to_string(),
                            ctx.connection_id.as_bytes().to_vec(),
                        );

                        let broadcast_msg = send_message(
                            generate_message_id(),
                            format!("[{}] {}", username, message_text).into_bytes(),
                            Some(broadcast_metadata),
                            None,
                        );

                        let broadcast_frame =
                            frame_with_message_command(broadcast_msg, Reliability::BestEffort);

                        // 广播给除发送者外的所有连接
                        let conn_id = ctx.connection_id.clone();
                        if let Err(e) = ctx.broadcast_except(&broadcast_frame, &conn_id).await {
                            error!("广播消息失败: {}", e);
                        }
                    }
                }
                // 返回 None 表示不需要发送响应，或返回 Some(Frame) 发送响应
                Ok(None)
            })
        })
        // ============================================================
        // 处理连接建立事件
        // ============================================================
        .on_connect(move |conn_id, _ctx| {
            let usernames = Arc::clone(&usernames_for_connect);
            Box::pin(async move {
                let conn_id = conn_id.to_string();
                info!("[聊天室] ✅ 用户 {} 加入聊天室", &conn_id[..8.min(conn_id.len())]);

                // 初始化用户名（使用默认名称）
                {
                    let mut usernames = usernames.lock().await;
                    usernames
                        .entry(conn_id.clone())
                        .or_insert_with(|| format!("用户_{}", &conn_id[..8.min(conn_id.len())]));
                }

                Ok(())
            })
        })
        // ============================================================
        // 处理连接断开事件
        // ============================================================
        .on_disconnect(move |conn_id, _ctx| {
            let usernames = Arc::clone(&usernames_for_disconnect);
            Box::pin(async move {
                let conn_id = conn_id.to_string();
                let username = {
                    let mut usernames = usernames.lock().await;
                    usernames.remove(&conn_id)
                };

                let display_name = username
                    .as_deref()
                    .unwrap_or(&conn_id[..8.min(conn_id.len())]);
                info!("[聊天室] ❌ {} 离开了聊天室", display_name);
                Ok(())
            })
        })
        .build()?;

    // 启动服务器
    server.start().await?;
    info!("✅ 聊天室服务器已启动：0.0.0.0:8080");
    info!("使用 ws:// 协议连接（非 wss://）");
    info!("\n服务器运行中，按 Ctrl+C 停止...");

    // 等待停止信号
    tokio::signal::ctrl_c().await?;

    info!("\n正在停止服务器...");
    server.stop().await?;
    info!("服务器已停止");

    Ok(())
}
```

## 关键说明

### 1. 闭包参数说明

- **`on_message`**：
  - `frame: &Frame` - 接收到的消息 Frame
  - `ctx: &MessageContext` - 消息上下文，包含 `connection_id` 和 `ServerHandle`（用于广播、发送消息等）
  - 返回值：`Result<Option<Frame>>` - 返回 `None` 表示不需要响应，或返回 `Some(Frame)` 发送响应

- **`on_connect`**：
  - `conn_id: &str` - 连接ID
  - `ctx: &MessageContext` - 消息上下文
  - 返回值：`Result<()>`

- **`on_disconnect`**：
  - `conn_id: &str` - 连接ID
  - `ctx: &MessageContext` - 消息上下文
  - 返回值：`Result<()>`

### 2. 消息广播

使用 `ctx.broadcast_except()` 可以广播消息给除指定连接外的所有连接：

```rust
// 广播给除发送者外的所有连接
ctx.broadcast_except(&broadcast_frame, &ctx.connection_id).await?;

// 广播给所有连接（包括发送者）
ctx.broadcast(&broadcast_frame).await?;

// 发送给指定连接
ctx.send(&frame, &target_connection_id).await?;
```

### 3. 共享状态管理

由于闭包需要 `move` 语义，如果需要共享状态，可以使用 `Arc<Mutex<T>>`：

```rust
let shared_state: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

// 在每个闭包中克隆 Arc
let state_for_message = Arc::clone(&shared_state);
.on_message(move |frame, ctx| {
    let state = Arc::clone(&state_for_message);
    Box::pin(async move {
        let mut state = state.lock().await;
        // 使用 state...
        Ok(None)
    })
})
```

### 4. 运行示例

```bash
# 使用默认日志级别（info）
cargo run --example simple_server

# 使用 debug 级别查看详细信息
RUST_LOG=debug cargo run --example simple_server
```
