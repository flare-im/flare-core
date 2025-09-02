# Common 模块使用示例

本目录包含了 `flare-core` 中 `common` 模块的 WebSocket 连接使用示例，展示了如何建立简单的服务端和客户端联动。

## 示例文件

### `websocket_connection_demo.rs` - WebSocket 连接演示
演示如何使用 WebSocket 协议建立服务端和客户端联动。

**特性：**
- WebSocket 服务端监听
- WebSocket 客户端连接
- 实时消息收发
- 用户交互输入
- 心跳机制
- 事件处理

**运行方式：**
```bash
# 启动服务端
RUST_LOG=info cargo run --example websocket_connection_demo server

# 启动客户端
RUST_LOG=info cargo run --example websocket_connection_demo client
```

**演示内容：**
1. **服务端**: 监听 WebSocket 连接，接收并打印客户端消息
2. **客户端**: 连接到服务端，用户可以输入消息发送
3. **实时通信**: 客户端输入的消息会实时发送到服务端
4. **心跳监控**: 自动心跳检测连接状态

**使用步骤：**
1. 先启动服务端：`cargo run --example websocket_connection_demo server`
2. 再启动客户端：`cargo run --example websocket_connection_demo client`
3. 在客户端输入消息，服务端会收到并打印
4. 输入 'quit' 退出客户端

**特性：**
- 协议特性分析和配置
- 多协议同时连接
- 配置优化策略
- 协议动态切换
- 统一事件处理

**运行方式：**
```bash
# 运行完整演示（需要启用相应协议特性）
cargo run --example unified_connection_demo --features websocket,quic

# 仅启用 WebSocket
cargo run --example unified_connection_demo --features websocket

# 仅启用 QUIC
cargo run --example unified_connection_demo --features quic
```

**演示内容：**
1. **协议特性演示** - 分析不同协议的能力和配置选项
2. **多协议连接演示** - 同时使用多种协议建立连接
3. **配置优化演示** - 针对不同场景的配置优化策略
4. **协议切换演示** - 动态切换不同协议的连接

## 核心概念演示

### 连接工厂 (ConnectionFactory)
```rust
use flare_core::connections::ConnectionFactory;

let factory = ConnectionFactory::new();

// 创建客户端连接
let client_conn = factory.create_client_connection(config).await?;

// 创建服务端连接
let server_conn = factory.create_server_connection(config).await?;
```

### 连接配置 (ConnectionConfig)
```rust
use flare_core::connections::ConnectionConfig;

// 基础客户端配置
let config = ConnectionConfig::client("id".to_string(), "addr".to_string())
    .with_type(ConnectionType::WebSocket)
    .with_heartbeat(30000, 10000)
    .with_reconnect(5, 1000);

// 预定义配置
let perf_config = ConnectionConfig::high_performance("perf".to_string(), "addr".to_string());
let latency_config = ConnectionConfig::low_latency("latency".to_string(), "addr".to_string());
let stable_config = ConnectionConfig::stable("stable".to_string(), "addr".to_string());
```

### 事件处理 (ConnectionEventHandler)
```rust
use flare_core::connections::{ConnectionEventHandler, DefaultConnectionEventHandler};

// 使用默认事件处理器
let default_handler = Arc::new(DefaultConnectionEventHandler::new());

// 自定义事件处理器
pub struct CustomEventHandler;

#[async_trait::async_trait]
impl ConnectionEventHandler for CustomEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        println!("连接已建立: {}", connection_id);
    }
    
    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        println!("连接已断开: {} - 原因: {}", connection_id, reason);
    }
    
    // ... 实现其他事件处理方法
}
```

### 消息收发
```rust
use flare_core::protocol::Frame;

// 发送消息
let message = Frame::new(
    "Hello, World!".to_string(),
    "text".to_string(),
    None,
);
connection.send_message(message).await?;

// 接收消息
if let Some(message) = connection.receive_message().await? {
    println!("收到消息: {:?}", message);
}
```

### 心跳管理
```rust
// 客户端启动心跳
connection.start_heartbeat().await?;

// 服务端启动心跳监控
connection.start_heartbeat_monitoring().await?;

// 停止心跳
connection.stop_heartbeat().await?;
connection.stop_heartbeat_monitoring().await?;
```

### 连接状态监控
```rust
// 获取连接状态
let state = connection.get_state().await;
let is_active = connection.is_active().await;
let last_activity = connection.get_last_activity().await;

// 检查连接健康状态（服务端）
let is_healthy = connection.is_healthy().await;

// 获取连接统计
let stats = connection.get_connection_stats().await;
```

## 配置选项详解

### 心跳配置
- `heartbeat_interval_ms`: 心跳发送间隔（毫秒）
- `heartbeat_timeout_ms`: 心跳超时时间（毫秒）
- `max_missed_heartbeats`: 最大丢失心跳次数

### 重连配置（仅客户端）
- `auto_reconnect`: 是否自动重连
- `max_reconnect_attempts`: 最大重连次数
- `reconnect_delay_ms`: 重连延迟时间（毫秒）

### 缓冲区配置
- `buffer_size`: 缓冲区大小（字节）
- `max_message_size`: 最大消息大小（字节）

### 超时配置
- `timeout_ms`: 连接超时时间（毫秒）
- `heartbeat_monitor_timeout_ms`: 心跳监控超时（服务端）
- `cleanup_interval_ms`: 连接清理间隔（服务端）

## 预定义配置

### 高性能配置
- 256KB 缓冲区
- 16MB 最大消息
- 15秒心跳间隔
- 适合高吞吐量场景

### 低延迟配置
- 32KB 缓冲区
- 1MB 最大消息
- 10秒心跳间隔
- 适合实时通信场景

### 稳定连接配置
- 自动重连
- 最多10次重连
- 1分钟心跳间隔
- 适合长时间连接场景

### 服务端高并发配置
- 128KB 缓冲区
- 8MB 最大消息
- 30秒监控超时
- 适合高并发服务端

## 错误处理

所有示例都包含完整的错误处理：

```rust
match connection.send_message(message).await {
    Ok(_) => info!("消息发送成功"),
    Err(e) => error!("消息发送失败: {}", e),
}
```

## 日志输出

示例使用 `tracing` 进行日志记录，包括：
- 连接建立和断开
- 消息发送和接收
- 心跳状态变化
- 错误和警告信息
- 连接统计信息

## 运行要求

确保在 `Cargo.toml` 中启用了相应的特性：

```toml
[features]
default = ["logging"]
websocket = ["tokio-tungstenite", "rustls", "rustls-pemfile", "rustls-native-certs"]
quic = ["quinn", "rustls", "rustls-pemfile", "rustls-native-certs"]
full = ["websocket", "quic", "logging"]
```

## 注意事项

1. **模拟实现**: 当前示例使用模拟的网络连接，实际使用时需要真实的网络环境
2. **端口冲突**: 确保示例中使用的端口没有被其他服务占用
3. **依赖管理**: 根据需要的协议启用相应的特性
4. **错误处理**: 生产环境中应该实现更完善的错误处理和重试机制
5. **资源清理**: 示例会自动清理资源，实际使用时需要确保资源正确释放

## 扩展建议

1. **真实网络**: 将模拟连接替换为真实的网络实现
2. **TLS 支持**: 添加 TLS 加密支持
3. **负载均衡**: 实现连接负载均衡
4. **监控集成**: 集成 Prometheus 等监控系统
5. **配置热更新**: 支持运行时配置更新
