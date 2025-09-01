# Common 模块说明文档

## 概述

`common` 模块是 flare-core 的核心抽象层，提供了统一的连接管理、错误处理、协议定义和性能监控功能。该模块设计为协议无关，支持多种网络协议的扩展，包括 WebSocket、QUIC、TCP 和 UDP。

## 模块结构

```
src/common/
├── connections/          # 连接抽象和实现
│   ├── traits.rs        # 连接接口定义
│   ├── types.rs         # 连接类型、配置和协议特性
│   ├── factory.rs       # 连接工厂和原始连接处理器
│   ├── manager.rs       # 客户端连接管理器
│   ├── quic.rs          # QUIC 协议实现
│   ├── websocket.rs     # WebSocket 协议实现
│   └── mod.rs           # 模块导出
├── error.rs              # 错误类型定义
├── protocol.rs           # 协议消息定义
└── mod.rs                # 模块入口
```

## 核心组件

### 1. 连接抽象 (connections)

#### 基础接口
- **`Connection`**: 所有连接的基础接口
  - `get_id()`: 获取连接ID
  - `get_state()`: 获取连接状态
  - `is_active()`: 检查连接是否活跃
  - `get_config()`: 获取连接配置
  - `get_last_activity()`: 获取最后活跃时间
  - `update_last_activity()`: 更新最后活跃时间

#### 客户端连接
- **`ClientConnection`**: 客户端连接接口
  - `connect()`: 建立连接
  - `disconnect()`: 断开连接
  - `send_message()`: 发送消息
  - `receive_message()`: 接收消息
  - `try_reconnect()`: 尝试重连
  - `start_heartbeat()`: 启动心跳
  - `stop_heartbeat()`: 停止心跳

#### 服务端连接
- **`ServerConnection`**: 服务端连接接口
  - `accept()`: 接受连接
  - `close()`: 关闭连接
  - `send_message()`: 发送消息
  - `receive_message()`: 接收消息
  - `start_heartbeat_monitoring()`: 启动心跳监控
  - `is_healthy()`: 检查连接健康状态

### 2. 连接类型和协议特性 (types)

#### 连接类型
```rust
pub enum ConnectionType {
    WebSocket,    // WebSocket 协议
    Quic,         // QUIC 协议
    Tcp,          // TCP 协议 (预留)
    Udp,          // UDP 协议 (预留)
}
```

#### 协议特性
```rust
pub enum ProtocolFeature {
    Bidirectional,    // 双向通信
    Streaming,        // 流式传输
    Reliable,         // 可靠传输
    Ordered,          // 有序传输
    Tls,              // TLS 加密
    Heartbeat,        // 心跳支持
    Reconnection,     // 重连支持
}
```

#### 连接配置
```rust
pub struct ConnectionConfig {
    pub id: String,                           // 连接ID
    pub connection_type: ConnectionType,      // 连接类型
    pub role: ConnectionRole,                 // 连接角色 (Client/Server)
    pub remote_addr: String,                  // 远程地址
    pub heartbeat_interval_ms: u64,           // 心跳间隔
    pub heartbeat_timeout_ms: u64,            // 心跳超时
    pub auto_reconnect: bool,                 // 自动重连
    pub max_reconnect_attempts: u32,          // 最大重连次数
    pub protocol_config: ProtocolConfig,      // 协议特定配置
    // ... 更多配置选项
}
```

#### 协议配置
```rust
pub struct ProtocolConfig {
    pub websocket: WebSocketConfig,           // WebSocket 特定配置
    pub quic: QuicConfig,                     // QUIC 特定配置
    pub tcp: TcpConfig,                       // TCP 特定配置 (预留)
    pub udp: UdpConfig,                       // UDP 特定配置 (预留)
}

pub struct WebSocketConfig {
    pub subprotocols: Vec<String>,            // 子协议列表
    pub max_frame_size: usize,                // 最大帧大小
    pub enable_compression: bool,             // 启用压缩
}

pub struct QuicConfig {
    pub max_concurrent_streams: u32,          // 最大并发流数
    pub initial_max_data: u64,                // 初始最大数据量
    pub initial_max_stream_data_bidi_local: u64, // 本地双向流初始最大数据量
    pub enable_congestion_control: bool,      // 启用拥塞控制
}
```

#### 连接状态
```rust
pub enum ConnectionState {
    Initializing,    // 初始化
    Connecting,      // 连接中
    Connected,       // 已连接
    Ready,          // 就绪
    Disconnecting,   // 断开中
    Disconnected,    // 已断开
    Failed,         // 连接失败
    Reconnecting,    // 重连中
    Error,          // 错误状态
}
```

### 3. 连接工厂 (factory)

```rust
pub struct ConnectionFactory;

impl ConnectionFactory {
    // 创建客户端连接
    pub async fn create_client_connection(&self, config: ConnectionConfig) -> Result<Box<dyn ClientConnection>>;
    
    // 创建服务端连接
    pub async fn create_server_connection(&self, config: ConnectionConfig) -> Result<Box<dyn ServerConnection>>;
}

// 原始连接处理器
pub struct RawConnectionHandler;

impl RawConnectionHandler {
    // 从 WebSocket 流创建服务端连接
    pub async fn from_websocket(stream: TcpStream, config: ConnectionConfig) -> Result<Box<dyn ServerConnection>>;
    
    // 从 QUIC 连接创建服务端连接
    pub async fn from_quic(connection: quinn::Connection, config: ConnectionConfig) -> Result<Box<dyn ServerConnection>>;
}
```

### 4. 连接管理器 (manager)

#### 客户端连接管理器
```rust
pub struct ConnectionManager {
    // 管理多个客户端连接
    // 处理重连、心跳、负载均衡等
}

pub struct ManagerConfig {
    pub max_connections: usize,               // 最大连接数
    pub connection_timeout: Duration,         // 连接超时
    pub health_check_interval: Duration,      // 健康检查间隔
}
```

### 5. 事件处理 (traits)

```rust
pub trait ConnectionEventHandler {
    async fn on_connected(&self, connection_id: &str);
    async fn on_disconnected(&self, connection_id: &str, reason: &str);
    async fn on_error(&self, connection_id: &str, error: &str);
    async fn on_message_received(&self, connection_id: &str, message: &UnifiedProtocolMessage);
    async fn on_heartbeat_timeout(&self, connection_id: &str);
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8);
}

// 默认事件处理器
pub struct DefaultConnectionEventHandler;

// 统一事件处理器
pub struct UnifiedEventHandler;
```

## 使用方式

### 1. 创建客户端连接

```rust
use flare_core::connections::{ConnectionFactory, ConnectionConfig, ConnectionType, ProtocolConfig, WebSocketConfig};

// 创建连接配置
let config = ConnectionConfig::client("client_001".to_string(), "ws://localhost:8080".to_string())
    .with_type(ConnectionType::WebSocket)
    .with_heartbeat(30000, 10000)
    .with_reconnect(5, 1000)
    .with_websocket_config(WebSocketConfig {
        subprotocols: vec!["binary".to_string()],
        max_frame_size: 1024 * 1024,
        enable_compression: true,
    });

// 创建连接工厂
let factory = ConnectionFactory::new();

// 创建客户端连接
let mut connection = factory.create_client_connection(config).await?;

// 建立连接
connection.connect().await?;

// 启动心跳
connection.start_heartbeat().await?;
```

### 2. 创建服务端连接

```rust
use flare_core::connections::{ConnectionFactory, ConnectionConfig, ConnectionType, ProtocolConfig, QuicConfig};

// 创建服务端配置
let config = ConnectionConfig::server("server_001".to_string(), "0.0.0.0:8080".to_string())
    .with_type(ConnectionType::Quic)
    .with_heartbeat_monitoring(60000, 300000)
    .with_quic_config(QuicConfig {
        max_concurrent_streams: 100,
        initial_max_data: 1024 * 1024 * 10,
        initial_max_stream_data_bidi_local: 1024 * 1024,
        enable_congestion_control: true,
    });

// 创建服务端连接
let mut connection = factory.create_server_connection(config).await?;

// 接受连接
connection.accept().await?;

// 启动心跳监控
connection.start_heartbeat_monitoring().await?;
```

### 3. 协议特性分析

```rust
use flare_core::connections::{ConnectionConfig, ConnectionType, ProtocolFeature};

let config = ConnectionConfig::client("test".to_string(), "addr".to_string())
    .with_type(ConnectionType::WebSocket);

// 获取协议特性
let features = config.get_protocol_features();
println!("WebSocket 特性: {:?}", features);
// 输出: [Bidirectional, Streaming, Reliable, Ordered, Tls, Heartbeat, Reconnection]
```

### 4. 消息收发

```rust
use flare_core::protocol::{UnifiedProtocolMessage, Frame, MessageType, Reliability};

// 创建消息
let frame = Frame::new(
    MessageType::Data,
    1,
    Reliability::Reliable,
    b"Hello, World!".to_vec(),
);
let message = UnifiedProtocolMessage::new(frame, None, 1);

// 发送消息
connection.send_message(message).await?;

// 接收消息
if let Some(message) = connection.receive_message().await? {
    // 处理接收到的消息
    println!("收到消息: {:?}", message);
}
```

### 5. 事件处理

```rust
use flare_core::connections::{DefaultConnectionEventHandler, ConnectionEventHandler};

// 创建事件处理器
let event_handler = Arc::new(DefaultConnectionEventHandler::new());

// 设置事件处理器
connection.set_event_handler(event_handler).await;

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

## 配置选项

### 1. 高性能配置
```rust
let config = ConnectionConfig::high_performance("id".to_string(), "addr".to_string());
// 256KB 缓冲区，16MB 最大消息，15秒心跳间隔
```

### 2. 低延迟配置
```rust
let config = ConnectionConfig::low_latency("id".to_string(), "addr".to_string());
// 32KB 缓冲区，1MB 最大消息，10秒心跳间隔
```

### 3. 稳定连接配置
```rust
let config = ConnectionConfig::stable("id".to_string(), "addr".to_string());
// 自动重连，最多10次，1分钟心跳间隔
```

### 4. 服务端高并发配置
```rust
let config = ConnectionConfig::server_high_concurrency("id".to_string(), "addr".to_string());
// 128KB 缓冲区，8MB 最大消息，30秒监控超时
```

## 协议特性对比

| 特性 | WebSocket | QUIC | TCP | UDP |
|------|-----------|------|-----|-----|
| 双向通信 | ✅ | ✅ | ✅ | ✅ |
| 流式传输 | ✅ | ✅ | ✅ | ✅ |
| 可靠传输 | ✅ | ✅ | ✅ | ❌ |
| 有序传输 | ✅ | ✅ | ✅ | ❌ |
| TLS 加密 | ✅ | ✅ | ✅ | ❌ |
| 心跳支持 | ✅ | ✅ | ✅ | ❌ |
| 重连支持 | ✅ | ✅ | ✅ | ❌ |

## 错误处理

所有连接操作都返回 `Result<T, FlareError>`，常见的错误类型包括：

- `ConnectionFailed`: 连接失败
- `MessageSendFailed`: 消息发送失败
- `MessageReceiveFailed`: 消息接收失败
- `HeartbeatTimeout`: 心跳超时
- `InvalidConfiguration`: 配置无效

## 性能特性

1. **异步设计**: 所有 I/O 操作都是异步的，支持高并发
2. **连接池**: 支持连接复用和管理
3. **心跳机制**: 自动检测连接健康状态
4. **重连策略**: 智能重连机制，支持指数退避
5. **统计监控**: 提供详细的连接统计信息
6. **协议优化**: 针对不同协议的特性进行优化

## 扩展性

该模块设计为高度可扩展的：

1. **协议扩展**: 可以轻松添加新的协议支持（TCP、UDP 接口已预留）
2. **事件扩展**: 可以添加自定义事件类型
3. **配置扩展**: 可以添加新的配置选项和协议特定配置
4. **监控扩展**: 可以集成自定义监控系统
5. **特性扩展**: 可以添加新的协议特性

## 网络实现

### WebSocket 实现
- 使用 `tokio-tungstenite` 库
- 支持 WSS (TLS) 和 WS 协议
- 集成心跳机制 (Ping/Pong)
- 支持子协议和压缩

### QUIC 实现
- 使用 `quinn` 库
- 支持多流并发
- 内置拥塞控制
- 零 RTT 连接

### TCP/UDP 实现
- 接口已预留，等待具体实现
- 支持自定义配置选项

## 注意事项

1. **资源管理**: 连接使用完毕后应及时关闭
2. **错误处理**: 所有异步操作都应该处理错误
3. **心跳配置**: 心跳间隔应该根据网络环境调整
4. **并发安全**: 连接对象可以在多个任务间安全共享
5. **内存管理**: 大量连接时注意内存使用情况
6. **协议选择**: 根据应用需求选择合适的协议
7. **特性依赖**: 某些功能需要启用相应的 feature 标志

## 示例和测试

完整的示例代码位于 `examples/common/` 目录：

- `unified_connection_demo.rs`: 统一连接架构演示
- `README.md`: 示例使用说明

运行示例：
```bash
# 运行完整演示
RUST_LOG=info cargo run --example unified_connection_demo

# 启用特定协议特性
cargo run --example unified_connection_demo --features websocket,quic
```
