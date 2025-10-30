# Connections 模块文档

## 📖 模块概述

连接模块提供统一的网络连接抽象，支持WebSocket、QUIC等多种协议。模块采用**事件驱动**架构和**连接池**优化，专为高并发、低延迟场景设计。

## 🎯 设计目标

- **协议统一**: 不同协议使用相同接口
- **超低延迟**: QUIC比WebSocket延迟降低34%
- **连接复用**: 智能连接池管理
- **事件驱动**: 异步事件处理机制
- **自动恢复**: 断线重连和故障转移

## 🏗️ 架构设计

```
connections/
├── traits.rs          # 核心连接接口
├── types.rs           # 连接类型和配置
├── quic.rs            # QUIC协议实现
├── websocket.rs       # WebSocket协议实现
├── factory.rs         # 连接工厂
├── pool.rs            # 连接池管理
├── manager.rs         # 连接管理器
├── builder.rs         # 连接构建器
├── event.rs           # 事件处理
└── mod.rs             # 模块导出
```

### 🔧 核心接口

```rust
#[async_trait]
pub trait Connection: Send + Sync {
    fn get_id(&self) -> &str;
    async fn get_state(&self) -> ConnectionState;
    async fn is_active(&self) -> bool;
    async fn send_message(&mut self, frame: Frame) -> Result<()>;
    async fn disconnect(&mut self) -> Result<()>;
}

#[async_trait]
pub trait ClientConnection: Connection {
    async fn connect(&mut self) -> Result<()>;
    async fn reconnect(&mut self) -> Result<()>;
}

#[async_trait]
pub trait ServerConnection: Connection {
    async fn accept(&mut self) -> Result<()>;
}
```

## 🚀 支持的协议

### 1️⃣ QUIC - 下一代协议 🚀
```rust
let config = ConnectionConfig::client("client1", "127.0.0.1:4433")
    .with_type(ConnectionType::Quic);
let connection = factory.create_client_connection(config).await?;
```
- **性能**: 比WebSocket延迟降低34%
- **特性**: 0-RTT重连、多路复用、无头阻塞
- **场景**: 高性能应用、实时游戏
- **优势**: 内置加密、连接迁移

### 2️⃣ WebSocket - 经典可靠 📡
```rust
let config = ConnectionConfig::client("client1", "ws://localhost:8080")
    .with_type(ConnectionType::WebSocket);
let connection = factory.create_client_connection(config).await?;
```
- **性能**: 稳定可靠的双向通信
- **特性**: 广泛支持、易于调试
- **场景**: Web应用、兼容性要求高的系统
- **优势**: 成熟生态、防火墙友好

## 📊 性能对比

基于实际测试数据：

| 协议 | 平均延迟 | 吞吐量 | 并发流 | 连接建立 | 推荐场景 |
|------|---------|--------|--------|---------|---------|
| **QUIC** | 8.2ms | 12K msg/s | 100+ | 0-1 RTT | 🏆 高性能 |
| **WebSocket** | 12.5ms | 8.5K msg/s | 1 | 3+ RTT | 📡 通用 |

## 🔧 使用方式

### 基础连接
```rust
use flare_core::common::connections::{
    ConnectionFactory, ConnectionConfig, ConnectionType
};

// 创建连接工厂
let factory = ConnectionFactory::new();

// 客户端连接
let config = ConnectionConfig::client("client1", "ws://localhost:8080")
    .with_type(ConnectionType::WebSocket)
    .with_heartbeat(30000, 5000)  // 30s心跳间隔, 5s超时
    .with_reconnect(5, 1000);     // 5次重试, 1s间隔

let mut connection = factory.create_client_connection(config).await?;
connection.connect().await?;

// 发送消息
let frame = Frame::new(MessageType::Data, 1, Reliability::AtLeastOnce, b"Hello".to_vec());
connection.send_message(frame).await?;
```

### 连接池使用
```rust
use flare_core::common::connections::{ConnectionPool, ConnectionPoolConfig};

// 创建连接池
let pool_config = ConnectionPoolConfig {
    max_connections_per_target: 10,
    preconnect_targets: vec!["server1:8080".to_string()],
    preconnect_count: 3,
    ..Default::default()
};

let pool = ConnectionPool::new(pool_config);

// 获取连接（自动复用或创建）
let connection = pool.get_connection("server1:8080", ConnectionType::Quic).await?;

// 连接池统计
let stats = pool.get_stats().await;
println!("连接池命中率: {:.1}%", stats.hit_rate * 100.0);
```

### 事件处理
```rust
use flare_core::common::connections::{ConnectionEvent, DefConnectionEventHandler};

#[derive(Debug)]
struct MyEventHandler;

#[async_trait::async_trait]
impl ConnectionEvent for MyEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        println!("连接建立: {}", connection_id);
    }
    
    async fn on_message_received(&self, connection_id: &str, frame: &Frame) {
        println!("收到消息: {} - {:?}", connection_id, frame.get_message_type());
    }
    
    async fn on_disconnected(&self, connection_id: &str) {
        println!("连接断开: {}", connection_id);
    }
}

// 设置事件处理器
let handler = Arc::new(MyEventHandler);
connection.set_connection_event_handler(handler).await;
```

### 连接构建器
```rust
use flare_core::common::connections::ConnectionBuilder;

// 超低延迟QUIC连接
let connection = ConnectionBuilder::client("ultra_client", "127.0.0.1:4433")
    .with_quic()
    .with_ultra_low_latency()
    .with_bincode_serialization()
    .build_and_connect().await?;

// 调试友好的WebSocket连接
let connection = ConnectionBuilder::client("debug_client", "ws://localhost:8080")
    .with_websocket()
    .with_json_serialization()
    .with_debug_logging()
    .build_and_connect().await?;
```

## ⚙️ 配置参数

### ConnectionConfig
```rust
pub struct ConnectionConfig {
    /// 连接ID
    pub id: String,
    /// 连接类型
    pub connection_type: ConnectionType,
    /// 连接角色 (客户端/服务端)
    pub role: ConnectionRole,
    /// 远程地址
    pub remote_addr: String,
    /// 连接超时 (ms)
    pub timeout_ms: u64,
    /// 心跳间隔 (ms)
    pub heartbeat_interval_ms: u64,
    /// 心跳超时 (ms)
    pub heartbeat_timeout_ms: u64,
    /// 自动重连
    pub auto_reconnect: bool,
    /// 最大重连次数
    pub max_reconnect_attempts: u32,
    /// 重连延迟 (ms)
    pub reconnect_delay_ms: u64,
}
```

### QUIC专用配置
```rust
pub struct QuicConfig {
    /// 最大并发流数
    pub max_concurrent_streams: u32,        // 默认: 100
    /// 初始流窗口大小
    pub initial_stream_window: u32,         // 默认: 65536 (64KB)
    /// 连接窗口大小
    pub connection_window: u32,             // 默认: 262144 (256KB)
    /// 拥塞控制算法
    pub congestion_control: String,         // 默认: "bbr"
}

// 超低延迟QUIC配置
let quic_config = QuicConfig {
    max_concurrent_streams: 10,
    initial_stream_window: 16384,  // 16KB
    connection_window: 65536,      // 64KB
    congestion_control: "bbr".to_string(),
};
```

## 🎯 性能优化

### 1. 连接池优化
```rust
// 预连接关键服务
let pool_config = ConnectionPoolConfig {
    preconnect_targets: vec![
        "critical-service:8080".to_string(),
        "user-service:8081".to_string(),
    ],
    preconnect_count: 5,
    max_connections_per_target: 20,
};

// 连接复用
let connection = pool.get_connection(&target, ConnectionType::Quic).await?;
```

### 2. QUIC参数调优
```rust
// 超低延迟配置
let config = ConnectionConfig::client(id, addr)
    .with_type(ConnectionType::Quic)
    .with_quic_config(QuicConfig {
        max_concurrent_streams: 5,      // 减少并发
        initial_stream_window: 8192,    // 小窗口
        enable_0rtt: true,             // 启用0-RTT
        idle_timeout_ms: 200,          // 快速超时
    });
```

### 3. 事件优化
```rust
// 轻量级事件处理器
#[derive(Debug)]
struct FastEventHandler;

#[async_trait::async_trait]
impl ConnectionEvent for FastEventHandler {
    async fn on_message_received(&self, _: &str, frame: &Frame) {
        // 最小化处理逻辑
        tokio::spawn(async move {
            process_message_async(frame).await;
        });
    }
    
    // 其他事件使用空实现
    async fn on_connected(&self, _: &str) {}
    async fn on_disconnected(&self, _: &str) {}
}
```

## 🔍 扩展指南

### 自定义协议实现
```rust
use flare_core::common::connections::{Connection, ClientConnection};

#[derive(Debug)]
pub struct MyConnection {
    id: String,
    config: ConnectionConfig,
    state: Arc<RwLock<ConnectionState>>,
}

#[async_trait::async_trait]
impl Connection for MyConnection {
    fn get_id(&self) -> &str { &self.id }
    
    async fn get_state(&self) -> ConnectionState {
        *self.state.read().await
    }
    
    async fn send_message(&mut self, frame: Frame) -> Result<()> {
        // 实现自定义协议的消息发送
        my_protocol_send(&frame).await
    }
    
    // 实现其他必需方法...
}

#[async_trait::async_trait]
impl ClientConnection for MyConnection {
    async fn connect(&mut self) -> Result<()> {
        // 实现自定义协议的连接建立
        my_protocol_connect(&self.config.remote_addr).await
    }
}
```

### 自定义事件处理器
```rust
#[derive(Debug)]
pub struct MetricsEventHandler {
    metrics: Arc<Mutex<ConnectionMetrics>>,
}

#[async_trait::async_trait]
impl ConnectionEvent for MetricsEventHandler {
    async fn on_message_received(&self, connection_id: &str, frame: &Frame) {
        let mut metrics = self.metrics.lock().await;
        metrics.messages_received += 1;
        metrics.bytes_received += frame.get_payload().len() as u64;
    }
    
    async fn on_message_sent(&self, connection_id: &str, frame: &Frame) {
        let mut metrics = self.metrics.lock().await;
        metrics.messages_sent += 1;
        metrics.bytes_sent += frame.get_payload().len() as u64;
    }
}
```

## 📈 监控与诊断

### 连接状态监控
```rust
// 获取连接状态
let state = connection.get_state().await;
println!("连接状态: {:?}", state);

// 检查连接活跃性
if connection.is_active().await {
    println!("连接正常");
} else {
    println!("连接异常，尝试重连");
    connection.reconnect().await?;
}
```

### 连接池监控
```rust
let stats = pool.get_stats().await;
println!("连接池统计:");
println!("  总连接数: {}", stats.total_connections);
println!("  命中率: {:.1}%", stats.hit_rate * 100.0);
println!("  平均连接时间: {:.1}ms", stats.avg_connection_time_ms);
```

### 性能指标
```rust
#[derive(Debug)]
pub struct ConnectionMetrics {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub connection_count: u64,
    pub reconnection_count: u64,
    pub avg_latency_ms: f64,
}
```

## 🚨 注意事项

### 资源管理
- 及时关闭不需要的连接
- 监控连接池大小避免资源泄漏
- 合理设置连接超时时间

### 错误处理
```rust
match connection.connect().await {
    Ok(()) => println!("连接成功"),
    Err(FlareError::ConnectionTimeout(_)) => {
        // 连接超时，可能网络问题
        tokio::time::sleep(Duration::from_secs(1)).await;
        connection.reconnect().await?;
    }
    Err(FlareError::ConnectionRefused(_)) => {
        // 连接被拒绝，可能服务未启动
        return Err(FlareError::general_error("服务不可用"));
    }
    Err(e) => return Err(e),
}
```

### 线程安全
- 连接对象实现了`Send + Sync`
- 多线程环境下使用`Arc`包装
- 事件处理器需要线程安全

## 🧪 测试与验证

### 单元测试
```bash
cargo test connections
```

### 集成测试
```bash
cargo test test_quic_websocket_integration
```

### 性能测试
```bash
cargo run --example quic_vs_websocket_benchmark
```

## 📚 相关资源

- [QUIC协议文档](https://datatracker.ietf.org/doc/html/rfc9000)
- [WebSocket规范](https://tools.ietf.org/html/rfc6455)
- [Quinn QUIC库](https://github.com/quinn-rs/quinn)
- [Tokio-Tungstenite](https://github.com/snapview/tokio-tungstenite)
- [性能测试报告](../../QUIC_SUMMARY.md)

---

*统一连接抽象 - 让协议选择成为配置*