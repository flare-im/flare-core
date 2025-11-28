# Flare Core 服务端构建器使用指南

本文档介绍 Flare Core 提供的三种服务端构建器，帮助您根据需求选择合适的构建器。

## 📋 目录

- [三种构建器对比](#三种构建器对比)
- [SimpleServerBuilder（毛坯房）](#simpleserverbuilder毛坯房)
- [ObserverServerBuilder（基本装修）](#observerserverbuilder基本装修)
- [FlareServerBuilder（精装修）](#flareserverbuilder精装修)
- [功能对比表](#功能对比表)
- [迁移指南](#迁移指南)

---

## 三种构建器对比

Flare Core 提供三种不同抽象级别的服务端构建器，从简单到复杂：

| 构建器 | 抽象级别 | 适用场景 | 特点 |
|--------|---------|---------|------|
| **SimpleServerBuilder** | 最低 | 快速原型、小型应用 | 最小实现，零配置，轻量级 |
| **ObserverServerBuilder** | 中等 | 自定义处理、设备管理 | 自定义观察器，设备管理，事件处理 |
| **FlareServerBuilder** | 最高 | 生产环境、企业应用 | 完整功能，消息管道，中间件支持 |

### 架构层次

```
┌─────────────────────────────────────────────────────────┐
│         FlareServerBuilder (精装修)                     │
│  ┌──────────────────────────────────────────────────┐  │
│  │  MessagePipeline + Middleware + Processor        │  │
│  │  序列化协商 + 压缩 + 加密 + 设备管理 + 认证        │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                        ▲
                        │ 基于
┌─────────────────────────────────────────────────────────┐
│      ObserverServerBuilder (基本装修)                    │
│  ┌──────────────────────────────────────────────────┐  │
│  │  ConnectionHandler + DeviceManager + EventHandler  │  │
│  │  自定义观察器 + 设备管理 + 事件处理                 │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                        ▲
                        │ 基于
┌─────────────────────────────────────────────────────────┐
│         SimpleServerBuilder (毛坯房)                     │
│  ┌──────────────────────────────────────────────────┐  │
│  │  闭包处理 + 基本配置                                │  │
│  │  最小实现，零依赖                                   │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                        ▲
                        │ 基于
┌─────────────────────────────────────────────────────────┐
│              HybridServer (核心实现)                     │
│  ┌──────────────────────────────────────────────────┐  │
│  │  多协议支持 + 连接管理 + 心跳检测                   │  │
│  │  WebSocket + QUIC + ServerCore                    │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

---

## SimpleServerBuilder（毛坯房）

### 概述

最简单的构建器，提供最小实现，没有任何"装修"。适合快速原型开发和小型应用。

### 特点

- ✅ **最小依赖**：只提供基本的消息处理（闭包）
- ✅ **零配置**：使用默认配置即可运行
- ✅ **轻量级**：不包含中间件、管道等高级功能
- ✅ **快速上手**：几行代码即可启动服务器

### 适用场景

- 快速原型开发
- 小型应用
- 学习和测试
- 需要完全控制消息处理流程的场景

### 基本使用

```rust
use flare_core::server::builder::ServerBuilder;
use flare_core::common::protocol::{Frame, frame_with_message_command, generate_message_id, Reliability};
use flare_core::common::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // 创建服务器
    let mut server = ServerBuilder::new("0.0.0.0:8080")
        // 设置消息处理函数
        .on_message(|frame, ctx| async move {
            println!("收到消息: {:?}", frame);
            // 可以发送回复
            Ok(None)
        })
        // 设置连接建立处理
        .on_connect(|connection_id, ctx| async move {
            println!("新连接: {}", connection_id);
            Ok(())
        })
        // 设置连接断开处理
        .on_disconnect(|connection_id, ctx| async move {
            println!("连接断开: {}", connection_id);
            Ok(())
        })
        .build()?;
    
    // 启动服务器
    server.start().await?;
    
    // 发送消息示例
    let frame = frame_with_message_command(
        generate_message_id(),
        b"Hello".to_vec(),
        None,
        None,
    );
    server.send_to("connection_id", &frame).await?;
    
    // 等待停止信号
    tokio::signal::ctrl_c().await?;
    server.stop().await?;
    
    Ok(())
}
```

### 配置选项

```rust
ServerBuilder::new("0.0.0.0:8080")
    // 协议配置
    .with_protocol(TransportProtocol::WebSocket)
    // 或启用多协议
    .with_protocols(vec![TransportProtocol::WebSocket, TransportProtocol::QUIC])
    
    // 连接配置
    .with_max_connections(1000)
    .with_heartbeat(HeartbeatConfig::default())
    
    // 序列化和压缩（用于协商）
    .with_default_format(SerializationFormat::Json)
    .with_default_compression(CompressionAlgorithm::None)
    
    // TLS 配置（QUIC 需要）
    .with_tls(TlsConfig::default())
    
    // 认证（可选）
    .enable_auth()
    .with_authenticator(authenticator)
    .with_auth_timeout(Duration::from_secs(30))
    
    .build()?;
```

### 限制

- ❌ 不支持中间件
- ❌ 不支持消息管道
- ❌ 不支持自定义观察器
- ❌ 不支持设备管理
- ❌ 不支持事件处理器

---

## ObserverServerBuilder（基本装修）

### 概述

提供基本实现，用户可以自定义观察器和处理器。支持设备管理、事件处理等基本功能。

### 特点

- ✅ **自定义观察器**：实现 `ConnectionHandler` trait 自定义消息处理
- ✅ **设备管理**：支持设备冲突策略和多端管理
- ✅ **事件处理**：支持自定义事件处理器
- ✅ **连接管理**：支持共享连接管理器
- ✅ **灵活扩展**：可以添加自定义的观察器和处理器

### 适用场景

- 需要自定义消息处理逻辑
- 需要设备管理和多端控制
- 需要事件驱动的架构
- 需要共享连接状态（多服务器实例）

### 基本使用

```rust
use flare_core::server::{ConnectionHandler, ObserverServerBuilder};
use flare_core::common::protocol::Frame;
use flare_core::common::error::Result;
use std::sync::Arc;
use async_trait::async_trait;

// 实现自定义连接处理器
struct MyConnectionHandler {
    // 自定义字段
}

#[async_trait]
impl ConnectionHandler for MyConnectionHandler {
    async fn handle_frame(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        println!("处理消息: {:?}", frame);
        // 自定义处理逻辑
        Ok(None)
    }
    
    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        println!("连接建立: {}", connection_id);
        Ok(())
    }
    
    async fn on_disconnect(&self, connection_id: &str) -> Result<()> {
        println!("连接断开: {}", connection_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 创建设备管理器
    let device_manager = Arc::new(DeviceManager::new(
        DeviceConflictStrategyBuilder::new()
            .platform_exclusive()
            .build()
    ));
    
    // 创建自定义处理器
    let handler = Arc::new(MyConnectionHandler {});
    
    // 创建服务器
    let mut server = ObserverServerBuilder::new("0.0.0.0:8080")
        .with_handler(handler)
        .with_device_manager(device_manager)
        .build()?;
    
    server.start().await?;
    
    tokio::signal::ctrl_c().await?;
    server.stop().await?;
    
    Ok(())
}
```

### 高级功能

#### 设备管理

```rust
use flare_core::common::device::{DeviceManager, DeviceConflictStrategyBuilder};

let device_manager = Arc::new(DeviceManager::new(
    DeviceConflictStrategyBuilder::new()
        .platform_exclusive()  // 同平台互斥
        // 或
        .device_exclusive()    // 同设备互斥
        // 或
        .allow_all()           // 允许所有
        .build()
));

ObserverServerBuilder::new("0.0.0.0:8080")
    .with_device_manager(device_manager)
    .build()?;
```

#### 事件处理器

```rust
use flare_core::server::events::handler::ServerEventHandler;
use async_trait::async_trait;

struct MyEventHandler;

#[async_trait]
impl ServerEventHandler for MyEventHandler {
    async fn handle_system_command(&self, cmd: &SystemCommand, connection_id: &str) -> Result<Option<Frame>> {
        // 处理系统命令
        Ok(None)
    }
    
    async fn handle_message_command(&self, cmd: &MessageCommand, connection_id: &str) -> Result<Option<Frame>> {
        // 处理消息命令
        Ok(None)
    }
}

ObserverServerBuilder::new("0.0.0.0:8080")
    .with_event_handler(Arc::new(MyEventHandler))
    .build()?;
```

#### 共享连接管理器

```rust
use flare_core::server::connection::ConnectionManager;

// 创建共享的连接管理器
let connection_manager = Arc::new(ConnectionManager::new());

// 多个服务器实例共享同一个连接管理器
let server1 = ObserverServerBuilder::new("0.0.0.0:8080")
    .with_connection_manager(Arc::clone(&connection_manager))
    .build()?;

let server2 = ObserverServerBuilder::new("0.0.0.0:8081")
    .with_connection_manager(connection_manager)
    .build()?;
```

### 配置选项

```rust
ObserverServerBuilder::new("0.0.0.0:8080")
    // 必须：设置连接处理器
    .with_handler(handler)
    
    // 可选：设备管理
    .with_device_manager(device_manager)
    
    // 可选：事件处理器
    .with_event_handler(event_handler)
    
    // 可选：共享连接管理器
    .with_connection_manager(connection_manager)
    
    // 协议配置
    .with_protocols(vec![TransportProtocol::WebSocket, TransportProtocol::QUIC])
    .with_protocol_address(TransportProtocol::WebSocket, "0.0.0.0:8080".to_string())
    .with_protocol_address(TransportProtocol::QUIC, "0.0.0.0:8081".to_string())
    
    // 其他配置（同 SimpleServerBuilder）
    .with_max_connections(1000)
    .with_default_format(SerializationFormat::Protobuf)
    .with_default_compression(CompressionAlgorithm::Gzip)
    
    .build()?;
```

### 限制

- ❌ 不支持消息管道
- ❌ 不支持中间件
- ❌ 不支持自动序列化/压缩协商（需要手动处理）

---

## FlareServerBuilder（精装修）

### 概述

提供完整功能，包含所有 `common` 和 `server` 模块的能力。用户只需简单配置即可使用，也可以自定义中间件、处理器等扩展功能。

### 特点

- ✅ **消息管道**：自动处理序列化、压缩、加密
- ✅ **中间件支持**：日志、性能监控、验证等
- ✅ **处理器链**：可组合多个处理器
- ✅ **序列化协商**：自动协商最佳序列化格式（JSON/Protobuf）
- ✅ **压缩协商**：自动协商压缩算法（Gzip/Zstd/None）
- ✅ **加密支持**：AES-256-GCM 加密
- ✅ **设备管理**：完整的设备冲突策略
- ✅ **认证机制**：JWT Token 认证
- ✅ **心跳检测**：自动心跳和超时管理
- ✅ **多协议支持**：WebSocket + QUIC 双协议
- ✅ **简单易用**：只需实现 `MessageListener` 即可
- ✅ **高度可扩展**：可以自定义中间件、处理器覆盖默认实现

### 适用场景

- 生产环境
- 需要完整功能的企业应用
- 需要高性能和可扩展性的场景
- 需要统一消息处理流程的场景

### 基本使用

```rust
use flare_core::server::builder::{FlareServerBuilder, MessageListener};
use flare_core::common::protocol::Frame;
use flare_core::common::error::Result;
use std::sync::Arc;
use async_trait::async_trait;

// 实现消息监听器
struct MyListener;

#[async_trait]
impl MessageListener for MyListener {
    async fn on_message(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        println!("收到消息: {:?}", frame);
        Ok(None)
    }
    
    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        println!("连接建立: {}", connection_id);
        Ok(())
    }
    
    async fn on_disconnect(&self, connection_id: &str, reason: Option<&str>) -> Result<()> {
        println!("连接断开: {}", connection_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let listener = Arc::new(MyListener);
    
    // 创建服务器（自动包含所有功能）
    let server = FlareServerBuilder::new("0.0.0.0:8080")
        .with_listener(listener)
        .build()?;
    
    server.start().await?;
    
    tokio::signal::ctrl_c().await?;
    server.stop().await?;
    
    Ok(())
}
```

### 中间件使用

```rust
use flare_core::common::message::{
    LoggingMiddleware, MetricsMiddleware, ValidationMiddleware, LogLevel
};

FlareServerBuilder::new("0.0.0.0:8080")
    .with_listener(listener)
    
    // 添加中间件（按添加顺序执行）
    .with_middleware(Arc::new(
        LoggingMiddleware::new("ServerLogging")
            .with_level(LogLevel::Info)
    ))
    .with_middleware(Arc::new(
        MetricsMiddleware::new("ServerMetrics")
    ))
    .with_middleware(Arc::new(
        ValidationMiddleware::new("ServerValidation", |frame| {
            // 自定义验证逻辑
            if frame.message_id.is_empty() {
                Err(FlareError::message_format_error("Message ID is empty".to_string()))
            } else {
                Ok(())
            }
        })
    ))
    
    .build()?;
```

### 自定义处理器

```rust
use flare_core::common::message::{MessageProcessor, MessageContext, FunctionProcessor};

// 使用函数处理器
let echo_processor = Arc::new(FunctionProcessor::new("EchoProcessor", |ctx| async move {
    // 处理逻辑
    Ok(None)
}));

FlareServerBuilder::new("0.0.0.0:8080")
    .with_listener(listener)
    .with_processor(echo_processor)
    .build()?;
```

### 完整配置示例

```rust
use flare_core::common::device::{DeviceManager, DeviceConflictStrategyBuilder};
use flare_core::common::message::{LoggingMiddleware, MetricsMiddleware, LogLevel};
use flare_core::common::compression::CompressionAlgorithm;
use flare_core::common::protocol::SerializationFormat;
use flare_core::common::config_types::{TransportProtocol, HeartbeatConfig};

let device_manager = Arc::new(DeviceManager::new(
    DeviceConflictStrategyBuilder::new()
        .platform_exclusive()
        .build()
));

let server = FlareServerBuilder::new("0.0.0.0:8080")
    // 必须：设置消息监听器
    .with_listener(listener)
    
    // 中间件
    .with_middleware(Arc::new(LoggingMiddleware::new("ServerLogging")))
    .with_middleware(Arc::new(MetricsMiddleware::new("ServerMetrics")))
    
    // 设备管理
    .with_device_manager(device_manager)
    
    // 协议配置
    .with_protocols(vec![TransportProtocol::WebSocket, TransportProtocol::QUIC])
    .with_protocol_address(TransportProtocol::WebSocket, "0.0.0.0:8080".to_string())
    .with_protocol_address(TransportProtocol::QUIC, "0.0.0.0:8081".to_string())
    
    // 序列化和压缩（用于协商）
    .with_default_format(SerializationFormat::Json)  // 初始使用 JSON，协商后可能切换到 Protobuf
    .with_default_compression(CompressionAlgorithm::Gzip)
    
    // 连接配置
    .with_max_connections(2000)
    .with_connection_timeout(Duration::from_secs(60))
    .with_heartbeat(HeartbeatConfig::default()
        .with_interval(Duration::from_secs(30))
        .with_timeout(Duration::from_secs(90)))
    
    // 设备冲突策略
    .with_device_conflict_strategy(DeviceConflictStrategy::PlatformExclusive)
    
    // 认证（可选）
    .enable_auth()
    .with_authenticator(authenticator)
    .with_auth_timeout(Duration::from_secs(30))
    
    .build()?;
```

### 自动功能

使用 `FlareServerBuilder` 时，以下功能会自动启用：

1. **序列化协商**：客户端连接时自动协商序列化格式（JSON/Protobuf）
2. **压缩协商**：自动协商压缩算法（Gzip/Zstd/None）
3. **加密协商**：自动协商加密方式（AES-256-GCM/None）
4. **心跳检测**：自动检测连接健康状态
5. **连接管理**：自动管理连接生命周期
6. **消息解析**：根据协商结果自动解析消息

### 扩展能力

#### 自定义中间件

```rust
use flare_core::common::message::{MessageMiddleware, MessageContext};

struct MyCustomMiddleware {
    name: String,
}

#[async_trait]
impl MessageMiddleware for MyCustomMiddleware {
    async fn before(&self, ctx: &mut MessageContext) -> Result<()> {
        println!("[{}] 处理前: {:?}", self.name, ctx.frame);
        Ok(())
    }
    
    async fn after(&self, ctx: &MessageContext, result: &mut Result<Option<Frame>>) {
        println!("[{}] 处理后: {:?}", self.name, result);
    }
    
    fn name(&self) -> &str {
        &self.name
    }
}

FlareServerBuilder::new("0.0.0.0:8080")
    .with_listener(listener)
    .with_middleware(Arc::new(MyCustomMiddleware { name: "Custom".to_string() }))
    .build()?;
```

#### 自定义处理器

```rust
use flare_core::common::message::{MessageProcessor, MessageContext};

struct MyCustomProcessor {
    name: String,
}

#[async_trait]
impl MessageProcessor for MyCustomProcessor {
    async fn process(&self, ctx: &MessageContext) -> Result<Option<Frame>> {
        // 自定义处理逻辑
        Ok(None)
    }
    
    fn name(&self) -> &str {
        &self.name
    }
}

FlareServerBuilder::new("0.0.0.0:8080")
    .with_listener(listener)
    .with_processor(Arc::new(MyCustomProcessor { name: "Custom".to_string() }))
    .build()?;
```

---

## 功能对比表

| 功能 | SimpleServerBuilder | ObserverServerBuilder | FlareServerBuilder |
|------|---------------------|----------------------|-------------------|
| **消息处理** | ✅ 闭包 | ✅ ConnectionHandler | ✅ MessageListener |
| **中间件** | ❌ | ❌ | ✅ |
| **消息管道** | ❌ | ❌ | ✅ |
| **序列化协商** | ✅ 手动 | ✅ 手动 | ✅ 自动 |
| **压缩协商** | ✅ 手动 | ✅ 手动 | ✅ 自动 |
| **加密支持** | ❌ | ❌ | ✅ |
| **设备管理** | ❌ | ✅ | ✅ |
| **事件处理器** | ❌ | ✅ | ✅ |
| **连接管理器** | ❌ | ✅ | ✅ |
| **心跳检测** | ✅ | ✅ | ✅ |
| **多协议支持** | ✅ | ✅ | ✅ |
| **认证** | ✅ | ✅ | ✅ |
| **自定义观察器** | ❌ | ✅ | ❌ |
| **自定义中间件** | ❌ | ❌ | ✅ |
| **自定义处理器** | ❌ | ❌ | ✅ |
| **代码复杂度** | ⭐ 低 | ⭐⭐ 中 | ⭐⭐⭐ 高 |
| **功能完整性** | ⭐ 基础 | ⭐⭐ 中等 | ⭐⭐⭐ 完整 |

---

## 迁移指南

### 从 SimpleServerBuilder 迁移到 ObserverServerBuilder

```rust
// 之前：使用闭包
let server = ServerBuilder::new("0.0.0.0:8080")
    .on_message(|frame, ctx| async move {
        // 处理逻辑
        Ok(None)
    })
    .build()?;

// 之后：使用 ConnectionHandler
struct MyHandler;
#[async_trait]
impl ConnectionHandler for MyHandler {
    async fn handle_frame(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        // 处理逻辑
        Ok(None)
    }
}

let server = ObserverServerBuilder::new("0.0.0.0:8080")
    .with_handler(Arc::new(MyHandler))
    .build()?;
```

### 从 ObserverServerBuilder 迁移到 FlareServerBuilder

```rust
// 之前：使用 ConnectionHandler
struct MyHandler;
#[async_trait]
impl ConnectionHandler for MyHandler {
    async fn handle_frame(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        // 处理逻辑
        Ok(None)
    }
}

let server = ObserverServerBuilder::new("0.0.0.0:8080")
    .with_handler(Arc::new(MyHandler))
    .build()?;

// 之后：使用 MessageListener（更简单）
struct MyListener;
#[async_trait]
impl MessageListener for MyListener {
    async fn on_message(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        // 处理逻辑（自动处理序列化、压缩等）
        Ok(None)
    }
}

let server = FlareServerBuilder::new("0.0.0.0:8080")
    .with_listener(Arc::new(MyListener))
    .with_middleware(Arc::new(LoggingMiddleware::new("Logging")))
    .build()?;
```

---

## 总结

- **SimpleServerBuilder**：适合快速原型和小型应用，提供最小实现
- **ObserverServerBuilder**：适合需要自定义处理和设备管理的场景
- **FlareServerBuilder**：适合生产环境，提供完整功能和最佳实践

根据您的需求选择合适的构建器，也可以从简单到复杂逐步迁移。

