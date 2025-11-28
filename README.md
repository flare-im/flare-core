# Flare Core

[![Crates.io](https://img.shields.io/crates/v/flare-core.svg)](https://crates.io/crates/flare-core)
[![Documentation](https://docs.rs/flare-core/badge.svg)](https://docs.rs/flare-core)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-blue.svg)](https://www.rust-lang.org/)

**Flare Core** 是一个高性能、可靠的即时通讯长连接工具包，专为 Rust 设计。它提供了简洁的 API 和强大的功能，让开发者能够轻松构建实时通信应用。

> **注意**: 本文档使用中文编写。For English documentation, please refer to [docs.rs/flare-core](https://docs.rs/flare-core).

## ✨ 核心特性

### 🚀 多协议支持
- **WebSocket** - 基于标准 WebSocket 协议，支持 Web 和移动端
- **QUIC** - 基于 UDP 的现代传输协议，提供更低的延迟和更好的性能
- **协议竞速** - 客户端可以同时尝试多个协议，自动选择最快的连接

### 🔐 认证与安全
- **Token 认证** - 支持可配置的 token 验证机制
- **自定义认证器** - 实现 `Authenticator` trait 提供自定义验证逻辑
- **认证超时** - 可配置的认证超时时间
- **连接状态管理** - 只有已验证的连接才能收发业务消息

### 📱 多设备管理
- **设备冲突策略** - 支持多种设备管理策略：
  - 平台互斥：同一用户同一平台只能有一个设备在线
  - 移动端互斥：同一用户只能有一个移动端设备在线
  - 完全互斥：同一用户只能有一个设备在线
  - 移动端和PC共存：移动端之间互斥，PC端之间互斥，但移动端和PC端可以同时在线
  - 完全开放：允许所有设备同时在线
- **设备信息** - 支持设备ID、平台、型号、版本等信息

### 🔄 序列化协商
- **多格式支持** - 支持 Protobuf 和 JSON 序列化格式
- **压缩算法** - 支持 None、Gzip、Zstd 压缩算法
- **协商机制** - 客户端和服务端自动协商最优的序列化格式和压缩算法
- **强制模式** - 客户端可以强制指定格式（适用于不支持某些格式的平台）

### 💓 心跳检测
- **自动心跳** - 服务端自动检测连接超时
- **客户端心跳** - 客户端自动发送心跳保持连接
- **可配置** - 心跳间隔和超时时间可配置

### 🏗️ 灵活的构建模式
- **Flare 模式** - 使用 `FlareClientBuilder`/`FlareServerBuilder`，只需实现简单的 `MessageListener` 接口，自动集成所有功能
- **消息处理管道** - 统一的消息处理流程，支持中间件、自动序列化/压缩
- **观察者模式** - 实现 `ConnectionHandler`/`ConnectionObserver` trait 处理消息
- **简单模式** - 使用闭包定义消息处理逻辑
- **事件处理器** - 支持细化的命令处理和事件观察
- **中间件系统** - 支持日志、监控、验证等中间件

### 📦 模块化设计
- **清晰的架构** - 协议层、核心层、业务层分离
- **易于扩展** - 支持自定义认证器、事件处理器、设备管理器等
- **类型安全** - 充分利用 Rust 的类型系统保证安全性

## 📦 安装

### 使用 Cargo

```toml
[dependencies]
flare-core = "0.1.0"
```

### 功能特性

当前版本包含以下特性（可通过 `features` 启用，未来版本）：

- `default` - 默认功能集（当前包含所有功能）

## 🚀 快速开始

### 服务端示例

#### 观察者模式（推荐）

```rust
use flare_core::server::*;
use flare_core::common::*;
use std::sync::Arc;

// 实现 ConnectionHandler
struct MyHandler;

#[async_trait::async_trait]
impl ConnectionHandler for MyHandler {
    async fn handle_frame(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        // 处理消息
        Ok(None)
    }

    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        println!("新连接: {}", connection_id);
        Ok(())
    }

    async fn on_disconnect(&self, connection_id: &str) -> Result<()> {
        println!("连接断开: {}", connection_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let handler = Arc::new(MyHandler);
    
    let mut server = ObserverServerBuilder::new("0.0.0.0:8080")
        .with_handler(handler)
        .build()?;
    
    server.start().await?;
    tokio::signal::ctrl_c().await?;
    server.stop().await?;
    Ok(())
}
```

#### 简单模式

```rust
use flare_core::server::*;
use flare_core::common::*;

#[tokio::main]
async fn main() -> Result<()> {
    let mut server = ServerBuilder::new("0.0.0.0:8080")
        .on_message(|frame, ctx| async move {
            // 处理消息
            Ok(None)
        })
        .on_connect(|conn_id, ctx| async move {
            println!("新连接: {}", conn_id);
            Ok(())
        })
        .build()?;
    
    server.start().await?;
    tokio::signal::ctrl_c().await?;
    server.stop().await?;
    Ok(())
}
```

### 客户端示例

#### 观察者模式（推荐）

```rust
use flare_core::client::*;
use flare_core::transport::events::*;
use std::sync::Arc;

// 实现 ConnectionObserver
struct MyObserver;

impl ConnectionObserver for MyObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Connected => println!("已连接"),
            ConnectionEvent::Disconnected(reason) => println!("断开: {}", reason),
            ConnectionEvent::Message(data) => {
                // 处理消息
            }
            ConnectionEvent::Error(e) => eprintln!("错误: {:?}", e),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let observer = Arc::new(MyObserver);
    
    let mut client = ObserverClientBuilder::new("ws://127.0.0.1:8080")
        .with_observer(observer)
        .build_with_race()
        .await?;
    
    // 发送消息
    let frame = /* ... */;
    client.send_frame(&frame).await?;
    
    tokio::signal::ctrl_c().await?;
    client.disconnect().await?;
    Ok(())
}
```

#### 简单模式

```rust
use flare_core::client::*;

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = ClientBuilder::new("ws://127.0.0.1:8080")
        .on_message(|frame| {
            println!("收到消息: {:?}", frame);
            Ok(())
        })
        .on_event(|event| {
            println!("事件: {:?}", event);
        })
        .build_with_race()
        .await?;
    
    // 使用客户端...
    Ok(())
}
```

## 📚 核心模块

### 服务端模块 (`server`)

- **`HybridServer`** - 混合服务端，支持多协议监听
- **`FlareServerBuilder`** - Flare 服务端构建器（推荐，最简单）
- **`ServerBuilder`** / **`ObserverServerBuilder`** - 服务端构建器（高级定制）
- **`MessageListener`** - 消息监听器接口（消息管道模式）
- **`ConnectionManager`** - 连接管理器
- **`ServerCore`** - 服务端核心功能（连接管理、心跳检测、协商处理）
- **`Authenticator`** - 认证器接口
- **`DeviceManager`** - 设备管理器
- **`ServerEventHandler`** - 事件处理器接口

### 客户端模块 (`client`)

- **`HybridClient`** - 混合客户端，支持协议竞速
- **`FlareClientBuilder`** - Flare 客户端构建器（推荐，最简单）
- **`ClientBuilder`** / **`ObserverClientBuilder`** - 客户端构建器（高级定制）
- **`MessageListener`** - 消息监听器接口（消息管道模式）
- **`ClientCore`** - 客户端核心功能（状态管理、心跳管理、消息路由）
- **`ConnectionObserver`** - 连接观察者接口
- **`ClientEventHandler`** - 事件处理器接口

### 公共模块 (`common`)

- **`MessageParser`** - 消息解析器（支持 Protobuf 和 JSON，默认JSON）
- **`MessagePipeline`** - 消息处理管道（支持中间件、自动序列化/压缩）
- **`MessageMiddleware`** - 消息处理中间件（日志、监控、验证等）
- **`MessageProcessor`** - 消息处理器（业务逻辑处理）
- **`MessageListener`** - 消息监听器接口（消息管道模式使用）
- **`LoggingMiddleware`** - 日志中间件
- **`MetricsMiddleware`** - 性能监控中间件
- **`ValidationMiddleware`** - 验证中间件
- **`CompressionAlgorithm`** - 压缩算法（None、Gzip、Zstd）
- **`SerializationFormat`** - 序列化格式（Protobuf、JSON，默认JSON）
- **`DeviceInfo`** - 设备信息
- **`DeviceConflictStrategy`** - 设备冲突策略
- **`FlareError`** - 统一错误类型

### 传输模块 (`transport`)

- **`Connection`** - 连接接口
- **`ConnectionEvent`** - 连接事件
- **`WebSocketTransport`** - WebSocket 传输实现
- **`QUICTransport`** - QUIC 传输实现

## 🔧 高级功能

### 认证配置

```rust
use flare_core::server::*;

// 自定义认证器
struct MyAuthenticator;

#[async_trait::async_trait]
impl Authenticator for MyAuthenticator {
    async fn authenticate(
        &self,
        token: &str,
        connection_id: &str,
        device_info: Option<&DeviceInfo>,
        metadata: Option<&HashMap<String, Vec<u8>>>,
    ) -> Result<AuthResult> {
        // 验证 token
        if token == "valid_token" {
            Ok(AuthResult::success(Some("user123".to_string())))
        } else {
            Ok(AuthResult::failure("Token 无效".to_string()))
        }
    }
}

let authenticator = Arc::new(MyAuthenticator);
let server = ObserverServerBuilder::new("0.0.0.0:8080")
    .enable_auth()
    .with_authenticator(authenticator)
    .with_auth_timeout(Duration::from_secs(30))
    .build()?;
```

### 设备管理

```rust
use flare_core::server::*;
use flare_core::common::device::*;

let device_manager = Arc::new(DeviceManager::new(
    DeviceConflictStrategyBuilder::new()
        .platform_exclusive() // 平台互斥
        .build()
));

let server = ObserverServerBuilder::new("0.0.0.0:8080")
    .with_device_manager(device_manager)
    .build()?;
```

### 序列化协商

```rust
// 服务端：设置默认格式（默认使用JSON）
let server = ObserverServerBuilder::new("0.0.0.0:8080")
    .with_default_format(SerializationFormat::Json) // 默认JSON，可选Protobuf
    .with_default_compression(CompressionAlgorithm::None) // 默认不压缩
    .build()?;

// 客户端：不指定格式（使用服务端默认JSON）
let client = ObserverClientBuilder::new("ws://127.0.0.1:8080")
    // 不调用 with_format()，将使用服务端默认JSON
    .build_with_race()
    .await?;

// 客户端：指定格式（非强制，服务端优先使用客户端格式）
let client = ObserverClientBuilder::new("ws://127.0.0.1:8080")
    .with_format(SerializationFormat::Protobuf) // 指定格式，服务端优先使用
    .with_compression(CompressionAlgorithm::Gzip)
    .build_with_race()
    .await?;

// 客户端：强制指定格式（服务端必须使用）
let client = ObserverClientBuilder::new("ws://127.0.0.1:8080")
    .force_format(SerializationFormat::Json) // 强制使用 JSON
    .build_with_race()
    .await?;
```

**协商规则**：
- **默认**：服务端默认使用 JSON + 不压缩
- **客户端不指定**：使用服务端默认 JSON
- **客户端指定（非强制）**：服务端优先使用客户端格式
- **客户端强制**：服务端必须使用客户端格式
- **连接成功后**：CONNECT_ACK 返回最终确定的格式、压缩方式、加密方式
- **后续消息**：客户端和服务端都使用协商后的格式处理消息

### 协议竞速

```rust
use flare_core::common::config_types::TransportProtocol;

let client = ObserverClientBuilder::new("127.0.0.1:8080")
    .with_protocol_race(vec![
        TransportProtocol::QUIC,      // 优先级 0（最高）
        TransportProtocol::WebSocket,  // 优先级 1
    ])
    .with_protocol_url(TransportProtocol::WebSocket, "ws://127.0.0.1:8080".to_string())
    .with_protocol_url(TransportProtocol::QUIC, "quic://127.0.0.1:8081".to_string())
    .build_with_race()
    .await?;
```

## 📖 文档

### API 文档

完整的 API 文档请查看 [docs.rs/flare-core](https://docs.rs/flare-core)

### 示例

项目包含丰富的示例，位于 `examples/` 目录：

- **基础示例**
  - `websocket_server.rs` / `websocket_client.rs` - WebSocket 基础示例
  - `quic_server.rs` / `quic_client.rs` - QUIC 基础示例
  - `hybrid_server.rs` / `hybrid_client.rs` - 混合协议示例

- **高级示例**
  - `negotiation_server.rs` / `negotiation_client.rs` - 序列化协商和设备管理示例
  - `auth_server.rs` / `auth_client.rs` - 认证示例
  - `simple_server.rs` / `simple_client.rs` - 简单模式示例

运行示例：

```bash
# 查看所有示例
cargo run --example <example_name>

# 查看示例说明
cat examples/README.md
```

## 🏗️ 架构设计

### 模块分层

```
┌─────────────────────────────────────────┐
│          应用层 (Application)            │
│  - ConnectionHandler / ConnectionObserver │
│  - EventHandler                          │
│  - Authenticator                         │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│          核心层 (Core)                    │
│  - ServerCore / ClientCore               │
│  - ConnectionManager                     │
│  - HeartbeatDetector / HeartbeatManager  │
│  - DeviceManager                         │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│          协议层 (Protocol)                │
│  - WebSocketServer / WebSocketClient     │
│  - QUICServer / QUICClient               │
│  - HybridServer / HybridClient           │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│          传输层 (Transport)                │
│  - WebSocketTransport                    │
│  - QUICTransport                         │
│  - Connection trait                      │
└─────────────────────────────────────────┘
```

### 核心组件

1. **ServerCore** / **ClientCore**
   - 统一管理连接状态、心跳、消息路由
   - 处理协商、设备冲突、认证等核心逻辑

2. **ConnectionManager**
   - 管理所有活跃连接
   - 支持按连接ID、用户ID查询
   - 维护连接状态和元数据

3. **MessageParser**
   - 支持 Protobuf 和 JSON 自动检测
   - 支持压缩/解压缩
   - 协商后动态更新解析器

4. **DeviceManager**
   - 管理用户设备
   - 实现设备冲突策略
   - 处理设备踢出逻辑

## 🔒 安全性

- **认证机制** - 支持可配置的 token 验证
- **连接状态** - 只有已验证的连接才能收发业务消息
- **TLS 支持** - 支持 TLS/SSL 加密（WebSocket 和 QUIC）
- **错误处理** - 完善的错误处理和日志记录

## 📊 性能特性

- **异步架构** - 基于 Tokio 的高性能异步运行时
- **零拷贝** - 尽可能减少数据拷贝
- **连接复用** - 高效的连接管理
- **协议竞速** - 自动选择最快的协议

## 📦 发布到 crates.io

### 安装

```bash
cargo add flare-core
```

### 发布准备

发布前请确保：

1. ✅ 所有代码已通过测试和 lint 检查
2. ✅ 文档已更新
3. ✅ 版本号已更新
4. ✅ README.md 和 Cargo.toml 已完善

详细发布指南请查看 [PUBLISH.md](PUBLISH.md)。

### 发布命令

```bash
# 1. 登录 crates.io（首次发布需要）
cargo login <your-token>

# 2. 检查打包
cargo package

# 3. 发布
cargo publish
```

## 🤝 贡献

欢迎贡献！请查看 [CONTRIBUTING.md](CONTRIBUTING.md) 了解详细信息。

## 📄 许可证

本项目采用 MIT 许可证。详见 [LICENSE](LICENSE) 文件。

## 🔗 相关链接

- [文档](https://docs.rs/flare-core)
- [GitHub 仓库](https://github.com/flare-team/flare-core)
- [问题反馈](https://github.com/flare-team/flare-core/issues)
- [crates.io](https://crates.io/crates/flare-core)

## 📝 版本历史

### 0.1.0 (当前版本)

- ✨ 初始发布
- 支持 WebSocket 和 QUIC 协议
- 支持协议竞速
- 支持序列化协商（默认JSON，支持Protobuf/JSON）
- 支持压缩算法（None/Gzip/Zstd）
- 支持多设备管理
- 支持 Token 认证
- 提供观察者模式和简单模式两种构建方式
- 完善的事件处理机制
- 消息处理管道（MessagePipeline）支持中间件
- 统一的消息处理流程，自动序列化/压缩
- 连接成功后返回完整协商结果（格式、压缩、加密）

---

**Flare Core** - 让实时通信变得简单 🚀

