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

### 🏗️ 统一的构建模式架构

Flare Core 采用**统一的三种构建模式**，客户端和服务端都提供相同的抽象层次，确保架构一致性和代码复用。

#### 三种模式概览

| 模式 | 构建器 | 实现方式 | 抽象级别 | 适用场景 |
|------|--------|---------|---------|---------|
| **简单模式** | `ClientBuilder` / `ServerBuilder` | 闭包（Closure） | ⭐ 最低 | 快速原型、学习测试、小型应用 |
| **观察者模式** | `ObserverClientBuilder` / `ObserverServerBuilder` | Trait 实现 | ⭐⭐ 中等 | 自定义处理、事件驱动、需要基本功能 |
| **Flare 模式** | `FlareClientBuilder` / `FlareServerBuilder` | 完整功能集 | ⭐⭐⭐ 最高 | 生产环境、企业应用、完整功能需求 |

#### 架构设计原则

1. **公共逻辑统一处理**：所有模式共享底层实现（`HybridClient`/`HybridServer`），避免代码重复
2. **渐进式增强**：从简单到复杂，按需选择，无需为兼容性保留冗余代码
3. **类型安全**：充分利用 Rust 类型系统，编译期保证正确性
4. **零成本抽象**：高级抽象不带来运行时开销

#### 客户端三种模式

**简单模式（ClientBuilder）**
- ✅ 使用闭包处理消息和事件
- ✅ 最小依赖，零配置
- ✅ 适合快速原型和学习

**观察者模式（ObserverClientBuilder）**
- ✅ 实现 `ConnectionObserver` trait
- ✅ 支持自定义事件处理
- ✅ 支持消息路由

**Flare 模式（FlareClientBuilder）**
- ✅ 实现 `MessageListener` trait
- ✅ 完整的消息管道（序列化、压缩、加密）
- ✅ 中间件支持、自动重连、协议竞速

#### 服务端三种模式

**简单模式（ServerBuilder）**
- ✅ 使用闭包处理消息和连接事件
- ✅ 最小依赖，零配置
- ✅ 适合快速原型和学习

**观察者模式（ObserverServerBuilder）**
- ✅ 实现 `ServerEventHandler` trait（必需）
- ✅ 自动消息路由到对应处理方法
- ✅ 自动 ACK 处理和错误响应
- ✅ 支持设备管理和认证

**Flare 模式（FlareServerBuilder）**
- ✅ 实现 `ServerEventHandler` trait（必需）
- ✅ 完整功能：设备管理、认证、心跳、多协议
- ✅ 序列化协商、压缩算法协商
- ✅ 生产环境推荐

#### 统一的核心特性

所有模式都基于统一的底层实现，共享以下核心能力：

- **多协议支持**：WebSocket + QUIC，自动协议竞速（客户端）或多协议监听（服务端）
- **序列化协商**：自动协商最佳序列化格式（JSON/Protobuf）和压缩算法（None/Gzip/Zstd）
- **心跳机制**：自动心跳检测和超时管理
- **连接管理**：统一的连接生命周期管理
- **错误处理**：完善的错误处理和日志记录
- **类型安全**：充分利用 Rust 类型系统保证安全性

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

#### Flare 模式（FlareServerBuilder，推荐用于生产环境）

详见：[服务端 Flare 模式示例](doc/server-flare-mode-example.md)

#### 观察者模式（ObserverServerBuilder）

详见：[服务端观察者模式示例](doc/server-observer-mode-example.md)

#### 简单模式（ServerBuilder）

详见：[服务端简单模式示例](doc/server-simple-mode-example.md)

### 客户端示例

#### Flare 模式（FlareClientBuilder，推荐用于生产环境）

详见：[客户端 Flare 模式示例](doc/client-flare-mode-example.md)

#### 观察者模式（ObserverClientBuilder）

详见：[客户端观察者模式示例](doc/client-observer-mode-example.md)

#### 简单模式（ClientBuilder）

详见：[客户端简单模式示例](doc/client-simple-mode-example.md)

## 📚 核心模块

### 服务端模块 (`server`)

- **`HybridServer`** - 混合服务端，支持多协议监听
- **`FlareServerBuilder`** - Flare 服务端构建器（推荐，最简单）
- **`ServerBuilder`** / **`ObserverServerBuilder`** - 服务端构建器（高级定制）
- **`MessageListener`** - 消息监听器接口（用于自定义命令处理）
- **`ServerEventHandler`** - 事件处理器接口（**必需**，核心接口，处理消息命令、通知命令、系统命令和连接事件）
- **`ConnectionHandler`** - 连接处理器接口（用于简单构建器）
- **`ConnectionManager`** - 连接管理器
- **`ServerCore`** - 服务端核心功能（连接管理、心跳检测、协商处理）
- **`Authenticator`** - 认证器接口
- **`DeviceManager`** - 设备管理器

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

### 构建模式详细对比

#### 服务端构建模式对比

| 特性 | 简单模式 | 观察者模式 | Flare 模式 |
|------|---------|-----------|-----------|
| **构建器** | `ServerBuilder` | `ObserverServerBuilder` | `FlareServerBuilder` |
| **实现方式** | 闭包（Closure） | `ServerEventHandler` trait | `ServerEventHandler` trait |
| **功能完整性** | ⭐ 最小实现 | ⭐⭐ 基本功能 | ⭐⭐⭐ 完整功能 |
| **自动 ACK** | ❌ 需手动处理 | ✅ 自动处理 | ✅ 自动处理 |
| **错误处理** | ❌ 需手动处理 | ✅ 自动处理 | ✅ 自动处理 |
| **设备管理** | ❌ | ✅ | ✅ |
| **认证机制** | ✅ | ✅ | ✅ |
| **序列化协商** | ✅ | ✅ | ✅ |
| **心跳检测** | ✅ | ✅ | ✅ |
| **适用场景** | 快速原型、学习 | 自定义处理逻辑 | 生产环境 |

#### 客户端构建模式对比

| 特性 | 简单模式 | 观察者模式 | Flare 模式 |
|------|---------|-----------|-----------|
| **构建器** | `ClientBuilder` | `ObserverClientBuilder` | `FlareClientBuilder` |
| **实现方式** | 闭包（Closure） | `ConnectionObserver` trait | `MessageListener` trait |
| **功能完整性** | ⭐ 最小实现 | ⭐⭐ 基本功能 | ⭐⭐⭐ 完整功能 |
| **消息管道** | ❌ | ❌ | ✅ |
| **中间件支持** | ❌ | ❌ | ✅ |
| **自动重连** | ❌ | ❌ | ✅ |
| **协议竞速** | ✅ | ✅ | ✅ |
| **序列化协商** | ✅ | ✅ | ✅ |
| **心跳管理** | ✅ | ✅ | ✅ |
| **适用场景** | 快速原型、学习 | 自定义处理逻辑 | 生产环境 |

#### 架构设计原则

**1. 公共逻辑统一处理**
- 所有模式共享底层实现（`HybridClient`/`HybridServer`）
- 协议层、核心层、业务层清晰分离
- 避免代码重复，维护成本低

**2. 渐进式增强**
- 从简单到复杂，按需选择
- 无需为兼容性保留冗余代码
- 高级模式完全兼容低级模式的功能

**3. 类型安全**
- 充分利用 Rust 类型系统
- 编译期保证正确性
- 零成本抽象，无运行时开销

**4. 统一的事件处理**
- **服务端**：`ServerEventHandler` 是观察者模式和 Flare 模式的核心接口
  - 细化的命令处理：按命令类型处理（消息、通知、自定义、系统事件）
  - 自动路由：`ServerMessageWrapper` 自动将消息路由到对应方法
  - 自动 ACK：框架自动处理 ACK 响应和错误响应
  - 生命周期管理：统一处理连接建立、断开、错误等事件

- **客户端**：`MessageListener`（Flare 模式）和 `ConnectionObserver`（观察者模式）
  - 统一的事件处理接口
  - 支持消息管道和中间件（Flare 模式）
  - 灵活的事件驱动架构

#### 如何选择构建模式？

**简单模式**
- ✅ 快速原型开发
- ✅ 学习和测试
- ✅ 小型应用
- ✅ 需要完全控制消息处理流程

**观察者模式**
- ✅ 需要自定义消息处理逻辑
- ✅ 需要事件驱动的架构
- ✅ 不需要完整功能集
- ✅ 需要灵活扩展

**Flare 模式（推荐）**
- ✅ 生产环境
- ✅ 需要完整功能的企业应用
- ✅ 需要高性能和可扩展性
- ✅ 需要统一消息处理流程

### 事件处理器（ServerEventHandler）

`ServerEventHandler` 是服务端的核心接口，**必须实现**。它提供了细化的命令处理方法：

```rust
use flare_core::server::events::handler::ServerEventHandler;
use flare_core::common::protocol::*;
use async_trait::async_trait;

struct MyEventHandler;

#[async_trait]
impl ServerEventHandler for MyEventHandler {
    // 处理消息命令（如发送消息）
    async fn handle_message_command_by_type(
        &self,
        command: &MessageCommand,
        msg_type: MessageType,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        match msg_type {
            MessageType::Send => {
                println!("收到发送消息请求: {:?}", command);
                // 处理消息发送
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    // 处理通知命令
    async fn handle_notification_command(
        &self,
        command: &NotificationCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        println!("收到通知: {:?}", command);
        Ok(None)
    }

    // 处理自定义命令
    async fn handle_custom_command(
        &self,
        command: &CustomCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        println!("收到自定义命令: {}", command.name);
        Ok(None)
    }

    // 处理系统事件
    async fn handle_system_event(
        &self,
        frame: &Frame,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        println!("收到系统事件");
        Ok(None)
    }

    // 连接建立完成（在 CONNECT 协商完成后调用）
    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        println!("连接建立: {}", connection_id);
        Ok(())
    }

    // 连接断开
    async fn on_disconnect(&self, connection_id: &str, reason: Option<&str>) -> Result<()> {
        println!("连接断开: {}, 原因: {:?}", connection_id, reason);
        Ok(())
    }

    // 连接错误
    async fn on_error(&self, connection_id: &str, error: &str) -> Result<()> {
        eprintln!("连接错误: {}, 错误: {}", connection_id, error);
        Ok(())
    }
}

// 使用
let event_handler = Arc::new(MyEventHandler);
let server = FlareServerBuilder::new("0.0.0.0:8080")
    .with_event_handler(event_handler)
    .build()?;
```

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
let server = FlareServerBuilder::new("0.0.0.0:8080")
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
│  - ServerEventHandler (必需)            │
│  - MessageListener (自定义命令)          │
│  - ConnectionHandler (简单构建器)        │
│  - Authenticator                         │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│          核心层 (Core)                    │
│  - ServerCore / ClientCore               │
│  - DefaultServerMessageObserver          │
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

2. **DefaultServerMessageObserver**
   - 默认的消息观察者实现
   - 自动路由消息命令到 `ServerEventHandler`
   - 处理系统命令（PING/PONG/CONNECT）
   - 管理连接生命周期事件

3. **ServerEventHandler**（必需）
   - 核心事件处理接口，**必须实现**
   - 提供细化的命令处理方法（消息、通知、自定义、系统事件）
   - 处理连接生命周期事件（on_connect、on_disconnect、on_error）
   - 由 `DefaultServerMessageObserver` 自动调用

4. **ConnectionManager**
   - 管理所有活跃连接
   - 支持按连接ID、用户ID查询
   - 维护连接状态和元数据

5. **MessageParser**
   - 支持 Protobuf 和 JSON 自动检测
   - 支持压缩/解压缩
   - 协商后动态更新解析器

6. **DeviceManager**
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

### 性能指标

| 指标 | 目标值 | 说明 |
|------|--------|------|
| **消息处理延迟** | P99 < 50ms | 端到端消息处理延迟 |
| **连接建立延迟** | P99 < 100ms | 从连接到协商完成 |
| **内存占用** | < 2GB/10K连接 | 单实例内存占用 |
| **吞吐量** | 10万+ TPS/实例 | 单实例消息处理能力 |

### 性能优化建议

1. **使用协商后的格式**：协商完成后使用 Protobuf + Gzip 可以获得最佳性能
2. **合理使用日志级别**：生产环境建议使用 `RUST_LOG=warn`，避免热路径日志开销
3. **连接管理**：及时清理超时连接，避免内存泄漏
4. **消息批处理**：对于批量消息，考虑使用批处理 API

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
- **ServerEventHandler 作为核心接口**：提供细化的命令处理和事件观察
- **DefaultServerMessageObserver**：自动路由消息到 ServerEventHandler
- 提供 Flare 模式（推荐）和观察者模式两种构建方式
- 消息处理管道（MessagePipeline）支持中间件
- 统一的消息处理流程，自动序列化/压缩
- 连接成功后返回完整协商结果（格式、压缩、加密）

---

**Flare Core** - 让实时通信变得简单 🚀

