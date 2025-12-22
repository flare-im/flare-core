# Flare Core 示例项目

本目录包含 `flare-core` 库的各种使用示例，展示了不同的构建模式和协议支持。

## 示例列表

### 1. 简单模式（Simple Mode）

使用闭包定义消息处理逻辑，无需实现 trait，适合快速开发。

#### `simple_server.rs` - 简单模式服务端

**特点：**
- 使用 `ServerBuilder`，最简单的构建方式
- 使用闭包定义消息处理逻辑（`on_message`, `on_connect`, `on_disconnect`）
- 仅支持 WebSocket 协议（ws://）
- 不需要 TLS/SSL 证书
- 监听端口：8080

**启动命令：**
```bash
RUST_LOG=info cargo run --example simple_server
```

#### `simple_client.rs` - 简单模式客户端

**特点：**
- 使用 `ClientBuilder`，最简单的构建方式
- 使用闭包定义消息和事件处理逻辑
- 自动连接管理、心跳检测
- 仅支持 WebSocket 协议（ws://）

**启动命令：**
```bash
RUST_LOG=info cargo run --example simple_client
```

---

### 2. 观察者模式（Observer Mode）

使用 trait 实现消息处理，支持多协议和共享连接管理，适合需要更多控制的场景。

#### `observer_server.rs` - 观察者模式服务端

**特点：**
- 使用 `ObserverServerBuilder`
- 实现 `ServerEventHandler` trait 处理消息和连接事件
- 支持多协议（WebSocket + QUIC）
- 共享 ConnectionManager 统一管理连接
- WebSocket: ws://0.0.0.0:8080
- QUIC: quic://0.0.0.0:8081

**启动命令：**
```bash
RUST_LOG=info cargo run --example observer_server
```

#### `observer_client.rs` - 观察者模式客户端

**特点：**
- 使用 `ObserverClientBuilder`
- 实现 `ConnectionObserver` trait 接收消息
- 支持协议竞速，自动选择最快的协议（WebSocket 或 QUIC）
- 心跳检测、自动重连

**启动命令：**
```bash
RUST_LOG=info cargo run --example observer_client
```

---

### 3. Flare 模式（Flare Mode）

最完整的实现，支持所有功能特性，推荐用于生产环境。

#### `flare_chat_server.rs` - Flare 模式服务端

**特点：**
- 使用 `FlareServerBuilder`，功能最完整
- 实现 `ServerEventHandler` trait，自动消息路由和 ACK 处理
- 设备管理（设备冲突策略）
- 序列化协商（JSON/Protobuf）
- 压缩协商（Gzip/Zstd/None）
- 加密支持（AES-256-GCM）
- 多协议支持（WebSocket + QUIC）

**启动命令：**
```bash
RUST_LOG=info cargo run --example flare_chat_server
```

#### `flare_chat_client.rs` - Flare 模式客户端

**特点：**
- 使用 `FlareClientBuilder`，功能最完整
- 实现 `MessageListener` trait 接收消息
- 消息管道（中间件支持）
- 协议竞速
- 设备管理
- 加密支持

**启动命令：**
```bash
RUST_LOG=info cargo run --example flare_chat_client -- user123
```

---

### 4. QUIC 协议示例

使用基础结构直接构建 QUIC 客户端和服务端，展示 QUIC 协议的使用。

#### `quic_server.rs` - QUIC 服务端

**特点：**
- 使用 `HybridServer` 直接构建
- 实现 `ConnectionHandler` trait
- 需要 TLS 证书（自动生成到 certs/ 目录）
- 监听端口：8081

**启动命令：**
```bash
RUST_LOG=info cargo run --example quic_server
```

#### `quic_client.rs` - QUIC 客户端

**特点：**
- 使用 `HybridClient` 直接构建
- 实现 `ConnectionObserver` trait 接收消息
- 手动添加观察者和处理连接状态

**启动命令：**
```bash
RUST_LOG=info cargo run --example quic_client
```

---

### 5. WebSocket 协议示例

使用基础结构直接构建 WebSocket 客户端和服务端，展示 WebSocket 协议的使用。

#### `websocket_server.rs` - WebSocket 服务端

**特点：**
- 使用 `HybridServer` 直接构建
- 实现 `ConnectionHandler` trait
- 纯 WebSocket 连接（ws://），不使用 TLS/SSL
- 监听端口：8080

**启动命令：**
```bash
RUST_LOG=info cargo run --example websocket_server
```

#### `websocket_client.rs` - WebSocket 客户端

**特点：**
- 使用 `HybridClient` 直接构建
- 实现 `ConnectionObserver` trait 接收消息
- 手动添加观察者和处理连接状态

**启动命令：**
```bash
RUST_LOG=info cargo run --example websocket_client
```

---

## 构建模式对比

| 模式 | Builder | 实现方式 | 特点 | 适用场景 |
|------|---------|---------|------|----------|
| 简单模式 | `ServerBuilder`<br>`ClientBuilder` | 闭包 | 最简单，无需实现 trait | 快速原型开发 |
| 观察者模式 | `ObserverServerBuilder`<br>`ObserverClientBuilder` | `ServerEventHandler`<br>`ConnectionObserver` | 多协议、共享连接管理 | 需要更多控制的高级场景 |
| Flare 模式 | `FlareServerBuilder`<br>`FlareClientBuilder` | `ServerEventHandler`<br>`MessageListener` | 完整功能、消息管道、中间件 | 生产环境推荐 |

## 协议支持

- **WebSocket**: 标准 WebSocket 协议，支持 ws:// 和 wss://
- **QUIC**: 基于 UDP 的 QUIC 协议，需要 TLS 证书

## 运行所有示例

```bash
# 编译所有示例
cargo build --examples

# 运行特定示例
cargo run --example <example_name>
```

## 注意事项

1. **QUIC 协议需要 TLS 证书**：QUIC 示例会自动生成自签名证书到 `certs/` 目录
2. **日志级别**：所有示例默认使用 `info` 级别日志，可通过 `RUST_LOG` 环境变量调整
3. **端口冲突**：确保示例使用的端口（8080, 8081）未被占用

## 示例功能

所有示例都实现了基本的聊天室功能：
- 用户连接/断开
- 消息发送/接收
- 消息广播
- 用户名管理

不同示例展示了不同的构建方式和功能特性，可以根据需求选择合适的示例作为起点。
