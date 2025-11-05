# Flare Core 示例说明

本文档介绍所有可用的示例程序，包括它们的用途、构建方式和启动命令。

## 示例分类

### 1. 基础结构示例（直接使用 HybridServer/HybridClient）

这些示例展示了如何使用基础结构直接构建服务器和客户端，需要手动实现 trait 和管理连接状态。

#### `websocket_server.rs` - WebSocket 服务端（基础结构）

**用途：**
- 使用基础结构（HybridServer）直接构建 WebSocket 服务端
- 展示如何实现 `ConnectionHandler` trait 来处理消息
- 手动管理服务器引用和连接状态

**特点：**
- 仅支持 WebSocket 协议（ws://）
- 不需要 TLS/SSL 证书
- 监听端口：8080

**启动命令：**
```bash
# 默认使用 debug 级别日志（方便调试）
cargo run --example websocket_server

# 如果需要更改日志级别，使用环境变量
RUST_LOG=info cargo run --example websocket_server
```

#### `websocket_client.rs` - WebSocket 客户端（基础结构）

**用途：**
- 使用基础结构（HybridClient）直接构建 WebSocket 客户端
- 展示如何实现 `ConnectionObserver` trait 来接收消息
- 手动添加观察者和处理连接状态

**启动命令：**
```bash
# 默认使用 debug 级别日志（方便调试）
cargo run --example websocket_client
```

#### `quic_server.rs` - QUIC 服务端（基础结构）

**用途：**
- 使用基础结构（HybridServer）直接构建 QUIC 服务端
- 展示如何实现 `ConnectionHandler` trait 来处理消息
- 手动管理服务器引用和连接状态

**特点：**
- 仅支持 QUIC 协议
- 需要 TLS 证书（自动生成到 `certs/` 目录）
- 监听端口：8081

**启动命令：**
```bash
# 默认使用 debug 级别日志（方便调试）
cargo run --example quic_server
```

#### `quic_client.rs` - QUIC 客户端（基础结构）

**用途：**
- 使用基础结构（HybridClient）直接构建 QUIC 客户端
- 展示如何实现 `ConnectionObserver` trait 来接收消息
- 手动添加观察者和处理连接状态

**启动命令：**
```bash
# 默认使用 debug 级别日志（方便调试）
cargo run --example quic_client
```

---

### 2. 观察者模式 Builder 示例

这些示例使用观察者模式的 Builder，适合需要更多控制的高级场景，支持多协议和共享连接管理。

#### `hybrid_server.rs` - 混合服务端（观察者模式 Builder）

**用途：**
- 使用观察者模式的 Builder（ObserverServerBuilder）构建服务端
- 同时监听 WebSocket 和 QUIC 协议
- 使用共享的 ConnectionManager 管理连接状态

**特点：**
- 支持多协议（WebSocket + QUIC）
- WebSocket: ws://0.0.0.0:8080
- QUIC: quic://0.0.0.0:8081（需要 TLS 证书）
- 需要实现 `ConnectionHandler` trait

**启动命令：**
```bash
# 默认使用 debug 级别日志（方便调试）
cargo run --example hybrid_server
```

#### `hybrid_client.rs` - 混合客户端（观察者模式 Builder）

**用途：**
- 使用观察者模式的 Builder（ObserverClientBuilder）构建客户端
- 使用协议竞速连接服务器，自动选择最快的协议（WebSocket 或 QUIC）
- 需要实现 `ConnectionObserver` trait

**启动命令：**
```bash
# 默认使用 debug 级别日志（方便调试）
cargo run --example hybrid_client
```

---

### 3. 简单模式 Builder 示例

这些示例使用简单模式的 Builder，使用闭包定义消息处理逻辑，无需实现 trait，适合快速开发。

#### `simple_server.rs` - 简化服务端（简单模式 Builder）

**用途：**
- 使用简单模式的 Builder（ServerBuilder）构建服务端
- 使用闭包定义消息处理逻辑（无需实现 trait）
- 通过 MessageContext 进行广播等操作

**特点：**
- 仅支持 WebSocket 协议（ws://）
- 不需要 TLS/SSL 证书
- 监听端口：8080
- 最简单的使用方式

**启动命令：**
```bash
# 默认使用 debug 级别日志（方便调试）
cargo run --example simple_server
```

#### `simple_client.rs` - 简化客户端（简单模式 Builder）

**用途：**
- 使用简单模式的 Builder（ClientBuilder）构建客户端
- 使用闭包定义消息和事件处理逻辑（无需实现 trait）
- 自动处理观察者注册和连接管理

**启动命令：**
```bash
# 默认使用 debug 级别日志（方便调试）
cargo run --example simple_client
```

---

## 快速开始

### 1. 运行简单模式示例（推荐新手）

**终端 1 - 启动服务端：**
```bash
cargo run --example simple_server
```

**终端 2 - 启动客户端：**
```bash
cargo run --example simple_client
```

### 2. 运行 WebSocket 基础结构示例

**终端 1 - 启动服务端：**
```bash
cargo run --example websocket_server
```

**终端 2 - 启动客户端：**
```bash
cargo run --example websocket_client
```

### 3. 运行 QUIC 基础结构示例

**终端 1 - 启动服务端：**
```bash
cargo run --example quic_server
```

**终端 2 - 启动客户端：**
```bash
cargo run --example quic_client
```

### 4. 运行混合多协议示例

**终端 1 - 启动服务端：**
```bash
cargo run --example hybrid_server
```

**终端 2 - 启动客户端（协议竞速）：**
```bash
cargo run --example hybrid_client
```

---

## 日志级别说明

所有示例**默认使用 `debug` 级别日志**（方便调试），可以通过环境变量 `RUST_LOG` 覆盖：

```bash
# Debug 级别（默认，显示详细信息，方便调试）
cargo run --example simple_server

# Info 级别（只显示重要信息）
RUST_LOG=info cargo run --example simple_server

# Warn 级别（只显示警告和错误）
RUST_LOG=warn cargo run --example simple_server

# Error 级别（只显示错误）
RUST_LOG=error cargo run --example simple_server

# Trace 级别（显示最详细的信息）
RUST_LOG=trace cargo run --example simple_server

# 指定特定模块的日志级别
RUST_LOG=debug,flare_core=trace cargo run --example simple_server
```

---

## 构建方式对比

| 示例类型 | 构建方式 | 需要实现 Trait | 适用场景 |
|---------|---------|---------------|---------|
| 基础结构 | `HybridServer::new()`<br>`HybridClient::connect_with_config()` | ✅ ConnectionHandler<br>✅ ConnectionObserver | 需要完全控制的复杂场景 |
| 观察者模式 Builder | `ObserverServerBuilder`<br>`ObserverClientBuilder` | ✅ ConnectionHandler<br>✅ ConnectionObserver | 多协议、共享连接管理 |
| 简单模式 Builder | `ServerBuilder`<br>`ClientBuilder` | ❌ 使用闭包 | 快速开发、简单场景 |

---

## 注意事项

1. **端口占用**：确保端口 8080（WebSocket）和 8081（QUIC）未被占用
2. **TLS 证书**：QUIC 协议需要 TLS 证书，首次运行会自动生成到 `certs/` 目录
3. **协议匹配**：确保客户端和服务端使用相同的协议
4. **日志级别**：调试时建议使用 `debug` 级别，生产环境使用 `info` 或 `warn`

---

## 故障排查

### 端口已被占用
```bash
# Linux/macOS 查看端口占用
lsof -i :8080
lsof -i :8081

# 或使用 netstat
netstat -an | grep 8080
```

### 连接失败
- 检查服务端是否已启动
- 检查防火墙设置
- 检查协议是否匹配（WebSocket vs QUIC）
- 查看日志输出获取详细信息

### 证书问题（QUIC）
- 确保 `certs/` 目录存在且有写入权限
- 首次运行会自动生成证书
- 如果证书损坏，删除 `certs/` 目录后重新运行

---

## 更多信息

查看各个示例的源代码注释了解更多实现细节：
- 基础结构示例：`websocket_server.rs`, `websocket_client.rs`, `quic_server.rs`, `quic_client.rs`
- 观察者模式 Builder：`hybrid_server.rs`, `hybrid_client.rs`
- 简单模式 Builder：`simple_server.rs`, `simple_client.rs`

