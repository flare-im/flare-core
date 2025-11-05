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

## 协商和设备管理示例

### negotiation_server.rs - 协商和设备管理服务器

**功能特性：**
- ✅ 客户端可以协商序列化格式（JSON/Protobuf）
- ✅ 客户端可以协商压缩算法（none/gzip）
- ✅ 移动端互斥：同一用户只能有一个移动端设备在线
- ✅ 支持多端同时在线（Web/PC + 一个移动端）
- ✅ 设备信息管理：记录设备ID、平台、型号、版本等
- ✅ 设备冲突处理：自动踢掉冲突的旧设备

**启动命令：**
```bash
RUST_LOG=debug cargo run --example negotiation_server
```

**客户端连接时需要发送：**
- `device_id`: 设备唯一标识
- `platform`: 平台类型（web/pc/android/ios/harmonyos）
- `format`: 序列化格式（protobuf/json）
- `compression`: 压缩算法（none/gzip）
- `model`: 设备型号（可选）
- `app_version`: 应用版本（可选）
- `system_version`: 系统版本（可选）

**服务器日志示例：**
```
✅ 新连接: 1234567890
   设备: Android (ID: device-001, 平台: Android)
   型号: Samsung Galaxy S23
   应用版本: 1.0.0
   序列化格式: Json, 压缩: None
   用户 ID: user-123
   用户当前在线设备数: 1
```

### negotiation_client.rs - 协商和设备管理客户端

**功能特性：**
- ✅ 发送协商信息（序列化格式、压缩算法）
- ✅ 发送设备信息（设备ID、平台等）
- ✅ 支持聊天室功能

**启动命令：**
```bash
RUST_LOG=debug cargo run --example negotiation_client
```

**注意：** 当前客户端示例为简化版本，实际应用中需要在连接建立后立即发送 CONNECT 消息，包含所有协商信息。

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

## 认证示例

### `auth_server.rs` - 认证聊天室服务器

演示如何使用 token 认证功能。

**功能特性：**
- 启用 token 认证
- 自定义认证器（只接受 token='12345'）
- 认证超时检测
- 只有已验证的连接才能收发业务消息

**启动命令：**
```bash
RUST_LOG=debug cargo run --example auth_server
```

**配置说明：**
- `enable_auth()`: 启用认证功能
- `with_authenticator()`: 设置认证器（自定义验证逻辑）
- `with_auth_timeout()`: 设置认证超时时间（默认 30 秒）

**认证机制：**
- 客户端连接后，发送 CONNECT 消息（包含 token）
- 服务端验证 token，只有 token='12345' 才能通过
- 验证通过后，连接被标记为已验证
- 只有已验证的连接才能收发业务消息

### `auth_client.rs` - 认证聊天室客户端

演示客户端如何使用 token 进行认证。

**功能特性：**
- 通过 `with_token()` 设置 token
- Token 自动在 CONNECT 消息中发送
- 支持交互式输入 token
- 支持命令行参数和环境变量指定 token

**启动命令：**
```bash
# 使用正确 token（12345，默认值）
RUST_LOG=debug cargo run --example auth_client

# 使用错误 token（通过命令行参数）
RUST_LOG=debug cargo run --example auth_client -- wrong_token

# 通过环境变量指定 token
TOKEN=12345 RUST_LOG=debug cargo run --example auth_client
```

**测试场景：**
1. **正确 token**：使用 `12345` 应该成功连接并收发消息
2. **错误 token**：使用其他值会被服务端拒绝，连接失败
3. **未提供 token**：连接会被服务端拒绝

**认证日志：**
- `[ClientCore] 已添加 token 到 CONNECT 消息元数据`：显示 token 已发送
- `[ServerCore] 🔐 开始验证 token`：显示服务端开始验证
- `[ServerCore] ✅ Token 验证成功`：显示验证通过
- `[ServerCore] ❌ Token 验证失败`：显示验证失败
- `[ServerCore] ✅ 连接已标记为已验证`：显示连接已验证

---

## 更多信息

查看各个示例的源代码注释了解更多实现细节：
- 基础结构示例：`websocket_server.rs`, `websocket_client.rs`, `quic_server.rs`, `quic_client.rs`
- 观察者模式 Builder：`hybrid_server.rs`, `hybrid_client.rs`
- 简单模式 Builder：`simple_server.rs`, `simple_client.rs`
- 认证示例：`auth_server.rs`, `auth_client.rs`

