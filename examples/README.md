# Flare Core 示例程序

本目录包含 Flare Core 的统一客户端和服务端使用示例。

## 示例说明

### 1. unified_client.rs - 统一客户端示例

演示如何使用 `UnifiedClient` 进行：
- **单协议连接**：仅使用 WebSocket 或 QUIC 协议连接服务器
- **协议竞速**：同时尝试多个协议，自动选择最先成功的协议

#### 运行方式

```bash
# 先启动服务端（在另一个终端）
cargo run --example unified_server

# 然后运行客户端
cargo run --example unified_client
```

#### 功能演示

1. **WebSocket 单协议连接**：尝试连接到 `ws://localhost:8080`
2. **QUIC 单协议连接**：尝试连接到 `quic://localhost:8081`
3. **协议竞速**：同时尝试 WebSocket 和 QUIC，选择最先成功的协议

### 2. unified_server.rs - 统一服务端示例

演示如何使用 `UnifiedServer` 进行：
- **单协议监听**：仅监听 WebSocket 或 QUIC 协议
- **多协议监听**：同时监听多个协议（WebSocket + QUIC）

#### 运行方式

```bash
cargo run --example unified_server
```

#### 功能演示

1. **WebSocket 单协议服务端**：监听 `0.0.0.0:8080`
2. **QUIC 单协议服务端**：监听 `0.0.0.0:8081`
3. **多协议服务端**：同时监听 `0.0.0.0:8082`（支持 WebSocket 和 QUIC）

## 测试流程

### 基础测试

1. **启动服务端**：
   ```bash
   cargo run --example unified_server
   ```

2. **测试单协议客户端**（在另一个终端）：
   ```bash
   cargo run --example unified_client
   ```

3. **观察输出**：
   - 服务端应该显示连接建立和断开的消息
   - 客户端应该显示连接成功、协议选择和消息发送的信息

### 协议竞速测试

1. **确保服务端支持多个协议**（使用多协议模式）

2. **运行客户端竞速模式**：
   ```bash
   cargo run --example unified_client
   ```

3. **验证竞速结果**：
   - 客户端应该尝试多个协议
   - 最终选择最先成功的协议
   - 输出显示使用的协议类型

## 代码示例

### 客户端 - 单协议连接

```rust
use flare_core::{UnifiedClient, ClientConfig, TransportProtocol};

let config = ClientConfig::new("ws://localhost:8080".to_string())
    .websocket();
let client = UnifiedClient::connect_with_config(config).await?;
```

### 客户端 - 协议竞速

```rust
use flare_core::{UnifiedClient, ClientConfig, TransportProtocol};

let config = ClientConfig::new("ws://localhost:8080".to_string())
    .with_protocol_race(vec![
        TransportProtocol::WebSocket,
        TransportProtocol::QUIC,
    ])
    .with_race_timeout(Duration::from_secs(5));
let client = UnifiedClient::connect_with_race(config).await?;
```

### 服务端 - 多协议监听

```rust
use flare_core::{UnifiedServer, ServerConfig, TransportProtocol, ConnectionHandler};

let config = ServerConfig::new("0.0.0.0:8080".to_string())
    .with_protocols(vec![
        TransportProtocol::WebSocket,
        TransportProtocol::QUIC,
    ]);
let mut server = UnifiedServer::new(config, handler)?;
server.start().await?;
```

## 注意事项

1. **端口配置**：确保示例中使用的端口（8080, 8081, 8082）没有被其他程序占用
2. **网络环境**：如果服务器地址不是 localhost，需要修改配置中的地址
3. **协议支持**：QUIC 协议可能需要额外的系统配置，如果 QUIC 连接失败，这是正常的
4. **错误处理**：示例代码包含了基本的错误处理，在实际使用中应该根据需要进行扩展

## 故障排除

### 连接失败

- 检查服务端是否正在运行
- 检查端口是否被占用
- 检查防火墙设置
- 检查服务器地址和端口是否正确

### QUIC 协议问题

- QUIC 协议在某些环境下可能不可用
- 可以只使用 WebSocket 协议进行测试
- 检查系统是否支持 QUIC

## 下一步

- 查看 `src/client/unified.rs` 了解 UnifiedClient 的实现
- 查看 `src/server/unified.rs` 了解 UnifiedServer 的实现
- 查看 `src/common/config.rs` 了解配置选项
- 参考主文档了解更多高级功能

