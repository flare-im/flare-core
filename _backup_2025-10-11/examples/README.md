# Flare Core 示例

本目录包含了 Flare Core 的各种使用示例，展示了如何使用 `ConnectionFactory` 创建不同类型的连接。

## 证书生成

在运行 QUIC 示例之前，需要先生成 TLS 证书：

```bash
# 生成证书
cargo run --example cert_generator

# 或者使用脚本自动生成并运行 QUIC 示例
./run_quic_examples.sh
```

生成的证书文件：
- `certs/server.crt` / `certs/server.key` - 服务器证书和私钥
- `certs/client.crt` / `certs/client.key` - 客户端证书和私钥

## QUIC 示例

### 服务端示例

```bash
cargo run --example quic_server_example
```

服务端将：
- 监听 `127.0.0.1:8081`
- 使用生成的服务器证书
- 接受客户端连接
- 处理消息并发送响应

### 客户端示例

```bash
cargo run --example quic_client_example
```

客户端将：
- 连接到 `127.0.0.1:8081`
- 使用服务器证书验证服务器身份
- 发送 5 条测试消息
- 接收服务器响应

### 自定义主机名示例

```bash
cargo run --example quic_custom_hostname_example
```

这个示例展示了如何配置不同的主机名进行 QUIC 连接：
- 测试 `localhost`、`127.0.0.1`、`flare-core-server` 等不同主机名
- 演示证书验证的工作原理
- 只有匹配证书的主机名才能成功连接

### 运行完整示例

```bash
# 使用脚本自动运行（推荐）
./run_quic_examples.sh

# 或者手动运行
# 终端 1: 启动服务端
cargo run --example quic_server_example

# 终端 2: 启动客户端
cargo run --example quic_client_example
```

## WebSocket 示例

### 服务端示例

```bash
cargo run --example websocket_server_example
```

服务端将：
- 监听 `127.0.0.1:8080`
- 接受 WebSocket 连接
- 发送欢迎消息给客户端
- 处理客户端消息

### 客户端示例

```bash
cargo run --example websocket_client_example
```

客户端将：
- 连接到 `ws://127.0.0.1:8080`
- 发送认证消息
- 发送测试消息
- 接收服务端响应

### 运行完整 WebSocket 示例

```bash
# 使用脚本自动运行（推荐）
./test_websocket_connection.sh

# 或者手动运行
# 终端1：启动服务端
cargo run --example websocket_server_example

# 终端2：运行客户端
cargo run --example websocket_client_example
```

客户端将：
- 连接到 `ws://127.0.0.1:8080`
- 发送认证消息
- 发送测试消息
- 接收服务端响应

## 示例特性

### 使用 ConnectionFactory

所有示例都使用最新的 `ConnectionFactory` 架构：

```rust
// 客户端连接
let mut client_connection = ConnectionFactory::create_client(config).await?;

// 服务端端点
let endpoint = ConnectionFactory::create_quic_server_endpoint(config).await?;

// 服务端连接
let server_connection = ConnectionFactory::from_quic_with_handler(
    quic_connection, 
    config, 
    event_handler
).await?;
```

### 证书配置

QUIC 示例使用生成的证书进行安全通信：

```rust
// 服务端配置
config.protocol_config.quic.server.cert_path = "certs/server.crt".to_string();
config.protocol_config.quic.server.key_path = "certs/server.key".to_string();

// 客户端配置
config.protocol_config.quic.client.server_cert_path = Some("certs/server.crt".to_string());
config.protocol_config.quic.client.skip_server_verification = false;
```

### 事件处理

所有示例都包含完整的事件处理器：

- `on_connected` - 连接建立
- `on_disconnected` - 连接断开
- `on_message_received` - 消息接收
- `on_message_sent` - 消息发送
- `on_error` - 错误处理
- `on_heartbeat_timeout` - 心跳超时
- `on_quality_changed` - 连接质量变化

## 故障排除

### 证书问题

如果遇到证书相关错误：

1. 删除现有证书：`rm -rf certs/`
2. 重新生成证书：`cargo run --example cert_generator`
3. 检查证书文件是否存在

**常见错误**：
- `certificate not valid for name "flare-core-server"`：这是正常现象，客户端会使用 `localhost` 作为主机名进行连接
- 证书验证失败：确保证书文件存在且格式正确

### 端口冲突

如果遇到端口冲突：

- QUIC 服务端默认使用 `8081` 端口
- WebSocket 服务端默认使用 `8083` 端口
- 可以在代码中修改端口号

### 连接问题

如果客户端无法连接到服务端：

1. 确保服务端已启动
2. 检查防火墙设置
3. 验证证书配置
4. 查看日志输出

## 开发说明

### 添加新示例

1. 在 `examples/common/` 目录下创建新的示例文件
2. 在 `Cargo.toml` 中添加示例配置
3. 使用 `ConnectionFactory` 创建连接
4. 实现事件处理器
5. 添加适当的错误处理

### 修改现有示例

- 所有示例都使用 `ConnectionFactory` 统一创建连接
- 配置通过 `ConnectionConfig` 结构体设置
- 事件处理通过实现 `ConnectionEvent` trait
- 消息构造使用 `Frame` 和相关命令结构体
