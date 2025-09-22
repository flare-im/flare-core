# 客户端示例

本目录包含各种客户端示例，演示如何使用flare-core创建不同类型的客户端。

## 示例列表

### 基本客户端
- [basic_client.rs](basic_client.rs) - 基本的客户端连接示例
- [fast_client.rs](fast_client.rs) - 使用FastClient的简化客户端示例

### 协议相关客户端
- [websocket_client.rs](websocket_client.rs) - WebSocket客户端连接示例
- [quic_client.rs](quic_client.rs) - QUIC客户端连接示例

### 特殊功能客户端
- [auth_client.rs](auth_client.rs) - 带认证的客户端示例
- [event_client.rs](event_client.rs) - 事件处理客户端示例
- [protocol_race_client.rs](protocol_race_client.rs) - 协议竞速客户端示例

## 运行示例

要运行任何示例，首先确保相应的服务端正在运行：

```bash
# 运行基本客户端示例
cargo run --example basic_client

# 运行FastClient示例
cargo run --example fast_client

# 运行WebSocket客户端示例
cargo run --example websocket_client

# 运行QUIC客户端示例
cargo run --example quic_client
```

## 配置说明

每个示例都可以通过修改源代码中的客户端参数进行配置：

- 服务器地址和端口
- 序列化格式
- 心跳间隔和超时
- 重连设置
- 认证配置

### 序列化配置

客户端现在支持灵活的序列化配置，可以与服务端保持一致：

```rust
// 创建统一的序列化配置
let serialization_config = SerializationConfig::builder()
    .format(SerializationFormat::Protobuf)  // 使用Protobuf序列化
    .build();

// 在客户端配置中设置
let client_builder = FastClientBuilder::new()
    .with_serialization(serialization_config);
```

### 客户端和服务端序列化一致性

为了确保消息能够正确解析，客户端和服务端应该使用相同的序列化格式：

```rust
// 客户端
client_builder = client_builder.with_serialization(SerializationConfig::builder()
    .format(SerializationFormat::Protobuf)
    .build());

// 服务端
server_config = server_config.with_serialization_format(SerializationFormat::Protobuf);
```

### 错误处理

客户端提供了完善的错误处理机制，包括连接错误、重连失败等情况的处理：

```rust
// 启用自动重连
client_builder = client_builder
    .with_auto_reconnect(true)
    .with_reconnect_params(5, 2000);  // 最多重连5次，每次间隔2秒

// 通过事件处理器处理各种错误情况
#[async_trait::async_trait]
impl flare_core::common::connections::event::ConnectionEvent for MyEventHandler {
    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("连接错误: {} - 错误: {}", connection_id, error);
        // 处理特定错误，如连接被拒绝
        if error.contains("Connection refused") {
            std::process::exit(1);
        }
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        error!("重连失败: {} - 尝试次数: {} - 错误: {}", connection_id, attempt, error);
        // 当重连失败次数达到上限时退出程序
        if attempt >= 5 {
            std::process::exit(1);
        }
    }
}
```