# 服务端示例

本目录包含各种服务端示例，演示如何使用flare-core创建不同类型的服务器。

## 示例列表

### WebSocket 服务端
- [websocket_server.rs](websocket_server.rs) - 基本的WebSocket服务端示例

### QUIC 服务端
- [quic_server.rs](quic_server.rs) - 基本的QUIC服务端示例

### 双协议服务端
- [dual_protocol_server.rs](dual_protocol_server.rs) - 同时支持WebSocket和QUIC的服务端示例

## 运行示例

要运行任何示例：

```bash
# 运行QUIC服务端示例
cargo run --example quic_server

# 运行WebSocket服务端示例
cargo run --example websocket_server

# 运行双协议服务端示例
cargo run --example dual_protocol_server
```

## 配置说明

每个示例都可以通过修改源代码中的服务器参数进行配置：

- 监听地址和端口
- 序列化格式
- 心跳间隔和超时
- 最大连接数
- 连接超时设置

### 序列化配置

服务端现在支持统一的序列化配置，可以与客户端保持一致：

```rust
// 创建统一的序列化配置
let serialization_config = SerializationConfig::builder()
    .format(SerializationFormat::Protobuf)  // 使用Protobuf序列化
    .build();

// 在服务端配置中设置
let mut server_config = ServerConfig::default_websocket();
server_config = server_config.with_serialization_config(serialization_config);
```

### 客户端和服务端序列化一致性

为了确保消息能够正确解析，客户端和服务端应该使用相同的序列化格式：

```rust
// 服务端
server_config = server_config.with_serialization_format(SerializationFormat::Protobuf);

// 客户端
client_builder = client_builder.with_serialization(SerializationConfig::builder()
    .format(SerializationFormat::Protobuf)
    .build());
```