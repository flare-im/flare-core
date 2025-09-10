# Common Examples

本目录包含了flare-core中通用功能的使用示例，展示了如何直接使用src/common模块中的连接功能。

## 目录结构

```
common/
├── quic_client_example.rs     # QUIC客户端连接示例
├── quic_server_example.rs     # QUIC服务端连接示例
├── websocket_client_example.rs # WebSocket客户端连接示例
├── websocket_server_example.rs # WebSocket服务端连接示例
└── README.md                  # 本说明文件
```

## 示例说明

### QUIC连接示例

#### quic_client_example.rs
- 展示如何创建和使用QUIC客户端连接
- 演示QUIC连接的配置和事件处理
- 包含完整的消息发送和接收流程
- 使用Protobuf序列化和LZ4压缩

#### quic_server_example.rs
- 展示如何创建和运行QUIC服务端
- 演示QUIC服务端的配置和消息处理
- 使用EchoMessageHandler回显客户端消息
- 支持TLS加密通信

### WebSocket连接示例

#### websocket_client_example.rs
- 展示如何创建和使用WebSocket客户端连接
- 演示WebSocket连接的配置和事件处理
- 包含完整的消息发送和接收流程
- 使用Bincode序列化和LZ4压缩

#### websocket_server_example.rs
- 展示如何创建和运行WebSocket服务端
- 演示WebSocket服务端的配置和消息处理
- 使用EchoMessageHandler回显客户端消息

## 使用方法

### 运行QUIC示例

1. 首先确保有TLS证书（可以使用项目提供的证书生成脚本）：
   ```bash
   ./scripts/generate_certs.sh
   ```

2. 在一个终端中运行QUIC服务端：
   ```bash
   cargo run --example quic_server_example
   ```

3. 在另一个终端中运行QUIC客户端：
   ```bash
   cargo run --example quic_client_example
   ```

### 运行WebSocket示例

1. 在一个终端中运行WebSocket服务端：
   ```bash
   cargo run --example websocket_server_example
   ```

2. 在另一个终端中运行WebSocket客户端：
   ```bash
   cargo run --example websocket_client_example
   ```

## 功能特性

### 连接管理
- 统一的连接接口，支持WebSocket和QUIC协议
- 自动心跳机制，保持连接活跃
- 连接状态监控和事件处理
- 错误处理和重连机制

### 消息处理
- 多种序列化格式支持（JSON、Bincode、Protobuf等）
- 消息压缩支持（LZ4、Snappy、GZIP）
- 消息可靠性保证（尽力而为、至少一次、恰好一次、有序）
- 异步消息处理管道

### 性能优化
- 零拷贝序列化器
- 自适应压缩器
- CPU亲和性绑定
- 内存优化技术

## 事件处理

所有示例都实现了完整的事件处理机制：

- `on_connected`: 连接建立事件
- `on_disconnected`: 连接断开事件
- `on_error`: 错误事件
- `on_message_received`: 消息接收事件
- `on_message_sent`: 消息发送事件
- `on_heartbeat_timeout`: 心跳超时事件
- `on_quality_changed`: 连接质量变化事件
- `on_statistics_updated`: 统计信息更新事件

## 配置选项

### 连接配置
- 心跳间隔和超时设置
- TLS/SSL加密支持
- 序列化格式选择
- 压缩算法配置

### 协议特定配置
- QUIC: 流控制、拥塞控制、并发流数量
- WebSocket: 子协议、扩展、压缩阈值

## 扩展使用

这些示例可以作为基础模板进行扩展：

1. 实现自定义消息处理器
2. 添加认证和授权机制
3. 集成业务逻辑处理
4. 实现连接池管理
5. 添加监控和日志功能