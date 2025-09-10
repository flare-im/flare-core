# Flare-Core 示例程序

本目录包含了用于演示flare-core功能的关键示例程序，分为客户端和服务端两类。

## 客户端示例

### [websocket_client.rs](file:///Users/hg/workspace/rust/flare-core/examples/client/websocket_client.rs)
- 演示WebSocket客户端的完整功能
- 展示超低延迟优化技术（零拷贝序列化、LZ4压缩、异步Pipeline等）
- 包含心跳机制、事件处理、错误处理等功能

### [auth_client.rs](file:///Users/hg/workspace/rust/flare-core/examples/client/auth_client.rs)
- 演示客户端认证流程
- 展示如何与服务端进行安全认证

### [im_client.rs](file:///Users/hg/workspace/rust/flare-core/examples/client/im_client.rs)
- IM客户端示例
- 展示如何连接到IM网关服务器并进行基本的IM操作

## 服务端示例

### [websocket_server.rs](file:///Users/hg/workspace/rust/flare-core/examples/server/websocket_server.rs)
- 基础WebSocket服务器示例
- 展示最简单的WebSocket服务端实现

### [im_gateway.rs](file:///Users/hg/workspace/rust/flare-core/examples/server/im_gateway.rs)
- 完整的IM网关服务器示例
- 集成了认证、多端控制、消息广播等功能
- 适合生产环境使用

## 使用方法

运行示例程序：

```bash
# 运行WebSocket服务器
cargo run --example websocket_server

# 运行WebSocket客户端
cargo run --example websocket_client

# 运行IM网关服务器
cargo run --example im_gateway

# 运行认证客户端
cargo run --example auth_client

# 运行IM客户端
cargo run --example im_client
```