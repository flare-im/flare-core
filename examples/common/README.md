# QUIC 简单通信示例

这个目录包含了使用quinn直接实现的简单QUIC客户端和服务端示例，支持TLS证书认证。这些示例直接使用quinn的API，不依赖flare-core的抽象层。

## 文件说明

- `cert_generator.rs` - 证书生成工具，用于生成QUIC通信所需的TLS证书
- `simple_quic_server.rs` - 简单的QUIC服务端示例
- `simple_quic_client.rs` - 简单的QUIC客户端示例

## 使用方法

### 1. 生成证书

首先运行证书生成工具来创建TLS证书：

```bash
cargo run --example cert_generator
```

这将在`certs/`目录下生成以下文件：
- `server.crt/server.key` - 服务器证书和私钥
- `client.crt/client.key` - 客户端证书和私钥

### 2. 启动服务端

在一个终端中启动QUIC服务端：

```bash
cargo run --example simple_quic_server
```

服务端将在`127.0.0.1:8081`上监听连接。

### 3. 运行客户端

在另一个终端中运行QUIC客户端：

```bash
cargo run --example simple_quic_client
```

客户端将连接到服务端，发送几条测试消息，并接收响应。

## 功能特性

- **TLS加密**: 使用自签名证书进行TLS加密通信
- **双向流**: 支持QUIC双向流进行消息收发
- **错误处理**: 包含完整的错误处理和日志记录
- **简单易用**: 代码简洁，易于理解和修改
- **纯quinn实现**: 直接使用quinn API，不依赖其他抽象层
- **证书跳过验证**: 客户端跳过服务器证书验证（仅用于测试）

## 通信流程

1. 客户端连接到服务端
2. 建立TLS加密连接
3. 客户端打开双向流
4. 客户端发送消息
5. 服务端接收消息并发送响应
6. 客户端接收响应
7. 连接关闭

## 自定义修改

你可以根据需要修改以下内容：

- **消息内容**: 在`simple_quic_client.rs`中修改发送的消息
- **服务端响应**: 在`simple_quic_server.rs`中修改服务端的响应逻辑
- **端口和地址**: 修改服务端监听的地址和端口
- **证书配置**: 修改证书生成参数

## 注意事项

- 这些示例使用自签名证书，仅用于开发和测试
- 生产环境应使用由受信任CA签发的证书
- 客户端跳过了服务器证书验证，仅用于演示目的