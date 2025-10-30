# Server模块设计文档

## 📋 概述

Server模块是flare-core中负责服务端功能的核心模块，提供统一的接口来管理多种协议（WebSocket、QUIC）的连接，支持单协议和双协议模式。

## 🏗️ 架构设计

### 模块组织

```
server/
├── config.rs          # 服务端配置
├── server.rs          # 聚合型服务端实现
├── websocket.rs       # WebSocket服务端实现
├── quic.rs            # QUIC服务端实现
├── manager/           # 连接管理器
│   ├── connection_manager.rs
│   └── user_connection_manager.rs
├── adapter/           # 事件适配器
│   └── server_event_adapter.rs
├── fast/              # 高性能轻量级服务端
│   ├── mod.rs
│   ├── server.rs
│   ├── event_handler.rs
│   ├── message_handler.rs
│   └── auth.rs
└── mod.rs             # 模块声明
```

### 设计原则

1. **协议无关性**：通过统一的接口抽象，支持多种传输协议
2. **可扩展性**：支持用户自定义的服务端扩展
3. **高性能**：提供轻量级的FastServer实现
4. **易用性**：提供简单直观的API

## 🚀 核心功能

### 1. 聚合型服务端 (AggregationServer)

聚合型服务端提供统一的接口来管理多种协议的连接：

```rust
use flare_core::server::config::{ServerConfig, ProtocolConfig};
use flare_core::server::server::{AggregationServer, ServerBuilder};

// 创建WebSocket服务端配置
let ws_config = ProtocolConfig::new()
    .with_listen_addr("127.0.0.1:9003".to_string())
    .with_max_connections(1000);

let config = ServerConfig::new()
    .with_server_type(flare_core::server::config::ServerType::WebSocket)
    .with_websocket_config(ws_config);

// 创建并启动聚合型服务端
let server = ServerBuilder::new(config).build();
server.start().await?;
```

### 2. FastServer

FastServer提供高性能、轻量级的服务端实现：

```rust
use flare_core::server::config::{ServerConfig, ProtocolConfig};
use flare_core::server::fast::server::FastServer;

// 创建WebSocket服务端配置
let ws_config = ProtocolConfig::new()
    .with_listen_addr("127.0.0.1:9004".to_string())
    .with_max_connections(1000);

let config = ServerConfig::new()
    .with_server_type(flare_core::server::config::ServerType::WebSocket)
    .with_websocket_config(ws_config);

// 创建并启动FastServer
let fast_server = FastServer::new(config);
fast_server.start().await?;
```

### 3. 支持的协议

#### WebSocket服务端
- 支持标准WebSocket协议
- 提供连接管理功能
- 支持自定义事件处理

#### QUIC服务端
- 支持QUIC协议
- 提供TLS加密
- 支持连接多路复用

#### 双协议模式
- 同时支持WebSocket和QUIC
- 统一的连接管理
- 灵活的配置选项

## 🛠️ 配置选项

### ServerConfig

服务端配置提供了丰富的选项来定制服务端行为：

```rust
let config = ServerConfig::new()
    .with_server_type(ServerType::WebSocket)  // 服务器类型
    .with_websocket_config(ws_config)         // WebSocket配置
    .with_quic_config(quic_config)            // QUIC配置
    .with_connection_timeout_ms(30000)        // 连接超时
    .with_heartbeat_config(10000, 5000, 3)    // 心跳配置
    .with_max_connections(1000)               // 最大连接数
    .with_serialization_codec(PayloadCodec::Json); // 序列化格式
```

### ProtocolConfig

协议配置用于配置特定协议的行为：

```rust
let ws_config = ProtocolConfig::new()
    .with_listen_addr("127.0.0.1:9003".to_string())  // 监听地址
    .with_max_connections(1000)                      // 最大连接数
    .enable_tls()                                    // 启用TLS（QUIC）
    .with_tls_config(tls_config);                    // TLS配置
```

## 🎯 使用场景

### 1. 简单的WebSocket服务

适用于需要快速搭建WebSocket服务的场景：

```rust
let config = ServerConfig::default_websocket();
let server = ServerBuilder::new(config).build();
server.start().await?;
```

### 2. 安全的QUIC服务

适用于需要高性能、安全连接的场景：

```rust
let config = ServerConfig::default_quic("cert.pem".to_string(), "key.pem".to_string());
let server = ServerBuilder::new(config).build();
server.start().await?;
```

### 3. 双协议服务

适用于需要同时支持多种协议的场景：

```rust
let config = ServerConfig::default_dual_protocol("cert.pem".to_string(), "key.pem".to_string());
let server = ServerBuilder::new(config).build();
server.start().await?;
```

## 🔧 扩展能力

### 自定义事件处理器

用户可以通过实现`ConnectionEvent` trait来自定义事件处理逻辑：

```rust
use flare_core::common::connections::traits::ConnectionEvent;
use flare_core::common::protocol::frame::Frame;

struct MyEventHandler;

impl ConnectionEvent for MyEventHandler {
    fn on_connected(&self) {
        println!("客户端连接建立");
    }
    
    fn on_message_received(&self, frame: Frame) {
        println!("收到消息: {:?}", frame);
    }
}
```

### 自定义消息处理器

FastServer支持自定义消息处理器：

```rust
use flare_core::server::fast::message_handler::MessageHandler;
use flare_core::common::protocol::frame::Frame;

struct MyMessageHandler;

impl MessageHandler for MyMessageHandler {
    fn handle_message(&self, user_id: String, frame: Frame) -> Result<(), FlareError> {
        // 处理业务逻辑
        println!("用户 {} 发送消息: {:?}", user_id, frame);
        Ok(())
    }
}
```

## 📊 性能特点

### 聚合型服务端
- ✅ 支持多种协议
- ✅ 统一的连接管理
- ✅ 丰富的配置选项
- ⚠️ 相对较重的实现

### FastServer
- ✅ 高性能、轻量级
- ✅ 简单易用的API
- ✅ 低内存占用
- ⚠️ 功能相对简化

## 📈 最佳实践

### 1. 选择合适的服务器类型

- **WebSocket服务**：选择`ServerType::WebSocket`
- **QUIC服务**：选择`ServerType::Quic`
- **双协议服务**：选择`ServerType::Dual`

### 2. 合理配置连接参数

```rust
let config = ServerConfig::new()
    .with_max_connections(10000)              // 根据预期负载设置
    .with_heartbeat_config(30000, 15000, 3)   // 合理的心跳配置
    .with_buffer_size(65536);                 // 适当的缓冲区大小
```

### 3. 使用连接管理器

通过连接管理器来管理连接的生命周期：

```rust
// 获取连接管理器
let manager = server.get_connection_manager();

// 获取连接统计信息
let stats = manager.stats_snapshot();

// 清理超时连接
manager.cleanup(60000).await?;
```

## 🧪 测试建议

### 1. 功能测试

- 验证各种协议的连接建立和断开
- 测试消息的发送和接收
- 验证心跳机制的正常工作

### 2. 性能测试

- 测试高并发连接下的性能表现
- 验证内存使用情况
- 测试长时间运行的稳定性

### 3. 安全测试

- 验证TLS连接的安全性
- 测试连接超时和清理机制
- 验证认证和授权功能

## 📚 相关文档

- [配置文档](config.rs) - 详细的服务端配置说明
- [WebSocket服务端](websocket.rs) - WebSocket服务端实现
- [QUIC服务端](quic.rs) - QUIC服务端实现
- [示例代码](../../examples/server_demo.rs) - 使用示例

## 🚨 注意事项

1. **证书配置**：使用QUIC时必须提供有效的TLS证书和私钥
2. **端口权限**：监听低端口（< 1024）可能需要管理员权限
3. **资源清理**：确保正确停止服务端以释放资源
4. **错误处理**：妥善处理启动和运行过程中的错误

## 📝 版本历史

### v0.1.0
- 初始版本
- 支持WebSocket和QUIC服务端
- 提供聚合型服务端和FastServer两种实现
- 基本的连接管理和配置功能