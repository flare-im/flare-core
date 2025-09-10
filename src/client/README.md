# 客户端模块

## 概述

客户端模块提供了完整的客户端实现，支持WebSocket和QUIC协议。该模块的核心特性是协议竞速功能，能够自动选择最优的连接协议，同时支持用户手动选择特定协议。

## 架构设计

### 核心组件

1. **Client** - 客户端主类，负责连接管理、消息发送等核心功能
2. **ClientConfig** - 客户端配置，支持灵活的连接参数设置
3. **ProtocolRacer** - 协议竞速器，实现多协议并发连接和最优选择

### 协议支持

- **WebSocket** - 经典的双向通信协议，广泛支持
- **QUIC** - 下一代传输协议，低延迟、高可靠性

## 功能特性

### 1. 协议竞速

协议竞速是客户端的核心功能，能够同时尝试多种协议并选择最优的连接：

```rust
use flare_core::client::{Client, ClientConfig, ProtocolSelection};
use flare_core::common::connections::types::ConnectionType;

// 为不同协议指定不同的服务器地址
let config = ClientConfig::new(
    "ws://127.0.0.1:8080".to_string(),  // WebSocket地址
    "127.0.0.1:8081".to_string()       // QUIC地址
).with_protocol_selection(ProtocolSelection::Auto);

let mut client = Client::new(config);
client.connect().await?; // 自动选择最优协议
```

### 2. 单一协议选择

用户也可以选择使用特定的协议：

```rust
use flare_core::client::{Client, ClientConfig, ProtocolSelection};
use flare_core::common::connections::types::ConnectionType;

// 仅使用QUIC
let config = ClientConfig::new(
    "ws://127.0.0.1:8080".to_string(),  // WebSocket地址
    "127.0.0.1:8081".to_string()       // QUIC地址
).with_quic_only();

// 仅使用WebSocket
let config = ClientConfig::new(
    "ws://127.0.0.1:8080".to_string(),  // WebSocket地址
    "127.0.0.1:8081".to_string()       // QUIC地址
).with_websocket_only();
```

### 3. 心跳机制

客户端负责发起心跳，服务端负责检查和超时处理：

```rust
// 客户端发送心跳
client.send_heartbeat().await?;

// 自动心跳（根据配置自动发送）
```

### 4. 请求-响应模式（类似REST接口）

客户端支持发送请求并等待响应的模式，类似于REST接口：

```rust
use flare_core::client::Client;
use flare_core::common::protocol::{Frame, MessageType, Reliability};
use serde_json::json;

// 发送请求并等待响应
let request = Frame::new(
    MessageType::Data,
    1, // 请求ID
    Reliability::ExactlyOnce,
    serde_json::to_vec(&json!({"action": "get_user_info"}))?,
);

match client.send_request(request).await {
    Ok(response) => {
        println!("收到响应: {:?}", response);
    }
    Err(e) => {
        println!("请求失败: {}", e);
    }
}
```

### 5. 连接管理

支持自动重连、连接状态管理等功能：

```rust
// 检查连接状态
if client.is_connected().await {
    // 发送消息
    client.send_message(message).await?;
}

// 断开连接
client.disconnect().await?;
```

## 使用方式

### 基本使用

```rust
use flare_core::client::{Client, ClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建客户端配置，为不同协议指定不同的服务器地址
    let config = ClientConfig::new(
        "ws://127.0.0.1:8080".to_string(),  // WebSocket地址
        "127.0.0.1:8081".to_string()       // QUIC地址
    );
    
    // 创建客户端实例
    let mut client = Client::new(config);
    
    // 连接到服务器
    client.connect().await?;
    
    // 发送消息
    let message = Frame::text(1, "Hello, Server!".to_string());
    client.send_message(message).await?;
    
    // 断开连接
    client.disconnect().await?;
    
    Ok(())
}
```

### 协议竞速使用

```rust
use flare_core::client::{Client, ClientConfig, ProtocolSelection};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建支持协议竞速的配置
    let config = ClientConfig::new(
        "ws://127.0.0.1:8080".to_string(),  // WebSocket地址
        "127.0.0.1:8081".to_string()       // QUIC地址
    ).with_protocol_selection(ProtocolSelection::Auto)
    .with_heartbeat(5000, 2000) // 5秒心跳，2秒超时
    .with_request_timeout(3000); // 3秒请求超时
    
    let mut client = Client::new(config);
    client.connect().await?; // 自动选择QUIC或WebSocket
    
    Ok(())
}
```

### 请求-响应模式使用

```rust
use flare_core::client::Client;
use flare_core::common::protocol::{Frame, MessageType, Reliability};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建客户端（配置同上）
    let mut client = Client::new(config);
    client.connect().await?;
    
    // 发送认证请求
    let auth_request = Frame::new(
        MessageType::Connect,
        1,
        Reliability::ExactlyOnce,
        serde_json::to_vec(&json!({"token": "user_token"}))?,
    );
    
    match client.send_request(auth_request).await {
        Ok(response) => println!("认证成功"),
        Err(e) => println!("认证失败: {}", e),
    }
    
    Ok(())
}
```

## 高级功能

### 1. 自定义序列化

```rust
use flare_core::client::{Client, ClientConfig};
use flare_core::common::serialization::{SerializationFormat, SerializationConfig};

let config = ClientConfig::new(
    "ws://127.0.0.1:8080".to_string(),  // WebSocket地址
    "127.0.0.1:8081".to_string()       // QUIC地址
).with_serialization(
    SerializationFormat::Json,
    SerializationConfig::default()
);
```

### 2. 连接事件处理

```rust
use flare_core::common::connections::traits::ConnectionEvent;

struct MyEventHandler;

#[async_trait::async_trait]
impl ConnectionEvent for MyEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        println!("连接已建立: {}", connection_id);
    }
    
    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        println!("连接已断开: {} - 原因: {}", connection_id, reason);
    }
    
    // ... 其他事件处理方法
}
```

## 性能优化

### 1. 连接复用

客户端支持连接复用，避免频繁建立和断开连接：

```rust
// 保持连接活跃
if !client.is_connected().await {
    client.connect().await?;
}
```

### 2. 心跳优化

合理配置心跳间隔和超时时间：

```rust
let config = ClientConfig::new(
    "ws://127.0.0.1:8080".to_string(),  // WebSocket地址
    "127.0.0.1:8081".to_string()       // QUIC地址
).with_heartbeat(10000, 5000); // 10秒心跳，5秒超时
```

### 3. 请求超时优化

合理配置请求超时时间：

```rust
let config = ClientConfig::new(
    "ws://127.0.0.1:8080".to_string(),  // WebSocket地址
    "127.0.0.1:8081".to_string()       // QUIC地址
).with_request_timeout(3000); // 3秒请求超时
```

## 应用场景

### 1. IM应用客户端

```rust
use flare_core::client::{Client, ClientConfig};

// IM客户端通常需要低延迟和高可靠性
let config = ClientConfig::new(
    "ws://im-server:8080".to_string(),  // WebSocket地址
    "im-server:8081".to_string()       // QUIC地址
).with_quic_only(); // 优先使用QUIC获得更低延迟
```

### 2. Web应用客户端

```rust
// Web应用通常使用WebSocket以获得更好的兼容性
let config = ClientConfig::new(
    "ws://web-server:8080".to_string(),  // WebSocket地址
    "web-server:8081".to_string()       // QUIC地址
).with_websocket_only();
```

### 3. 混合场景

```rust
// 混合场景使用协议竞速自动选择最优协议
let config = ClientConfig::new(
    "ws://server:8080".to_string(),  // WebSocket地址
    "server:8081".to_string()       // QUIC地址
).with_protocol_selection(ProtocolSelection::Auto);
```

## 配置参数

### ClientConfig

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| server_addresses | HashMap<ConnectionType, String> | - | 协议类型到服务器地址的映射 |
| protocol_selection | ProtocolSelection | Auto | 协议选择模式 |
| enable_auto_reconnect | bool | true | 是否启用自动重连 |
| max_reconnect_attempts | u32 | 5 | 最大重连尝试次数 |
| reconnect_delay_ms | u64 | 1000 | 重连延迟（毫秒） |
| heartbeat_interval_ms | u64 | 10000 | 心跳间隔（毫秒） |
| heartbeat_monitor_timeout_ms | u64 | 30000 | 心跳监控超时（毫秒） |
| enable_auto_heartbeat_response | bool | true | 是否启用自动心跳响应 |
| request_timeout_ms | u64 | 5000 | 请求超时时间（毫秒） |

## 错误处理

客户端提供了完善的错误处理机制：

```rust
use flare_core::common::error::FlareError;

match client.connect().await {
    Ok(_) => println!("连接成功"),
    Err(FlareError::ConnectionFailed(msg)) => {
        println!("连接失败: {}", msg);
    }
    Err(e) => {
        println!("其他错误: {:?}", e);
    }
}

// 请求-响应模式的错误处理
match client.send_request(request).await {
    Ok(response) => println!("收到响应"),
    Err(FlareError::Timeout(msg)) => {
        println!("请求超时: {}", msg);
    }
    Err(e) => {
        println!("请求失败: {:?}", e);
    }
}
```

## 扩展性

### 自定义协议支持

可以通过扩展ConnectionFactory来支持新的协议：

```rust
use flare_core::common::connections::traits::ConnectionFactory;

struct CustomConnectionFactory;

#[async_trait::async_trait]
impl ConnectionFactory for CustomConnectionFactory {
    async fn create_client_connection(&self, config: ConnectionConfig) -> Result<Box<dyn ClientConnection>> {
        // 实现自定义协议连接创建逻辑
        todo!()
    }
    
    // ... 其他方法
}
```

## 最佳实践

1. **优先使用协议竞速**：让客户端自动选择最优协议
2. **合理配置心跳**：根据网络环境调整心跳间隔和超时时间
3. **处理连接事件**：实现ConnectionEvent trait处理连接状态变化
4. **错误重试**：启用自动重连机制处理网络波动
5. **资源管理**：及时断开不需要的连接释放资源
6. **请求超时**：合理设置请求超时时间避免长时间阻塞