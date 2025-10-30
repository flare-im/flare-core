# 客户端模块

## 概述

客户端模块提供了完整的客户端实现，支持WebSocket和QUIC协议竞速。该模块包含基础客户端和Fast客户端两种实现方式：

1. **基础客户端**：提供与服务端对应的基础功能，设计良好的扩展接口，方便用户进行功能扩展
2. **Fast客户端（开箱即用）**：内置心跳机制，自动处理连接认证，封装消息发送功能，实现断线自动重连，支持协议切换，集成长连接核心功能，用户可直接使用无需关注底层实现细节，提供简单易用的API接口

## 功能特性

### 基础功能
- [x] 支持WebSocket和QUIC协议
- [x] 协议竞速（Protocol Racing）
- [x] 可配置的连接参数
- [x] 消息序列化/反序列化
- [x] 请求-响应模式
- [x] 心跳机制
- [x] 连接状态管理

### 认证功能
- [x] 可配置的认证流程
- [x] 支持认证成功/失败处理
- [x] 认证超时处理

### 高级功能（Fast客户端）
- [x] 自动心跳
- [x] 自动重连
- [x] 事件处理机制
- [x] 协议切换
- [x] 统计信息收集

## 模块结构

```
src/client/
├── client.rs          # 基础客户端实现
├── config.rs          # 客户端配置
├── protocol_racing.rs # 协议竞速实现
├── auth.rs            # 认证管理
├── fast.rs            # Fast客户端实现
├── event.rs           # 客户端事件处理
├── adapter/           # 适配器模块
│   └── client_event_adapter.rs # 客户端事件适配器
└── mod.rs             # 模块声明
```

## 使用指南

### 基础客户端

基础客户端提供了核心的连接和消息处理功能，适合需要自定义行为的场景。

```rust
use flare_core::client::{Client, ClientConfig};

// 创建客户端配置
let config = ClientConfig::default();

// 创建客户端实例
let mut client = Client::new(config);

// 连接到服务器
client.connect().await?;

// 发送消息
let message = Frame::text("Hello, server!".to_string());
client.send_message(message).await?;

// 断开连接
client.disconnect().await?;
```

### Fast客户端

Fast客户端提供了开箱即用的功能，内置了心跳、自动重连等高级特性。

```rust
use flare_core::client::{FastClientBuilder, FastClient};

// 使用构建器创建Fast客户端
let mut client = FastClientBuilder::new()
    .with_websocket_only()  // 仅使用WebSocket协议
    .with_heartbeat(10000, 30000)  // 心跳间隔10秒，超时30秒
    .with_auto_reconnect(true)  // 启用自动重连
    .build();

// 启动客户端
client.start().await?;

// 发送消息
let message = Frame::text("Hello, server!".to_string());
client.send_message(message).await?;

// 停止客户端
client.stop().await?;
```

### 认证配置

客户端支持可配置的认证流程：

```rust
use flare_core::client::{ClientConfig, AuthConfig};

let auth_config = AuthConfig {
    enabled: true,
    user_id: Some("user123".to_string()),
    platform: Some("mobile".to_string()),
    token: Some("auth_token".to_string()),
    timeout_ms: 5000,
};

let config = ClientConfig::default()
    .with_auth_config(auth_config);
```

当启用认证时，客户端会在连接建立后自动发送认证请求，并等待服务端响应。只有认证成功后，连接才可正常使用。

### 事件处理机制

客户端支持事件处理机制，用户可以通过实现[ClientEvent](event.rs) trait来自定义事件处理逻辑：

```rust
use flare_core::client::{Client, ClientConfig, ClientEvent, ClientEventAdapter};
use std::sync::Arc;

// 实现自定义事件处理器
struct MyClientEventHandler;

#[async_trait::async_trait]
impl ClientEvent for MyClientEventHandler {
    async fn on_control_command(&self, cmd: &ControlCmd) {
        // 处理控制命令
    }
    
    async fn on_message_command(&self, message: &MessageCmd) {
        // 处理消息命令
    }
    
    // ... 其他事件处理方法
    
    async fn on_authenticated(&self) {
        // 认证成功处理
    }
    
    async fn on_authentication_failed(&self, error: &str) {
        // 认证失败处理
    }
}

// 创建客户端并使用自定义事件处理器
let event_handler = Arc::new(MyClientEventHandler);
let client = Client::with_event_handler(config, event_handler);
```

系统提供了默认的事件处理器[DefClientEventHandler](event.rs)，用户可以直接使用或继承。

## 配置选项

### ClientConfig

| 字段 | 类型 | 描述 |
|------|------|------|
| server_addresses | HashMap<Transport, String> | 服务器地址映射 |
| transport | Transport | 传输类型 |
| protocol_selection | ProtocolSelection | 协议选择模式 |
| enable_auto_reconnect | bool | 是否启用自动重连 |
| max_reconnect_attempts | u32 | 最大重连尝试次数 |
| reconnect_delay_ms | u64 | 重连延迟（毫秒） |
| heartbeat_interval_ms | u64 | 心跳间隔（毫秒） |
| heartbeat_monitor_timeout_ms | u64 | 心跳监控超时（毫秒） |
| enable_auto_heartbeat_response | bool | 是否启用自动心跳响应 |
| serialization_format | SerializationFormat | 序列化格式 |
| serialization_config | SerializationConfig | 序列化配置 |
| request_timeout_ms | u64 | 请求超时时间（毫秒） |
| auth_config | AuthConfig | 认证配置 |

### AuthConfig

| 字段 | 类型 | 描述 |
|------|------|------|
| enabled | bool | 是否启用认证 |
| user_id | Option<String> | 用户ID |
| platform | Option<String> | 平台信息 |
| token | Option<String> | 认证令牌 |
| timeout_ms | u64 | 认证超时时间（毫秒） |

## 示例代码

更多示例请参考[examples/client/](../../examples/client/)目录：

- [基础客户端示例](../../examples/client/basic_client.rs)
- [Fast客户端示例](../../examples/client/fast_client.rs)
- [认证客户端示例](../../examples/client/auth_client.rs)
- [事件处理客户端示例](../../examples/client/event_client.rs)

## 错误处理

客户端使用[FlareError](../common/error.rs)来处理各种错误情况，包括网络错误、认证失败、超时等。

## 性能优化

- 使用异步I/O提高并发性能
- 连接池管理减少连接建立开销
- 消息批处理减少网络传输次数
- 心跳机制保持连接活跃

## 扩展性

客户端设计遵循开闭原则，用户可以通过以下方式扩展功能：

1. 实现自定义事件处理器
2. 扩展配置选项
3. 实现自定义序列化器
4. 添加新的协议支持
