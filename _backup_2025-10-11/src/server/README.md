# Flare Server 模块说明

## 概述

Flare Server 模块提供了完整的服务器端实现，支持 QUIC 和 WebSocket 两种协议。该模块设计灵活，可单独启动任一协议服务，也可同时启动两种协议服务。

## 模块结构

```
server/
├── server.rs          # 服务端主类
├── service.rs         # 服务接口和消息处理器
├── quic.rs            # QUIC 服务端实现
├── websocket.rs       # WebSocket 服务端实现
├── auth.rs            # 认证管理器实现
├── manager/           # 连接管理器模块
│   ├── mod.rs         # 模块入口
│   ├── traits.rs      # 连接管理器接口定义
│   ├── connection_based.rs  # 基于连接的管理器实现
│   └── user_based.rs  # 基于用户的管理器实现
└── mod.rs             # 模块导出
```

## 核心组件

### 1. Server 主类

[Server](file:///Users/hg/workspace/rust/flare-core/src/server/mod.rs#L11-L11) 是服务端的核心类，负责管理配置、连接管理器和服务实例。

#### 主要功能：
- 启动和停止服务
- 管理连接管理器
- 注册消息处理器
- 支持泛型连接管理器

#### 使用示例：

```rust
use std::sync::Arc;
use flare_core::{
    server::{
        Server, ServerConfig, ConnectionBasedManager,
        EchoMessageHandler,
    },
};

// 创建连接管理器
let connection_manager = Arc::new(ConnectionBasedManager::new());

// 创建服务器配置
let config = ServerConfig {
    websocket_addr: Some("127.0.0.1:8080".to_string()),
    quic_addr: Some("127.0.0.1:8081".to_string()),
    enable_tls: false,
    tls_cert_path: None,
    tls_key_path: None,
    max_connections: 1000,
    connection_timeout_ms: 30000,
};

// 创建服务器实例
let mut server = Server::new(config, connection_manager);

// 注册消息处理器
let echo_handler = Arc::new(EchoMessageHandler);
server.register_message_handler(echo_handler).await;

// 启动服务器
server.start().await?;
```

### 2. 连接管理器

连接管理器负责管理所有客户端连接，提供连接的添加、移除、查找和消息发送功能。

#### 2.1 ConnectionBasedManager (基于连接的管理器)

按连接ID独立管理每个连接，适用于简单的连接管理需求。

```rust
use flare_core::server::ConnectionBasedManager;

let manager = ConnectionBasedManager::new();
```

#### 2.2 UserBasedManager (基于用户的管理器)

按用户ID管理连接，支持一个用户多个连接，适用于需要按用户维度管理连接的场景。

```rust
use flare_core::server::UserBasedManager;

let manager = UserBasedManager::new();
```

### 3. 认证管理器

认证管理器负责处理两阶段连接认证流程：

1. 第一阶段：建立连接
2. 第二阶段：身份认证
3. 认证通过后才加入到连接管理器中

#### 3.1 AuthManager (认证管理器)

[AuthManager](file:///Users/hg/workspace/rust/flare-core/src/server/auth.rs#L115-L126) 负责管理待认证连接和处理认证请求。

#### 3.2 AuthHandler (认证处理器)

[AuthHandler](file:///Users/hg/workspace/rust/flare-core/src/server/auth.rs#L55-L65) trait 定义了认证接口，可以实现自定义认证逻辑。

#### 3.3 SimpleAuthHandler (简单认证处理器)

[SimpleAuthHandler](file:///Users/hg/workspace/rust/flare-core/src/server/auth.rs#L68-L84) 提供了基于Token的简单认证实现。

#### 使用示例：

```rust
use std::sync::Arc;
use flare_core::server::{
    auth::{SimpleAuthHandler, AuthManager},
    ConnectionBasedManager,
};
use std::time::Duration;

// 创建认证处理器
let auth_handler: Arc<dyn AuthHandler> = Arc::new(SimpleAuthHandler::new());
auth_handler.add_user("token123".to_string(), "user1".to_string()).await;

// 创建认证管理器
let auth_manager = Arc::new(AuthManager::new(auth_handler, Duration::from_secs(30)));

// 创建连接管理器
let connection_manager = Arc::new(ConnectionBasedManager::new());
```

### 4. 协议服务

#### 4.1 QUIC 服务

[QuicServer](file:///Users/hg/workspace/rust/flare-core/src/server/mod.rs#L15-L15) 提供基于 QUIC 协议的服务端实现，具有低延迟、高可靠性等特点。

#### 4.2 WebSocket 服务

[WebSocketServer](file:///Users/hg/workspace/rust/flare-core/src/server/mod.rs#L16-L16) 提供基于 WebSocket 协议的服务端实现，兼容性好，适用于 Web 应用。

## 连接处理流程

### 1. 连接建立流程

1. 客户端发起连接请求
2. 服务端接受连接并创建 [ServerConnection](file:///Users/hg/workspace/rust/flare-core/src/common/connections/traits.rs#L121-L142)
3. 将连接添加到认证管理器的待认证列表
4. 启动消息处理循环

### 2. 认证流程

1. 客户端发送认证消息（MessageType::Connect）
2. 服务端验证认证信息
3. 认证成功后将连接添加到连接管理器
4. 认证失败则断开连接

### 3. 消息处理流程

1. 从连接接收消息
2. 检查连接是否已认证
3. 已认证连接：调用注册的消息处理器处理消息
4. 未认证连接：检查是否为认证消息，否则返回错误
5. 如有响应消息，发送回客户端
6. 更新连接统计信息

### 4. 连接关闭流程

1. 连接断开或超时
2. 从认证管理器或连接管理器中移除连接
3. 清理相关资源

## 两阶段认证解决方案

无论是 WebSocket 还是 QUIC 连接，都实现了两阶段处理机制：

1. **第一阶段 - 连接建立**：
   - WebSocket 和 QUIC 协议各自建立底层连接
   - 连接建立后立即添加到认证管理器的待认证列表

2. **第二阶段 - 身份认证**：
   - 客户端发送认证消息（MessageType::Connect 类型）
   - 服务端验证认证信息
   - 认证成功后将连接从待认证列表移除并添加到连接管理器

### 认证参数传递差异处理

- **WebSocket**：可以通过URL参数、HTTP头部或首次消息携带认证信息
- **QUIC**：由于协议限制，通过首次消息携带认证信息

两种协议都使用统一的认证消息格式，确保认证处理逻辑的一致性。

## 使用 common 连接

Flare Server 模块使用 common 模块中的连接抽象：

- [ServerConnection](file:///Users/hg/workspace/rust/flare-core/src/common/connections/traits.rs#L121-L142): 服务端连接接口
- [ConnectionEvent](file:///Users/hg/workspace/rust/flare-core/src/common/connections/event.rs#L27-L27): 连接事件处理器接口
- [ConnectionConfig](file:///Users/hg/workspace/rust/flare-core/src/common/connections/types.rs#L15-L15): 连接配置
- [Frame](file:///Users/hg/workspace/rust/flare-core/src/common/protocol/frame.rs#L21-L21): 消息帧

这些组件提供了统一的连接管理和消息处理接口，确保了服务端与客户端的兼容性。

## 示例程序

查看 `examples/server/` 目录下的示例程序：

- `basic_server.rs`: 基本服务器示例
- `auth_server.rs`: 两阶段认证服务器示例
- `connection_based_server.rs`: 使用基于连接的管理器
- `user_based_server.rs`: 使用基于用户的管理器
- `quic_server.rs`: 仅启动 QUIC 服务
- `websocket_server.rs`: 仅启动 WebSocket 服务

查看 `examples/client/` 目录下的示例程序：

- `auth_client.rs`: 两阶段认证客户端示例

## 错误处理

服务端使用 [ServerError](file:///Users/hg/workspace/rust/flare-core/src/server/mod.rs#L23-L32) 枚举来表示各种错误情况：

```rust
pub enum ServerError {
    General(String),           // 一般错误
    Connection(FlareError),    // 连接错误
    Io(std::io::Error),       // IO错误
    AddrParse(ParseError),    // 地址解析错误
}
```