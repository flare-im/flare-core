# FastServer - 融合功能的服务端代理

FastServer作为服务端的核心代理，整合了连接生命周期管理和服务端事件处理功能，提供统一的接口来协调所有服务端操作。

## 功能特性

- **统一接口**：提供简化的API来管理服务端操作
- **连接管理**：集成UserConnectionManager进行用户和连接管理
- **事件处理**：内置系统事件处理器处理连接生命周期事件
- **消息处理**：支持自定义消息处理器
- **消息发送**：提供统一的消息发送器，支持发送控制命令、消息、通知和事件
- **多协议支持**：支持WebSocket和QUIC协议
- **统计信息**：提供服务统计信息查询

## 架构设计

FastServer的设计遵循以下原则：

1. **分层架构**：将复杂功能分解为独立的组件
2. **松耦合**：各组件之间通过接口交互，降低依赖
3. **可扩展性**：支持自定义消息处理器和事件处理器
4. **类型安全**：充分利用Rust的类型系统确保正确性

## 使用示例

```rust
use std::sync::Arc;
use tokio::time::sleep;
use std::time::Duration;

use flare_core::server::fast::FastServer;
use flare_core::server::config::{ServerConfig, ProtocolConfig, ServerType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // 创建服务端配置
    let ws_config = ProtocolConfig::new()
        .with_listen_addr("127.0.0.1:8080".to_string())
        .with_max_connections(1000);
        
    let config = ServerConfig::new()
        .with_server_type(ServerType::WebSocket)
        .with_websocket_config(ws_config)
        .with_connection_timeout_ms(30000)
        .with_heartbeat_interval_ms(10000);

    // 创建FastServer实例
    let server = FastServer::new(None);
    
    // 启动服务端
    server.start(config).await?;
    
    println!("FastServer已启动，监听地址: 127.0.0.1:8080");
    println!("按 Ctrl+C 停止服务端");
    
    // 运行一段时间
    sleep(Duration::from_secs(600)).await;
    
    // 停止服务端
    server.stop().await;
    
    Ok(())
}
```

## 配置说明

### ServerConfig

服务端的主要配置，包含以下字段：

- `server_type`: 服务器类型（WebSocket、QUIC或双协议）
- `websocket_config`: WebSocket协议配置
- `quic_config`: QUIC协议配置
- `connection_timeout_ms`: 连接超时时间（毫秒）
- `heartbeat_interval_ms`: 心跳间隔（毫秒）
- `max_connections`: 最大连接数

### ProtocolConfig

协议配置，包含以下字段：

- `listen_addr`: 监听地址
- `max_connections`: 最大连接数
- `enable_tls`: 是否启用TLS
- `tls_config`: TLS配置（启用TLS时必须提供）

## API参考

### FastServer

主要的服务端代理类，提供以下方法：

- `new()`: 创建新的FastServer实例
- `start()`: 启动服务
- `stop()`: 停止服务
- `send_message_to_user()`: 发送消息给用户
- `get_stats()`: 获取服务统计信息
- `get_server()`: 获取基础服务实现
- `get_user_connection_manager()`: 获取用户连接管理器
- `get_message_handler()`: 获取消息处理器
- `get_system_event_handler()`: 获取系统事件处理器
- `get_message_sender()`: 获取消息发送器
- `get_config()`: 获取当前配置

### MessageSender

消息发送器，提供以下方法：

- `send_control_command()`: 发送控制命令
- `send_message_command()`: 发送消息命令
- `send_notification_command()`: 发送通知命令
- `send_event_command()`: 发送事件命令
- `send_message_to_user()`: 向指定用户发送消息
- `broadcast_message()`: 广播消息到所有用户
- `send_connect_command()`: 发送连接命令
- `send_connect_ack_command()`: 发送连接确认命令
- `send_disconnect_command()`: 发送断开连接命令
- `send_auth_request_command()`: 发送认证请求命令
- `send_auth_response_command()`: 发送认证响应命令
- `send_message_send_command()`: 发送消息发送命令
- `send_message_ack_command()`: 发送消息确认命令
- `send_data_command()`: 发送数据命令
- `send_system_notification_command()`: 发送系统通知命令
- `send_broadcast_notification_command()`: 发送广播通知命令
- `send_alert_notification_command()`: 发送警报通知命令

## 扩展功能

### 自定义消息处理器

可以通过实现`MessageHandler` trait来创建自定义消息处理器：

```rust
use flare_core::server::fast::message_handler::{MessageHandler, ConnectionEventType};
use flare_core::common::protocol::Frame;
use flare_core::common::error::Result;

pub struct CustomMessageHandler;

#[async_trait::async_trait]
impl MessageHandler for CustomMessageHandler {
    async fn handle_user_message(&self, user_id: &str, connection_id: &str, message: &Frame) -> Result<()> {
        // 处理用户消息逻辑
        Ok(())
    }
    
    async fn handle_authentication_request(&self, connection_id: &str, user_id: &str, platform: &str, token: &str) -> Result<()> {
        // 处理认证请求逻辑
        Ok(())
    }
    
    async fn handle_connection_event(&self, event: ConnectionEventType, connection_id: &str, details: Option<&str>) -> Result<()> {
        // 处理连接事件逻辑
        Ok(())
    }
}
```

然后在创建FastServer时传入：

```rust
let custom_handler = Arc::new(CustomMessageHandler);
let server = FastServer::new(Some(custom_handler));
```

### 自定义认证提供者

可以通过实现`AuthProvider` trait来创建自定义认证提供者：

```rust
use flare_core::server::fast::auth::AuthProvider;
use flare_core::common::error::Result;

pub struct CustomAuthProvider;

#[async_trait::async_trait]
impl AuthProvider for CustomAuthProvider {
    async fn validate_token(&self, user_id: &str, platform: &str, token: &str) -> Result<bool> {
        // 实现自定义认证逻辑
        Ok(true)
    }
    
    async fn get_user_info(&self, user_id: &str) -> Result<Option<Vec<u8>>> {
        // 实现获取用户信息逻辑
        Ok(Some(vec![]))
    }
}
```

然后在创建FastServer时传入：

```rust
let custom_auth_provider = Arc::new(CustomAuthProvider);
// 在需要认证的地方使用
```

### 认证超时和自动清理

UserConnectionManager支持自动清理超时的待验证连接：

```rust
use std::time::Duration;
use flare_core::server::manager::{ConnectionManager, UserConnectionManager};
use std::sync::Arc;

// 创建用户连接管理器，设置认证超时时间
let base_manager = Arc::new(ConnectionManager::new());
let user_manager = Arc::new(UserConnectionManager::with_config(
    base_manager,
    Duration::from_secs(30) // 30秒认证超时
));

// 启动认证超时清理任务
let _cleanup_task = user_manager.start_auth_timeout_cleanup_task().await;
```

### 使用消息发送器

可以通过FastServer获取消息发送器并使用它发送各种类型的消息：

```rust
// 获取消息发送器
let message_sender = server.get_message_sender();

// 发送系统通知
message_sender.send_system_notification_command(
    "connection_id",
    "欢迎使用Flare Core服务!".to_string()
).await?;

// 发送消息给用户
message_sender.send_message_to_user(
    "user_id",
    frame // Frame对象
).await?;
```