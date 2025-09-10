# Flare Server 使用指南

## 目录

1. [快速开始](#快速开始)
2. [配置服务器](#配置服务器)
3. [选择连接管理器](#选择连接管理器)
4. [处理消息](#处理消息)
5. [完整示例](#完整示例)
6. [常见问题](#常见问题)

## 快速开始

### 1. 添加依赖

在 `Cargo.toml` 中添加依赖：

```toml
[dependencies]
flare-core = { path = "../flare-core" }
tokio = { version = "1.0", features = ["full"] }
tracing = "0.1"
tracing-subscriber = "0.3"
```

### 2. 基本用法

```rust
use std::sync::Arc;
use flare_core::{
    server::{
        Server, ServerConfig, ConnectionBasedManager,
        EchoMessageHandler,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    // 创建连接管理器
    let connection_manager = Arc::new(ConnectionBasedManager::new());
    
    // 创建服务器配置
    let config = ServerConfig::default();
    
    // 创建服务器实例
    let mut server = Server::new(config, connection_manager);
    
    // 注册消息处理器
    let echo_handler = Arc::new(EchoMessageHandler);
    server.register_message_handler(echo_handler).await;
    
    // 启动服务器
    server.start().await?;
    
    println!("服务器已启动:");
    println!("  WebSocket地址: 127.0.0.1:8080");
    println!("  QUIC地址: 127.0.0.1:8081");
    println!("按 Ctrl+C 停止服务器");
    
    // 等待中断信号
    tokio::signal::ctrl_c().await?;
    
    // 停止服务器
    server.stop().await?;
    
    println!("服务器已停止");
    Ok(())
}
```

## 配置服务器

### ServerConfig 结构

[ServerConfig](server/struct.ServerConfig.html) 用于配置服务器的各种参数：

```rust
pub struct ServerConfig {
    /// WebSocket监听地址
    pub websocket_addr: Option<String>,
    /// QUIC监听地址
    pub quic_addr: Option<String>,
    /// 是否启用TLS
    pub enable_tls: bool,
    /// TLS证书路径
    pub tls_cert_path: Option<String>,
    /// TLS私钥路径
    pub tls_key_path: Option<String>,
    /// 最大连接数
    pub max_connections: usize,
    /// 连接超时时间（毫秒）
    pub connection_timeout_ms: u64,
}
```

### 配置示例

#### 仅启用 WebSocket

```rust
let config = ServerConfig {
    websocket_addr: Some("127.0.0.1:8080".to_string()),
    quic_addr: None,
    enable_tls: false,
    tls_cert_path: None,
    tls_key_path: None,
    max_connections: 1000,
    connection_timeout_ms: 30000,
};
```

#### 仅启用 QUIC

```rust
let config = ServerConfig {
    websocket_addr: None,
    quic_addr: Some("127.0.0.1:8081".to_string()),
    enable_tls: false,
    tls_cert_path: None,
    tls_key_path: None,
    max_connections: 1000,
    connection_timeout_ms: 30000,
};
```

#### 同时启用两种协议

```rust
let config = ServerConfig {
    websocket_addr: Some("127.0.0.1:8080".to_string()),
    quic_addr: Some("127.0.0.1:8081".to_string()),
    enable_tls: false,
    tls_cert_path: None,
    tls_key_path: None,
    max_connections: 1000,
    connection_timeout_ms: 30000,
};
```

## 选择连接管理器

Flare Server 提供了两种连接管理器实现：

### ConnectionBasedManager (基于连接的管理器)

适用于简单的连接管理需求，每个连接独立管理。

```rust
use flare_core::server::ConnectionBasedManager;

let manager = ConnectionBasedManager::new();
```

### UserBasedManager (基于用户的管理器)

适用于需要按用户维度管理连接的场景，支持一个用户多个连接。

```rust
use flare_core::server::UserBasedManager;

let manager = UserBasedManager::new();
```

## 处理消息

### 实现自定义消息处理器

```rust
use std::sync::Arc;
use flare_core::{
    server::service::MessageHandler,
    common::{
        error::Result,
        protocol::Frame,
    },
};

pub struct CustomMessageHandler;

#[async_trait::async_trait]
impl MessageHandler for CustomMessageHandler {
    async fn handle_message(&self, connection_id: String, message: Frame) -> Result<Option<Frame>> {
        // 处理消息逻辑
        println!("收到来自连接 {} 的消息: {:?}", connection_id, message);
        
        // 可以返回响应消息，或者返回 None 表示不需要响应
        Ok(None)
    }
}
```

### 使用内置的 EchoMessageHandler

```rust
use flare_core::server::EchoMessageHandler;

let echo_handler = Arc::new(EchoMessageHandler);
```

## 完整示例

### 使用 ConnectionBasedManager

```rust
use std::sync::Arc;
use flare_core::{
    server::{
        Server, ServerConfig, ConnectionBasedManager,
        EchoMessageHandler,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    // 创建基于连接的管理器
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
    
    println!("服务器已启动:");
    println!("  WebSocket地址: 127.0.0.1:8080");
    println!("  QUIC地址: 127.0.0.1:8081");
    println!("按 Ctrl+C 停止服务器");
    
    // 等待中断信号
    tokio::signal::ctrl_c().await?;
    
    // 停止服务器
    server.stop().await?;
    
    println!("服务器已停止");
    Ok(())
}
```

### 使用 UserBasedManager

```rust
use std::sync::Arc;
use flare_core::{
    server::{
        Server, ServerConfig, UserBasedManager,
        service::MessageHandler,
    },
    common::{
        error::Result,
        protocol::Frame,
    },
};

pub struct UserMessageHandler;

#[async_trait::async_trait]
impl MessageHandler for UserMessageHandler {
    async fn handle_message(&self, connection_id: String, message: Frame) -> Result<Option<Frame>> {
        println!("收到来自连接 {} 的消息", connection_id);
        // 处理用户消息
        Ok(None)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    // 创建基于用户的管理器
    let user_manager = Arc::new(UserBasedManager::new());
    
    // 创建服务器配置
    let config = ServerConfig {
        websocket_addr: Some("127.0.0.1:8082".to_string()),
        quic_addr: Some("127.0.0.1:8083".to_string()),
        enable_tls: false,
        tls_cert_path: None,
        tls_key_path: None,
        max_connections: 1000,
        connection_timeout_ms: 30000,
    };
    
    // 创建服务器实例
    let mut server = Server::new(config, user_manager);
    
    // 注册消息处理器
    let user_handler = Arc::new(UserMessageHandler);
    server.register_message_handler(user_handler).await;
    
    // 启动服务器
    server.start().await?;
    
    println!("基于用户的服务器已启动:");
    println!("  WebSocket地址: 127.0.0.1:8082");
    println!("  QUIC地址: 127.0.0.1:8083");
    println!("按 Ctrl+C 停止服务器");
    
    // 等待中断信号
    tokio::signal::ctrl_c().await?;
    
    // 停止服务器
    server.stop().await?;
    
    println!("服务器已停止");
    Ok(())
}
```

## 常见问题

### 1. 如何自定义连接管理器？

实现 [ConnectionManager](manager/traits/trait.ConnectionManager.html) trait：

```rust
use std::sync::Arc;
use std::time::Duration;
use flare_core::{
    server::manager::traits::{ConnectionManager, ManagerStats},
    common::{
        error::Result,
        connections::traits::ServerConnection,
        protocol::Frame,
    },
};

pub struct CustomConnectionManager;

#[async_trait::async_trait]
impl ConnectionManager for CustomConnectionManager {
    async fn add_connection(&self, connection: Arc<dyn ServerConnection>) -> Result<()> {
        // 实现添加连接逻辑
        Ok(())
    }
    
    async fn remove_connection(&self, connection_id: &str) -> Result<()> {
        // 实现移除连接逻辑
        Ok(())
    }
    
    // 实现其他必需的方法...
    
    async fn get_stats(&self) -> ManagerStats {
        // 实现统计信息逻辑
        ManagerStats {
            total_connections: 0,
            active_connections: 0,
            total_messages: 0,
            average_quality: 100,
            uptime: Duration::from_secs(0),
        }
    }
}
```

### 2. 如何处理 TLS？

在配置中启用 TLS 并提供证书路径：

```rust
let config = ServerConfig {
    websocket_addr: Some("127.0.0.1:8080".to_string()),
    quic_addr: Some("127.0.0.1:8081".to_string()),
    enable_tls: true,
    tls_cert_path: Some("/path/to/cert.pem".to_string()),
    tls_key_path: Some("/path/to/key.pem".to_string()),
    max_connections: 1000,
    connection_timeout_ms: 30000,
};
```

### 3. 如何监控服务器状态？

通过连接管理器获取统计信息：

```rust
// 获取统计信息
let stats = server.get_connection_manager().get_stats().await;
println!("总连接数: {}", stats.total_connections);
println!("活跃连接数: {}", stats.active_connections);
println!("总消息数: {}", stats.total_messages);
```