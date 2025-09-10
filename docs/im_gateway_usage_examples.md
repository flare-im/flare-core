# Flare IM 网关服务器使用示例

## 目录

1. [基本使用](#基本使用)
2. [自定义认证](#自定义认证)
3. [自定义消息处理](#自定义消息处理)
4. [多端控制](#多端控制)
5. [TLS配置](#tls配置)
6. [监控和日志](#监控和日志)

## 基本使用

### 启动IM网关服务器

```rust
use std::sync::Arc;
use flare_core::{
    server::{
        Server, ServerConfig, UserBasedManager,
        EnhancedMessageHandler, LoggingMessageHandler, BroadcastMessageHandler,
        auth::SimpleAuthHandler,
    },
    common::protocol::{Frame, MessageType},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化 TLS 加密提供程序
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("无法初始化 TLS 加密提供程序");
    
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    // 创建基于用户的连接管理器（支持一个用户多个连接）
    let connection_manager = Arc::new(UserBasedManager::new());
    
    // 创建认证处理器
    let auth_handler = Arc::new(SimpleAuthHandler::new());
    // 添加测试用户
    auth_handler.add_user("user_token_12345".to_string(), "user_001".to_string()).await;
    
    // 创建服务器配置
    let config = ServerConfig {
        websocket_addr: Some("127.0.0.1:8080".to_string()),
        quic_addr: Some("127.0.0.1:8081".to_string()),
        enable_tls: false,
        tls_cert_path: None,
        tls_key_path: None,
        max_connections: 1000,
        connection_timeout_ms: 30000,
        heartbeat_interval_ms: 5000,
        enable_auto_cleanup: true,
    };
    
    // 创建服务器实例
    let mut server = Server::new(config, connection_manager.clone());
    
    // 创建消息处理器链
    let logging_handler = Arc::new(LoggingMessageHandler);
    let enhanced_handler = Arc::new(EnhancedMessageHandler::new(logging_handler));
    
    // 注册广播消息处理器用于聊天消息
    let broadcast_handler = Arc::new(BroadcastMessageHandler::new(connection_manager.clone()));
    enhanced_handler.register_typed_handler("Data".to_string(), broadcast_handler).await;
    
    // 注册消息处理器
    server.register_message_handler(enhanced_handler).await;
    
    // 启动服务器
    server.start().await?;
    
    println!("IM 网关服务器已启动");
    
    // 等待中断信号
    tokio::signal::ctrl_c().await?;
    
    // 停止服务器
    server.stop().await?;
    
    Ok(())
}
```

## 自定义认证

### 实现自定义认证处理器

```rust
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use flare_core::{
    server::auth::{AuthHandler, Platform},
    common::error::Result,
};

/// 自定义认证处理器
pub struct CustomAuthHandler {
    /// 用户凭证映射 (username:password -> user_id)
    users: Arc<RwLock<HashMap<String, String>>>,
}

impl CustomAuthHandler {
    /// 创建新的自定义认证处理器
    pub fn new() -> Self {
        let users = Arc::new(RwLock::new(HashMap::new()));
        Self { users }
    }

    /// 添加用户凭证
    pub async fn add_user(&self, username: String, password: String, user_id: String) {
        let key = format!("{}:{}", username, password);
        let mut users = self.users.write().await;
        users.insert(key, user_id);
    }
}

#[async_trait::async_trait]
impl AuthHandler for CustomAuthHandler {
    async fn authenticate(&self, auth_data: Vec<u8>) -> Result<String> {
        // 解析认证数据 (假设是 username:password 格式)
        let auth_str = String::from_utf8(auth_data)
            .map_err(|e| flare_core::common::error::FlareError::general_error(format!("无效的认证数据: {}", e)))?;
        
        let users = self.users.read().await;
        if let Some(user_id) = users.get(&auth_str) {
            Ok(user_id.clone())
        } else {
            Err(flare_core::common::error::FlareError::authentication_failed("无效的用户名或密码".to_string()))
        }
    }
    
    async fn authenticate_with_platform(
        &self, 
        auth_data: Vec<u8>, 
        platform: Option<Platform>,
        device_id: Option<String>,
        app_version: Option<String>,
    ) -> Result<String> {
        // 执行基本认证
        let user_id = self.authenticate(auth_data).await?;
        
        // 记录平台信息（可以用于日志或统计）
        if let Some(platform) = platform {
            println!("用户 {} 从 {:?} 平台登录", user_id, platform);
            if let Some(device_id) = device_id {
                println!("设备ID: {}", device_id);
            }
            if let Some(app_version) = app_version {
                println!("应用版本: {}", app_version);
            }
        }
        
        Ok(user_id)
    }
}

// 使用自定义认证处理器
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ... 初始化代码 ...
    
    // 创建自定义认证处理器
    let auth_handler = Arc::new(CustomAuthHandler::new());
    auth_handler.add_user("admin".to_string(), "password123".to_string(), "admin_001".to_string()).await;
    
    // ... 其他代码 ...
}
```

## 自定义消息处理

### 实现自定义消息处理器

```rust
use std::sync::Arc;
use flare_core::{
    server::service::MessageHandler,
    common::{
        error::Result,
        protocol::{Frame, MessageType},
    },
};

/// 自定义IM消息处理器
pub struct CustomIMMessageHandler;

#[async_trait::async_trait]
impl MessageHandler for CustomIMMessageHandler {
    async fn handle_message(&self, connection_id: String, message: Frame) -> Result<Option<Frame>> {
        match message.message_type {
            // 处理聊天消息
            MessageType::Data => {
                let content = String::from_utf8_lossy(&message.payload);
                println!("收到聊天消息 from {}: {}", connection_id, content);
                
                // 可以在这里添加消息处理逻辑
                // 例如：存储到数据库、转发给其他用户等
                
                // 返回响应消息（可选）
                let response = Frame::new(
                    MessageType::Data,
                    message.id,
                    message.reliability,
                    b"消息已收到".to_vec(),
                );
                Ok(Some(response))
            }
            // 处理用户上线事件
            MessageType::CustomEvent if message.payload == b"user_online" => {
                println!("用户上线: {}", connection_id);
                // 可以在这里添加用户上线逻辑
                // 例如：更新用户状态、发送欢迎消息等
                Ok(None)
            }
            // 处理用户下线事件
            MessageType::CustomEvent if message.payload == b"user_offline" => {
                println!("用户下线: {}", connection_id);
                // 可以在这里添加用户下线逻辑
                // 例如：更新用户状态、通知其他用户等
                Ok(None)
            }
            // 其他消息类型
            _ => {
                println!("收到未知消息类型 from {}: {:?}", connection_id, message.message_type);
                Ok(None)
            }
        }
    }
}

// 使用自定义消息处理器
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ... 初始化代码 ...
    
    // 创建自定义消息处理器
    let custom_handler = Arc::new(CustomIMMessageHandler);
    
    // 注册消息处理器
    server.register_message_handler(custom_handler).await;
    
    // ... 其他代码 ...
}
```

## 多端控制

### 查询用户在线平台

```rust
use flare_core::server::auth::Platform;

// 查询用户的所有在线平台
async fn get_user_platforms(auth_manager: &flare_core::server::auth::AuthManager, user_id: &str) -> Vec<Platform> {
    auth_manager.get_user_online_platforms(user_id).await
}

// 查询用户在特定平台的连接
async fn get_user_connection_on_platform(
    auth_manager: &flare_core::server::auth::AuthManager, 
    user_id: &str, 
    platform: &Platform
) -> Option<String> {
    auth_manager.get_user_connection_on_platform(user_id, platform).await
}

// 强制用户在特定平台下线
async fn force_logout_platform(
    auth_manager: &flare_core::server::auth::AuthManager, 
    user_id: &str, 
    platform: &Platform
) -> Option<String> {
    auth_manager.force_logout_platform(user_id, platform).await
}
```

### 客户端发送平台信息

```rust
use flare_core::{
    client::{Client, ClientConfig},
    common::protocol::{Frame, MessageType},
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ... 客户端初始化代码 ...
    
    // 发送带平台信息的认证消息
    let auth_data = json!({
        "username": "admin",
        "password": "password123"
    });
    
    let mut auth_frame = Frame::new(
        MessageType::Connect,
        1,
        flare_core::common::protocol::Reliability::ExactlyOnce,
        serde_json::to_vec(&auth_data)?,
    );
    
    // 添加平台信息到元数据
    auth_frame.add_metadata("platform", b"android");
    auth_frame.add_metadata("device_id", b"android_device_123");
    auth_frame.add_metadata("app_version", b"2.1.0");
    
    client.send_message(auth_frame).await?;
    
    // ... 其他代码 ...
}
```

## TLS配置

### 启用TLS加密

```rust
use flare_core::server::{Server, ServerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ... 初始化代码 ...
    
    // 创建启用TLS的服务器配置
    let config = ServerConfig {
        websocket_addr: Some("127.0.0.1:8080".to_string()),
        quic_addr: Some("127.0.0.1:8081".to_string()),
        enable_tls: true,
        tls_cert_path: Some("/path/to/cert.pem".to_string()),
        tls_key_path: Some("/path/to/key.pem".to_string()),
        max_connections: 1000,
        connection_timeout_ms: 30000,
        heartbeat_interval_ms: 5000,
        enable_auto_cleanup: true,
    };
    
    // ... 其他代码 ...
}
```

### 生成自签名证书

```bash
# 生成私钥
openssl genrsa -out key.pem 2048

# 生成证书签名请求
openssl req -new -key key.pem -out csr.pem

# 生成自签名证书
openssl x509 -req -days 365 -in csr.pem -signkey key.pem -out cert.pem
```

## 监控和日志

### 配置详细日志

```rust
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化详细日志
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    
    tracing::subscriber::set_global_default(subscriber)
        .expect("无法设置全局默认日志订阅者");
    
    // ... 其他代码 ...
}
```

### 定期输出服务器统计信息

```rust
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ... 服务器初始化代码 ...
    
    // 每30秒显示一次服务器统计信息
    let stats_manager = server.get_connection_manager().clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let stats = stats_manager.get_stats().await;
            println!("服务器统计 - 总连接数: {}, 活跃连接数: {}, 总消息数: {}", 
                stats.total_connections, stats.active_connections, stats.total_messages);
        }
    });
    
    // ... 其他代码 ...
}
```

### 自定义日志处理器

```rust
use std::sync::Arc;
use flare_core::{
    server::service::MessageHandler,
    common::{
        error::Result,
        protocol::Frame,
    },
};

/// 自定义日志处理器
pub struct CustomLoggingHandler;

#[async_trait::async_trait]
impl MessageHandler for CustomLoggingHandler {
    async fn handle_message(&self, connection_id: String, message: Frame) -> Result<Option<Frame>> {
        // 记录消息日志到自定义系统
        println!("[{}] 收到消息: {:?}", connection_id, message);
        
        // 可以将日志发送到外部系统
        // 例如：写入数据库、发送到日志服务等
        
        // 不返回响应消息
        Ok(None)
    }
}
```

通过这些示例，您可以了解如何在实际项目中使用Flare IM网关服务器的各种功能。每个示例都展示了特定功能的实现方式，您可以根据自己的需求进行组合和扩展。