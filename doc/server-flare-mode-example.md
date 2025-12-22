# 服务端 Flare 模式示例

Flare 模式提供完整功能，包含所有 `common` 和 `server` 模块的能力，是最强大的构建模式。

## 特点

- ✅ **必须实现 `ServerEventHandler` trait**（核心接口）
- ✅ **自动消息路由**：`ServerMessageWrapper` 自动将消息路由到对应的处理方法
- ✅ **自动 ACK 处理**：如果 handler 返回 `None`，框架自动发送 ACK
- ✅ **错误处理**：处理失败时自动发送错误 ACK，确保客户端能收到响应
- ✅ **设备管理**：完整的设备冲突策略（平台互斥、移动端互斥等）
- ✅ **认证机制**：JWT Token 认证（可选）
- ✅ **心跳检测**：自动心跳和超时管理
- ✅ **多协议支持**：WebSocket + QUIC 双协议
- ✅ **序列化协商**：自动协商最佳序列化格式和压缩算法
- ✅ **加密支持**：支持 AES-256-GCM 等加密算法
- ✅ **连接管理**：完整的连接状态管理和统计

## 适用场景

- 生产环境
- 需要完整功能的企业应用
- 需要高性能和可扩展性的场景
- 需要统一消息处理流程的场景
- 需要设备管理的多端应用
- 需要加密通信的安全应用

## 完整示例

```rust
use async_trait::async_trait;
use flare_core::common::compression::CompressionAlgorithm;
use flare_core::common::config_types::{HeartbeatConfig, TransportProtocol};
use flare_core::common::device::{DeviceConflictStrategyBuilder, DeviceManager};
use flare_core::common::encryption::{Aes256GcmEncryptor, EncryptionAlgorithm, EncryptionUtil};
use flare_core::common::error::Result;
use flare_core::common::protocol::{
    Frame, MessageCommand, Reliability, SerializationFormat,
    frame_with_message_command, generate_message_id,
};
use flare_core::server::connection::{ConnectionManager, ConnectionManagerTrait};
use flare_core::server::events::handler::ServerEventHandler;
use flare_core::server::handle::{DefaultServerHandle, ServerHandle};
use flare_core::server::FlareServerBuilder;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

/// 聊天室事件处理器（实现 ServerEventHandler）
struct ChatRoomHandler {
    usernames: Arc<Mutex<HashMap<String, String>>>,
    server_handle: Arc<Mutex<Option<Arc<dyn ServerHandle>>>>,
    connection_manager: Arc<Mutex<Option<Arc<dyn ConnectionManagerTrait>>>>,
}

impl ChatRoomHandler {
    fn new() -> Self {
        Self {
            usernames: Arc::new(Mutex::new(HashMap::new())),
            server_handle: Arc::new(tokio::sync::Mutex::new(None)),
            connection_manager: Arc::new(Mutex::new(None)),
        }
    }

    async fn set_server_handle(&self, handle: Arc<dyn ServerHandle>) {
        *self.server_handle.lock().await = Some(handle);
    }

    async fn set_connection_manager(&self, manager: Arc<dyn ConnectionManagerTrait>) {
        *self.connection_manager.lock().await = Some(manager);
    }

    async fn broadcast_message_except(&self, frame: &Frame, exclude_connection_id: &str) {
        let handle_guard = self.server_handle.lock().await;
        if let Some(ref handle) = *handle_guard {
            if let Err(e) = handle.broadcast_except(frame, exclude_connection_id).await {
                error!("广播消息失败: {}", e);
            }
        }
    }
}

#[async_trait]
impl ServerEventHandler for ChatRoomHandler {
    // ============================================================
    // 处理消息命令（发送消息）
    // ============================================================
    async fn handle_message(
        &self,
        command: &MessageCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        // 提取消息内容
        let message_text = String::from_utf8_lossy(&command.payload);

        // 获取或创建用户名
        let username = {
            let mut usernames = self.usernames.lock().await;
            usernames
                .entry(connection_id.to_string())
                .or_insert_with(|| {
                    if let Some(username_bytes) = command.metadata.get("username") {
                        String::from_utf8_lossy(username_bytes).to_string()
                    } else {
                        format!("用户_{}", &connection_id[..8.min(connection_id.len())])
                    }
                })
                .clone()
        };

        info!("[聊天室] {} 说: {}", username, message_text);

        // 构建广播消息
        let mut broadcast_metadata = HashMap::new();
        broadcast_metadata.insert("username".to_string(), username.as_bytes().to_vec());
        broadcast_metadata.insert(
            "connection_id".to_string(),
            connection_id.as_bytes().to_vec(),
        );

        let broadcast_msg = flare_core::common::protocol::send_message(
            generate_message_id(),
            format!("[{}] {}", username, message_text).into_bytes(),
            Some(broadcast_metadata),
            None,
        );

        let broadcast_frame =
            frame_with_message_command(broadcast_msg, Reliability::BestEffort);

        // 广播给除发送者外的所有连接
        self.broadcast_message_except(&broadcast_frame, connection_id)
            .await;

        // 返回 None 表示使用自动 ACK
        Ok(None)
    }

    // ============================================================
    // 处理 ACK 消息命令（可选实现）
    // ============================================================
    async fn handle_ack(
        &self,
        _command: &MessageCommand,
        _connection_id: &str,
    ) -> Result<Option<Frame>> {
        // 处理 ACK 消息（如果需要）
        Ok(None)
    }

    // ============================================================
    // 连接建立完成
    // ============================================================
    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        info!("[聊天室] ✅ 用户 {} 加入聊天室", connection_id);
        Ok(())
    }

    // ============================================================
    // 连接断开
    // ============================================================
    async fn on_disconnect(&self, connection_id: &str, reason: Option<&str>) -> Result<()> {
        let username = {
            let mut usernames = self.usernames.lock().await;
            usernames.remove(connection_id)
        };

        let display_name = username
            .as_deref()
            .unwrap_or(&connection_id[..8.min(connection_id.len())]);
        info!("[聊天室] ❌ {} 离开了聊天室", display_name);

        if let Some(reason) = reason {
            debug!("断开原因: {}", reason);
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("🚀 启动 Flare 聊天室服务器（完整功能演示）");

    // ============================================================
    // 1. 注册加密器（可选，用于加密通信）
    // ============================================================
    // 注意：在生产环境中，密钥应该从安全配置中读取，不要硬编码
    let encryption_key = b"01234567890123456789012345678901"; // 32 bytes for AES-256
    let encryptor = Aes256GcmEncryptor::new(encryption_key)?;
    EncryptionUtil::register_custom(Arc::new(encryptor));
    info!("🔐 已注册 AES-256-GCM 加密器");

    // ============================================================
    // 2. 创建连接管理器（可选，用于共享连接状态）
    // ============================================================
    let connection_manager = Arc::new(ConnectionManager::new());

    // ============================================================
    // 3. 创建聊天室事件处理器
    // ============================================================
    let chat_handler = Arc::new(ChatRoomHandler {
        usernames: Arc::new(Mutex::new(HashMap::new())),
        server_handle: Arc::new(Mutex::new(None)),
        connection_manager: Arc::new(Mutex::new(None)),
    });

    // ============================================================
    // 4. 创建设备管理器（用于设备冲突管理）
    // ============================================================
    let device_manager = Arc::new(DeviceManager::new(
        DeviceConflictStrategyBuilder::new()
            .platform_exclusive() // 平台互斥：同一用户同一平台只能有一个设备在线
            // 其他策略选项：
            // .mobile_exclusive()      // 移动端互斥：同一用户只能有一个移动端设备在线
            // .mobile_and_pc_coexist() // 移动端和PC端共存：同一用户可以同时有1个移动端和1个PC端在线
            // .fully_exclusive()       // 完全互斥：同一用户只能有一个设备在线
            // .allow_all()             // 允许所有设备同时在线
            .build(),
    ));

    // ============================================================
    // 5. 使用 FlareServerBuilder 构建服务器
    // ============================================================
    let server = FlareServerBuilder::new("0.0.0.0:8080", chat_handler.clone())
        // 连接和设备管理
        .with_connection_manager(connection_manager.clone())
        .with_device_manager(device_manager)
        // 协商配置（服务端默认格式、压缩和加密）
        .with_default_format(SerializationFormat::Protobuf) // 默认 Protobuf（客户端可指定格式）
        .with_default_compression(CompressionAlgorithm::Gzip) // 默认 Gzip 压缩
        .with_default_encryption(EncryptionAlgorithm::Aes256Gcm) // 默认 AES-256-GCM 加密
        // 协议配置（多协议支持）
        .with_protocols(vec![TransportProtocol::WebSocket, TransportProtocol::QUIC])
        .with_protocol_address(TransportProtocol::WebSocket, "0.0.0.0:8080".to_string())
        .with_protocol_address(TransportProtocol::QUIC, "0.0.0.0:8081".to_string())
        // 其他配置
        .with_max_connections(2000)
        .with_connection_timeout(std::time::Duration::from_secs(60))
        .with_heartbeat(
            HeartbeatConfig::default()
                .with_interval(std::time::Duration::from_secs(30))
                .with_timeout(std::time::Duration::from_secs(90)),
        )
        // 可选：启用认证（需要设置 authenticator）
        // .enable_auth()
        // .with_authenticator(authenticator)
        .build()?;

    // ============================================================
    // 6. 获取 ServerHandle 并设置到 handler
    // ============================================================
    let (server_handle, manager_trait) =
        if let Some(manager_trait) = server.get_server_handle_components() {
            let handle: Arc<dyn ServerHandle> =
                Arc::new(DefaultServerHandle::new(manager_trait.clone()));
            (handle, manager_trait)
        } else {
            return Err("无法获取连接管理器".into());
        };
    chat_handler.set_server_handle(server_handle).await;
    chat_handler.set_connection_manager(manager_trait).await;

    // ============================================================
    // 7. 启动服务器
    // ============================================================
    server.start().await?;

    info!("✅ 服务器已启动");
    info!("   WebSocket: ws://127.0.0.1:8080");
    info!("   QUIC: quic://127.0.0.1:8081");
    info!("\n服务器运行中，按 Ctrl+C 停止...");

    // 等待停止信号
    tokio::signal::ctrl_c().await?;

    info!("\n正在停止服务器...");
    server.stop().await?;
    info!("服务器已停止");

    Ok(())
}
```

## 关键说明

### 1. ServerEventHandler trait

必须实现 `ServerEventHandler` trait，包含以下方法：

- **`handle_message`**：处理消息命令（发送消息）
- **`handle_ack`**：处理 ACK 消息命令（可选）
- **`handle_notification_command`**：处理通知命令（可选）
- **`handle_custom_command`**：处理自定义命令（可选）
- **`on_connect`**：连接建立完成时调用
- **`on_disconnect`**：连接断开时调用

### 2. 设备管理

支持多种设备冲突策略：

```rust
DeviceConflictStrategyBuilder::new()
    .platform_exclusive()      // 平台互斥（推荐）
    .mobile_exclusive()        // 移动端互斥
    .mobile_and_pc_coexist()   // 移动端和PC端共存
    .fully_exclusive()         // 完全互斥
    .allow_all()               // 允许所有设备同时在线
    .build()
```

### 3. 加密支持

需要先注册加密器，然后在配置中启用：

```rust
// 注册加密器（必须与服务端和客户端使用相同的密钥）
let encryption_key = b"01234567890123456789012345678901"; // 32 bytes for AES-256
let encryptor = Aes256GcmEncryptor::new(encryption_key)?;
EncryptionUtil::register_custom(Arc::new(encryptor));

// 在构建器中启用加密
.with_default_encryption(EncryptionAlgorithm::Aes256Gcm)
```

### 4. 序列化协商

可以设置服务端默认的序列化格式、压缩方式和加密算法：

```rust
.with_default_format(SerializationFormat::Protobuf) // 或 Json
.with_default_compression(CompressionAlgorithm::Gzip) // 或 None, Zstd
.with_default_encryption(EncryptionAlgorithm::Aes256Gcm) // 或 None
```

客户端可以在连接时指定格式，服务端会根据客户端能力协商使用。

### 5. 认证机制（可选）

如果需要启用认证：

```rust
// 创建认证器
struct MyAuthenticator;

impl Authenticator for MyAuthenticator {
    async fn authenticate(
        &self,
        token: &str,
        connection_id: &str,
        device_info: Option<&DeviceInfo>,
        metadata: Option<&HashMap<String, Vec<u8>>>,
    ) -> Result<AuthResult> {
        // 实现认证逻辑
        if token == "valid_token" {
            Ok(AuthResult::success(Some("user_id".to_string())))
        } else {
            Ok(AuthResult::failure("Invalid token".to_string()))
        }
    }
}

// 在构建器中启用认证
.enable_auth()
.with_authenticator(Arc::new(MyAuthenticator))
```

### 6. 运行示例

```bash
# 使用默认日志级别（info）
cargo run --example flare_chat_server

# 使用 debug 级别查看详细信息
RUST_LOG=debug cargo run --example flare_chat_server
```
