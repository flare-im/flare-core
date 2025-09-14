# 长连接框架设计方案

## 1. 总体目标

-   支持多种传输协议（WebSocket、QUIC 等）。
-   保持 **Connection 接口抽象层**，屏蔽协议差异。
-   区分 **客户端连接 (ClientConnection)** 与 **服务端连接
    (ServerConnection)**。
-   支持 **心跳检测、事件通知、错误处理、认证、统计、重连机制**。
-   确保代码的 **健壮性、可扩展性、易维护性**。

------------------------------------------------------------------------

## 2. 核心接口设计

### Connection 基础接口

``` rust
#[async_trait]
pub trait Connection: Send + Sync {
    fn get_id(&self) -> &str;
    async fn get_state(&self) -> ConnectionState;
    async fn is_active(&self) -> bool;
    fn get_config(&self) -> &ConnectionConfig;
    async fn get_last_activity(&self) -> std::time::Instant;
    async fn update_last_activity(&self);
    async fn send_heartbeat(&self) -> Result<()>;
    async fn send_heartbeat_response(&self, data: Option<Vec<u8>>) -> Result<()>;
    async fn set_heartbeat_response_handler(&mut self, handler: Option<HeartbeatResponseHandler>);
    async fn has_received_heartbeat(&self) -> bool;
    async fn reset_heartbeat_state(&self);
    async fn set_connection_event_handler(&mut self, handler: Arc<dyn ConnectionEvent>);
    async fn send_error_notification(&self, error_code: u32, error_message: &str) -> Result<()>;
    async fn send_close_notification(&self, reason: &str) -> Result<()> { Ok(()) }
}
```

------------------------------------------------------------------------

## 3. 客户端连接接口

客户端需要：主动建立连接、断开、重连逻辑。

``` rust
#[async_trait]
pub trait ClientConnection: Connection + Send + Sync {
    async fn connect(&self) -> Result<()>;
    async fn disconnect(&self) -> Result<()>;
    async fn send_message(&self, message: Frame) -> Result<()>;
    async fn try_reconnect(&self) -> Result<()>;
    async fn needs_reconnect(&self) -> bool;
    async fn get_reconnect_attempts(&self) -> u32;
    async fn reset_reconnect_attempts(&self);
}
```

------------------------------------------------------------------------

## 4. 服务端连接接口

服务端需要：接受连接、管理生命周期、绑定用户信息。

``` rust
#[async_trait]
pub trait ServerConnection: Connection + Send + Sync {
    async fn accept(&self) -> Result<()>;
    async fn close(&self) -> Result<()>;
    async fn send_message(&self, message: Frame) -> Result<()>;
    async fn is_healthy(&self) -> bool;
    fn get_client_info(&self) -> Option<String>;
    async fn get_connection_stats(&self) -> ConnectionStats;
    async fn get_user_id(&self) -> Option<String> { None }
    async fn set_user_id(&self, user_id: String) { let _ = user_id; }
}
```

------------------------------------------------------------------------

## 5. 协议抽象层

设计 `Transport` 层，隐藏协议差异：

``` rust
#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, frame: Frame) -> Result<()>;
    async fn receive(&self) -> Result<Frame>;
    async fn close(&self) -> Result<()>;
    fn protocol(&self) -> ProtocolType; // WebSocket / QUIC
}
```

具体实现：

-   `WebSocketTransport`
-   `QuicTransport`

这样 `Connection` 依赖 `Transport`，协议可替换。

------------------------------------------------------------------------

## 6. 健壮性设计

1.  **心跳检测**
    -   客户端定时发心跳。
    -   服务端检测超时关闭。
2.  **事件驱动**
    -   使用 `ConnectionEvent` 通知（连接建立、关闭、错误、消息到达）。
3.  **错误恢复**
    -   客户端：指数退避重连。\
    -   服务端：连接隔离，异常不影响全局。
4.  **认证与用户绑定**
    -   服务端连接可绑定 `user_id`。\
    -   提供 `set_user_id` 和 `get_user_id`。
5.  **统计与监控**
    -   记录：消息收发数、心跳丢失率、延迟。\
    -   提供 `ConnectionStats`。

------------------------------------------------------------------------

## 7. 客户端与服务端差异性

  ------------------------------------------------------------------------
  特性            客户端连接                  服务端连接
                  (ClientConnection)          (ServerConnection)
  --------------- --------------------------- ----------------------------
  连接建立        主动 `connect()`            被动 `accept()`

  断开处理        主动 `disconnect()`         被动 `close()`

  重连逻辑        支持自动重连、指数退避      通常不需要

  用户信息绑定    可选                        必须（认证后绑定 user_id）

  监控统计        本地重连次数、延迟          全局连接数、消息数、错误率

  协议选择        WebSocket/QUIC              WebSocket/QUIC
  ------------------------------------------------------------------------

------------------------------------------------------------------------

## 8. 扩展性设计

-   **新协议支持**：实现 `Transport` 即可。\
-   **新认证方式**：在 `ServerConnection` 扩展。\
-   **分布式支持**：在 `ConnectionManager` 层增加路由。\
-   **消息序列化**：抽象 `FrameCodec`，支持 JSON/Protobuf/MsgPack。

------------------------------------------------------------------------

## 9. 连接管理器

提供统一管理：

``` rust
pub trait ConnectionManager {
    fn add(&self, conn: Arc<dyn Connection>);
    fn remove(&self, id: &str);
    fn get(&self, id: &str) -> Option<Arc<dyn Connection>>;
    fn broadcast(&self, frame: Frame);
    fn stats(&self) -> ConnectionManagerStats;
}
```

------------------------------------------------------------------------

## 10. 总结

-   使用 **Connection** 作为统一抽象。\
-   将 **客户端 / 服务端逻辑分离**。\
-   通过 **Transport 层解耦协议实现**（WebSocket、QUIC）。\
-   提供 **心跳、错误处理、认证、统计、事件回调**。\
-   支持 **可扩展性**：新协议、新认证、新序列化。

该设计可作为一个 **通用长连接框架**，适用于
IM、实时推送、游戏服务器等场景。
