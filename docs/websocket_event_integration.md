# WebSocket 连接事件完整集成

## 概述

本文档详细说明了在 `websocket.rs` 中如何充分利用 `event.rs` 中的 `ConnectionEvent` 事件，确保在合适的时机触发合适的事件。

## 事件触发时机详解

### 1. 连接生命周期事件

#### `on_connected` - 连接建立事件
**触发时机：**
- 客户端：`ClientConnection::connect()` 方法成功建立连接后
- 服务端：`ServerConnection::accept()` 方法接受连接后

**代码位置：**
```rust
// 客户端连接建立
if let Some(handler) = &*self.event_handler.read().await {
    let handler = Arc::clone(handler);
    tokio::spawn(async move {
        handler.on_connected(&id).await;
    });
}
```

#### `on_disconnected` - 连接断开事件
**触发时机：**
- 主动断开：调用 `disconnect()` 或 `close()` 方法时
- 被动断开：接收到 WebSocket Close 帧时
- 对端关闭：WebSocket 流结束时

**代码位置：**
```rust
// 主动断开
handler.on_disconnected(&id, "主动断开").await;

// 对端关闭
handler.on_disconnected(&id, "对端关闭连接").await;
```

#### `on_error` - 连接错误事件
**触发时机：**
- WebSocket 读取错误
- WebSocket 发送错误
- 连接状态异常

**代码位置：**
```rust
// WebSocket 发送错误
if let Err(e) = ws_stream.send(ws_msg).await {
    handler.on_error(&id_clone, &err_text).await;
}
```

### 2. 消息处理事件

#### `on_message_received` - 消息接收事件
**触发时机：**
- 接收到 WebSocket 文本消息时
- 接收到 WebSocket 二进制消息时
- 消息解析为统一协议 Frame 后

**代码位置：**
```rust
// 文本和二进制消息处理
let msg_clone = unified_msg.clone();
tokio::spawn(async move { 
    handler.on_message_received(&id_clone, &msg_clone).await;
    
    // 如果是心跳消息，额外触发心跳接收事件
    if msg_clone.is_heartbeat() {
        handler.on_heartbeat_received(&id_clone).await;
    }
});
```

#### `on_message_sent` - 消息发送事件
**触发时机：**
- WebSocket 消息成功发送后
- 通过发送任务异步触发

**代码位置：**
```rust
// 消息发送成功后触发
if let Some(handler) = &*event_handler.read().await {
    let msg_clone = out_msg.clone();
    tokio::spawn(async move { 
        handler.on_message_sent(&id_clone, &msg_clone).await;
        
        // 如果是心跳消息，额外触发心跳发送事件
        if msg_clone.is_heartbeat() {
            handler.on_heartbeat_sent(&id_clone).await;
        }
    });
}
```

### 3. 心跳监控事件

#### `on_heartbeat_sent` - 心跳发送事件
**触发时机：**
- 调用 `send_heartbeat()` 方法时
- 发送心跳类型消息时

#### `on_heartbeat_received` - 心跳接收事件
**触发时机：**
- 接收到心跳类型消息时
- 在 `on_message_received` 中检测到心跳消息时

#### `on_heartbeat_timeout` - 心跳超时事件
**触发时机：**
- 后台监控任务检测到心跳超时时
- 最后活跃时间超过配置的超时阈值

**实现方式：**
```rust
// 心跳监控任务
let mut interval = tokio::time::interval(heartbeat_interval);
loop {
    interval.tick().await;
    
    let elapsed = last_activity.read().await.elapsed();
    if elapsed > heartbeat_timeout {
        handler.on_heartbeat_timeout(&id_clone).await;
    }
}
```

### 4. 连接质量事件

#### `on_quality_changed` - 连接质量变化事件
**触发时机：**
- 后台监控任务定期计算连接质量
- 质量评分变化超过阈值（10分）时

**质量计算逻辑：**
```rust
let quality_score = if elapsed > heartbeat_timeout {
    0u8 // 超时，质量为0
} else if elapsed > heartbeat_interval {
    let ratio = elapsed.as_millis() as f64 / heartbeat_timeout.as_millis() as f64;
    ((1.0 - ratio) * 100.0).max(10.0) as u8 // 最低10分
} else {
    100u8 // 正常，满分
};
```

### 5. 重连事件

#### `on_reconnect_started` - 重连开始事件
**触发时机：**
- 调用 `try_reconnect()` 方法开始重连时

#### `on_reconnected` - 重连成功事件
**触发时机：**
- 重连操作成功完成时

#### `on_reconnect_failed` - 重连失败事件
**触发时机：**
- 重连操作失败时
- 超过最大重连次数时

**重连逻辑：**
```rust
// 触发重连开始事件
handler.on_reconnect_started(&id, attempts + 1).await;

// 尝试重连
match self.connect().await {
    Ok(_) => {
        // 触发重连成功事件
        handler.on_reconnected(&id, attempts + 1).await;
    }
    Err(e) => {
        // 触发重连失败事件
        handler.on_reconnect_failed(&id, attempts + 1, &error_msg).await;
    }
}
```

### 6. 统计信息事件

#### `on_statistics_updated` - 统计信息更新事件
**触发时机：**
- 后台监控任务定期触发
- 与心跳监控任务同步运行

**实现方式：**
```rust
// 定期触发统计更新事件
if let Some(handler) = &*event_handler.read().await {
    let stats_snapshot = stats.read().await.clone();
    handler.on_statistics_updated(&id_clone, &stats_snapshot).await;
}
```

## 事件触发架构

### 异步事件分发
所有事件都通过 `tokio::spawn` 异步触发，避免阻塞主要业务逻辑：

```rust
tokio::spawn(async move {
    handler.on_event(&id, &data).await;
});
```

### 后台监控任务
启动独立的后台任务监控连接状态：

```rust
// 心跳监控任务
let _heartbeat_monitor_task = tokio::spawn(async move {
    let mut interval = tokio::time::interval(heartbeat_interval);
    loop {
        interval.tick().await;
        // 检查心跳超时
        // 计算连接质量
        // 触发统计更新
    }
});
```

### 事件处理器管理
通过 `Arc<RwLock<Option<Arc<dyn ConnectionEvent>>>>` 安全管理事件处理器：

```rust
if let Some(handler) = &*self.event_handler.read().await {
    let handler = Arc::clone(handler);
    // 异步触发事件
}
```

## 完整事件流程示例

1. **连接建立流程：**
   `connect()` → 建立连接 → `on_connected` → 启动监控任务

2. **消息发送流程：**
   `send_message()` → 消息入队 → 发送任务处理 → `on_message_sent` → (`on_heartbeat_sent`)

3. **消息接收流程：**
   WebSocket 接收 → 解析消息 → `on_message_received` → (`on_heartbeat_received`)

4. **监控流程：**
   定时检查 → 质量计算 → `on_quality_changed` → 超时检测 → `on_heartbeat_timeout` → `on_statistics_updated`

5. **重连流程：**
   `try_reconnect()` → `on_reconnect_started` → 重连尝试 → `on_reconnected`/`on_reconnect_failed`

6. **断开流程：**
   `disconnect()` → 清理资源 → `on_disconnected`

## 使用建议

1. **事件处理器实现：** 继承 `ConnectionEvent` trait 并实现所需的事件处理逻辑
2. **性能考虑：** 事件处理应该快速返回，避免阻塞连接处理
3. **错误处理：** 在事件处理中妥善处理异常，避免影响连接稳定性
4. **状态同步：** 可通过事件更新外部状态，保持数据一致性

## 示例代码

参考 `examples/websocket_event_integration_demo.rs` 获取完整的事件集成演示代码。