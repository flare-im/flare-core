# 长连接标准规范

## 1. 连接状态管理

### 1.1 状态枚举定义
```rust
pub enum ConnectionState {
    Initializing,  // 初始化中
    Ready,         // 就绪
    Connected,     // 已连接
    Disconnected,  // 已断开
    Error,         // 错误
}
```

### 1.2 状态转换规则
- Initializing → Ready: 连接准备就绪
- Ready → Connected: 连接已建立
- Connected → Disconnected: 连接断开
- Any → Error: 发生错误

### 1.3 状态操作接口
```rust
trait BaseConnection {
    fn state(&self) -> ConnectionState;
    fn ready(&self) -> Result<(), FlareError>;
    fn connected(&self) -> Result<(), FlareError>;
    fn set_state(&self, state: ConnectionState) -> Result<(), FlareError>;
}
```

## 2. 心跳机制

### 2.1 心跳参数配置
- 心跳间隔: 默认10秒
- 心跳超时: 默认30秒
- 最大丢失心跳数: 默认3次

### 2.2 心跳流程
1. 定期发送Ping消息
2. 等待Pong响应
3. 记录往返时间(RTT)
4. 检测超时情况
5. 更新连接质量

### 2.3 心跳事件回调
```rust
trait ConnectionEvent {
    fn on_heartbeat_ping(&self);
    fn on_heartbeat_pong(&self, rtt_ms: u32);
    fn on_heartbeat_timeout(&self);
}
```

## 3. 统计信息收集

### 3.1 统计数据结构
```rust
pub struct ConnectionStats {
    pub established_epoch_ms: u64,      // 连接建立时间
    pub last_activity_epoch_ms: u64,    // 最后活动时间
    pub messages_sent: u64,             // 发送消息数
    pub bytes_sent: u64,                // 发送字节数
    pub messages_received: u64,         // 接收消息数
    pub bytes_received: u64,            // 接收字节数
    pub heartbeat_pings: u64,           // 发送心跳数
    pub heartbeat_pongs: u64,           // 接收心跳数
    pub missed_heartbeats: u32,         // 丢失心跳数
    pub avg_rtt_ms: Option<u32>,        // 平均往返时间
    pub quality: Option<u8>,            // 连接质量评分(0-100)
}
```

### 3.2 统计信息更新
- 消息收发时更新计数
- 心跳时更新Ping/Pong计数
- 超时时更新丢失心跳数
- 定期计算连接质量

### 3.3 统计事件回调
```rust
trait ConnectionEvent {
    fn on_statistics_updated(&self, stats: ConnectionStats);
    fn on_quality_changed(&self, quality: u8);
}
```

## 4. 消息处理流程

### 4.1 消息编码/解码
- 支持多种序列化格式(JSON, Protobuf等)
- 统一的消息帧结构
- 自动化的编解码处理

### 4.2 消息收发接口
```rust
trait BaseConnection {
    fn send_message(&self, frame: Frame) -> Result<(), FlareError>;
}
```

### 4.3 消息事件回调
```rust
trait ConnectionEvent {
    fn on_message_received(&self, frame: Frame);
    fn on_message_sent(&self, frame: Frame);
}
```

## 5. 错误处理机制

### 5.1 错误类型定义
- 连接失败
- 消息发送失败
- 协议错误
- 超时错误
- 其他通用错误

### 5.2 错误处理接口
```rust
trait ConnectionEvent {
    fn on_error(&self, err: FlareError);
    fn on_disconnected(&self, reason: Option<String>);
}
```

### 5.3 重连机制(客户端)
```rust
trait ClientConnection {
    fn connect(&self) -> Result<(), FlareError>;
    fn disconnect(&self, reason: Option<String>) -> Result<(), FlareError>;
}
```

## 6. 连接生命周期管理

### 6.1 客户端连接生命周期
```
Idle -> connect() -> Connecting -> Connected -> disconnect() -> Disconnected
                         ↓ 失败         ↓ 异常断开
                     Disconnected -> reconnect (可选)
```

### 6.2 服务端连接生命周期
```
Pending -> accept() -> Accepting -> Connected -> close() -> Closed
                          ↓ 失败         ↓ 异常断开
                        Closed        Closed
```

## 7. 事件处理机制

### 7.1 事件回调接口
所有连接事件通过ConnectionEvent trait回调通知：
- 连接建立/断开
- 消息收发
- 心跳检测
- 统计信息更新
- 错误处理

### 7.2 事件注册接口
```rust
trait BaseConnection {
    fn set_event_handler(&self, handler: Arc<dyn ConnectionEvent>);
}
```

## 8. 性能与可靠性

### 8.1 性能要求
- 低延迟消息传输
- 高吞吐量处理
- 内存使用优化
- 线程安全设计

### 8.2 可靠性保证
- 消息不丢失
- 连接自动恢复
- 错误优雅处理
- 资源正确释放