# Flare Core 命令系统设计文档

## 概述

Flare Core 的命令系统采用分层设计，将命令按照功能划分为五大类：控制类、数据类、消息类、通知类和事件类。每个大类下包含具体的子命令，所有命令都支持状态标记和原因说明，便于服务端响应和错误处理。

## 命令分类结构

### 1. 控制类命令 (Control Commands)
控制类命令用于处理连接控制、认证、心跳等核心功能。

**子命令类型：**
- `Connect` - 客户端连接请求
- `ConnectAck` - 服务端连接响应
- `Disconnect` - 断开连接请求
- `AuthRequest` - 认证请求
- `AuthResponse` - 认证响应
- `Ping` - 心跳请求
- `Pong` - 心跳响应
- `Error` - 错误响应
- `Custom` - 自定义控制命令

### 2. 数据类命令 (Data Commands)
数据类命令用于传输业务数据，无需ACK确认。

**子命令类型：**
- `Send` - 发送数据
- `Resend` - 重发数据

### 3. 消息类命令 (Message Commands)
消息类命令用于传输需要ACK确认的消息。

**子命令类型：**
- `Send` - 发送消息
- `Ack` - 消息确认
- `Data` - 数据传输（作为消息的变体）
- `Custom` - 自定义消息命令

### 4. 通知类命令 (Notification Commands)
通知类命令用于系统通知和广播。

**子命令类型：**
- `System` - 系统通知
- `Broadcast` - 广播通知
- `Alert` - 警报通知
- `Custom` - 自定义通知命令

### 5. 事件类命令 (Event Commands)
事件类命令用于处理连接状态变化等事件。

**子命令类型：**
- `Open` - 连接打开事件
- `Close` - 连接关闭事件
- `Reconnect` - 重连事件
- `Custom` - 自定义事件命令

## 命令结构设计

### 主命令枚举
```rust
pub enum Command {
    Control(ControlCmd),
    Data(DataCmd),
    Message(MessageCmd),
    Notification(NotificationCmd),
    Event(EventCmd),
}
```

### 状态标记和原因
所有服务端响应命令都包含状态标记和原因字段：

```rust
pub struct MessageAckCommand {
    /// 状态码
    pub status: i32,
    /// 状态消息
    pub status_message: Option<String>,
    /// 是否成功
    pub success: bool,
    /// 错误码
    pub error_code: Option<u32>,
    /// 错误消息
    pub error_message: Option<String>,
}
```

## 使用示例

### 客户端发送连接请求
```rust
let connect_command = Command::Control(ControlCmd::Connect(
    ConnectCommand::new(
        "client_123".to_string(),
        "auto".to_string(),
        "web".to_string(),
        "1.0.0".to_string()
    )
));
```

### 服务端成功响应
```rust
let connect_ack_success = Command::Control(ControlCmd::ConnectAck(
    ConnectAckCommand::success("session_456".to_string())
));
```

### 服务端失败响应
```rust
let connect_ack_failure = Command::Control(ControlCmd::ConnectAck(
    ConnectAckCommand::failure(401, "Authentication failed".to_string())
));
```

## Protobuf 支持

命令系统同时支持 Protobuf 格式，便于跨语言通信。

### Protobuf 命令结构
```protobuf
message Command {
  oneof command_type {
    ControlCommand control = 1;
    DataCommandContainer data = 2;
    MessageCommandContainer message = 3;
    NotificationCommandContainer notification = 4;
    EventCommandContainer event = 5;
  }
}
```

## 网络传输

命令通过 Frame 结构在网络中传输：

```rust
let command_frame = Frame::command(1001, connect_command);
```

## 最佳实践

1. **合理使用命令类型**：根据业务需求选择合适的命令类型
2. **状态标记一致性**：确保服务端响应包含适当的状态标记和原因
3. **错误处理**：使用 Error 命令统一处理错误情况
4. **自定义扩展**：通过 Custom 命令支持业务特定功能
5. **性能优化**：利用短字符标识减少网络传输数据量

## 总结

Flare Core 的命令系统通过清晰的分类和统一的状态标记机制，提供了强大而灵活的命令处理能力。服务端响应命令的状态标记和原因字段使得错误处理和调试更加方便，提升了系统的可靠性和可维护性。