# 客户端模块重构设计

## 1. 设计目标

基于通用连接抽象层包装客户端专有特性，实现职责分离和可扩展性。

## 2. 模块结构

### 2.1 核心模块
- `enhanced_client.rs`: 增强型客户端，提供协议选择和协议竞速功能
- `protocol_racer.rs`: 协议竞速器，支持多协议并行连接
- `reconnect.rs`: 重连管理器，提供自动重连功能

### 2.2 连接模块
- `connections/websocket.rs`: WebSocket客户端连接实现
- `connections/quic.rs`: QUIC客户端连接实现

## 3. 客户端接口设计

### 3.1 EnhancedClient
```rust
pub struct EnhancedClient {
    connection: Option<Arc<dyn ClientConnection>>,
    config: ConnectionConfig,
    reconnect_manager: Option<ReconnectManager>,
}

impl EnhancedClient {
    pub fn new(config: ConnectionConfig) -> Self;
    pub fn connect_with_protocol(&mut self, transport: Transport) -> Result<(), FlareError>;
    pub async fn connect_with_race(&mut self, addresses: Vec<String>, protocols: Vec<Transport>, handler: Option<Arc<dyn ConnectionEvent>>) -> Result<(), FlareError>;
    pub fn enable_auto_reconnect(&mut self, max_retries: u32, retry_interval_ms: u64);
    pub fn disconnect(&mut self) -> Result<(), FlareError>;
    pub fn send_message(&self, frame: Frame) -> Result<(), FlareError>;
    pub fn set_event_handler(&self, handler: Arc<dyn ConnectionEvent>) -> Result<(), FlareError>;
    pub fn is_connected(&self) -> bool;
    pub fn get_stats(&self) -> Option<ConnectionStats>;
}
```

### 3.2 WebSocketClient
```rust
pub struct WebSocketClient {
    connection: Arc<WebSocketClientConn>,
}

impl WebSocketClient {
    pub fn new(config: ConnectionConfig) -> Result<Self, FlareError>;
    pub fn connect(&self) -> Result<(), FlareError>;
    pub fn disconnect(&self, reason: Option<String>) -> Result<(), FlareError>;
    pub fn send_message(&self, frame: Frame) -> Result<(), FlareError>;
    pub fn set_event_handler(&self, handler: Arc<dyn ConnectionEvent>);
    pub fn state(&self) -> ConnectionState;
    pub fn stats(&self) -> ConnectionStats;
}
```

### 3.3 QuicClient
```rust
pub struct QuicClient {
    connection: Arc<QuicClientConn>,
}

impl QuicClient {
    pub fn new(config: ConnectionConfig) -> Result<Self, FlareError>;
    pub fn connect(&self) -> Result<(), FlareError>;
    pub fn disconnect(&self, reason: Option<String>) -> Result<(), FlareError>;
    pub fn send_message(&self, frame: Frame) -> Result<(), FlareError>;
    pub fn set_event_handler(&self, handler: Arc<dyn ConnectionEvent>);
    pub fn state(&self) -> ConnectionState;
    pub fn stats(&self) -> ConnectionStats;
}
```

## 4. 重构实现步骤

1. 创建独立的WebSocket客户端连接实现
2. 创建独立的QUIC客户端连接实现
3. 更新EnhancedClient以使用新的连接实现
4. 保持向后兼容性
5. 更新相关示例和测试

## 5. 职责分离

### 5.1 通用连接层
- 提供统一的连接接口和基础实现
- 处理连接状态管理、统计信息收集等通用功能

### 5.2 客户端专有层
- 处理客户端特有的功能，如主动连接、重连等
- 提供协议特定的实现细节
- 包装通用连接接口以提供客户端友好的API