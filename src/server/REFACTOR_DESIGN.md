# 服务端模块重构设计

## 1. 设计目标

基于通用连接抽象层包装服务端专有特性，实现职责分离和可扩展性。

## 2. 模块结构

### 2.1 核心模块
- `server.rs`: 聚合型服务端，支持多协议服务
- `config.rs`: 服务端配置管理
- `traits.rs`: 服务端接口定义

### 2.2 服务模块
- `servers/websocket.rs`: WebSocket服务端实现
- `servers/quic.rs`: QUIC服务端实现

### 2.3 管理模块
- `manager/`: 连接管理器，处理连接的添加、移除和统计
- `events/`: 事件处理机制，处理连接事件和消息分发

## 3. 服务端接口设计

### 3.1 AggregationServer
```rust
pub struct AggregationServer {
    config: ServerConfig,
    is_running: Arc<AtomicBool>,
    protocol_services: Vec<Arc<dyn ProtocolService>>,
    connection_manager: Arc<dyn ConnectionManager>,
    heartbeat_task: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    event_handler: Arc<tokio::sync::RwLock<Option<Arc<dyn EnhancedEventHandler>>>>,
}

impl AggregationServer {
    pub fn new(config: ServerConfig) -> Self;
    pub fn new_with_connection_manager(config: ServerConfig, connection_manager: Arc<dyn ConnectionManager>) -> Self;
    pub fn add_protocol_service(&mut self, service: Arc<dyn ProtocolService>);
    pub async fn start(&self) -> Result<(), FlareError>;
    pub async fn stop(&self) -> Result<(), FlareError>;
    pub fn config(&self) -> &ServerConfig;
    pub fn is_running(&self) -> bool;
    pub fn connection_manager(&self) -> &Arc<dyn ConnectionManager>;
    pub fn protocol_services(&self) -> &[Arc<dyn ProtocolService>];
    pub async fn set_event_handler(&self, handler: Arc<dyn EnhancedEventHandler>);
    pub async fn remove_event_handler(&self);
    pub async fn get_event_handler_adapter(&self) -> EventHandlerAdapter;
}
```

### 3.2 ProtocolService
```rust
#[async_trait::async_trait]
pub trait ProtocolService: Send + Sync {
    async fn start(&self, connection_manager: Arc<dyn ConnectionManager>) -> Result<(), FlareError>;
    async fn stop(&self) -> Result<(), FlareError>;
    fn name(&self) -> &str;
}
```

### 3.3 WebSocketServer
```rust
pub struct WebSocketServer {
    cfg: ServerConfig,
    connection_manager: Option<Arc<dyn ConnectionManager>>,
}

impl WebSocketServer {
    pub fn new(cfg: ServerConfig, connection_manager: Arc<dyn ConnectionManager>) -> Self;
    pub fn connection_manager(&self) -> Option<&Arc<dyn ConnectionManager>>;
}
```

### 3.4 QuicServer
```rust
pub struct QuicServer {
    cfg: ServerConfig,
    connection_manager: Option<Arc<dyn ConnectionManager>>,
}

impl QuicServer {
    pub fn new(cfg: ServerConfig, connection_manager: Arc<dyn ConnectionManager>) -> Self;
    pub fn connection_manager(&self) -> Option<&Arc<dyn ConnectionManager>>;
}
```

## 4. 重构实现步骤

1. 更新WebSocket服务端实现，基于通用连接抽象层
2. 更新QUIC服务端实现，基于通用连接抽象层
3. 保持向后兼容性
4. 更新相关示例和测试

## 5. 职责分离

### 5.1 通用连接层
- 提供统一的连接接口和基础实现
- 处理连接状态管理、统计信息收集等通用功能

### 5.2 服务端专有层
- 处理服务端特有的功能，如监听连接、接受连接等
- 提供协议特定的实现细节
- 包装通用连接接口以提供服务端友好的API

### 5.3 事件处理层
- 处理连接事件和消息分发
- 提供增强型事件处理器支持
- 实现事件适配器模式