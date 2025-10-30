# 统一连接抽象层设计文档

## 1. 设计目标

创建一个标准化的、可复用的长连接抽象层，屏蔽底层协议（QUIC、WebSocket等）差异，提供一致的API和功能。

## 2. 核心原则

### 2.1 统一连接抽象
- 提供统一的连接接口，屏蔽底层协议差异
- 连接抽象不依赖于特定的传输协议，可以适配不同的传输协议

### 2.2 长连接标准
定义长连接的标准化接口和行为规范，包括：
- 连接状态管理（状态转换、查询）
- 心跳机制（Ping/Pong、超时检测）
- 统计信息收集（消息计数、字节流量、质量指标）
- 消息处理流程（可靠传输、流量控制）
- 错误处理机制（异常通知、重连策略）

### 2.3 职责分离
- common模块：提供标准化的通用连接抽象层，包含核心接口和基础实现
- client/server模块：基于通用连接抽象层，包装成适配客户端和服务端专有特性的实现
- 专有特性应通过适配器或装饰器模式扩展通用连接功能

## 3. 架构设计

### 3.1 核心接口

#### 3.1.1 ConnectionEvent (事件回调接口)
```rust
pub trait ConnectionEvent: Send + Sync + 'static {
    /// 连接建立成功时触发
    fn on_connected(&self) {}
    
    /// 连接断开时触发
    fn on_disconnected(&self, reason: Option<String>) {}
    
    /// 发生错误时触发
    fn on_error(&self, err: FlareError) {}
    
    /// 接收到消息时触发
    fn on_message_received(&self, frame: Frame) {}
    
    /// 消息发送成功时触发
    fn on_message_sent(&self, frame: Frame) {}
    
    /// 发送心跳 Ping 时触发
    fn on_heartbeat_ping(&self) {}
    
    /// 接收到心跳 Pong 时触发
    fn on_heartbeat_pong(&self, rtt_ms: u32) {}
    
    /// 心跳超时时触发
    fn on_heartbeat_timeout(&self) {}
    
    /// 连接质量变化时触发
    fn on_quality_changed(&self, quality: u8) {}
    
    /// 统计信息更新时触发
    fn on_statistics_updated(&self, stats: ConnectionStats) {}
}
```

#### 3.1.2 BaseConnection (基础连接接口)
```rust
pub trait BaseConnection: Send + Sync {
    /// 发送消息
    fn send_message(&self, frame: Frame) -> Result<(), FlareError>;
    
    /// 设置事件处理器
    fn set_event_handler(&self, handler: Arc<dyn ConnectionEvent>);
    
    /// 获取当前连接状态
    fn state(&self) -> ConnectionState;
    
    /// 标记连接为就绪状态
    fn ready(&self) -> Result<(), FlareError>;
    
    /// 标记连接为已建立状态
    fn connected(&self) -> Result<(), FlareError>;
    
    /// 设置连接状态为指定状态
    fn set_state(&self, state: ConnectionState) -> Result<(), FlareError>;
    
    /// 获取统计信息
    fn stats(&self) -> ConnectionStats;
    
    /// 获取最后活动时间
    fn last_activity_epoch_ms(&self) -> u64;
    
    /// 获取连接ID
    fn id(&self) -> String;
}
```

### 3.2 核心实现

#### 3.2.1 EnhancedConnection (增强型通用连接)
提供跨协议的增强型通用连接功能，整合了WebSocket和QUIC连接的共性功能：
- 连接状态管理
- 统计信息收集
- 消息收发处理
- 心跳检测机制
- 事件处理机制

#### 3.2.2 ConnectionFactory (连接工厂)
用于创建不同协议的连接实例：
- 支持WebSocket和QUIC协议
- 提供统一的创建接口

## 4. 长连接标准规范

### 4.1 连接状态管理
定义标准化的连接状态枚举：
```rust
pub enum ConnectionState {
    Initializing,  // 初始化中
    Ready,         // 就绪
    Connected,     // 已连接
    Disconnected,  // 已断开
    Error,         // 错误
}
```

### 4.2 心跳机制
- 定期发送Ping消息
- 检测Pong响应
- 超时检测和处理
- 连接质量评估

### 4.3 统计信息收集
收集以下统计信息：
- 消息计数（发送/接收）
- 字节流量（发送/接收）
- 心跳统计（Ping/Pong计数）
- 连接质量指标
- 平均往返时间(RTT)

### 4.4 消息处理流程
- 消息编码/解码
- 可靠传输保证
- 流量控制
- 消息优先级处理

### 4.5 错误处理机制
- 异常通知
- 重连策略
- 错误日志记录
- 连接恢复机制

## 5. 职责分离架构

### 5.1 common模块
提供标准化的通用连接抽象层：
- 核心接口定义
- 基础实现
- 公共工具类

### 5.2 client/server模块
基于通用连接抽象层，包装成适配客户端和服务端专有特性的实现：
- 客户端：主动连接、重连机制
- 服务端：被动接受连接、连接管理

## 6. 非功能性要求

### 6.1 专注核心功能
- 专注于连接建立后的通用功能实现
- 不涉及连接创建、初始化和管理逻辑

### 6.2 架构要求
- 确保架构简洁、职责清晰
- 保证稳定可靠、支持扩展
- 提供统一的事件处理机制
- 支持连接生命周期管理