# flare-core 架构重构方案

**版本**: v1.0  
**日期**: 2025-10-15  
**目标**: 清晰的模块职责划分，高内聚低耦合，支持未来协议扩展

---

## 📋 重构目标

### 核心问题
1. **职责混乱**: client/server 特有逻辑混在 common 模块中
2. **依赖混乱**: 模块间依赖关系不清晰
3. **扩展性差**: 添加新协议（HTTP/3、gRPC）困难
4. **代码重复**: 客户端和服务端有大量相似代码

### 重构原则
- ✅ **单一职责**: 每个模块只负责一个明确的功能领域
- ✅ **依赖倒置**: client/server 依赖 common 的抽象，而非具体实现
- ✅ **开闭原则**: 对扩展开放，对修改关闭
- ✅ **接口隔离**: 提供最小必要接口

---

## 🏗️ 模块职责划分

### 1. `common` 模块 - 通用抽象与工具

**职责**: 定义通用接口、协议处理、工具类

```
src/common/
├── connections/           # 连接抽象
│   ├── traits.rs         # ✅ ClientConnection, ServerConnection traits
│   ├── config.rs         # ✅ ConnectionConfig 配置
│   ├── factory.rs        # ✅ ConnectionFactory 工厂
│   ├── enums.rs          # ✅ Transport, ConnectionState 枚举
│   ├── types.rs          # ✅ ConnectionStats（旧版，已废弃）
│   ├── stats.rs          # ✅ AtomicStats（高性能统计）
│   ├── ratelimit.rs      # ✅ 流量控制（TokenBucket）
│   └── monitor.rs        # ✅ 监控工具（质量评分）
│
├── protocol/             # 协议处理
│   ├── frame.rs          # ✅ Frame 定义
│   ├── commands.rs       # ✅ Command 枚举
│   ├── reliability.rs    # ✅ Reliability 可靠性
│   └── factory.rs        # ✅ FrameFactory
│
├── serialization/        # 序列化
│   ├── json.rs
│   ├── protobuf.rs
│   └── msgpack.rs
│
└── error.rs             # ✅ FlareError 统一错误
```

**不应包含**:
- ❌ 具体的连接实现（WebSocketClientConn, QuicClientConn）
- ❌ 客户端/服务端特有逻辑（connect, listen）
- ❌ 传输层细节（tokio-tungstenite, quinn）

---

### 2. `client` 模块 - 客户端连接实现

**职责**: 实现客户端连接、重连、协议竞速

```
src/client/
├── connections/          # 🆕 客户端连接实现
│   ├── mod.rs
│   ├── websocket.rs     # 🆕 WebSocketClient（从 common 迁移）
│   ├── quic.rs          # 🆕 QuicClient（从 common 迁移）
│   └── base.rs          # 🆕 BaseClient（公共客户端逻辑）
│
├── protocol_racer.rs    # ✅ 协议竞速
├── reconnect.rs         # ✅ 重连逻辑
├── auth.rs              # ✅ 认证
├── config.rs            # 🆕 客户端配置
└── mod.rs
```

**职责**:
- ✅ 实现 `ClientConnection` trait
- ✅ 管理客户端生命周期（connect, disconnect, reconnect）
- ✅ 心跳管理（客户端发送 Ping）
- ✅ 协议竞速（同时尝试多个协议）
- ✅ 自动重连逻辑

---

### 3. `server` 模块 - 服务端监听与管理

**职责**: 实现服务端监听、连接管理、负载均衡

```
src/server/
├── connections/          # 🆕 服务端连接实现
│   ├── mod.rs
│   ├── websocket.rs     # 🆕 WebSocketServerConn（从 common 迁移）
│   ├── quic.rs          # 🆕 QuicServerConn（从 common 迁移）
│   └── base.rs          # 🆕 BaseServerConn（公共服务端逻辑）
│
├── listener/            # 🆕 监听器
│   ├── mod.rs
│   ├── websocket.rs     # ✅ WebSocket 监听器
│   └── quic.rs          # ✅ QUIC 监听器
│
├── manager/             # ✅ 连接管理
│   ├── connection_manager.rs
│   └── load_balancer.rs
│
├── adapter/             # ✅ 事件适配器
│   └── server_event_adapter.rs
│
├── config.rs            # ✅ 服务端配置
├── quic.rs              # ✅ QuicServer（监听逻辑）
├── websocket.rs         # ✅ WebSocketServer（监听逻辑）
└── mod.rs
```

**职责**:
- ✅ 实现 `ServerConnection` trait
- ✅ 管理服务端生命周期（listen, accept, close）
- ✅ 连接池管理（ConnectionManager）
- ✅ 负载均衡与分片
- ✅ 心跳管理（服务端响应 Pong）

---

## 🔄 重构路径

### Phase 1: 创建新结构（不破坏现有代码）✅

1. **创建客户端连接目录**
```bash
mkdir -p src/client/connections
```

2. **创建服务端连接目录**
```bash
mkdir -p src/server/connections
mkdir -p src/server/listener
```

3. **编写重构计划文档**（本文档）✅

---

### Phase 2: 迁移客户端逻辑 🔄

#### 2.1 提取 WebSocketClient

**从**: `src/common/connections/websocket.rs` 的 `WebSocketClientConn`  
**到**: `src/client/connections/websocket.rs` 的 `WebSocketClient`

**改动**:
```rust
// 旧代码（common）
pub struct WebSocketClientConn { ... }
impl ClientConnection for WebSocketClientConn { ... }

// 新代码（client）
pub struct WebSocketClient { ... }
impl ClientConnection for WebSocketClient { ... }
```

**保留在 common**:
- ❌ 不保留具体实现

**迁移到 client**:
- ✅ 完整的客户端连接逻辑
- ✅ connect() 实现
- ✅ 心跳任务
- ✅ 读写分离

#### 2.2 提取 QuicClient

**从**: `src/common/connections/quic.rs` 的 `QuicClientConn`  
**到**: `src/client/connections/quic.rs` 的 `QuicClient`

**改动**: 同 WebSocketClient

---

### Phase 3: 迁移服务端逻辑 🔄

#### 3.1 保持 WebSocketServerConn

**位置**: `src/server/connections/websocket.rs`（新建）

**从**: `src/common/connections/websocket.rs` 的 `WebSocketServerConn`  
**到**: `src/server/connections/websocket.rs` 的 `WebSocketServerConn`

**改动**:
```rust
// server/connections/websocket.rs
pub struct WebSocketServerConn { ... }
impl ServerConnection for WebSocketServerConn { ... }
```

#### 3.2 保持 QuicServerConn

**位置**: `src/server/connections/quic.rs`（新建）

**改动**: 同 WebSocketServerConn

---

### Phase 4: 清理 common 模块 🔄

#### 4.1 删除具体实现

**删除文件**:
- ❌ `src/common/connections/websocket.rs`（已迁移）
- ❌ `src/common/connections/quic.rs`（已迁移）

**保留文件**:
- ✅ `src/common/connections/traits.rs`
- ✅ `src/common/connections/config.rs`
- ✅ `src/common/connections/factory.rs`（需修改）
- ✅ `src/common/connections/stats.rs`
- ✅ `src/common/connections/ratelimit.rs`
- ✅ `src/common/connections/monitor.rs`

#### 4.2 更新 ConnectionFactory

**修改**: `src/common/connections/factory.rs`

**原理**: 工厂方法委托给 client/server 模块

```rust
// 旧代码
impl ConnectionFactory {
    pub fn create_client(config: ConnectionConfig) -> Result<Arc<dyn ClientConnection>> {
        match config.transport {
            Transport::WebSocket => Ok(Arc::new(WebSocketClientConn::from_config(config))),
            // ...
        }
    }
}

// 新代码（使用 feature gate 或 依赖注入）
impl ConnectionFactory {
    pub fn create_client(config: ConnectionConfig) -> Result<Arc<dyn ClientConnection>> {
        // 委托给 client 模块
        crate::client::connections::create_client(config)
    }
}
```

**或者**: 将 Factory 移到 lib.rs 层面，作为门面模式

---

### Phase 5: 优化依赖关系 🔄

#### 5.1 依赖图

```
┌─────────┐
│  lib.rs │ (门面层)
└────┬────┘
     │
     ├──────────┬──────────┬────────────┐
     │          │          │            │
┌────▼────┐ ┌──▼───┐ ┌───▼────┐ ┌─────▼─────┐
│ common  │ │client│ │ server │ │ examples  │
└────┬────┘ └──┬───┘ └───┬────┘ └───────────┘
     │         │          │
     │    ┌────▼──────────▼────┐
     │    │   depends on       │
     └────►     common          │
          └────────────────────┘
```

**规则**:
- ✅ client → common（只依赖 traits、config、stats 等）
- ✅ server → common（同上）
- ❌ common → client（禁止）
- ❌ common → server（禁止）
- ❌ client ↔ server（禁止）

#### 5.2 可见性控制

```rust
// common/connections/traits.rs
pub trait ClientConnection { ... }  // 公开接口

// client/connections/websocket.rs
pub struct WebSocketClient { ... }  // 公开实现
impl ClientConnection for WebSocketClient { ... }

// client/connections/base.rs
pub(crate) struct BaseClient { ... }  // 模块内部
```

---

### Phase 6: 统一规范 🔄

#### 6.1 命名规范

| 类型 | 命名规则 | 示例 |
|------|---------|------|
| 客户端连接 | `{Protocol}Client` | `WebSocketClient`, `QuicClient` |
| 服务端连接 | `{Protocol}ServerConn` | `WebSocketServerConn`, `QuicServerConn` |
| 监听器 | `{Protocol}Listener` | `WebSocketListener`, `QuicListener` |
| 配置 | `{Module}Config` | `ClientConfig`, `ServerConfig` |
| 工厂 | `{Type}Factory` | `ConnectionFactory`, `FrameFactory` |

#### 6.2 错误处理

**统一使用 FlareError**:
```rust
pub enum FlareError {
    ConnectionFailed(String),
    NetworkError(String),        // 🆕 网络错误
    TimeoutError(String),         // 🆕 超时错误
    AuthenticationFailed(String),
    RateLimitExceeded(String),    // 🆕 限流错误
    SerializationError(String),
    MessageSendFailed(String),
    HeartbeatTimeout(u32),
    Other(String),
}
```

#### 6.3 注释规范

```rust
/// WebSocket 客户端连接
///
/// # 职责
/// - 建立 WebSocket 连接
/// - 管理心跳机制
/// - 处理消息收发
///
/// # 示例
/// ```rust
/// let config = ConnectionConfig::default();
/// let client = WebSocketClient::new(config);
/// client.connect()?;
/// ```
pub struct WebSocketClient { ... }
```

---

## 📊 重构前后对比

### 重构前

```
src/common/connections/
├── websocket.rs         # 2000+ 行，客户端+服务端混在一起
├── quic.rs              # 1500+ 行，客户端+服务端混在一起
├── factory.rs           # 直接依赖具体实现
└── ...

问题:
❌ 职责不清
❌ 代码重复
❌ 难以测试
❌ 难以扩展
```

### 重构后

```
src/
├── common/connections/  # 纯粹的抽象
│   ├── traits.rs       # 接口定义
│   ├── config.rs       # 配置
│   ├── stats.rs        # 统计
│   └── ratelimit.rs    # 限流
│
├── client/connections/  # 客户端实现
│   ├── websocket.rs    # 1000行，专注客户端
│   └── quic.rs         # 800行，专注客户端
│
└── server/connections/  # 服务端实现
    ├── websocket.rs    # 1000行，专注服务端
    └── quic.rs         # 700行，专注服务端

优势:
✅ 职责清晰
✅ 代码复用
✅ 易于测试
✅ 易于扩展
```

---

## 🚀 实施计划

### Week 1: 准备阶段 ✅
- [x] 编写重构方案文档
- [x] 创建新目录结构
- [ ] 备份当前代码

### Week 2: 客户端迁移
- [ ] 提取 WebSocketClient
- [ ] 提取 QuicClient
- [ ] 更新 client/mod.rs
- [ ] 测试客户端功能

### Week 3: 服务端迁移
- [ ] 迁移 WebSocketServerConn
- [ ] 迁移 QuicServerConn
- [ ] 更新 server/mod.rs
- [ ] 测试服务端功能

### Week 4: 清理与优化
- [ ] 删除 common 中的具体实现
- [ ] 更新 ConnectionFactory
- [ ] 统一错误处理
- [ ] 添加文档注释

### Week 5: 测试与验证
- [ ] 端到端测试
- [ ] 性能基准测试
- [ ] 文档更新
- [ ] Code Review

---

## 🎯 验收标准

### 功能验收
- [ ] 所有现有功能保持不变
- [ ] 示例程序正常运行
- [ ] 单元测试全部通过

### 架构验收
- [ ] common 不包含具体实现
- [ ] client/server 独立且不互相依赖
- [ ] 依赖关系清晰（client/server → common）

### 代码质量
- [ ] 无编译警告
- [ ] 代码覆盖率 > 80%
- [ ] 文档完整

### 性能验收
- [ ] 性能无退化
- [ ] 内存占用无显著增加

---

## 📚 参考资料

- [Rust API 设计指南](https://rust-lang.github.io/api-guidelines/)
- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html)
- [Tokio 最佳实践](https://tokio.rs/tokio/topics/best-practices)

---

**文档版本**: v1.0  
**最后更新**: 2025-10-15  
**状态**: 📝 规划完成，待实施
