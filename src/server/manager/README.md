# 连接管理器模块

## 概述

连接管理器模块提供了多种连接管理策略，用于管理服务端的所有客户端连接。该模块设计灵活，支持不同的管理需求。

## 模块结构

```
manager/
├── mod.rs              # 模块入口和重新导出
├── traits.rs           # 连接管理器接口定义
├── connection_based.rs # 基于连接的管理器实现
└── user_based.rs       # 基于用户的管理器实现
```

## 核心接口

### ConnectionManager trait

[ConnectionManager](file:///Users/hg/workspace/rust/flare-core/src/server/manager/traits.rs#L16-L42) 是连接管理器的核心接口，定义了所有连接管理器必须实现的方法：

```rust
#[async_trait::async_trait]
pub trait ConnectionManager: Send + Sync {
    /// 添加连接
    async fn add_connection(&self, connection: Arc<dyn ServerConnection>) -> Result<()>;
    
    /// 移除连接
    async fn remove_connection(&self, connection_id: &str) -> Result<()>;
    
    /// 获取连接
    async fn get_connection(&self, connection_id: &str) -> Option<Arc<dyn ServerConnection>>;
    
    /// 获取所有连接
    async fn get_all_connections(&self) -> Vec<Arc<dyn ServerConnection>>;
    
    /// 获取连接数量
    async fn get_connection_count(&self) -> usize;
    
    /// 向指定连接发送消息
    async fn send_message_to_connection(&self, connection_id: &str, message: Frame) -> Result<()>;
    
    /// 广播消息到所有连接
    async fn broadcast_message(&self, message: Frame) -> Result<usize>;
    
    /// 清理不活跃的连接
    async fn cleanup_inactive_connections(&self, timeout: Duration) -> usize;
    
    /// 获取统计信息
    async fn get_stats(&self) -> ManagerStats;
    
    /// 清空所有连接
    async fn clear_all(&self);
    
    /// 检查是否需要清理
    async fn should_cleanup(&self) -> bool;
}
```

## 实现类

### 1. ConnectionBasedManager (基于连接的管理器)

[ConnectionBasedManager](file:///Users/hg/workspace/rust/flare-core/src/server/manager/connection_based.rs#L47-L56) 按连接ID独立管理每个连接，适用于简单的连接管理需求。

#### 特点：
- 每个连接独立管理
- 简单直接的管理方式
- 适用于连接数量不多且不需要按用户维度管理的场景

#### 使用示例：

```rust
use flare_core::server::ConnectionBasedManager;

let manager = ConnectionBasedManager::new();
```

### 2. UserBasedManager (基于用户的管理器)

[UserBasedManager](file:///Users/hg/workspace/rust/flare-core/src/server/manager/user_based.rs#L65-L77) 按用户ID管理连接，支持一个用户多个连接，适用于需要按用户维度管理连接的场景。

#### 特点：
- 按用户ID管理连接
- 支持一个用户多个连接
- 提供用户级别的操作接口
- 适用于需要按用户维度进行消息推送的场景

#### 使用示例：

```rust
use flare_core::server::UserBasedManager;

let manager = UserBasedManager::new();
```

## 与 common 连接的集成

连接管理器使用 common 模块中的连接抽象：

- [ServerConnection](file:///Users/hg/workspace/rust/flare-core/src/common/connections/traits.rs#L121-L142): 服务端连接接口
- [ConnectionStats](file:///Users/hg/workspace/rust/flare-core/src/common/connections/traits.rs#L145-L159): 连接统计信息
- [Frame](file:///Users/hg/workspace/rust/flare-core/src/common/protocol/frame.rs#L21-L21): 消息帧

这些组件提供了统一的连接管理和消息处理接口，确保了服务端与客户端的兼容性。

## 统计信息

所有连接管理器都提供统计信息：

```rust
/// 管理器统计信息
#[derive(Debug, Clone)]
pub struct ManagerStats {
    /// 总连接数
    pub total_connections: usize,
    /// 活跃连接数
    pub active_connections: usize,
    /// 总消息数
    pub total_messages: u64,
    /// 平均连接质量
    pub average_quality: u8,
    /// 服务器启动时间
    pub uptime: Duration,
}
```

## 使用建议

1. **简单场景**: 使用 [ConnectionBasedManager](file:///Users/hg/workspace/rust/flare-core/src/server/manager/connection_based.rs#L47-L56)
2. **用户维度管理**: 使用 [UserBasedManager](file:///Users/hg/workspace/rust/flare-core/src/server/manager/user_based.rs#L65-L77)
3. **自定义需求**: 实现 [ConnectionManager](file:///Users/hg/workspace/rust/flare-core/src/server/manager/traits.rs#L16-L42) trait