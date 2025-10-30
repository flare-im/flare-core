# Flare-Core 设计原则

## 核心理念

**"通用性优先、配置灵活、稳定至上"**

flare-core 是一个底层网络通信基础库，而非特定业务场景的定制化解决方案。所有模块的设计应遵循以下核心理念：

1. **通用性**：不绑定特定业务场景，提供可配置的参数而非硬编码的业务规则
2. **灵活性**：支持多种策略，可插拔扩展，留足配置空间供上层业务定制
3. **稳定性**：高性能、低开销、行为可预测，便于测试和调试
4. **扩展性**：清晰的接口定义，易于扩展和集成

---

## 一、通用性设计

### 1.1 避免硬编码业务规则

❌ **错误示例：硬编码微信的网络类型映射**

```rust
// 不要这样做！
pub enum NetworkType {
    WiFi,
    FourG,
    ThreeG,
    TwoG,
}

impl NetworkType {
    fn get_heartbeat_interval(&self) -> u64 {
        match self {
            NetworkType::WiFi => 60000,    // 微信的WiFi配置
            NetworkType::FourG => 30000,   // 微信的4G配置
            NetworkType::ThreeG => 20000,  // 微信的3G配置
            NetworkType::TwoG => 15000,    // 微信的2G配置
        }
    }
}
```

✅ **正确示例：提供可配置的参数**

```rust
// 应该这样做
pub struct HeartbeatConfig {
    pub initial_interval_ms: u64,
    pub min_interval_ms: u64,
    pub max_interval_ms: u64,
    pub timeout_threshold: u32,
    pub enable_adaptive: bool,
    // ... 更多可配置参数
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            initial_interval_ms: 30000,  // 提供合理的默认值
            min_interval_ms: 10000,
            max_interval_ms: 60000,
            timeout_threshold: 3,
            enable_adaptive: true,
        }
    }
}
```

**业务层可以根据需求自定义：**

```rust
// 业务层：模拟微信的WiFi配置
let wifi_config = HeartbeatConfig {
    initial_interval_ms: 60000,
    min_interval_ms: 30000,
    max_interval_ms: 120000,
    ..Default::default()
};

// 业务层：模拟微信的4G配置
let mobile_4g_config = HeartbeatConfig {
    initial_interval_ms: 30000,
    min_interval_ms: 15000,
    max_interval_ms: 60000,
    ..Default::default()
};

// 业务层：完全自定义的配置
let custom_config = HeartbeatConfig {
    initial_interval_ms: 45000,
    min_interval_ms: 20000,
    max_interval_ms: 90000,
    timeout_threshold: 5,
    enable_adaptive: false,
};
```

### 1.2 提供配置而非假设

基础库不应该假设上层业务的使用场景，而是提供足够的配置选项：

```rust
// ✅ 好的设计
pub struct ReconnectConfig {
    pub max_retries: u32,              // 最大重试次数
    pub initial_delay_ms: u64,         // 初始延迟
    pub max_delay_ms: u64,             // 最大延迟
    pub backoff_factor: f64,           // 退避系数
    pub jitter_factor: f64,            // 抖动系数
    pub enable_network_probe: bool,    // 是否启用网络探测
}

// ❌ 不好的设计
pub struct ReconnectConfig {
    pub strategy: ReconnectStrategy,   // 只提供几种预设策略，限制太多
}

pub enum ReconnectStrategy {
    Fast,      // 业务层无法控制具体参数
    Normal,    // 黑盒，不透明
    Slow,      // 无法定制
}
```

---

## 二、核心功能聚焦

### 2.1 单一职责原则

每个模块应专注于解决一个核心问题，不要混入过多业务逻辑：

| 模块 | 核心职责 | 不应包含的内容 |
|------|----------|----------------|
| **heartbeat** | 心跳检测、超时处理、RTT测量、自适应调节 | 网络类型判断、业务场景映射 |
| **reconnect** | 重连策略、指数退避、网络探测 | 业务层的重连决策（如用户登录状态） |
| **reliable** | 消息可靠性（Seq+ACK+重传） | 消息内容解析、业务逻辑处理 |
| **ratelimit** | 流量控制、令牌桶/漏桶算法 | 业务规则（如VIP用户特权） |

### 2.2 功能完整性

基础模块应提供完整的功能支持，但不强制使用：

```rust
// ✅ 提供完整的API，但允许按需使用
impl HeartbeatManager {
    // 基础功能
    pub fn get_interval(&self) -> u64 { }
    pub fn set_interval(&self, interval_ms: u64) -> u64 { }
    pub fn on_heartbeat_sent(&self) { }
    pub fn on_heartbeat_success(&self) { }
    pub fn on_heartbeat_timeout(&self) -> bool { }
    
    // 高级功能（可选）
    pub fn record_rtt(&self, rtt_ms: u32) { }
    pub fn get_avg_rtt(&self) -> Option<u32> { }
    pub fn get_rtt_jitter(&self) -> Option<f64> { }
    pub fn get_success_rate(&self) -> f64 { }
    
    // 工具方法
    pub fn reset_statistics(&self) { }
    pub fn config(&self) -> &HeartbeatConfig { }
}
```

---

## 三、配置灵活性

### 3.1 分层配置策略

提供三层配置机制：

1. **默认配置**：开箱即用，适用于大多数场景
2. **部分自定义**：使用 `..Default::default()` 语法覆盖特定参数
3. **完全自定义**：所有参数均可配置

```rust
// 1. 默认配置
let config = HeartbeatConfig::default();

// 2. 部分自定义
let config = HeartbeatConfig {
    initial_interval_ms: 20000,
    timeout_threshold: 5,
    ..Default::default()
};

// 3. 完全自定义
let config = HeartbeatConfig {
    initial_interval_ms: 45000,
    min_interval_ms: 20000,
    max_interval_ms: 90000,
    timeout_threshold: 5,
    enable_adaptive: false,
    rtt_window_size: 50,
    high_rtt_threshold_ms: 600,
    low_rtt_threshold_ms: 80,
    high_jitter_threshold_ms: 120.0,
    low_jitter_threshold_ms: 15.0,
    adaptive_decrease_factor: 0.75,
    adaptive_increase_factor: 1.25,
};
```

### 3.2 运行时动态调整

允许在运行时动态调整配置：

```rust
let manager = HeartbeatManager::new(config);

// 运行时调整
manager.set_interval(40000);

// 获取当前状态
let current_interval = manager.get_interval();
let success_rate = manager.get_success_rate();
```

---

## 四、性能与资源控制

### 4.1 使用高效的并发原语

```rust
// ✅ 使用原子操作和无锁结构
pub struct HeartbeatManager {
    config: HeartbeatConfig,
    current_interval_ms: AtomicU64,              // 原子操作，无锁
    rtt_history: Arc<RwLock<VecDeque<u32>>>,    // 读多写少用RwLock
    consecutive_timeouts: AtomicU64,
    total_heartbeats_sent: AtomicU64,
    total_heartbeats_success: AtomicU64,
}

// ❌ 避免过度使用互斥锁
pub struct HeartbeatManager {
    state: Arc<Mutex<HeartbeatState>>,  // 所有字段都锁在一起，效率低
}
```

### 4.2 控制内存占用

```rust
// ✅ 限制缓冲区大小
pub struct HeartbeatConfig {
    pub rtt_window_size: usize,  // 限制RTT历史记录队列长度
}

impl HeartbeatManager {
    pub fn record_rtt(&self, rtt_ms: u32) {
        if let Ok(mut history) = self.rtt_history.write() {
            if history.len() >= self.config.rtt_window_size {
                history.pop_front();  // 超出窗口大小时移除最旧的记录
            }
            history.push_back(rtt_ms);
        }
    }
}
```

### 4.3 性能指标

基础库应追求：

- **低延迟**：核心操作（如 `get_interval()`）应在 < 1μs 内完成
- **低开销**：原子操作避免锁竞争，减少系统调用
- **可扩展**：支持高并发场景（千万级连接）

---

## 五、测试完备性

### 5.1 单元测试覆盖

每个模块都应该有完整的单元测试：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() { /* 测试默认配置 */ }
    
    #[test]
    fn test_custom_config() { /* 测试自定义配置 */ }
    
    #[test]
    fn test_interval_clamping() { /* 测试边界条件 */ }
    
    #[test]
    fn test_timeout_threshold() { /* 测试超时逻辑 */ }
    
    #[test]
    fn test_rtt_recording() { /* 测试RTT统计 */ }
    
    #[test]
    fn test_adaptive_adjustment() { /* 测试自适应调节 */ }
    
    #[test]
    fn test_success_rate() { /* 测试成功率计算 */ }
    
    #[test]
    fn test_reset_statistics() { /* 测试重置功能 */ }
}
```

### 5.2 测试覆盖场景

- ✅ 正常场景：默认配置下的行为
- ✅ 边界条件：最小/最大值、空值处理
- ✅ 异常场景：超时、网络抖动、连续失败
- ✅ 并发场景：多线程访问的安全性
- ✅ 性能测试：基准测试、压力测试

---

## 六、文档与示例

### 6.1 模块级文档

每个模块都应包含清晰的文档：

```rust
//! 通用心跳管理模块
//! 
//! 提供灵活、可配置的心跳检测与自适应调节能力，适用于各种长连接场景。
//! 
//! # 核心功能
//! 
//! - **心跳间隔管理**：支持固定间隔或自适应调节
//! - **超时检测**：连续超时次数统计与阈值触发
//! - **RTT测量**：往返时延统计与抖动分析
//! 
//! # 设计原则
//! 
//! 1. **通用性**：不绑定特定业务场景，所有参数可配置
//! 2. **灵活性**：支持多种调节策略，可插拔扩展
//! 3. **高性能**：原子操作，无锁设计，低开销
//! 
//! # 使用示例
//! 
//! ```rust
//! use flare_core::common::connections::heartbeat::{HeartbeatConfig, HeartbeatManager};
//! 
//! let config = HeartbeatConfig::default();
//! let manager = HeartbeatManager::new(config);
//! 
//! let interval = manager.get_interval();
//! manager.on_heartbeat_success();
//! ```
```

### 6.2 API 文档

每个公共 API 都应有清晰的文档：

```rust
/// 获取当前心跳间隔（毫秒）
/// 
/// # 返回
/// 
/// 当前心跳间隔值（毫秒）
/// 
/// # 示例
/// 
/// ```
/// let manager = HeartbeatManager::new(config);
/// let interval = manager.get_interval();
/// assert!(interval >= config.min_interval_ms);
/// assert!(interval <= config.max_interval_ms);
/// ```
#[inline]
pub fn get_interval(&self) -> u64 {
    self.current_interval_ms.load(Ordering::Relaxed)
}
```

---

## 七、实践检查清单

在编写新模块或重构现有模块时，请使用以下检查清单：

### 设计阶段

- [ ] 模块职责是否单一明确？
- [ ] 是否避免了硬编码的业务规则？
- [ ] 是否提供了足够的配置选项？
- [ ] 配置是否有合理的默认值？
- [ ] API 是否清晰易懂？

### 实现阶段

- [ ] 是否使用了高效的并发原语（AtomicXxx、RwLock）？
- [ ] 是否控制了内存占用（限制缓冲区大小）？
- [ ] 是否避免了不必要的锁竞争？
- [ ] 错误处理是否完善？

### 测试阶段

- [ ] 是否覆盖了正常场景？
- [ ] 是否覆盖了边界条件？
- [ ] 是否覆盖了异常场景？
- [ ] 是否有并发安全性测试？
- [ ] 测试是否可重复执行？

### 文档阶段

- [ ] 模块级文档是否完整？
- [ ] API 文档是否清晰？
- [ ] 是否提供了使用示例？
- [ ] 是否说明了设计原则？

---

## 八、具体模块示例

### 8.1 heartbeat 模块（已重构）

✅ **符合设计原则的实现：**

- ✅ 通用性：移除了 `NetworkType` 枚举和硬编码的网络映射
- ✅ 灵活性：提供 `HeartbeatConfig` 结构体，12 个可配置参数
- ✅ 高性能：使用 `AtomicU64` 和 `RwLock`，低开销
- ✅ 完整测试：8 个单元测试，覆盖各种场景

### 8.2 reconnect 模块（已实现）

✅ **符合设计原则的实现：**

- ✅ 通用性：指数退避算法完全可配置
- ✅ 灵活性：支持自定义退避系数、抖动系数、最大重试次数
- ✅ 智能化：错误分类、网络探测、历史记录
- ✅ 资源控制：限制历史记录数量

### 8.3 reliable 模块（已实现）

✅ **符合设计原则的实现：**

- ✅ 通用性：Seq+ACK+重传机制，不绑定特定协议
- ✅ 灵活性：可配置超时时间、最大重试次数
- ✅ 功能完整：去重、重排序、超时管理
- ✅ 资源控制：限制缓冲区大小

---

## 九、未来优化方向

根据设计原则，以下是需要持续优化的方向：

### 9.1 连接池管理（待实现）

```rust
pub struct ConnectionPoolConfig {
    pub max_connections: usize,
    pub min_idle_connections: usize,
    pub max_idle_time_ms: u64,
    pub connection_timeout_ms: u64,
    pub enable_health_check: bool,
}
```

### 9.2 监控指标（待增强）

```rust
pub struct MetricsConfig {
    pub enable_prometheus: bool,
    pub export_interval_ms: u64,
    pub histogram_buckets: Vec<f64>,
}
```

### 9.3 流量控制（已实现 ratelimit）

```rust
pub struct RateLimitConfig {
    pub algorithm: RateLimitAlgorithm,  // TokenBucket/LeakyBucket
    pub capacity: u64,
    pub refill_rate: u64,
    pub enable_burst: bool,
}
```

---

## 十、总结

**flare-core 的设计哲学：**

> 作为底层基础库，我们的目标是提供**稳定、高效、健壮的通用工具**，而非针对特定业务场景的定制化实现。
> 
> 我们的职责是：
> - 提供可靠的基础能力
> - 提供足够的配置选项
> - 保持高性能和低开销
> - 保持清晰的接口定义
> 
> 我们不应该：
> - 假设上层业务的使用场景
> - 硬编码特定业务规则
> - 限制业务层的灵活性
> - 引入不必要的复杂性

**设计原则三句话：**

1. **稳定优先**：可靠性和性能是第一位的
2. **配置灵活**：留足配置空间给业务层定制
3. **扩展友好**：清晰的接口，易于扩展和集成

---

*最后更新：2025-10-15*
