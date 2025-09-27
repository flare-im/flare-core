# ServerConfig 增强说明

## 概述

本次增强对 `src/server/config.rs` 中的 `ServerConfig` 结构进行了全面优化，确保从 `ServerConfig` 转换到 `ConnectionConfig` 更加全面，并针对服务端配置进行了专门优化。

## 主要增强内容

### 1. 新增配置结构体

#### ServerPerformanceConfig - 服务端性能优化配置
```rust
pub struct ServerPerformanceConfig {
    pub worker_threads: usize,                    // 工作线程数
    pub enable_cpu_affinity: bool,               // 是否启用CPU亲和性
    pub enable_numa_awareness: bool,             // 是否启用NUMA感知
    pub memory_pool_size: usize,                 // 内存池大小
    pub enable_zero_copy: bool,                  // 是否启用零拷贝优化
    pub batch_size: usize,                       // 批量处理大小
    pub enable_connection_pool: bool,            // 是否启用连接池
    pub connection_pool_size: usize,             // 连接池大小
}
```

#### ServerSecurityConfig - 服务端安全配置
```rust
pub struct ServerSecurityConfig {
    pub enable_rate_limiting: bool,              // 是否启用速率限制
    pub max_connections_per_ip: usize,           // 每IP最大连接数
    pub rate_limit_per_second: u32,              // 请求速率限制
    pub enable_blacklist: bool,                  // 是否启用黑名单
    pub blacklist_file_path: Option<String>,     // 黑名单文件路径
    pub enable_whitelist: bool,                  // 是否启用白名单
    pub whitelist_file_path: Option<String>,     // 白名单文件路径
    pub max_message_size: usize,                 // 最大消息大小
    pub enable_message_encryption: bool,         // 是否启用消息加密
}
```

#### ServerMonitoringConfig - 服务端监控配置
```rust
pub struct ServerMonitoringConfig {
    pub enable_performance_monitoring: bool,     // 是否启用性能监控
    pub enable_connection_monitoring: bool,      // 是否启用连接监控
    pub monitoring_interval_ms: u64,             // 监控数据收集间隔
    pub enable_logging: bool,                    // 是否启用日志记录
    pub log_level: String,                       // 日志级别
    pub log_file_path: Option<String>,           // 日志文件路径
    pub enable_metrics: bool,                    // 是否启用指标收集
    pub metrics_port: Option<u16>,               // 指标导出端口
}
```

### 2. 增强的 ServerConfig 结构

在原有 `ServerConfig` 基础上新增了以下字段：
- `heartbeat_timeout_ms: u64` - 心跳超时时间
- `max_missed_heartbeats: u32` - 最大心跳丢失次数
- `buffer_size: usize` - 缓冲区大小
- `auto_heartbeat_response: bool` - 是否启用自动心跳响应
- `heartbeat_monitor_timeout_ms: u64` - 心跳监控超时
- `cleanup_interval_ms: u64` - 连接清理间隔
- `performance_config: ServerPerformanceConfig` - 性能优化配置
- `security_config: ServerSecurityConfig` - 安全配置
- `monitoring_config: ServerMonitoringConfig` - 监控配置

### 3. 优化的配置转换方法

#### 重构的转换逻辑
- 将原来的 `to_connection_config` 方法重构为更清晰的逻辑
- 新增 `create_websocket_connection_config` 和 `create_quic_connection_config` 私有方法
- 确保所有配置字段都能正确映射到 `ConnectionConfig`

#### 全面的配置映射
- 心跳配置（间隔、超时、最大丢失次数）
- 缓冲区配置（大小、最大消息大小）
- 服务端特有配置（自动心跳响应、监控配置）
- 性能配置（零拷贝、批量处理）
- 安全配置（速率限制、消息大小限制）

### 4. 预设配置方案

#### 高性能配置 - `high_performance_websocket()`
- 针对高并发、高吞吐量场景优化
- 启用CPU亲和性和NUMA感知
- 大缓冲区、零拷贝优化
- 大批量处理

#### 低延迟配置 - `low_latency_websocket()`
- 针对实时通信场景优化
- 小批量处理以降低延迟
- 禁用连接池以减少延迟
- 较短的心跳间隔

#### 稳定连接配置 - `stable_websocket()`
- 针对长时间连接、高可靠性场景优化
- 较长的心跳间隔和超时时间
- 稳定性优于性能
- 更多的容错机制

#### 生产环境配置 - `production_websocket()`
- 综合优化的生产环境配置
- 完整的监控和日志功能
- 安全防护机制
- 高性能和稳定性平衡

### 5. 配置验证功能

新增 `validate()` 方法，验证配置的有效性：
- 心跳配置合理性检查
- 协议配置完整性检查
- TLS配置有效性检查
- 数值范围合理性检查

### 6. 便捷的配置设置方法

新增多个便捷的设置方法：
- `with_heartbeat_config()` - 设置心跳配置
- `with_buffer_size()` - 设置缓冲区大小
- `with_auto_heartbeat_response()` - 设置自动心跳响应
- `with_heartbeat_monitoring()` - 设置心跳监控配置
- `with_performance_config()` - 设置性能配置
- `with_security_config()` - 设置安全配置
- `with_monitoring_config()` - 设置监控配置

## 使用示例

### 基本使用
```rust
let config = ServerConfig::default_websocket()
    .with_heartbeat_config(15000, 5000, 3)
    .with_buffer_size(128 * 1024)
    .with_serialization_format(SerializationFormat::Protobuf);
```

### 预设配置
```rust
// 高性能配置
let high_perf_config = ServerConfig::high_performance_websocket();

// 低延迟配置
let low_latency_config = ServerConfig::low_latency_websocket();

// 生产环境配置
let production_config = ServerConfig::production_websocket();
```

### 配置转换
```rust
let connection_config = server_config.to_connection_config("connection_id".to_string());
```

## 测试验证

创建了完整的测试示例 `examples/server/config_example.rs`，验证了：
- 所有配置类型的创建和验证
- 配置转换功能的正确性
- 预设配置的有效性
- 自定义配置的灵活性

## 总结

通过本次增强，`ServerConfig` 现在提供了：

1. **更全面的配置选项** - 涵盖性能、安全、监控等各个方面
2. **更智能的转换逻辑** - 确保所有配置都能正确映射到 `ConnectionConfig`
3. **更便捷的使用方式** - 提供预设配置和链式设置方法
4. **更强的验证能力** - 自动验证配置的有效性和合理性
5. **更好的扩展性** - 为未来的配置需求预留了扩展空间

这些增强使得服务端配置更加灵活、全面和易用，能够满足不同场景下的性能和安全需求。
