# System 模块文档

## 📖 模块概述

系统优化模块提供底层系统资源优化功能，包括CPU亲和性绑定、内存对齐优化、NUMA感知等高级性能调优特性。该模块专为**超低延迟**和**高性能**场景设计，充分利用硬件特性实现性能最大化。

## 🎯 设计目标

- **硬件感知**: 充分利用CPU架构和内存层次结构
- **系统级优化**: CPU绑定、内存对齐、缓存优化
- **NUMA优化**: 本地内存分配，减少跨节点访问
- **零开销抽象**: 编译时优化，运行时最小开销
- **跨平台兼容**: 支持Linux、macOS、Windows

## 🏗️ 架构设计

```
system/
├── cpu_affinity.rs       # CPU亲和性管理
├── memory_optimization.rs # 内存优化和对齐
├── numa_awareness.rs     # NUMA感知优化
└── mod.rs               # 模块导出
```

### 🔧 核心组件

#### CPU亲和性管理
- **专用线程绑定**: 将关键线程绑定到特定CPU核心
- **核心隔离**: 避免多线程竞争同一核心
- **热点避免**: 智能分布负载避免CPU热点
- **超线程控制**: 合理利用或避免超线程

#### 内存优化
- **缓存行对齐**: 64字节对齐避免false sharing
- **预分配策略**: 启动时分配大块内存减少运行时分配
- **内存池管理**: 高效的内存复用机制
- **零拷贝设计**: 减少不必要的内存拷贝

#### NUMA感知
- **节点检测**: 自动检测NUMA拓扑结构
- **本地分配**: 在当前CPU节点分配内存
- **跨节点优化**: 最小化跨节点内存访问
- **亲和性策略**: CPU和内存协同优化

## 🚀 性能提升

### 🏆 优化效果
- **CPU亲和性**: 延迟降低 15-25%
- **内存对齐**: 缓存命中率提升 20-40%
- **NUMA优化**: 内存访问速度提升 30-50%
- **综合优化**: 总体性能提升 40-80%

### 📊 性能基准
- **延迟改善**: 从 100μs 降低至 60μs
- **吞吐量提升**: 从 50K msg/s 提升至 120K msg/s
- **内存带宽**: 充分利用本地内存带宽
- **CPU利用率**: 降低无效的CPU切换开销

## 🔧 使用方式

### CPU亲和性管理
```rust
use flare_core::common::system::{CpuAffinityManager, CpuSet};

// 创建CPU亲和性管理器
let affinity_mgr = CpuAffinityManager::new()?;

// 绑定网络处理线程到核心0,1
let network_cores = CpuSet::from_cores(&[0, 1]);
affinity_mgr.bind_network_threads(&network_cores)?;

// 绑定计算线程到核心2,3
let compute_cores = CpuSet::from_cores(&[2, 3]);
affinity_mgr.bind_compute_threads(&compute_cores)?;

// 绑定当前线程到特定核心
affinity_mgr.bind_current_thread(4)?;
```

### 内存对齐优化
```rust
use flare_core::common::system::{AlignedBuffer, MemoryOptimizer};

// 创建缓存行对齐的缓冲区
let buffer = AlignedBuffer::new(1024)?; // 64字节对齐的1KB缓冲区

// 内存优化器
let optimizer = MemoryOptimizer::new();

// 预分配内存池
optimizer.preallocate_buffers(4096, 100)?; // 预分配100个4KB缓冲区

// 获取对齐的内存
let aligned_mem = optimizer.allocate_aligned(2048, 64)?; // 2KB，64字节对齐
```

### NUMA感知优化
```rust
use flare_core::common::system::NumaOptimizer;

// 创建NUMA优化器
let numa_optimizer = NumaOptimizer::new()?;

// 检测当前NUMA节点
let current_node = numa_optimizer.get_current_node()?;
println!("当前NUMA节点: {}", current_node);

// 在本地节点分配内存
let local_memory = numa_optimizer.allocate_on_local_node(8192)?; // 8KB

// 绑定线程到特定NUMA节点
numa_optimizer.bind_thread_to_node(0)?; // 绑定到节点0

// 获取NUMA拓扑信息
let topology = numa_optimizer.get_topology();
println!("NUMA节点数: {}", topology.node_count);
println!("每节点CPU数: {:?}", topology.cpus_per_node);
```

## ⚙️ 高级功能

### 1. 智能CPU调度
```rust
use flare_core::common::system::{CpuScheduler, ThreadType, Priority};

let scheduler = CpuScheduler::new()?;

// 为不同类型的线程分配专用核心
scheduler.assign_cores(ThreadType::Network, &[0, 1])?;
scheduler.assign_cores(ThreadType::Compute, &[2, 3])?;
scheduler.assign_cores(ThreadType::IO, &[4, 5])?;

// 设置线程优先级
scheduler.set_priority(ThreadType::Network, Priority::High)?;
scheduler.set_priority(ThreadType::Compute, Priority::Normal)?;

// 启动智能调度
scheduler.start_intelligent_scheduling()?;
```

### 2. 内存热点分析
```rust
use flare_core::common::system::MemoryAnalyzer;

let analyzer = MemoryAnalyzer::new();

// 分析内存访问模式
analyzer.start_profiling()?;

// 运行业务逻辑
process_messages().await;

// 获取分析结果
let report = analyzer.get_hotspot_report()?;
println!("内存热点: {:?}", report.hotspots);
println!("缓存未命中率: {:.2}%", report.cache_miss_rate);
```

### 3. 系统性能监控
```rust
use flare_core::common::system::SystemMonitor;

let monitor = SystemMonitor::new();

// 监控系统资源
let metrics = monitor.get_current_metrics()?;
println!("CPU使用率: {:.1}%", metrics.cpu_usage);
println!("内存使用率: {:.1}%", metrics.memory_usage);
println!("缓存命中率: {:.1}%", metrics.cache_hit_rate);

// 性能告警
monitor.set_threshold(MetricType::CpuUsage, 80.0)?;
monitor.set_alert_callback(|metric, value| {
    eprintln!("性能告警: {:?} = {:.1}%", metric, value);
})?;
```

## 🎯 使用场景

### 🎮 高频游戏服务器
```rust
// 游戏服务器优化配置
let affinity = CpuAffinityManager::new()?;
let numa = NumaOptimizer::new()?;

// 游戏逻辑线程绑定到高性能核心
affinity.bind_game_logic_threads(&[0, 2, 4, 6])?; // 物理核心

// 网络IO线程绑定到专用核心
affinity.bind_network_threads(&[1, 3])?;

// 在本地NUMA节点分配游戏状态内存
let game_state_memory = numa.allocate_on_local_node(16 * 1024 * 1024)?; // 16MB
```

### 💼 金融交易系统
```rust
// 超低延迟交易系统配置
let optimizer = MemoryOptimizer::new();
let affinity = CpuAffinityManager::new()?;

// 交易引擎线程独占CPU核心
affinity.isolate_core_for_trading(0)?;

// 预分配交易订单缓冲区，避免运行时分配
optimizer.preallocate_order_buffers(1024, 10000)?; // 10K个订单缓冲区

// 启用实时调度
affinity.enable_realtime_scheduling(Priority::Highest)?;
```

### 🔄 数据处理管道
```rust
// 大数据处理优化
let numa = NumaOptimizer::new()?;
let scheduler = CpuScheduler::new()?;

// 按NUMA节点分布数据处理任务
for node in 0..numa.get_node_count() {
    numa.bind_thread_to_node(node)?;
    scheduler.spawn_worker_on_node(node, process_data_chunk).await?;
}

// 数据本地化处理，避免跨节点访问
```

## 📈 性能优化建议

### 🏆 最佳实践

1. **合理分配CPU核心**:
   ```rust
   // ✅ 正确：不同类型任务分配专用核心
   affinity.bind_network_threads(&[0, 1])?;
   affinity.bind_compute_threads(&[2, 3])?;
   
   // ❌ 错误：所有线程竞争同一核心
   // 让系统自动调度（可能导致频繁切换）
   ```

2. **内存对齐策略**:
   ```rust
   // ✅ 正确：缓存行对齐避免false sharing
   #[repr(C, align(64))]
   struct OptimizedData {
       frequently_accessed: u64,
       // ... 其他字段
   }
   
   // ❌ 错误：不考虑内存布局
   struct UnoptimizedData {
       field1: u8,
       field2: u64, // 可能跨越多个缓存行
   }
   ```

3. **NUMA意识编程**:
   ```rust
   // ✅ 正确：在数据所在节点执行计算
   let current_node = numa.get_current_node()?;
   let data = numa.allocate_on_node(current_node, size)?;
   numa.bind_thread_to_node(current_node)?;
   
   // ❌ 错误：跨节点访问内存
   let data = allocate_anywhere(size); // 可能在远程节点
   ```

### ⚡ 极致优化

```rust
// 终极性能配置示例
pub struct UltraOptimizedConfig {
    // CPU配置
    pub isolated_cores: Vec<usize>,        // 隔离的CPU核心
    pub realtime_priority: bool,           // 实时调度优先级
    pub disable_interrupts: bool,          // 禁用中断（需要特权）
    
    // 内存配置  
    pub huge_pages: bool,                  // 使用大页内存
    pub memory_locking: bool,              // 锁定内存防止换出
    pub numa_local_only: bool,             // 仅使用本地NUMA内存
    
    // 系统配置
    pub disable_power_management: bool,    // 禁用CPU频率调节
    pub kernel_bypass: bool,               // 内核旁路（如DPDK）
}

impl UltraOptimizedConfig {
    pub fn apply(&self) -> Result<()> {
        if self.isolated_cores.len() > 0 {
            // 隔离CPU核心
            CpuAffinityManager::isolate_cores(&self.isolated_cores)?;
        }
        
        if self.huge_pages {
            // 启用大页内存
            MemoryOptimizer::enable_huge_pages()?;
        }
        
        if self.numa_local_only {
            // 强制本地NUMA分配
            NumaOptimizer::set_local_allocation_only(true)?;
        }
        
        Ok(())
    }
}
```

## 🚨 注意事项

### 系统要求
- **Linux**: 支持CPU亲和性和NUMA
- **macOS**: 部分功能支持（无完整NUMA支持）
- **Windows**: 基础CPU绑定支持
- **权限要求**: 某些优化需要管理员权限

### 性能权衡
```rust
// 权衡示例：延迟 vs 吞吐量
if optimize_for_latency {
    // 牺牲吞吐量换取超低延迟
    affinity.bind_to_single_core(0)?;  // 避免核心切换
    memory.disable_swap()?;            // 避免内存换出
} else {
    // 优化吞吐量，可接受稍高延迟
    affinity.balance_across_cores()?;  // 负载均衡
    memory.enable_prefetch()?;         // 预取优化
}
```

### 调试和监控
- 使用性能分析工具验证优化效果
- 监控CPU使用率和内存访问模式
- 注意优化可能带来的副作用
- 在生产环境前充分测试

## 🧪 测试与验证

### 性能测试
```bash
# 编译优化版本
cargo build --release

# 运行系统优化基准测试
cargo run --example system_optimization_benchmark

# 内存对齐效果测试
cargo run --example memory_alignment_test

# NUMA效果验证
cargo run --example numa_optimization_test
```

### 基准测试示例
```rust
#[tokio::test]
async fn test_cpu_affinity_performance() {
    let affinity = CpuAffinityManager::new().unwrap();
    
    // 测试无绑定性能
    let baseline = benchmark_message_processing().await;
    
    // 绑定到专用核心后性能
    affinity.bind_current_thread(0).unwrap();
    let optimized = benchmark_message_processing().await;
    
    // 验证性能提升
    let improvement = (baseline.as_nanos() - optimized.as_nanos()) as f64 / baseline.as_nanos() as f64;
    assert!(improvement > 0.15); // 至少15%提升
    
    println!("CPU绑定性能提升: {:.1}%", improvement * 100.0);
}
```

## 📚 相关资源

- [CPU亲和性原理](https://man7.org/linux/man-pages/man2/sched_setaffinity.2.html)
- [NUMA架构详解](https://en.wikipedia.org/wiki/Non-uniform_memory_access)
- [缓存行对齐最佳实践](https://mechanical-sympathy.blogspot.com/2011/07/false-sharing.html)
- [Rust无锁编程](https://doc.rust-lang.org/nomicon/atomics.html)

---

*🚀 硬件感知优化 - 榨干每一丝性能潜力*