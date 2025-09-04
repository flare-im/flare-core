# Messaging 模块文档

## 📖 模块概述

消息处理模块提供高级消息调度和队列管理功能，支持优先级处理、批量操作和智能调度。模块专为**高并发**和**超低延迟**场景设计，确保关键消息优先处理。

## 🎯 设计目标

- **优先级调度**: 系统消息优先，实时消息次之
- **超低延迟**: 关键路径 < 1ms 处理时间
- **高并发**: 支持 100K+ msg/s 吞吐量
- **智能管理**: 自动超时清理、背压控制
- **易于扩展**: 支持自定义优先级策略

## 🏗️ 架构设计

```
messaging/
├── priority_queue.rs   # 优先级消息队列
└── mod.rs             # 模块导出
```

### 🔧 核心组件

#### 优先级消息队列
- **数据结构**: BinaryHeap 实现的优先级队列
- **并发安全**: Arc + Mutex 保护共享状态
- **异步支持**: 完全异步的入队/出队操作
- **超时管理**: 自动清理过期消息

## 🚀 消息优先级系统

### 优先级定义
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessagePriority {
    System = 0,      // 系统关键消息（心跳、认证等）
    Realtime = 1,    // 实时消息（游戏操作、交易指令）
    High = 2,        // 高优先级（重要通知）
    Normal = 3,      // 普通优先级（常规数据）
    Low = 4,         // 低优先级（统计、日志等）
}
```

### 优先级权重配置
```rust
pub struct PriorityQueueConfig {
    // 权重分配：系统:实时:高:普通:低 = 50:30:15:4:1
    pub priority_weights: [u32; 5],
    pub max_queue_size: usize,           // 默认: 10000
    pub timeout_check_interval: Duration, // 默认: 100ms
}
```

## 📊 性能特征

### 🏆 处理延迟
- **系统消息**: < 0.1ms (最高优先级)
- **实时消息**: < 0.5ms (游戏、交易)
- **普通消息**: < 2ms (常规数据)
- **低优先级**: < 10ms (统计、日志)

### 🚀 吞吐量指标
- **入队性能**: > 1M ops/s
- **出队性能**: > 800K ops/s
- **批量处理**: > 100K msg/s
- **内存效率**: O(log n) 时间复杂度

## 🔧 使用方式

### 基础使用
```rust
use flare_core::common::messaging::{
    PriorityMessageQueue, MessagePriority,
    create_system_message, create_realtime_message
};

// 创建优先级队列
let queue = PriorityMessageQueue::default();

// 创建不同优先级的消息
let system_msg = create_system_message(frame1);      // 30s超时
let realtime_msg = create_realtime_message(frame2);  // 5s超时
let normal_msg = create_normal_message(frame3);      // 300s超时

// 入队（任意顺序）
queue.enqueue(normal_msg).await?;
queue.enqueue(system_msg).await?;
queue.enqueue(realtime_msg).await?;

// 出队（按优先级排序：系统 -> 实时 -> 普通）
while let Some(msg) = queue.dequeue().await? {
    println!("处理消息: {:?} (优先级: {:?})", 
             msg.frame.get_message_id(), msg.priority);
}
```

### 批量操作
```rust
// 批量出队提高效率
let messages = queue.dequeue_batch(10).await?;

for msg in messages {
    // 并行处理消息
    tokio::spawn(async move {
        process_message(msg.frame).await;
    });
}
```

### 自定义配置
```rust
use flare_core::common::messaging::PriorityQueueConfig;

let config = PriorityQueueConfig {
    max_queue_size: 50000,
    // 游戏场景：更倾向实时消息
    priority_weights: [40, 40, 15, 4, 1], // 系统:实时:高:普通:低
    timeout_check_interval: Duration::from_millis(50), // 50ms检查
    enable_adaptive_scheduling: true,
};

let queue = PriorityMessageQueue::new(config);
```

## ⚙️ 高级功能

### 1. 超时管理
```rust
// 消息会自动检查超时并清理
let msg = PriorityMessage::new(
    frame,
    MessagePriority::Realtime,
    Duration::from_millis(100)  // 100ms超时
);

queue.enqueue(msg).await?;

// 过期消息会被自动丢弃
if let Some(msg) = queue.dequeue().await? {
    assert!(!msg.is_expired());
}
```

### 2. 统计监控
```rust
let stats = queue.get_stats().await;
println!("队列统计:");
println!("  总消息数: {}", stats.total_messages);
println!("  处理速率: {:.0} msg/s", stats.processing_rate);
println!("  平均等待时间: {:?}", stats.avg_wait_time);
println!("  超时消息: {}", stats.expired_messages);
```

### 3. 背压控制
```rust
// 队列满时会返回错误，避免内存溢出
match queue.enqueue(msg).await {
    Ok(()) => println!("消息已入队"),
    Err(FlareError::GeneralError(msg)) if msg.contains("队列已满") => {
        // 触发背压处理
        handle_backpressure().await;
    }
    Err(e) => return Err(e),
}
```

## 🎯 使用场景

### 🎮 游戏场景
```rust
// 游戏操作优先处理
let player_action = create_realtime_message(action_frame);
let chat_message = create_normal_message(chat_frame);
let analytics = create_low_priority_message(analytics_frame);

queue.enqueue(analytics).await?;    // 最后处理
queue.enqueue(chat_message).await?; // 次要处理
queue.enqueue(player_action).await?; // 优先处理
```

### 💼 企业应用
```rust
// 系统监控优先
let heartbeat = create_system_message(heartbeat_frame);
let user_request = create_high_priority_message(request_frame);
let log_data = create_low_priority_message(log_frame);

// 处理顺序：heartbeat -> user_request -> log_data
```

### 📈 数据处理
```rust
// 实时数据优先
let market_data = create_realtime_message(market_frame);
let historical_data = create_normal_message(history_frame);
let backup_data = create_low_priority_message(backup_frame);
```

## 🔍 扩展指南

### 自定义优先级策略
```rust
use flare_core::common::messaging::MessagePriority;

// 根据消息内容动态调整优先级
fn determine_priority(frame: &Frame) -> MessagePriority {
    match frame.get_message_type() {
        MessageType::Heartbeat => MessagePriority::System,
        MessageType::Data if is_trading_data(frame) => MessagePriority::Realtime,
        MessageType::Data if is_user_action(frame) => MessagePriority::High,
        MessageType::Data => MessagePriority::Normal,
        MessageType::Log => MessagePriority::Low,
    }
}
```

### 自定义超时策略
```rust
fn calculate_timeout(priority: MessagePriority, data_size: usize) -> Duration {
    let base_timeout = match priority {
        MessagePriority::System => Duration::from_secs(30),
        MessagePriority::Realtime => Duration::from_secs(5),
        MessagePriority::High => Duration::from_secs(60),
        MessagePriority::Normal => Duration::from_secs(300),
        MessagePriority::Low => Duration::from_secs(600),
    };
    
    // 大消息适当延长超时
    if data_size > 10240 { // > 10KB
        base_timeout * 2
    } else {
        base_timeout
    }
}
```

## 📈 性能优化建议

### 🏆 最佳实践

1. **合理设置优先级**:
   ```rust
   // 避免过多系统消息阻塞其他消息
   let priority = if is_critical_system_msg() {
       MessagePriority::System
   } else if is_user_facing() {
       MessagePriority::Realtime
   } else {
       MessagePriority::Normal
   };
   ```

2. **批量处理优化**:
   ```rust
   // 一次性处理多个消息减少系统调用
   let batch = queue.dequeue_batch(32).await?;
   
   // 并行处理提高吞吐量
   let handles: Vec<_> = batch.into_iter()
       .map(|msg| tokio::spawn(process_message(msg)))
       .collect();
   
   futures::future::join_all(handles).await;
   ```

3. **内存管理**:
   ```rust
   // 定期检查队列大小
   if queue.len().await > 8000 {
       warn!("队列积压严重，当前长度: {}", queue.len().await);
       // 可以考虑丢弃低优先级消息
   }
   ```

### ⚡ 极致优化
```rust
// 预分配消息对象减少运行时分配
struct MessagePool {
    pool: Vec<PriorityMessage>,
}

impl MessagePool {
    fn acquire(&mut self, frame: Frame, priority: MessagePriority) -> PriorityMessage {
        if let Some(mut msg) = self.pool.pop() {
            msg.frame = frame;
            msg.priority = priority;
            msg.created_at = Instant::now();
            msg
        } else {
            PriorityMessage::new(frame, priority, Duration::from_secs(60))
        }
    }
}
```

## 🚨 注意事项

### 内存管理
- 监控队列长度避免内存溢出
- 设置合理的`max_queue_size`限制
- 及时处理消息避免积压

### 优先级滥用
```rust
// 避免滥用高优先级
// ❌ 错误：所有消息都设为系统优先级
let msg = create_system_message(ordinary_frame);

// ✅ 正确：根据实际重要性设置
let msg = match frame.get_message_type() {
    MessageType::Heartbeat => create_system_message(frame),
    MessageType::Data => create_normal_message(frame),
    MessageType::Log => create_low_priority_message(frame),
};
```

### 死锁预防
- 异步操作避免阻塞队列
- 消息处理失败时要及时释放资源
- 避免在消息处理中再次操作同一队列

## 🧪 测试与验证

### 单元测试
```bash
cargo test messaging
```

### 性能测试
```bash
cargo run --example priority_queue_benchmark
```

### 优先级验证
```rust
#[tokio::test]
async fn test_priority_ordering() {
    let queue = PriorityMessageQueue::default();
    
    // 乱序入队
    queue.enqueue(create_low_priority_message(frame1)).await.unwrap();
    queue.enqueue(create_system_message(frame2)).await.unwrap();
    queue.enqueue(create_normal_message(frame3)).await.unwrap();
    
    // 验证出队顺序
    let msg1 = queue.dequeue().await.unwrap().unwrap();
    assert_eq!(msg1.priority, MessagePriority::System);
    
    let msg2 = queue.dequeue().await.unwrap().unwrap();
    assert_eq!(msg2.priority, MessagePriority::Normal);
    
    let msg3 = queue.dequeue().await.unwrap().unwrap();
    assert_eq!(msg3.priority, MessagePriority::Low);
}
```

## 📚 相关资源

- [优先级队列算法](https://en.wikipedia.org/wiki/Priority_queue)
- [Rust BinaryHeap文档](https://doc.rust-lang.org/std/collections/struct.BinaryHeap.html)
- [Tokio异步编程](https://tokio.rs/tokio/tutorial)
- [性能测试结果](../../ULTRA_LOW_LATENCY_OPTIMIZATION_GUIDE.md)

---

*智能消息调度 - 让重要的事情优先处理*