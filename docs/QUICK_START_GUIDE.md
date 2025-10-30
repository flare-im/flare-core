# Flare-Core 快速开始指南

本指南帮助您快速上手 flare-core 的连接稳定性模块。

---

## 一、核心模块概览

flare-core 提供了三个核心的连接稳定性模块：

| 模块 | 功能 | 文件路径 |
|------|------|----------|
| **HeartbeatManager** | 智能心跳管理 | `src/common/connections/heartbeat.rs` |
| **SmartReconnectManager** | 智能重连管理 | `src/common/connections/reconnect.rs` |
| **ReliableMessageChannel** | 可靠消息传输 | `src/common/connections/reliable.rs` |

---

## 二、智能心跳管理

### 2.1 基础使用

```rust
use flare_core::common::connections::heartbeat::{HeartbeatConfig, HeartbeatManager};
use std::time::Duration;

#[tokio::main]
async fn main() {
    // 1. 使用默认配置创建心跳管理器
    let manager = HeartbeatManager::new(HeartbeatConfig::default());
    
    // 2. 在连接循环中使用
    loop {
        // 获取当前心跳间隔
        let interval = manager.get_interval();
        tokio::time::sleep(Duration::from_millis(interval)).await;
        
        // 记录心跳发送
        manager.on_heartbeat_sent();
        
        // 发送心跳并处理响应
        match send_heartbeat().await {
            Ok(rtt_ms) => {
                // 心跳成功
                manager.on_heartbeat_success();
                manager.record_rtt(rtt_ms);  // 自动触发自适应调节
            }
            Err(_) => {
                // 心跳超时
                if manager.on_heartbeat_timeout() {
                    // 连续超时次数达到阈值，触发重连
                    println!("连续超时，需要重连！");
                    break;
                }
            }
        }
    }
}

async fn send_heartbeat() -> Result<u32, Box<dyn std::error::Error>> {
    // 实际的心跳发送逻辑
    // 返回 RTT（毫秒）
    Ok(50)
}
```

### 2.2 自定义配置

#### 场景1：WiFi 环境（心跳间隔较长）

```rust
let wifi_config = HeartbeatConfig {
    initial_interval_ms: 60000,      // 60秒
    min_interval_ms: 30000,          // 最小30秒
    max_interval_ms: 120000,         // 最大2分钟
    timeout_threshold: 3,            // 连续3次超时触发重连
    enable_adaptive: true,           // 启用自适应
    ..Default::default()
};

let manager = HeartbeatManager::new(wifi_config);
```

#### 场景2：移动网络（心跳间隔较短）

```rust
let mobile_config = HeartbeatConfig {
    initial_interval_ms: 30000,      // 30秒
    min_interval_ms: 15000,          // 最小15秒
    max_interval_ms: 60000,          // 最大60秒
    timeout_threshold: 3,
    enable_adaptive: true,
    ..Default::default()
};

let manager = HeartbeatManager::new(mobile_config);
```

#### 场景3：弱网环境（更激进的检测）

```rust
let weak_network_config = HeartbeatConfig {
    initial_interval_ms: 20000,      // 20秒
    min_interval_ms: 10000,          // 最小10秒
    max_interval_ms: 40000,          // 最大40秒
    timeout_threshold: 2,            // 连续2次超时即触发重连
    enable_adaptive: true,
    high_rtt_threshold_ms: 300,      // 更低的高延迟阈值
    adaptive_decrease_factor: 0.7,   // 网络差时缩短30%
    ..Default::default()
};

let manager = HeartbeatManager::new(weak_network_config);
```

### 2.3 获取统计信息

```rust
// 获取成功率
let success_rate = manager.get_success_rate();
println!("心跳成功率: {:.2}%", success_rate * 100.0);

// 获取平均 RTT
if let Some(avg_rtt) = manager.get_avg_rtt() {
    println!("平均 RTT: {}ms", avg_rtt);
}

// 获取 RTT 抖动
if let Some(jitter) = manager.get_rtt_jitter() {
    println!("RTT 抖动: {:.2}ms", jitter);
}

// 获取连续超时次数
let timeouts = manager.get_consecutive_timeouts();
println!("连续超时: {}次", timeouts);
```

---

## 三、智能重连管理

### 3.1 基础使用

```rust
use flare_core::common::connections::reconnect::SmartReconnectManager;
use flare_core::common::error::FlareError;

#[tokio::main]
async fn main() {
    // 创建重连管理器
    let reconnect_mgr = SmartReconnectManager::new();
    
    loop {
        match connect().await {
            Ok(connection) => {
                // 连接成功，重置重试计数器
                reconnect_mgr.on_connect_success().await;
                println!("连接成功！");
                
                // 使用连接...
                handle_connection(connection).await;
            }
            Err(error) => {
                // 连接失败，计算重连延迟
                let delay = reconnect_mgr.on_connect_failed(error).await;
                
                println!("连接失败，{}ms 后重试...", delay);
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
        }
    }
}

async fn connect() -> Result<Connection, FlareError> {
    // 实际的连接逻辑
    Ok(Connection::new())
}
```

### 3.2 自定义重连策略

```rust
use flare_core::common::connections::reconnect::SmartReconnectManager;

let custom_reconnect = SmartReconnectManager::with_config(
    10,             // max_retries: 最大重试10次
    500,            // initial_delay_ms: 初始延迟500ms
    15000,          // max_delay_ms: 最大延迟15秒
    1.5,            // backoff_factor: 退避系数1.5倍
    0.15,           // jitter_factor: 抖动系数15%
    true            // enable_network_probe: 启用网络探测
);
```

### 3.3 重连延迟序列示例

默认配置下的重连延迟序列：

| 重试次数 | 延迟范围 | 说明 |
|---------|---------|------|
| 第1次 | 800-1200ms | 1s ± 20% |
| 第2次 | 1600-2400ms | 2s ± 20% |
| 第3次 | 3200-4800ms | 4s ± 20% |
| 第4次 | 6400-9600ms | 8s ± 20% |
| 第5次 | 12800-19200ms | 16s ± 20% |
| 第6次+ | 24000-36000ms | 30s ± 20%（达到上限） |

### 3.4 获取重连统计

```rust
// 获取当前重试次数
let retry_count = reconnect_mgr.get_retry_count();
println!("当前重试次数: {}", retry_count);

// 获取重连成功率
let success_rate = reconnect_mgr.get_success_rate();
println!("重连成功率: {:.2}%", success_rate * 100.0);

// 手动重置（连接恢复后）
reconnect_mgr.reset();
```

---

## 四、可靠消息传输

### 4.1 基础使用

```rust
use flare_core::common::connections::reliable::ReliableMessageChannel;
use flare_core::common::protocol::frame::Frame;

#[tokio::main]
async fn main() {
    // 创建可靠消息通道
    let channel = ReliableMessageChannel::new();
    
    // 发送可靠消息
    let payload = vec![1, 2, 3, 4, 5];
    let seq = channel.send_reliable(payload, |frame| {
        // 实际的发送函数
        send_frame_to_network(frame)
    }).await.unwrap();
    
    println!("消息已发送，序列号: {}", seq);
}

fn send_frame_to_network(frame: Frame) -> Result<(), FlareError> {
    // 实际的网络发送逻辑
    Ok(())
}
```

### 4.2 处理 ACK 响应

```rust
// 收到 ACK 响应时
channel.handle_ack(ack_seq);
println!("消息 {} 已确认", ack_seq);
```

### 4.3 处理接收消息（去重 + 重排序）

```rust
// 收到消息时
let frame = parse_incoming_frame();
let seq = extract_sequence_number(&frame);

if let Some(deliverable_frames) = channel.handle_received(frame, seq) {
    // 返回按序交付的消息列表
    for f in deliverable_frames {
        println!("交付消息: seq={}", f.message_id);
        process_message(f);
    }
} else {
    // 消息重复或乱序，已暂存到重排序缓冲区
    println!("消息 {} 乱序，暂存", seq);
}
```

### 4.4 监控统计

```rust
// 获取待确认消息数量
let pending = channel.pending_count();
println!("待确认消息: {}", pending);

// 获取重排序缓冲区大小
let buffered = channel.reorder_buffer_size();
println!("重排序缓冲区: {}", buffered);

// 清理超时的待确认消息
channel.cleanup_expired(60000);  // 60秒超时
```

---

## 五、综合示例：完整的连接管理

```rust
use flare_core::common::connections::{
    heartbeat::{HeartbeatConfig, HeartbeatManager},
    reconnect::SmartReconnectManager,
    reliable::ReliableMessageChannel,
};
use std::time::Duration;

struct ConnectionManager {
    heartbeat: HeartbeatManager,
    reconnect: SmartReconnectManager,
    reliable: ReliableMessageChannel,
}

impl ConnectionManager {
    fn new() -> Self {
        // 创建心跳管理器
        let heartbeat_config = HeartbeatConfig {
            initial_interval_ms: 30000,
            min_interval_ms: 15000,
            max_interval_ms: 60000,
            timeout_threshold: 3,
            enable_adaptive: true,
            ..Default::default()
        };
        
        Self {
            heartbeat: HeartbeatManager::new(heartbeat_config),
            reconnect: SmartReconnectManager::new(),
            reliable: ReliableMessageChannel::new(),
        }
    }
    
    async fn run(&self) {
        loop {
            // 1. 建立连接
            let connection = match self.connect().await {
                Ok(conn) => {
                    self.reconnect.on_connect_success().await;
                    conn
                }
                Err(error) => {
                    let delay = self.reconnect.on_connect_failed(error).await;
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    continue;
                }
            };
            
            // 2. 运行连接循环
            if let Err(_) = self.connection_loop(connection).await {
                println!("连接断开，准备重连...");
            }
        }
    }
    
    async fn connection_loop(&self, connection: Connection) -> Result<(), Box<dyn std::error::Error>> {
        // 启动心跳任务
        let heartbeat_task = tokio::spawn({
            let manager = self.heartbeat.clone();
            async move {
                loop {
                    let interval = manager.get_interval();
                    tokio::time::sleep(Duration::from_millis(interval)).await;
                    
                    manager.on_heartbeat_sent();
                    
                    match send_heartbeat().await {
                        Ok(rtt) => {
                            manager.on_heartbeat_success();
                            manager.record_rtt(rtt);
                        }
                        Err(_) => {
                            if manager.on_heartbeat_timeout() {
                                return; // 触发重连
                            }
                        }
                    }
                }
            }
        });
        
        // 启动消息接收任务
        let receive_task = tokio::spawn({
            let channel = self.reliable.clone();
            async move {
                loop {
                    match receive_message().await {
                        Ok((frame, seq)) => {
                            if let Some(frames) = channel.handle_received(frame, seq) {
                                for f in frames {
                                    process_message(f);
                                }
                            }
                        }
                        Err(_) => return,
                    }
                }
            }
        });
        
        // 等待任务完成
        tokio::select! {
            _ = heartbeat_task => {
                println!("心跳任务退出");
            }
            _ = receive_task => {
                println!("接收任务退出");
            }
        }
        
        Ok(())
    }
    
    async fn connect(&self) -> Result<Connection, FlareError> {
        // 实际的连接逻辑
        Ok(Connection::new())
    }
    
    async fn send_message(&self, payload: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        // 使用可靠传输发送消息
        let seq = self.reliable.send_reliable(payload, |frame| {
            send_frame_to_network(frame)
        }).await?;
        
        println!("消息已发送，序列号: {}", seq);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let manager = ConnectionManager::new();
    manager.run().await;
}
```

---

## 六、配置建议

### 6.1 不同网络环境的配置

#### WiFi 环境

```rust
HeartbeatConfig {
    initial_interval_ms: 60000,    // 60秒
    min_interval_ms: 30000,        // 30秒
    max_interval_ms: 120000,       // 2分钟
    timeout_threshold: 3,
    ..Default::default()
}
```

#### 4G/5G 移动网络

```rust
HeartbeatConfig {
    initial_interval_ms: 30000,    // 30秒
    min_interval_ms: 15000,        // 15秒
    max_interval_ms: 60000,        // 60秒
    timeout_threshold: 3,
    ..Default::default()
}
```

#### 3G 网络

```rust
HeartbeatConfig {
    initial_interval_ms: 20000,    // 20秒
    min_interval_ms: 10000,        // 10秒
    max_interval_ms: 40000,        // 40秒
    timeout_threshold: 2,
    ..Default::default()
}
```

#### 弱网/不稳定网络

```rust
HeartbeatConfig {
    initial_interval_ms: 15000,    // 15秒
    min_interval_ms: 5000,         // 5秒
    max_interval_ms: 30000,        // 30秒
    timeout_threshold: 2,          // 更快触发重连
    enable_adaptive: true,
    high_rtt_threshold_ms: 300,    // 更低的高延迟阈值
    adaptive_decrease_factor: 0.7, // 更激进的缩短
    ..Default::default()
}
```

### 6.2 不同应用场景的配置

#### IM 聊天应用

```rust
HeartbeatConfig {
    initial_interval_ms: 30000,
    timeout_threshold: 3,
    enable_adaptive: true,
    ..Default::default()
}

SmartReconnectManager::with_config(
    10,      // 最多重试10次
    1000,    // 初始1秒延迟
    30000,   // 最大30秒延迟
    2.0,     // 指数退避
    0.2,     // 20%抖动
    true     // 启用网络探测
)
```

#### 实时游戏

```rust
HeartbeatConfig {
    initial_interval_ms: 10000,    // 更短的心跳间隔
    min_interval_ms: 5000,
    max_interval_ms: 20000,
    timeout_threshold: 2,          // 快速检测断连
    enable_adaptive: true,
    high_rtt_threshold_ms: 150,    // 游戏对延迟敏感
    ..Default::default()
}
```

#### IoT 设备（省电模式）

```rust
HeartbeatConfig {
    initial_interval_ms: 300000,   // 5分钟
    min_interval_ms: 60000,        // 最小1分钟
    max_interval_ms: 600000,       // 最大10分钟
    timeout_threshold: 2,
    enable_adaptive: false,        // 禁用自适应（减少计算）
    ..Default::default()
}
```

---

## 七、性能优化建议

### 7.1 内存优化

```rust
// 限制 RTT 窗口大小
HeartbeatConfig {
    rtt_window_size: 20,  // 减少内存占用（默认30）
    ..Default::default()
}

// 定期清理过期消息
channel.cleanup_expired(60000);
```

### 7.2 并发优化

```rust
// 使用 Arc 共享管理器
let heartbeat = Arc::new(HeartbeatManager::new(config));
let heartbeat_clone = Arc::clone(&heartbeat);

// 多个任务可以安全共享
tokio::spawn(async move {
    heartbeat_clone.on_heartbeat_sent();
});
```

### 7.3 统计信息重置

```rust
// 定期重置统计信息（避免累积）
heartbeat.reset_statistics();
```

---

## 八、故障排查

### 8.1 心跳频繁超时

**可能原因**：
- 网络质量差
- 超时阈值设置过低
- 服务端处理慢

**解决方案**：
```rust
// 1. 增加超时阈值
HeartbeatConfig {
    timeout_threshold: 5,  // 增加到5次
    ..Default::default()
}

// 2. 延长心跳间隔
HeartbeatConfig {
    min_interval_ms: 20000,
    ..Default::default()
}
```

### 8.2 重连风暴

**可能原因**：
- 退避系数设置过小
- 多个客户端同时重连

**解决方案**：
```rust
// 增大退避系数和抖动
SmartReconnectManager::with_config(
    10,
    1000,
    30000,
    2.5,    // 更大的退避系数
    0.3,    // 更大的抖动（避免同时重连）
    true
)
```

### 8.3 消息积压

**可能原因**：
- 待确认消息过多
- 重排序缓冲区过大

**解决方案**：
```rust
// 定期清理过期消息
channel.cleanup_expired(30000);  // 30秒超时

// 监控队列大小
if channel.pending_count() > 100 {
    log::warn!("待确认消息过多: {}", channel.pending_count());
}
```

---

## 九、最佳实践

### 9.1 设计原则

✅ **DO**：
- 使用默认配置作为起点
- 根据实际网络环境调整参数
- 定期监控统计信息
- 记录重连历史进行分析

❌ **DON'T**：
- 不要硬编码业务规则
- 不要过度频繁的心跳（浪费资源）
- 不要忽略错误日志
- 不要在生产环境使用未测试的配置

### 9.2 监控指标

建议监控以下指标：
- 心跳成功率
- 平均 RTT
- RTT 抖动
- 重连次数
- 待确认消息数量
- 重排序缓冲区大小

### 9.3 日志记录

```rust
// 记录关键事件
if manager.on_heartbeat_timeout() {
    log::warn!(
        "心跳超时！连续超时次数: {}, 成功率: {:.2}%",
        manager.get_consecutive_timeouts(),
        manager.get_success_rate() * 100.0
    );
}

// 记录重连事件
log::info!(
    "开始重连，第 {} 次尝试，延迟 {}ms",
    reconnect_mgr.get_retry_count(),
    delay
);
```

---

## 十、下一步

- 阅读 [设计原则文档](./DESIGN_PRINCIPLES.md) 了解架构设计
- 阅读 [连接稳定性优化方案](./IM_CONNECTION_STABILITY_OPTIMIZATION.md) 了解完整方案
- 阅读 [状态报告](./CONNECTION_STABILITY_STATUS.md) 了解实施进度
- 查看单元测试了解更多使用场景

---

**文档版本**：v1.0  
**最后更新**：2025-10-15
