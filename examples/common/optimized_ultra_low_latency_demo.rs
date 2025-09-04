//! 统一超低延迟优化演示
//!
//! 展示所有优化技术的集成效果：
//! - 零拷贝Bincode序列化器
//! - 自适应LZ4压缩器
//! - 异步消息Pipeline
//! - 微批处理优化
//! - CPU亲和性绑定

use std::{sync::Arc, time::{Duration, Instant}};
use tokio::time::sleep;

use flare_core::common::{
    Frame, MessageType, Reliability,
    serialization::BincodeSerializer,
    compression::{Lz4Compressor, CompressionConfig},
    pipeline::AsyncMessagePipeline,
    system::CpuAffinityManager,
};

/// 性能测试配置
#[derive(Debug, Clone)]
pub struct UltraLowLatencyConfig {
    /// 消息数量
    pub message_count: usize,
    /// 消息大小（字节）
    pub message_size: usize,
    /// 并发数
    pub concurrency: usize,
}

impl Default for UltraLowLatencyConfig {
    fn default() -> Self {
        Self {
            message_count: 1000,
            message_size: 256,
            concurrency: 4,
        }
    }
}

/// 性能测试结果
#[derive(Debug, Clone)]
pub struct UltraLowLatencyResult {
    pub total_messages: usize,
    pub total_time: Duration,
    pub average_latency: Duration,
    pub p95_latency: Duration,
    pub p99_latency: Duration,
    pub max_latency: Duration,
    pub min_latency: Duration,
    pub throughput_msg_per_sec: f64,
    pub total_bytes_processed: u64,
    pub cpu_core_bound: Option<usize>,
}

impl UltraLowLatencyResult {
    pub fn new(
        latencies: &[Duration],
        total_bytes: u64,
        cpu_core: Option<usize>,
    ) -> Self {
        let mut sorted_latencies = latencies.to_vec();
        sorted_latencies.sort();

        let total_messages = latencies.len();
        let total_time: Duration = latencies.iter().sum();
        let average_latency = total_time / total_messages as u32;
        
        let p95_index = (total_messages as f64 * 0.95) as usize;
        let p99_index = (total_messages as f64 * 0.99) as usize;
        
        let p95_latency = sorted_latencies.get(p95_index.min(total_messages - 1))
            .copied().unwrap_or(Duration::ZERO);
        let p99_latency = sorted_latencies.get(p99_index.min(total_messages - 1))
            .copied().unwrap_or(Duration::ZERO);
        
        let max_latency = sorted_latencies.last().copied().unwrap_or(Duration::ZERO);
        let min_latency = sorted_latencies.first().copied().unwrap_or(Duration::ZERO);
        
        let throughput_msg_per_sec = if total_time.as_secs_f64() > 0.0 {
            total_messages as f64 / total_time.as_secs_f64()
        } else {
            0.0
        };

        Self {
            total_messages,
            total_time,
            average_latency,
            p95_latency,
            p99_latency,
            max_latency,
            min_latency,
            throughput_msg_per_sec,
            total_bytes_processed: total_bytes,
            cpu_core_bound: cpu_core,
        }
    }
    
    pub fn print_summary(&self) {
        println!("🔬 超低延迟优化演示结果");
        if let Some(core) = self.cpu_core_bound {
            println!("  CPU核心绑定: 核心{}", core);
        }
        println!("  消息数量: {}", self.total_messages);
        println!("  总耗时: {:.2}ms", self.total_time.as_secs_f64() * 1000.0);
        println!("  平均延迟: {:.3}ms", self.average_latency.as_secs_f64() * 1000.0);
        println!("  P95延迟: {:.3}ms", self.p95_latency.as_secs_f64() * 1000.0);
        println!("  P99延迟: {:.3}ms", self.p99_latency.as_secs_f64() * 1000.0);
        println!("  最大延迟: {:.3}ms", self.max_latency.as_secs_f64() * 1000.0);
        println!("  最小延迟: {:.3}ms", self.min_latency.as_secs_f64() * 1000.0);
        println!("  吞吐量: {:.0} msg/s", self.throughput_msg_per_sec);
        println!("  数据处理: {} 字节", self.total_bytes_processed);
        println!();
    }
}

/// 超低延迟优化演示器
pub struct UltraLowLatencyDemo {
    config: UltraLowLatencyConfig,
}

impl UltraLowLatencyDemo {
    pub fn new(config: UltraLowLatencyConfig) -> Self {
        Self { config }
    }
    
    /// 应用CPU亲和性优化
    pub fn apply_cpu_affinity(&self) -> Option<usize> {
        if let Ok(affinity_mgr) = CpuAffinityManager::new() {
            // 尝试绑定到核心0
            if let Err(e) = affinity_mgr.bind_current_thread(0) {
                println!("⚠️  CPU亲和性绑定失败: {}", e);
                None
            } else {
                println!("✅ 已绑定到CPU核心0");
                Some(0)
            }
        } else {
            println!("⚠️  无法创建CPU亲和性管理器");
            None
        }
    }
    
    /// 创建优化的Pipeline
    pub fn create_optimized_pipeline() -> AsyncMessagePipeline {
        let serializer = Arc::new(BincodeSerializer::new());
        let compressor = Arc::new(Lz4Compressor::ultra_fast());
        
        AsyncMessagePipeline::ultra_low_latency(
            serializer as Arc<dyn flare_core::common::serialization::FrameSerializer>,
            compressor as Arc<dyn flare_core::common::compression::Compressor>,
        )
    }
    
    /// 生成测试消息
    pub fn generate_test_message(&self, id: u64) -> Frame {
        // 生成有一定重复模式的数据，更接近真实场景
        let mut payload = Vec::with_capacity(self.config.message_size);
        let patterns = [
            "user_data",
            "timestamp:",
            "event_type:",
            "session_id:",
            "metadata:",
            "payload_data:",
        ];
        
        let mut pattern_idx = 0;
        while payload.len() < self.config.message_size {
            let pattern = patterns[pattern_idx % patterns.len()];
            payload.extend_from_slice(pattern.as_bytes());
            
            // 添加变化数据
            let num = (id % 1000).to_string();
            payload.extend_from_slice(num.as_bytes());
            payload.push(b' ');
            
            pattern_idx += 1;
        }
        
        payload.truncate(self.config.message_size);
        
        Frame::new(
            MessageType::Data,
            id,
            Reliability::AtLeastOnce,
            payload,
        )
    }
    
    /// 测试Pipeline性能
    pub async fn test_pipeline_performance(&self) -> UltraLowLatencyResult {
        println!("🚀 开始超低延迟Pipeline性能测试...");
        println!("   消息数量: {}", self.config.message_count);
        println!("   消息大小: {} 字节", self.config.message_size);
        println!("   并发数: {}", self.config.concurrency);
        println!();
        
        // 应用CPU亲和性优化
        let cpu_core = self.apply_cpu_affinity();
        
        // 创建优化的Pipeline
        let pipeline = Self::create_optimized_pipeline();
        println!("✅ 已创建优化的异步消息Pipeline");
        println!("   • 零拷贝Bincode序列化器");
        println!("   • 自适应LZ4压缩器");
        println!("   • 微批处理 (批大小: 2)");
        println!();
        
        let mut latencies = Vec::with_capacity(self.config.message_count);
        let mut total_bytes = 0u64;
        
        // 测试Pipeline处理性能
        for i in 1..=self.config.message_count {
            let message = self.generate_test_message(i as u64);
            total_bytes += message.get_payload().len() as u64;
            
            let start = Instant::now();
            
            // 通过Pipeline处理消息
            match pipeline.process_async(message).await {
                Ok(result) => {
                    let latency = start.elapsed();
                    latencies.push(latency);
                    
                    // 显示进度
                    if i % (self.config.message_count / 10).max(1) == 0 {
                        println!("   进度: {}/{} ({:.1}%)", i, self.config.message_count, 
                                (i as f64 / self.config.message_count as f64) * 100.0);
                    }
                }
                Err(e) => {
                    println!("❌ Pipeline处理失败: {}", e);
                    let latency = start.elapsed();
                    latencies.push(latency);
                }
            }
        }
        
        let result = UltraLowLatencyResult::new(&latencies, total_bytes, cpu_core);
        result.print_summary();
        
        result
    }
    
    /// 测试并发Pipeline性能
    pub async fn test_concurrent_pipeline_performance(&self) -> UltraLowLatencyResult {
        println!("🚀 开始并发Pipeline性能测试...");
        println!("   消息数量: {}", self.config.message_count);
        println!("   消息大小: {} 字节", self.config.message_size);
        println!("   并发数: {}", self.config.concurrency);
        println!();
        
        // 应用CPU亲和性优化
        let cpu_core = self.apply_cpu_affinity();
        
        // 创建优化的Pipeline
        let pipeline = Arc::new(Self::create_optimized_pipeline());
        println!("✅ 已创建优化的并发异步消息Pipeline");
        println!();
        
        let latencies = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let total_bytes = Arc::new(tokio::sync::Mutex::new(0u64));
        
        let chunk_size = self.config.message_count / self.config.concurrency;
        
        let mut tasks = Vec::new();
        
        for i in 0..self.config.concurrency {
            let start_idx = i * chunk_size + 1;
            let end_idx = if i == self.config.concurrency - 1 {
                self.config.message_count
            } else {
                start_idx + chunk_size - 1
            };
            
            let chunk_latencies = Arc::clone(&latencies);
            let chunk_bytes = Arc::clone(&total_bytes);
            let chunk_pipeline = Arc::clone(&pipeline);
            let chunk_size = self.config.message_size;
            
            let task = tokio::spawn(async move {
                let mut local_latencies = Vec::new();
                let mut local_bytes = 0u64;
                
                for idx in start_idx..=end_idx {
                    // 生成测试消息
                    let mut payload = Vec::with_capacity(chunk_size);
                    let patterns = ["test", "data", "payload", "message"];
                    let pattern = patterns[idx % patterns.len()];
                    payload.extend_from_slice(pattern.as_bytes());
                    payload.extend_from_slice(&idx.to_le_bytes());
                    payload.resize(chunk_size, b'x');
                    
                    let message = Frame::new(
                        MessageType::Data,
                        idx as u64,
                        Reliability::AtLeastOnce,
                        payload,
                    );
                    
                    local_bytes += message.get_payload().len() as u64;
                    
                    let start = Instant::now();
                    
                    // 通过Pipeline处理消息
                    match chunk_pipeline.process_async(message).await {
                        Ok(_) => {
                            let latency = start.elapsed();
                            local_latencies.push(latency);
                        }
                        Err(e) => {
                            println!("❌ Pipeline处理失败: {}", e);
                            let latency = start.elapsed();
                            local_latencies.push(latency);
                        }
                    }
                }
                
                // 合并结果
                {
                    let mut global_latencies = chunk_latencies.lock().await;
                    global_latencies.extend(local_latencies);
                }
                
                {
                    let mut global_bytes = chunk_bytes.lock().await;
                    *global_bytes += local_bytes;
                }
            });
            
            tasks.push(task);
        }
        
        // 等待所有任务完成
        for task in tasks {
            if let Err(e) = task.await {
                println!("❌ 并发任务失败: {}", e);
            }
        }
        
        let final_latencies = latencies.lock().await;
        let final_bytes = *total_bytes.lock().await;
        
        let result = UltraLowLatencyResult::new(&final_latencies, final_bytes, cpu_core);
        result.print_summary();
        
        result
    }
}

/// 主函数
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    println!("==========================================");
    println!("🚀 Flare-Core 超低延迟优化演示");
    println!("==========================================");
    println!();
    
    // 创建默认配置
    let config = UltraLowLatencyConfig::default();
    let demo = UltraLowLatencyDemo::new(config);
    
    // 测试单线程Pipeline性能
    let single_result = demo.test_pipeline_performance().await;
    
    // 等待一下
    sleep(Duration::from_millis(1000)).await;
    
    // 测试并发Pipeline性能
    let concurrent_config = UltraLowLatencyConfig {
        message_count: 2000,
        message_size: 512,
        concurrency: 8,
    };
    let concurrent_demo = UltraLowLatencyDemo::new(concurrent_config);
    let concurrent_result = concurrent_demo.test_concurrent_pipeline_performance().await;
    
    // 输出最终总结
    println!("==========================================");
    println!("📊 最终性能对比");
    println!("==========================================");
    println!("单线程Pipeline:");
    println!("  平均延迟: {:.3}ms", single_result.average_latency.as_secs_f64() * 1000.0);
    println!("  吞吐量: {:.0} msg/s", single_result.throughput_msg_per_sec);
    println!();
    println!("并发Pipeline:");
    println!("  平均延迟: {:.3}ms", concurrent_result.average_latency.as_secs_f64() * 1000.0);
    println!("  吞吐量: {:.0} msg/s", concurrent_result.throughput_msg_per_sec);
    println!();
    
    let improvement = if single_result.average_latency.as_nanos() > 0 {
        (single_result.throughput_msg_per_sec / concurrent_result.throughput_msg_per_sec) * 100.0
    } else {
        0.0
    };
    
    if improvement > 0.0 {
        println!("📈 并发吞吐量提升: {:.1}%", improvement - 100.0);
    }
    
    println!();
    println!("✨ 优化技术总结:");
    println!("   • 零拷贝Bincode序列化 - 最小化内存拷贝");
    println!("   • 自适应LZ4压缩 - 超低延迟压缩");
    println!("   • 异步Pipeline - 并行处理序列化/压缩/传输");
    println!("   • 微批处理 - 减少系统调用开销");
    println!("   • CPU亲和性绑定 - 减少上下文切换");
    println!();
    println!("✅ 演示完成!");
    
    Ok(())
}