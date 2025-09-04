//! 超低延迟优化演示
//!
//! 展示各种延迟优化技术的实际效果

use std::time::{Duration, Instant};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, timeout};

use flare_core::common::{
    Frame, MessageType, Reliability,
    FrameSerializer, SerializationFormat, SerializerFactory,
    Compressor, CompressionFormat, CompressorFactory,
    JsonSerializer,
    serialization::BincodeSerializer,
};

/// 性能测试配置
#[derive(Debug, Clone)]
pub struct PerformanceTestConfig {
    /// 测试消息数量
    pub message_count: usize,
    /// 消息大小范围
    pub message_size_range: (usize, usize),
    /// 并发数
    pub concurrency: usize,
    /// 延迟目标（毫秒）
    pub latency_target_ms: f64,
}

impl Default for PerformanceTestConfig {
    fn default() -> Self {
        Self {
            message_count: 1000,
            message_size_range: (128, 2048),
            concurrency: 1,
            latency_target_ms: 5.0, // 5ms目标
        }
    }
}

/// 性能测试结果
#[derive(Debug, Clone)]
pub struct PerformanceResult {
    pub test_name: String,
    pub total_messages: usize,
    pub total_time: Duration,
    pub average_latency: Duration,
    pub p95_latency: Duration,
    pub p99_latency: Duration,
    pub max_latency: Duration,
    pub min_latency: Duration,
    pub throughput_msg_per_sec: f64,
    pub meets_target: bool,
    pub compression_ratio: Option<f64>,
    pub total_bytes_processed: u64,
}

impl PerformanceResult {
    pub fn new(
        test_name: String,
        latencies: &[Duration],
        target_ms: f64,
        compression_ratio: Option<f64>,
        total_bytes: u64,
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
        
        let meets_target = average_latency.as_secs_f64() * 1000.0 <= target_ms;

        Self {
            test_name,
            total_messages,
            total_time,
            average_latency,
            p95_latency,
            p99_latency,
            max_latency,
            min_latency,
            throughput_msg_per_sec,
            meets_target,
            compression_ratio,
            total_bytes_processed: total_bytes,
        }
    }
    
    pub fn print_summary(&self) {
        println!("🔬 {}", self.test_name);
        println!("  消息数量: {}", self.total_messages);
        println!("  总耗时: {:.2}ms", self.total_time.as_secs_f64() * 1000.0);
        println!("  平均延迟: {:.3}ms {}", 
                 self.average_latency.as_secs_f64() * 1000.0,
                 if self.meets_target { "✅" } else { "❌" });
        println!("  P95延迟: {:.3}ms", self.p95_latency.as_secs_f64() * 1000.0);
        println!("  P99延迟: {:.3}ms", self.p99_latency.as_secs_f64() * 1000.0);
        println!("  最大延迟: {:.3}ms", self.max_latency.as_secs_f64() * 1000.0);
        println!("  最小延迟: {:.3}ms", self.min_latency.as_secs_f64() * 1000.0);
        println!("  吞吐量: {:.0} msg/s", self.throughput_msg_per_sec);
        
        if let Some(ratio) = self.compression_ratio {
            println!("  压缩比: {:.1}%", ratio * 100.0);
            println!("  数据处理: {} 字节", self.total_bytes_processed);
        }
        
        println!();
    }
}

/// 消息生成器
pub struct MessageGenerator {
    counter: Arc<Mutex<u64>>,
}

impl MessageGenerator {
    pub fn new() -> Self {
        Self {
            counter: Arc::new(Mutex::new(0)),
        }
    }
    
    /// 生成测试消息
    pub async fn generate_message(&self, size: usize) -> Frame {
        let mut counter = self.counter.lock().await;
        *counter += 1;
        
        // 生成有一定重复模式的数据，更接近真实场景
        let mut payload = Vec::with_capacity(size);
        let patterns = [
            "user_data",
            "timestamp:",
            "event_type:",
            "session_id:",
            "metadata:",
            "payload_data:",
        ];
        
        let mut pattern_idx = 0;
        while payload.len() < size {
            let pattern = patterns[pattern_idx % patterns.len()];
            payload.extend_from_slice(pattern.as_bytes());
            
            // 添加变化数据
            let num = (*counter % 1000).to_string();
            payload.extend_from_slice(num.as_bytes());
            payload.push(b' ');
            
            pattern_idx += 1;
        }
        
        payload.truncate(size);
        
        Frame::new(
            MessageType::Data,
            *counter,
            Reliability::AtLeastOnce,
            payload,
        )
    }
    
    /// 批量生成消息
    pub async fn generate_batch(&self, count: usize, size_range: (usize, usize)) -> Vec<Frame> {
        let mut messages = Vec::with_capacity(count);
        
        for _ in 0..count {
            let size = fastrand::usize(size_range.0..=size_range.1);
            messages.push(self.generate_message(size).await);
        }
        
        messages
    }
}

/// 性能测试器
pub struct PerformanceTester {
    config: PerformanceTestConfig,
    generator: MessageGenerator,
}

impl PerformanceTester {
    pub fn new(config: PerformanceTestConfig) -> Self {
        Self {
            config,
            generator: MessageGenerator::new(),
        }
    }
    
    /// 测试序列化器性能
    pub async fn test_serializer(
        &self,
        name: &str,
        serializer: &dyn FrameSerializer,
    ) -> PerformanceResult {
        let messages = self.generator.generate_batch(
            self.config.message_count,
            self.config.message_size_range,
        ).await;
        
        let mut latencies = Vec::with_capacity(self.config.message_count);
        let mut total_bytes = 0u64;
        
        for message in &messages {
            let start = Instant::now();
            
            // 序列化
            let serialized = serializer.serialize(message).await.unwrap();
            
            // 反序列化
            let _deserialized = serializer.deserialize(&serialized).await.unwrap();
            
            let latency = start.elapsed();
            latencies.push(latency);
            total_bytes += serialized.len() as u64;
        }
        
        PerformanceResult::new(
            format!("{} 序列化器", name),
            &latencies,
            self.config.latency_target_ms,
            None,
            total_bytes,
        )
    }
    
    /// 测试压缩器性能
    pub async fn test_compressor(
        &self,
        name: &str,
        compressor: &dyn Compressor,
    ) -> PerformanceResult {
        // 先生成序列化数据
        let serializer = BincodeSerializer::new();
        let messages = self.generator.generate_batch(
            self.config.message_count,
            self.config.message_size_range,
        ).await;
        
        let mut serialized_data = Vec::new();
        let mut original_total_bytes = 0u64;
        
        for message in &messages {
            let data = serializer.serialize(message).await.unwrap();
            original_total_bytes += data.len() as u64;
            serialized_data.push(data);
        }
        
        // 测试压缩性能
        let mut latencies = Vec::new();
        let mut compressed_total_bytes = 0u64;
        
        for data in &serialized_data {
            let start = Instant::now();
            
            // 压缩
            let compressed = compressor.compress(data).await.unwrap();
            
            // 解压
            let _decompressed = compressor.decompress(&compressed.data).await.unwrap();
            
            let latency = start.elapsed();
            latencies.push(latency);
            compressed_total_bytes += compressed.compressed_size as u64;
        }
        
        let compression_ratio = compressed_total_bytes as f64 / original_total_bytes as f64;
        
        PerformanceResult::new(
            format!("{} 压缩器", name),
            &latencies,
            self.config.latency_target_ms,
            Some(compression_ratio),
            compressed_total_bytes,
        )
    }
    
    /// 测试序列化+压缩组合
    pub async fn test_combination(
        &self,
        name: &str,
        serializer: &dyn FrameSerializer,
        compressor: &dyn Compressor,
    ) -> PerformanceResult {
        let messages = self.generator.generate_batch(
            self.config.message_count,
            self.config.message_size_range,
        ).await;
        
        let mut latencies = Vec::new();
        let mut original_bytes = 0u64;
        let mut final_bytes = 0u64;
        
        for message in &messages {
            let start = Instant::now();
            
            // 序列化
            let serialized = serializer.serialize(message).await.unwrap();
            original_bytes += serialized.len() as u64;
            
            // 压缩
            let compressed = compressor.compress(&serialized).await.unwrap();
            final_bytes += compressed.compressed_size as u64;
            
            // 解压
            let decompressed = compressor.decompress(&compressed.data).await.unwrap();
            
            // 反序列化
            let _restored = serializer.deserialize(&decompressed).await.unwrap();
            
            let latency = start.elapsed();
            latencies.push(latency);
        }
        
        let compression_ratio = final_bytes as f64 / original_bytes as f64;
        
        PerformanceResult::new(
            format!("{} + {}", name, compressor.name()),
            &latencies,
            self.config.latency_target_ms,
            Some(compression_ratio),
            final_bytes,
        )
    }
    
    /// 并发性能测试
    pub async fn test_concurrent_combination(
        &self,
        name: &str,
        serializer: &dyn FrameSerializer,
        compressor: &dyn Compressor,
    ) -> PerformanceResult {
        let messages = Arc::new(self.generator.generate_batch(
            self.config.message_count,
            self.config.message_size_range,
        ).await);
        
        let chunk_size = self.config.message_count / self.config.concurrency;
        let latencies = Arc::new(Mutex::new(Vec::new()));
        let total_bytes = Arc::new(Mutex::new(0u64));
        
        let mut tasks = Vec::new();
        
        for i in 0..self.config.concurrency {
            let start_idx = i * chunk_size;
            let end_idx = if i == self.config.concurrency - 1 {
                self.config.message_count
            } else {
                start_idx + chunk_size
            };
            
            let chunk_messages = Arc::clone(&messages);
            let chunk_latencies = Arc::clone(&latencies);
            let chunk_bytes = Arc::clone(&total_bytes);
            let serializer = serializer.clone_box();
            let compressor = compressor.clone_box();
            
            let task = tokio::spawn(async move {
                let mut local_latencies = Vec::new();
                let mut local_bytes = 0u64;
                
                for idx in start_idx..end_idx {
                    if let Some(message) = chunk_messages.get(idx) {
                        let start = Instant::now();
                        
                        // 序列化 + 压缩 + 解压 + 反序列化
                        let serialized = serializer.serialize(message).await.unwrap();
                        let compressed = compressor.compress(&serialized).await.unwrap();
                        let decompressed = compressor.decompress(&compressed.data).await.unwrap();
                        let _restored = serializer.deserialize(&decompressed).await.unwrap();
                        
                        let latency = start.elapsed();
                        local_latencies.push(latency);
                        local_bytes += compressed.compressed_size as u64;
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
            task.await.unwrap();
        }
        
        let final_latencies = latencies.lock().await.clone();
        let final_bytes = *total_bytes.lock().await;
        
        PerformanceResult::new(
            format!("{} (并发x{})", name, self.config.concurrency),
            &final_latencies,
            self.config.latency_target_ms,
            None,
            final_bytes,
        )
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 超低延迟优化性能演示");
    println!("========================================");
    
    let config = PerformanceTestConfig {
        message_count: 1000,
        message_size_range: (256, 1024),
        concurrency: 4,
        latency_target_ms: 5.0, // 目标5ms
    };
    
    println!("📊 测试配置:");
    println!("  消息数量: {}", config.message_count);
    println!("  消息大小: {}-{} 字节", config.message_size_range.0, config.message_size_range.1);
    println!("  延迟目标: {:.1}ms", config.latency_target_ms);
    println!("  并发数: {}", config.concurrency);
    println!();
    
    let tester = PerformanceTester::new(config);
    let mut results = Vec::new();
    
    // 1. 测试序列化器性能
    println!("🔧 序列化器性能测试:");
    println!("----------------------------------------");
    
    let json_serializer = JsonSerializer::new();
    let bincode_serializer = BincodeSerializer::new();
    
    let json_result = tester.test_serializer("JSON", &json_serializer).await;
    json_result.print_summary();
    results.push(json_result);
    
    let bincode_result = tester.test_serializer("Bincode", &bincode_serializer).await;
    bincode_result.print_summary();
    results.push(bincode_result);
    
    // 2. 测试压缩器性能
    println!("🗜️ 压缩器性能测试:");
    println!("----------------------------------------");
    
    let lz4_compressor = CompressorFactory::create_static(CompressionFormat::Lz4);
    let snappy_compressor = CompressorFactory::create_static(CompressionFormat::Snappy);
    let gzip_compressor = CompressorFactory::create_static(CompressionFormat::Gzip);
    
    let lz4_result = tester.test_compressor("LZ4", lz4_compressor.as_ref()).await;
    lz4_result.print_summary();
    results.push(lz4_result);
    
    let snappy_result = tester.test_compressor("Snappy", snappy_compressor.as_ref()).await;
    snappy_result.print_summary();
    results.push(snappy_result);
    
    let gzip_result = tester.test_compressor("Gzip", gzip_compressor.as_ref()).await;
    gzip_result.print_summary();
    results.push(gzip_result);
    
    // 3. 测试最优组合
    println!("🎯 最优组合性能测试:");
    println!("----------------------------------------");
    
    let ultra_combo_result = tester.test_combination(
        "Bincode + LZ4 (超低延迟)",
        &bincode_serializer,
        lz4_compressor.as_ref(),
    ).await;
    ultra_combo_result.print_summary();
    results.push(ultra_combo_result);
    
    let balanced_combo_result = tester.test_combination(
        "Bincode + Snappy (平衡)",
        &bincode_serializer,
        snappy_compressor.as_ref(),
    ).await;
    balanced_combo_result.print_summary();
    results.push(balanced_combo_result);
    
    let web_combo_result = tester.test_combination(
        "JSON + Snappy (Web友好)",
        &json_serializer,
        snappy_compressor.as_ref(),
    ).await;
    web_combo_result.print_summary();
    results.push(web_combo_result);
    
    // 4. 并发性能测试
    println!("⚡ 并发性能测试:");
    println!("----------------------------------------");
    
    let concurrent_result = tester.test_concurrent_combination(
        "Bincode + LZ4",
        &bincode_serializer,
        lz4_compressor.as_ref(),
    ).await;
    concurrent_result.print_summary();
    results.push(concurrent_result);
    
    // 5. 性能总结
    println!("📈 性能总结报告:");
    println!("========================================");
    
    // 按平均延迟排序
    results.sort_by(|a, b| a.average_latency.cmp(&b.average_latency));
    
    println!("🏆 延迟排行榜 (目标: {:.1}ms):", tester.config.latency_target_ms);
    for (i, result) in results.iter().enumerate() {
        let status = if result.meets_target { "✅" } else { "❌" };
        println!("  {}. {} - {:.3}ms {}",
                i + 1,
                result.test_name,
                result.average_latency.as_secs_f64() * 1000.0,
                status
        );
    }
    
    println!();
    
    // 找出最佳配置
    if let Some(best) = results.iter().find(|r| r.meets_target) {
        println!("🎯 推荐配置: {}", best.test_name);
        println!("  平均延迟: {:.3}ms", best.average_latency.as_secs_f64() * 1000.0);
        println!("  P99延迟: {:.3}ms", best.p99_latency.as_secs_f64() * 1000.0);
        println!("  吞吐量: {:.0} msg/s", best.throughput_msg_per_sec);
        
        if let Some(ratio) = best.compression_ratio {
            println!("  压缩效果: {:.1}% 压缩比", ratio * 100.0);
        }
    } else {
        println!("⚠️  没有配置满足 {:.1}ms 延迟目标", tester.config.latency_target_ms);
        println!("  建议调整目标或优化实现");
    }
    
    println!();
    
    // 性能优化建议
    println!("💡 性能优化建议:");
    println!("----------------------------------------");
    
    let fastest_serializer = results.iter()
        .filter(|r| r.test_name.contains("序列化器"))
        .min_by(|a, b| a.average_latency.cmp(&b.average_latency));
    
    if let Some(fastest) = fastest_serializer {
        println!("  1. 最快序列化器: {}", fastest.test_name);
    }
    
    let best_compressor = results.iter()
        .filter(|r| r.test_name.contains("压缩器"))
        .min_by(|a, b| a.average_latency.cmp(&b.average_latency));
    
    if let Some(best) = best_compressor {
        println!("  2. 最佳压缩器: {}", best.test_name);
    }
    
    println!("  3. 进一步优化建议:");
    println!("     - 使用零拷贝序列化器减少内存分配");
    println!("     - 实现自适应压缩策略");
    println!("     - 启用连接池和预连接");
    println!("     - 使用QUIC协议替代WebSocket");
    println!("     - 优化消息批处理策略");
    
    println!("\n✅ 性能演示完成！");
    
    Ok(())
}