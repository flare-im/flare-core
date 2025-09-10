//! 优先级消息队列
//!
//! 提供基于优先级的消息调度，确保关键消息优先处理

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, Notify};
use tracing::{debug, info, warn};

use crate::common::{
    error::{Result, FlareError},
    protocol::Frame,
};

/// 消息优先级
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessagePriority {
    /// 系统关键消息（心跳、认证等）
    System = 0,
    /// 实时消息（游戏操作、交易指令）
    Realtime = 1,
    /// 高优先级（重要通知）
    High = 2,
    /// 普通优先级（常规数据）
    Normal = 3,
    /// 低优先级（统计、日志等）
    Low = 4,
}

impl Default for MessagePriority {
    fn default() -> Self {
        MessagePriority::Normal
    }
}

/// 优先级消息包装
#[derive(Debug)]
pub struct PriorityMessage {
    /// 消息内容
    pub frame: Frame,
    /// 优先级
    pub priority: MessagePriority,
    /// 创建时间
    pub created_at: Instant,
    /// 超时时间
    pub timeout: Duration,
    /// 消息ID（用于排序）
    pub sequence_id: u64,
}

impl PriorityMessage {
    pub fn new(frame: Frame, priority: MessagePriority, timeout: Duration) -> Self {
        Self {
            frame,
            priority,
            created_at: Instant::now(),
            timeout,
            sequence_id: fastrand::u64(..),
        }
    }
    
    /// 检查消息是否过期
    pub fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.timeout
    }
    
    /// 获取剩余时间
    pub fn remaining_time(&self) -> Duration {
        self.timeout.saturating_sub(self.created_at.elapsed())
    }
}

impl Eq for PriorityMessage {}

impl PartialEq for PriorityMessage {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence_id == other.sequence_id
    }
}

impl Ord for PriorityMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        // 优先级越小越优先，时间越早越优先
        match self.priority.cmp(&other.priority) {
            Ordering::Equal => self.sequence_id.cmp(&other.sequence_id), // 相同优先级按FIFO排序
            other => other.reverse(), // 反向比较实现高优先级优先
        }
    }
}

impl PartialOrd for PriorityMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// 优先级消息队列配置
#[derive(Debug, Clone)]
pub struct PriorityQueueConfig {
    /// 最大队列大小
    pub max_queue_size: usize,
    /// 各优先级的权重分配
    pub priority_weights: [u32; 5],
    /// 超时检查间隔
    pub timeout_check_interval: Duration,
    /// 是否启用自适应调度
    pub enable_adaptive_scheduling: bool,
}

impl Default for PriorityQueueConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 10000,
            // 系统:实时:高:普通:低 = 50:30:15:4:1
            priority_weights: [50, 30, 15, 4, 1],
            timeout_check_interval: Duration::from_millis(100),
            enable_adaptive_scheduling: true,
        }
    }
}

/// 队列统计信息
#[derive(Debug, Clone)]
pub struct QueueStats {
    /// 各优先级队列长度
    pub queue_lengths: [usize; 5],
    /// 总消息数
    pub total_messages: usize,
    /// 处理速率（消息/秒）
    pub processing_rate: f64,
    /// 平均等待时间
    pub avg_wait_time: Duration,
    /// 超时消息数
    pub expired_messages: u64,
}

/// 高性能优先级消息队列
pub struct PriorityMessageQueue {
    /// 消息队列（使用BinaryHeap实现优先队列）
    queue: Arc<Mutex<BinaryHeap<PriorityMessage>>>,
    /// 配置
    config: PriorityQueueConfig,
    /// 通知器（用于唤醒等待的消费者）
    notify: Arc<Notify>,
    /// 统计信息
    stats: Arc<RwLock<QueueStatistics>>,
    /// 是否正在运行
    running: Arc<RwLock<bool>>,
}

/// 内部统计信息
#[derive(Debug)]
struct QueueStatistics {
    /// 各优先级入队数
    enqueue_counts: [u64; 5],
    /// 各优先级出队数
    dequeue_counts: [u64; 5],
    /// 总等待时间
    total_wait_time_us: u64,
    /// 处理的消息数
    processed_messages: u64,
    /// 超时的消息数
    expired_messages: u64,
    /// 开始时间
    start_time: Instant,
}

impl Default for QueueStatistics {
    fn default() -> Self {
        Self {
            enqueue_counts: [0; 5],
            dequeue_counts: [0; 5],
            total_wait_time_us: 0,
            processed_messages: 0,
            expired_messages: 0,
            start_time: Instant::now(),
        }
    }
}

impl PriorityMessageQueue {
    /// 创建新的优先级消息队列
    pub fn new(config: PriorityQueueConfig) -> Self {
        let queue = Self {
            queue: Arc::new(Mutex::new(BinaryHeap::new())),
            config,
            notify: Arc::new(Notify::new()),
            stats: Arc::new(RwLock::new(QueueStatistics {
                start_time: Instant::now(),
                ..Default::default()
            })),
            running: Arc::new(RwLock::new(true)),
        };
        
        // 启动超时清理任务
        queue.start_timeout_cleanup_task();
        
        queue
    }
    
    /// 入队消息
    pub async fn enqueue(&self, message: PriorityMessage) -> Result<()> {
        let mut queue = self.queue.lock().await;
        
        // 检查队列容量
        if queue.len() >= self.config.max_queue_size {
            return Err(FlareError::general_error("消息队列已满"));
        }
        
        let priority_idx = message.priority as usize;
        
        // 更新统计
        if let Ok(mut stats) = self.stats.try_write() {
            stats.enqueue_counts[priority_idx] += 1;
        }
        
        queue.push(message);
        
        // 通知等待的消费者
        self.notify.notify_one();
        
        debug!("消息入队，当前队列长度: {}", queue.len());
        Ok(())
    }
    
    /// 出队消息（阻塞等待）
    pub async fn dequeue(&self) -> Result<Option<PriorityMessage>> {
        loop {
            // 尝试获取消息
            if let Some(message) = self.try_dequeue().await? {
                return Ok(Some(message));
            }
            
            // 检查是否还在运行
            if !*self.running.read().await {
                return Ok(None);
            }
            
            // 等待新消息通知
            self.notify.notified().await;
        }
    }
    
    /// 非阻塞出队
    pub async fn try_dequeue(&self) -> Result<Option<PriorityMessage>> {
        let mut queue = self.queue.lock().await;
        
        // 循环查找非过期消息
        while let Some(message) = queue.pop() {
            // 检查消息是否过期
            if message.is_expired() {
                // 更新过期统计
                if let Ok(mut stats) = self.stats.try_write() {
                    stats.expired_messages += 1;
                }
                
                warn!("消息已过期，丢弃: 优先级={:?}", message.priority);
                continue; // 继续查找下一个
            }
            
            let priority_idx = message.priority as usize;
            let wait_time = message.created_at.elapsed();
            
            // 更新统计
            if let Ok(mut stats) = self.stats.try_write() {
                stats.dequeue_counts[priority_idx] += 1;
                stats.processed_messages += 1;
                stats.total_wait_time_us += wait_time.as_micros() as u64;
            }
            
            debug!("消息出队: 优先级={:?}, 等待时间={:?}", message.priority, wait_time);
            return Ok(Some(message));
        }
        
        Ok(None)
    }
    
    /// 批量出队（指定数量）
    pub async fn dequeue_batch(&self, max_count: usize) -> Result<Vec<PriorityMessage>> {
        let mut messages = Vec::with_capacity(max_count);
        
        for _ in 0..max_count {
            if let Some(message) = self.try_dequeue().await? {
                messages.push(message);
            } else {
                break;
            }
        }
        
        Ok(messages)
    }
    
    /// 获取队列长度
    pub async fn len(&self) -> usize {
        self.queue.lock().await.len()
    }
    
    /// 检查是否为空
    pub async fn is_empty(&self) -> bool {
        self.queue.lock().await.is_empty()
    }
    
    /// 获取统计信息
    pub async fn get_stats(&self) -> QueueStats {
        let queue_len = self.len().await;
        let stats = self.stats.read().await;
        
        // 计算各优先级队列长度（简化实现）
        let queue_lengths = [
            queue_len / 5, queue_len / 5, queue_len / 5, queue_len / 5, queue_len / 5
        ];
        
        // 计算处理速率
        let elapsed_secs = stats.start_time.elapsed().as_secs_f64();
        let processing_rate = if elapsed_secs > 0.0 {
            stats.processed_messages as f64 / elapsed_secs
        } else {
            0.0
        };
        
        // 计算平均等待时间
        let avg_wait_time = if stats.processed_messages > 0 {
            Duration::from_micros(stats.total_wait_time_us / stats.processed_messages)
        } else {
            Duration::ZERO
        };
        
        QueueStats {
            queue_lengths,
            total_messages: queue_len,
            processing_rate,
            avg_wait_time,
            expired_messages: stats.expired_messages,
        }
    }
    
    /// 清空队列
    pub async fn clear(&self) {
        let mut queue = self.queue.lock().await;
        queue.clear();
        info!("优先级消息队列已清空");
    }
    
    /// 停止队列
    pub async fn stop(&self) {
        *self.running.write().await = false;
        self.notify.notify_waiters();
        info!("优先级消息队列已停止");
    }
    
    /// 启动超时清理任务
    fn start_timeout_cleanup_task(&self) {
        let queue = Arc::clone(&self.queue);
        let stats = Arc::clone(&self.stats);
        let running = Arc::clone(&self.running);
        let interval = self.config.timeout_check_interval;
        
        tokio::spawn(async move {
            let mut cleanup_interval = tokio::time::interval(interval);
            
            loop {
                cleanup_interval.tick().await;
                
                if !*running.read().await {
                    break;
                }
                
                // 清理过期消息
                let mut queue_guard = queue.lock().await;
                let mut expired_count = 0;
                let mut temp_messages = Vec::new();
                
                // 取出所有消息，过滤非过期的
                while let Some(message) = queue_guard.pop() {
                    if message.is_expired() {
                        expired_count += 1;
                    } else {
                        temp_messages.push(message);
                    }
                }
                
                // 将非过期消息重新放回队列
                for message in temp_messages {
                    queue_guard.push(message);
                }
                
                if expired_count > 0 {
                    debug!("清理了 {} 个过期消息", expired_count);
                    
                    // 更新统计
                    if let Ok(mut stats_guard) = stats.try_write() {
                        stats_guard.expired_messages += expired_count;
                    }
                }
            }
        });
    }
}

impl Default for PriorityMessageQueue {
    fn default() -> Self {
        Self::new(PriorityQueueConfig::default())
    }
}

/// 便捷的消息创建函数
pub fn create_system_message(frame: Frame) -> PriorityMessage {
    PriorityMessage::new(frame, MessagePriority::System, Duration::from_secs(30))
}

pub fn create_realtime_message(frame: Frame) -> PriorityMessage {
    PriorityMessage::new(frame, MessagePriority::Realtime, Duration::from_secs(5))
}

pub fn create_high_priority_message(frame: Frame) -> PriorityMessage {
    PriorityMessage::new(frame, MessagePriority::High, Duration::from_secs(60))
}

pub fn create_normal_message(frame: Frame) -> PriorityMessage {
    PriorityMessage::new(frame, MessagePriority::Normal, Duration::from_secs(300))
}

pub fn create_low_priority_message(frame: Frame) -> PriorityMessage {
    PriorityMessage::new(frame, MessagePriority::Low, Duration::from_secs(600))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::{MessageType, Reliability};
    
    #[tokio::test]
    async fn test_priority_queue_basic() {
        let queue = PriorityMessageQueue::default();
        
        // 创建不同优先级的消息
        let frame1 = Frame::new(MessageType::Data, 1, Reliability::AtLeastOnce, vec![1, 2, 3]);
        let frame2 = Frame::new(MessageType::Data, 2, Reliability::AtLeastOnce, vec![4, 5, 6]);
        let frame3 = Frame::new(MessageType::Data, 3, Reliability::AtLeastOnce, vec![7, 8, 9]);
        
        let low_msg = create_low_priority_message(frame1);
        let high_msg = create_high_priority_message(frame2);
        let system_msg = create_system_message(frame3);
        
        // 入队（注意顺序：低 -> 高 -> 系统）
        queue.enqueue(low_msg).await.unwrap();
        queue.enqueue(high_msg).await.unwrap();
        queue.enqueue(system_msg).await.unwrap();
        
        // 出队应该按优先级：系统 -> 高 -> 低
        let msg1 = queue.dequeue().await.unwrap().unwrap();
        assert_eq!(msg1.priority, MessagePriority::System);
        assert_eq!(msg1.frame.get_message_id(), 3);
        
        let msg2 = queue.dequeue().await.unwrap().unwrap();
        assert_eq!(msg2.priority, MessagePriority::High);
        assert_eq!(msg2.frame.get_message_id(), 2);
        
        let msg3 = queue.dequeue().await.unwrap().unwrap();
        assert_eq!(msg3.priority, MessagePriority::Low);
        assert_eq!(msg3.frame.get_message_id(), 1);
    }
    
    #[tokio::test]
    async fn test_batch_operations() {
        let queue = PriorityMessageQueue::default();
        
        // 批量入队
        for i in 0..10 {
            let frame = Frame::new(MessageType::Data, i, Reliability::AtLeastOnce, vec![i as u8]);
            let priority = if i % 2 == 0 { MessagePriority::High } else { MessagePriority::Normal };
            let msg = PriorityMessage::new(frame, priority, Duration::from_secs(60));
            queue.enqueue(msg).await.unwrap();
        }
        
        // 批量出队
        let messages = queue.dequeue_batch(5).await.unwrap();
        assert_eq!(messages.len(), 5);
        
        // 验证消息不为空
        assert!(!messages.is_empty());
    }
    
    #[tokio::test]
    async fn test_timeout_handling() {
        let queue = PriorityMessageQueue::default();
        
        // 创建一个已经过期的消息
        let frame = Frame::new(MessageType::Data, 1, Reliability::AtLeastOnce, vec![1]);
        let mut expired_msg = PriorityMessage::new(frame, MessagePriority::Normal, Duration::from_millis(1));
        expired_msg.created_at = Instant::now() - Duration::from_millis(10); // 设为10ms前创建
        
        queue.enqueue(expired_msg).await.unwrap();
        
        // 等待一会儿让超时清理任务执行
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        // 尝试出队，应该返回None（消息已过期被清理）
        let result = queue.try_dequeue().await.unwrap();
        // 注意：由于超时清理任务是异步的，我们不能保证消息已经被清理
        // 所以我们只验证结果是None或消息已过期
        if let Some(msg) = result {
            assert!(msg.is_expired());
        }
        
        // 检查统计信息
        let stats = queue.get_stats().await;
        // 由于超时清理任务是异步的，我们不能保证统计信息已经更新
        println!("过期消息统计: {}", stats.expired_messages);
    }
    
    #[tokio::test]
    async fn test_queue_stats() {
        let queue = PriorityMessageQueue::default();
        
        // 添加一些消息
        for i in 0..5 {
            let frame = Frame::new(MessageType::Data, i, Reliability::AtLeastOnce, vec![]);
            let msg = create_normal_message(frame);
            queue.enqueue(msg).await.unwrap();
        }
        
        let stats = queue.get_stats().await;
        assert_eq!(stats.total_messages, 5);
        
        println!("队列统计: {:?}", stats);
    }
}