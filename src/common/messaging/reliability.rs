//! 可靠性管理器
//!
//! 提供确认、重传、去重功能

use crate::common::protocol::flare_core::Frame;
use crate::common::error::FlareError;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// 待确认的消息
struct PendingMessage {
    frame: Frame,
    send_time: Instant,
    retry_count: u32,
}

/// 可靠性管理器
pub struct ReliabilityManager {
    /// 待确认的消息（message_id -> PendingMessage）
    pending_acks: HashMap<String, PendingMessage>,
    /// 已接收的消息 ID（用于去重）
    received_ids: HashSet<String>,
    /// 超时时间
    timeout: Duration,
    /// 最大重试次数
    max_retries: u32,
}

impl ReliabilityManager {
    /// 创建新的可靠性管理器
    pub fn new() -> Self {
        Self {
            pending_acks: HashMap::new(),
            received_ids: HashSet::new(),
            timeout: Duration::from_secs(5),
            max_retries: 3,
        }
    }
    
    /// 设置超时时间
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    
    /// 设置最大重试次数
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }
    
    /// 发送消息并等待确认
    pub fn send_with_ack(&mut self, frame: Frame) -> Result<(), FlareError> {
        let message_id = frame.message_id.clone();
        
        self.pending_acks.insert(
            message_id,
            PendingMessage {
                frame,
                send_time: Instant::now(),
                retry_count: 0,
            },
        );
        
        Ok(())
    }
    
    /// 处理确认消息
    /// 返回 true 表示成功确认，false 表示消息不存在
    pub fn handle_ack(&mut self, message_id: &str) -> bool {
        self.pending_acks.remove(message_id).is_some()
    }
    
    /// 检查超时并返回需要重传的消息
    pub fn check_timeout(&mut self) -> Vec<Frame> {
        let mut retry_frames = Vec::new();
        let now = Instant::now();
        
        self.pending_acks.retain(|_, pending| {
            if now.duration_since(pending.send_time) > self.timeout {
                if pending.retry_count < self.max_retries {
                    // 需要重传
                    retry_frames.push(pending.frame.clone());
                    
                    // 更新重传信息
                    pending.send_time = now;
                    pending.retry_count += 1;
                    
                    true // 保留，等待下次检查
                } else {
                    // 超过最大重试次数，放弃
                    false // 移除
                }
            } else {
                true // 未超时，保留
            }
        });
        
        retry_frames
    }
    
    /// 去重检查
    /// 返回 true 表示是重复消息，false 表示是新消息
    pub fn is_duplicate(&mut self, message_id: &str) -> bool {
        if self.received_ids.contains(message_id) {
            true
        } else {
            self.received_ids.insert(message_id.to_string());
            false
        }
    }
    
    /// 清理旧的接收记录（避免内存泄漏）
    pub fn cleanup_received(&mut self, max_size: usize) {
        if self.received_ids.len() > max_size {
            // 简单策略：全部清空
            // 实际应用中可以使用 LRU 或时间窗口
            self.received_ids.clear();
        }
    }
    
    /// 获取待确认消息数量
    pub fn pending_count(&self) -> usize {
        self.pending_acks.len()
    }
    
    /// 获取已接收消息数量
    pub fn received_count(&self) -> usize {
        self.received_ids.len()
    }
}

impl Default for ReliabilityManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    fn create_frame(id: &str) -> Frame {
        Frame {
            message_id: id.to_string(),
            priority: 0,
            timestamp: 0,
            reliability: 1, // AtLeastOnce
            command: None,
            session_id: None,
            compression: None,
            encrypted: false,
            metadata: std::collections::HashMap::new(),
        }
    }
    
    #[test]
    fn test_ack_handling() {
        let mut manager = ReliabilityManager::new();
        
        let frame = create_frame("msg-001");
        manager.send_with_ack(frame).unwrap();
        
        assert_eq!(manager.pending_count(), 1);
        
        // 确认消息
        assert!(manager.handle_ack("msg-001"));
        assert_eq!(manager.pending_count(), 0);
        
        // 重复确认
        assert!(!manager.handle_ack("msg-001"));
    }
    
    #[test]
    fn test_duplicate_detection() {
        let mut manager = ReliabilityManager::new();
        
        // 第一次：新消息
        assert!(!manager.is_duplicate("msg-001"));
        
        // 第二次：重复消息
        assert!(manager.is_duplicate("msg-001"));
        
        // 不同消息
        assert!(!manager.is_duplicate("msg-002"));
    }
    
    #[test]
    fn test_timeout_retry() {
        let mut manager = ReliabilityManager::new()
            .with_timeout(Duration::from_millis(100))
            .with_max_retries(2);
        
        let frame = create_frame("msg-001");
        manager.send_with_ack(frame).unwrap();
        
        // 立即检查，不应该重传
        let retry = manager.check_timeout();
        assert_eq!(retry.len(), 0);
        
        // 等待超时
        std::thread::sleep(Duration::from_millis(150));
        
        // 第一次重传
        let retry = manager.check_timeout();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].message_id, "msg-001");
        
        // 仍有待确认消息
        assert_eq!(manager.pending_count(), 1);
    }
    
    #[test]
    fn test_max_retries() {
        let mut manager = ReliabilityManager::new()
            .with_timeout(Duration::from_millis(50))
            .with_max_retries(1);
        
        let frame = create_frame("msg-001");
        manager.send_with_ack(frame).unwrap();
        
        // 第一次重传
        std::thread::sleep(Duration::from_millis(60));
        let retry = manager.check_timeout();
        assert_eq!(retry.len(), 1);
        assert_eq!(manager.pending_count(), 1);
        
        // 第二次检查，超过最大重试次数，应该移除
        std::thread::sleep(Duration::from_millis(60));
        let retry = manager.check_timeout();
        assert_eq!(retry.len(), 0);
        assert_eq!(manager.pending_count(), 0);
    }
}
