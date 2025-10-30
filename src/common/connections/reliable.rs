//! 可靠消息传输模块
//! 
//! 实现类似TCP的ACK确认机制，保证消息可靠送达

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::time::Duration;
use crate::common::protocol::frame::Frame;
use crate::common::protocol::reliability::Reliability;
use crate::common::error::FlareError;

/// 待确认消息
#[derive(Clone, Debug)]
struct PendingMessage {
    /// 序列号
    #[allow(dead_code)]
    seq: u64,
    /// 消息帧
    #[allow(dead_code)]
    frame: Frame,
    /// 发送时间戳
    send_time: u64,
    /// 重传次数
    retry_count: u32,
}

/// 可靠消息通道
/// 
/// 核心特性：
/// - 消息Seq序列号（严格递增）
/// - ACK确认机制
/// - 超时重传（3s、6s、12s指数增长）
/// - 消息去重
/// - 顺序保证
pub struct ReliableMessageChannel {
    /// 下一个消息序列号
    next_seq: AtomicU64,
    /// 待确认消息队列
    pending_ack: Arc<RwLock<HashMap<u64, PendingMessage>>>,
    /// 重传超时时间（毫秒）
    retransmit_timeout_ms: u64,
    /// 最大重传次数
    max_retries: u32,
    /// 已接收消息序列号（用于去重）
    received_seqs: Arc<RwLock<HashMap<u64, u64>>>,
    /// 重排序缓冲区（保证顺序）
    reorder_buffer: Arc<RwLock<HashMap<u64, Frame>>>,
    /// 期望的下一个序列号
    expected_seq: AtomicU64,
}

impl Default for ReliableMessageChannel {
    fn default() -> Self {
        Self::new()
    }
}

impl ReliableMessageChannel {
    /// 创建新的可靠消息通道
    pub fn new() -> Self {
        Self {
            next_seq: AtomicU64::new(1),
            pending_ack: Arc::new(RwLock::new(HashMap::new())),
            retransmit_timeout_ms: 3000,  // 3秒超时
            max_retries: 5,
            received_seqs: Arc::new(RwLock::new(HashMap::new())),
            reorder_buffer: Arc::new(RwLock::new(HashMap::new())),
            expected_seq: AtomicU64::new(1),
        }
    }
    
    /// 发送可靠消息
    /// 
    /// # 参数
    /// - `payload`: 消息负载
    /// - `send_fn`: 实际发送函数
    /// 
    /// # 返回
    /// - `Ok(seq)`: 返回消息序列号
    /// - `Err(FlareError)`: 发送失败
    pub async fn send_reliable<F>(
        &self,
        payload: Vec<u8>,
        send_fn: F,
    ) -> Result<u64, FlareError>
    where
        F: Fn(Frame) -> Result<(), FlareError> + Send + 'static,
    {
        // 1. 分配序列号
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        
        // 2. 创建消息帧
        let frame = Frame {
            message_id: seq.to_string(),
            payload: payload.clone().into(),
            reliability: Reliability::AtLeastOnce,
            command: crate::common::protocol::commands::Command::Message(
                crate::common::protocol::commands::MessageCmd::Data(
                    crate::common::protocol::commands::DataCommand { data: payload }
                )
            ),
        };
        
        // 3. 发送消息
        send_fn(frame.clone())?;
        
        // 4. 加入待确认队列
        if let Ok(mut pending) = self.pending_ack.write() {
            pending.insert(seq, PendingMessage {
                seq,
                frame: frame.clone(),
                send_time: current_epoch_ms(),
                retry_count: 0,
            });
        }
        
        // 5. 启动超时重传任务
        let pending_clone = Arc::clone(&self.pending_ack);
        let timeout = self.retransmit_timeout_ms;
        let max_retries = self.max_retries;
        
        tokio::spawn(async move {
            let mut retry_count = 0;
            let mut current_timeout = timeout;
            
            while retry_count < max_retries {
                tokio::time::sleep(Duration::from_millis(current_timeout)).await;
                
                // 检查是否已收到ACK
                let need_retransmit = if let Ok(pending) = pending_clone.read() {
                    pending.contains_key(&seq)
                } else {
                    false
                };
                
                if !need_retransmit {
                    break; // 已收到ACK
                }
                
                // 重传
                retry_count += 1;
                if send_fn(frame.clone()).is_err() {
                    break; // 发送失败，放弃重传
                }
                
                // 更新重传次数
                if let Ok(mut pending) = pending_clone.write() {
                    if let Some(msg) = pending.get_mut(&seq) {
                        msg.retry_count = retry_count;
                    }
                }
                
                // 指数退避
                current_timeout *= 2;
            }
            
            // 达到最大重传次数，从队列移除
            if retry_count >= max_retries {
                if let Ok(mut pending) = pending_clone.write() {
                    pending.remove(&seq);
                }
            }
        });
        
        Ok(seq)
    }
    
    /// 处理ACK响应
    /// 
    /// # 参数
    /// - `ack_seq`: 确认的序列号
    pub fn handle_ack(&self, ack_seq: u64) {
        if let Ok(mut pending) = self.pending_ack.write() {
            pending.remove(&ack_seq);
        }
    }
    
    /// 处理接收到的消息（去重 + 重排序）
    /// 
    /// # 参数
    /// - `frame`: 接收到的消息帧
    /// - `seq`: 消息序列号
    /// 
    /// # 返回
    /// - `Some(frames)`: 按序交付的消息列表
    /// - `None`: 消息重复或乱序，暂存到重排序缓冲区
    pub fn handle_received(&self, frame: Frame, seq: u64) -> Option<Vec<Frame>> {
        // 1. 去重检查
        if let Ok(mut received) = self.received_seqs.write() {
            if received.contains_key(&seq) {
                return None; // 重复消息，丢弃
            }
            received.insert(seq, current_epoch_ms());
            
            // 清理过期记录（保留最近1000条）
            if received.len() > 1000 {
                let min_seq = seq.saturating_sub(1000);
                received.retain(|&k, _| k >= min_seq);
            }
        }
        
        // 2. 检查序列号
        let expected = self.expected_seq.load(Ordering::Relaxed);
        
        if seq == expected {
            // 正好是期望的序列号，立即交付
            self.expected_seq.fetch_add(1, Ordering::Relaxed);
            
            // 检查重排序缓冲区是否有后续消息
            let mut deliverable = vec![frame];
            if let Ok(mut buffer) = self.reorder_buffer.write() {
                let mut next_seq = expected + 1;
                while let Some(buffered_frame) = buffer.remove(&next_seq) {
                    deliverable.push(buffered_frame);
                    self.expected_seq.fetch_add(1, Ordering::Relaxed);
                    next_seq += 1;
                }
            }
            
            Some(deliverable)
        } else if seq > expected {
            // 序列号大于期望值，暂存到重排序缓冲区
            if let Ok(mut buffer) = self.reorder_buffer.write() {
                buffer.insert(seq, frame);
                
                // 限制缓冲区大小
                if buffer.len() > 100 {
                    // 清理最旧的消息
                    if let Some(&min_seq) = buffer.keys().min() {
                        buffer.remove(&min_seq);
                    }
                }
            }
            None
        } else {
            // 序列号小于期望值，可能是重复或延迟的消息
            None
        }
    }
    
    /// 获取待确认消息数量
    pub fn pending_count(&self) -> usize {
        if let Ok(pending) = self.pending_ack.read() {
            pending.len()
        } else {
            0
        }
    }
    
    /// 获取重排序缓冲区大小
    pub fn reorder_buffer_size(&self) -> usize {
        if let Ok(buffer) = self.reorder_buffer.read() {
            buffer.len()
        } else {
            0
        }
    }
    
    /// 清理超时的待确认消息
    pub fn cleanup_expired(&self, timeout_ms: u64) {
        let now = current_epoch_ms();
        
        if let Ok(mut pending) = self.pending_ack.write() {
            pending.retain(|_, msg| {
                now - msg.send_time < timeout_ms * (msg.retry_count as u64 + 1)
            });
        }
    }
}

/// 获取当前时间戳（毫秒）
#[inline]
fn current_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence_generation() {
        let channel = ReliableMessageChannel::new();
        
        let seq1 = channel.next_seq.fetch_add(1, Ordering::SeqCst);
        let seq2 = channel.next_seq.fetch_add(1, Ordering::SeqCst);
        let seq3 = channel.next_seq.fetch_add(1, Ordering::SeqCst);
        
        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(seq3, 3);
    }
    
    #[test]
    fn test_deduplication() {
        use crate::common::protocol::commands::{Command, MessageCmd, DataCommand};
        
        let channel = ReliableMessageChannel::new();
        
        let frame = Frame {
            message_id: "1".to_string(),
            payload: vec![1, 2, 3].into(),
            reliability: Reliability::AtLeastOnce,
            command: Command::Message(MessageCmd::Data(DataCommand { data: vec![1, 2, 3] })),
        };
        
        // 第一次接收
        let result1 = channel.handle_received(frame.clone(), 1);
        assert!(result1.is_some());
        
        // 重复接收（应该被去重）
        let result2 = channel.handle_received(frame.clone(), 1);
        assert!(result2.is_none());
    }
    
    #[test]
    fn test_reordering() {
        use crate::common::protocol::commands::{Command, MessageCmd, DataCommand};
        
        let channel = ReliableMessageChannel::new();
        
        let frame1 = Frame {
            message_id: "1".to_string(),
            payload: vec![1].into(),
            reliability: Reliability::AtLeastOnce,
            command: Command::Message(MessageCmd::Data(DataCommand { data: vec![1] })),
        };
        
        let frame2 = Frame {
            message_id: "2".to_string(),
            payload: vec![2].into(),
            reliability: Reliability::AtLeastOnce,
            command: Command::Message(MessageCmd::Data(DataCommand { data: vec![2] })),
        };
        
        let frame3 = Frame {
            message_id: "3".to_string(),
            payload: vec![3].into(),
            reliability: Reliability::AtLeastOnce,
            command: Command::Message(MessageCmd::Data(DataCommand { data: vec![3] })),
        };
        
        // 乱序接收：3 -> 1 -> 2
        
        // 接收seq=3（乱序，暂存）
        let result3 = channel.handle_received(frame3.clone(), 3);
        assert!(result3.is_none());
        assert_eq!(channel.reorder_buffer_size(), 1);
        
        // 接收seq=1（按序，立即交付）
        let result1 = channel.handle_received(frame1.clone(), 1);
        assert!(result1.is_some());
        assert_eq!(result1.unwrap().len(), 1);
        
        // 接收seq=2（按序，立即交付，同时交付seq=3）
        let result2 = channel.handle_received(frame2.clone(), 2);
        assert!(result2.is_some());
        let delivered = result2.unwrap();
        assert_eq!(delivered.len(), 2); // 应该交付 frame2 和 frame3
        assert_eq!(channel.reorder_buffer_size(), 0);
    }
}
