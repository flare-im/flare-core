//! 优先级消息队列
//!
//! 按 priority 字段排序的消息队列

use crate::common::protocol::frame::Frame;
use std::collections::BinaryHeap;
use std::cmp::Ordering;

/// 带优先级的 Frame 包装（简化版，使用消息ID作为优先级）
#[derive(Debug)]
struct PriorityFrame {
    // 使用消息ID的长度作为简单的优先级指示
    priority: usize,
    frame: Frame,
}

impl Ord for PriorityFrame {
    fn cmp(&self, other: &Self) -> Ordering {
        // 高优先级在前
        self.priority.cmp(&other.priority)
    }
}

impl PartialOrd for PriorityFrame {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for PriorityFrame {}

impl PartialEq for PriorityFrame {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.frame.message_id == other.frame.message_id
    }
}

/// 优先级消息队列
pub struct PriorityMessageQueue {
    queue: BinaryHeap<PriorityFrame>,
}

impl PriorityMessageQueue {
    /// 创建新的优先级队列
    pub fn new() -> Self {
        Self {
            queue: BinaryHeap::new(),
        }
    }
    
    /// 推入消息
    pub fn push(&mut self, frame: Frame) {
        let priority = frame.message_id.len(); // 简化：使用消息ID长度作为优先级
        
        self.queue.push(PriorityFrame {
            priority,
            frame,
        });
    }
    
    /// 弹出最高优先级的消息
    pub fn pop(&mut self) -> Option<Frame> {
        self.queue.pop().map(|pf| pf.frame)
    }
    
    /// 查看最高优先级的消息（不移除）
    pub fn peek(&self) -> Option<&Frame> {
        self.queue.peek().map(|pf| &pf.frame)
    }
    
    /// 队列长度
    pub fn len(&self) -> usize {
        self.queue.len()
    }
    
    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
    
    /// 清空队列
    pub fn clear(&mut self) {
        self.queue.clear();
    }
}

impl Default for PriorityMessageQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    
    fn create_frame(id: &str) -> Frame {
        Frame {
            message_id: id.to_string(),
            payload: Bytes::new(),
            reliability: crate::common::protocol::reliability::Reliability::BestEffort,
            command: crate::common::protocol::commands::Command::Control(
                crate::common::protocol::commands::ControlCmd::Ping
            ),
        }
    }
    
    #[test]
    fn test_priority_order() {
        let mut queue = PriorityMessageQueue::new();
        
        // 插入不同长度ID的消息（长度作为优先级）
        queue.push(create_frame("a")); // 优先级 1
        queue.push(create_frame("abcdefghij")); // 优先级 10
        queue.push(create_frame("abcde")); // 优先级 5
        
        // 高优先级（长ID）先出队
        assert_eq!(queue.pop().unwrap().message_id, "abcdefghij");
        assert_eq!(queue.pop().unwrap().message_id, "abcde");
        assert_eq!(queue.pop().unwrap().message_id, "a");
    }
    
    #[test]
    fn test_same_priority_order() {
        let mut queue = PriorityMessageQueue::new();
        
        // 相同长度ID的消息
        queue.push(create_frame("aaa"));
        queue.push(create_frame("bbb"));
        queue.push(create_frame("ccc"));
        
        // 顺序可能不固定，只验证数量
        assert_eq!(queue.len(), 3);
        queue.pop();
        queue.pop();
        queue.pop();
        assert_eq!(queue.len(), 0);
    }
    
    #[test]
    fn test_queue_operations() {
        let mut queue = PriorityMessageQueue::new();
        
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
        
        queue.push(create_frame("msg1"));
        assert_eq!(queue.len(), 1);
        assert!(!queue.is_empty());
        
        queue.clear();
        assert!(queue.is_empty());
    }
}
