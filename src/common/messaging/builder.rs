//! Frame 构建器
//!
//! 提供流式 API 构建复杂的 Frame

use crate::common::protocol::frame::Frame;
use crate::common::protocol::reliability::Reliability;
use crate::common::protocol::commands::{Command, ControlCmd};
use bytes::Bytes;

/// Frame 构建器
pub struct FrameBuilder {
    frame: Frame,
}

impl FrameBuilder {
    /// 创建新的构建器
    pub fn new(message_id: String) -> Self {
        Self {
            frame: Frame {
                message_id,
                payload: Bytes::new(),
                reliability: Reliability::BestEffort,
                command: Command::Control(ControlCmd::Ping),
            },
        }
    }
    
    /// 设置命令
    pub fn with_command(mut self, command: Command) -> Self {
        self.frame.command = command;
        self
    }
    
    /// 设置可靠性级别
    pub fn with_reliability(mut self, reliability: Reliability) -> Self {
        self.frame.reliability = reliability;
        self
    }
    
    /// 设置 payload
    pub fn with_payload(mut self, payload: Vec<u8>) -> Self {
        self.frame.payload = payload.into();
        self
    }
    
    /// 构建 Frame
    pub fn build(self) -> Frame {
        self.frame
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_builder_basic() {
        let frame = FrameBuilder::new("msg-001".to_string())
            .with_payload(vec![1, 2, 3])
            .build();
        
        assert_eq!(frame.message_id, "msg-001");
        assert_eq!(frame.payload, Bytes::from(vec![1, 2, 3]));
    }
}
