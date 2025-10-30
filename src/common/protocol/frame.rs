use super::reliability::Reliability;
use super::commands::Command;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct Frame {
    pub message_id: String,
    pub payload: Bytes,
    pub reliability: Reliability,
    pub command: Command,
}

impl Frame {
    pub fn new(
        command: Command,
        message_id: String,
        reliability: Reliability,
    ) -> Self {
        Self {
            command,
            message_id,
            reliability,
            payload: Bytes::new(),
        }
    }
}