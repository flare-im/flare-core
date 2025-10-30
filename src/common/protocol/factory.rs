use super::frame::Frame;
use super::reliability::Reliability;
use super::commands::{Command, MessageCmd, DataCommand, ControlCmd};
use uuid::Uuid;
use bytes::Bytes;

pub struct FrameFactory;

impl FrameFactory {
    pub fn generate_message_id() -> String { Uuid::new_v4().to_string() }

    pub fn create_data_frame(message_id: String, payload: Vec<u8>, reliability: Reliability) -> Result<Frame, String> {
        let data_cmd = DataCommand { data: payload.clone() };
        let command = Command::Message(MessageCmd::Data(data_cmd));
        Ok(Frame { message_id, payload: Bytes::from(payload), reliability, command })
    }

    pub fn create_ping_frame(message_id: String) -> Result<Frame, String> {
        let command = Command::Control(ControlCmd::Ping);
        Ok(Frame { message_id, payload: Bytes::new(), reliability: Reliability::BestEffort, command })
    }

    pub fn create_pong_frame(message_id: String) -> Result<Frame, String> {
        let command = Command::Control(ControlCmd::Pong);
        Ok(Frame { message_id, payload: Bytes::new(), reliability: Reliability::BestEffort, command })
    }
}