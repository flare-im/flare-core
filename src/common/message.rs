use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum ContentType {
    Json,
    Protobuf,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Header {
    pub message_id: String,
    pub timestamp: u64,
    pub content_type: ContentType,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Envelope {
    pub header: Header,
    pub payload: Vec<u8>,
}

pub trait Message {
    fn to_envelope(&self) -> Envelope;
    fn from_envelope(envelope: &Envelope) -> Self;
}