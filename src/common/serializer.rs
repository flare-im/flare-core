use crate::common::message::Envelope;
use std::error::Error;

pub trait Serializer {
    fn serialize(&self, envelope: &Envelope) -> Result<Vec<u8>, Box<dyn Error>>;
    fn deserialize(&self, data: &[u8]) -> Result<Envelope, Box<dyn Error>>;
}

pub struct JsonSerializer;

impl Serializer for JsonSerializer {
    fn serialize(&self, envelope: &Envelope) -> Result<Vec<u8>, Box<dyn Error>> {
        serde_json::to_vec(envelope).map_err(|e| e.into())
    }

    fn deserialize(&self, data: &[u8]) -> Result<Envelope, Box<dyn Error>> {
        serde_json::from_slice(data).map_err(|e| e.into())
    }
}