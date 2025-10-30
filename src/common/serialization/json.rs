use super::traits::Serializer;

pub struct JsonSerializer;

impl Serializer for JsonSerializer {
    fn serialize<T: serde::Serialize>(&self, v: &T) -> Result<Vec<u8>, String> {
        serde_json::to_vec(v).map_err(|e| e.to_string())
    }
    fn deserialize<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, String> {
        serde_json::from_slice(bytes).map_err(|e| e.to_string())
    }
}
