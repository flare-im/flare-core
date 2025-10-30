pub trait Serializer: Send + Sync {
    fn serialize<T: serde::Serialize>(&self, v: &T) -> Result<Vec<u8>, String>;
    fn deserialize<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, String>;
}
