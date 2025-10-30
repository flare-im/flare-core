use super::traits::Serializer;

pub struct ProtobufSerializer;

impl Serializer for ProtobufSerializer {
    fn serialize<T: serde::Serialize>(&self, v: &T) -> Result<Vec<u8>, String> {
        // 占位：真实实现应使用 prost 生成的类型，而不是 serde
        serde_json::to_vec(v).map_err(|e| e.to_string())
    }
    fn deserialize<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, String> {
        // 占位：真实实现应使用 prost 解码
        serde_json::from_slice(bytes).map_err(|e| e.to_string())
    }
}
