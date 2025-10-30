// 序列化架构改进方案示例代码
// 本文件展示了消除 SerializationFormat 冗余的实现方案

//! # 方案 1：消除 SerializationFormat（推荐）
//! 
//! ## 当前设计（存在冗余）
//! ```rust
//! // serialization/mod.rs
//! pub enum SerializationFormat {
//!     Json,
//!     Protobuf,
//! }
//! 
//! // parsing/codec.rs
//! pub enum PayloadCodec {
//!     Json,
//!     Protobuf,
//! }
//! 
//! impl PayloadCodec {
//!     pub fn from_format(format: SerializationFormat) -> Self {
//!         match format {
//!             SerializationFormat::Json => PayloadCodec::Json,
//!             SerializationFormat::Protobuf => PayloadCodec::Protobuf,
//!         }
//!     }
//! }
//! ```
//! 
//! ## 改进设计（统一到 PayloadCodec）
//! ```rust
//! // parsing/codec.rs
//! pub enum PayloadCodec {
//!     Json,
//!     Protobuf,
//! }
//! 
//! // serialization/mod.rs 只保留配置结构
//! pub struct SerializationConfig {
//!     pub format: crate::common::parsing::PayloadCodec,  // 直接使用 PayloadCodec
//! }
//! 
//! // 使用示例
//! let parser = MessageParser::new(PayloadCodec::Json);
//! ```

use crate::common::error::FlareError;

/// 改进方案 1：直接使用 PayloadCodec
/// 
/// 优点：
/// - 消除冗余定义
/// - 简化转换逻辑
/// - 减少维护成本
/// 
/// 缺点：
/// - 需要修改现有 API（破坏性变更）
pub mod solution_1_remove_redundancy {
    use super::*;
    
    // PayloadCodec 作为唯一的格式定义
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub enum PayloadCodec {
        #[default]
        Json,
        Protobuf,
    }
    
    // 配置直接使用 PayloadCodec
    #[derive(Debug, Clone, Default)]
    pub struct SerializationConfig {
        pub format: PayloadCodec,
    }
    
    // MessageParser 直接接受 PayloadCodec
    pub struct MessageParser {
        payload_codec: PayloadCodec,
    }
    
    impl MessageParser {
        pub fn new(codec: PayloadCodec) -> Self {
            Self {
                payload_codec: codec,
            }
        }
    }
    
    // 使用示例
    #[cfg(test)]
    mod tests {
        use super::*;
        
        #[test]
        fn example_usage() {
            // 直接使用 PayloadCodec，无需转换
            let parser = MessageParser::new(PayloadCodec::Json);
            
            // 配置也直接使用
            let config = SerializationConfig {
                format: PayloadCodec::Protobuf,
            };
        }
    }
}

/// 改进方案 2：使用宏自动生成
/// 
/// 优点：
/// - 减少重复代码
/// - 添加新格式时修改点少
/// - DRY 原则
/// 
/// 缺点：
/// - 宏调试困难
/// - IDE 支持可能不佳
pub mod solution_2_macro_generation {
    use super::*;
    
    // 定义宏用于自动生成枚举和方法
    macro_rules! define_payload_codecs {
        (
            $(
                $variant:ident {
                    name: $name:expr,
                    extension: $ext:expr,
                    mime: $mime:expr,
                    binary: $binary:expr,
                }
            ),* $(,)?
        ) => {
            /// Payload 编解码器枚举
            #[derive(Debug, Clone, Copy, PartialEq, Eq)]
            pub enum PayloadCodec {
                $($variant,)*
            }
            
            impl PayloadCodec {
                /// 获取编解码器名称
                pub fn name(&self) -> &'static str {
                    match self {
                        $(PayloadCodec::$variant => $name,)*
                    }
                }
                
                /// 获取文件扩展名
                pub fn file_extension(&self) -> &'static str {
                    match self {
                        $(PayloadCodec::$variant => $ext,)*
                    }
                }
                
                /// 获取 MIME 类型
                pub fn mime_type(&self) -> &'static str {
                    match self {
                        $(PayloadCodec::$variant => $mime,)*
                    }
                }
                
                /// 判断是否为二进制格式
                pub fn is_binary(&self) -> bool {
                    match self {
                        $(PayloadCodec::$variant => $binary,)*
                    }
                }
                
                /// 判断是否为文本格式
                pub fn is_text(&self) -> bool {
                    !self.is_binary()
                }
            }
        };
    }
    
    // 使用宏定义所有格式
    define_payload_codecs! {
        Json {
            name: "json",
            extension: "json",
            mime: "application/json",
            binary: false,
        },
        Protobuf {
            name: "protobuf",
            extension: "pb",
            mime: "application/x-protobuf",
            binary: true,
        },
        // 添加新格式只需在这里添加一项即可
        // MsgPack {
        //     name: "msgpack",
        //     extension: "msgpack",
        //     mime: "application/msgpack",
        //     binary: true,
        // },
    }
    
    // encode 和 decode 方法仍需手动实现（因为逻辑差异较大）
    impl PayloadCodec {
        pub fn encode<T: serde::Serialize>(&self, data: &T) -> Result<Vec<u8>, FlareError> {
            match self {
                PayloadCodec::Json => {
                    serde_json::to_vec(data)
                        .map_err(|e| FlareError::serialization_error(format!("JSON encoding failed: {}", e)))
                }
                PayloadCodec::Protobuf => {
                    // TODO: 实现真正的 Protobuf 编码
                    serde_json::to_vec(data)
                        .map_err(|e| FlareError::serialization_error(format!("Protobuf encoding failed: {}", e)))
                }
            }
        }
        
        pub fn decode<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, FlareError> {
            match self {
                PayloadCodec::Json => {
                    serde_json::from_slice(bytes)
                        .map_err(|e| FlareError::general_error(format!("JSON decoding failed: {}", e)))
                }
                PayloadCodec::Protobuf => {
                    // TODO: 实现真正的 Protobuf 解码
                    serde_json::from_slice(bytes)
                        .map_err(|e| FlareError::general_error(format!("Protobuf decoding failed: {}", e)))
                }
            }
        }
    }
    
    #[cfg(test)]
    mod tests {
        use super::*;
        
        #[test]
        fn test_macro_generated_methods() {
            let json = PayloadCodec::Json;
            assert_eq!(json.name(), "json");
            assert_eq!(json.file_extension(), "json");
            assert_eq!(json.mime_type(), "application/json");
            assert!(!json.is_binary());
            assert!(json.is_text());
            
            let protobuf = PayloadCodec::Protobuf;
            assert_eq!(protobuf.name(), "protobuf");
            assert_eq!(protobuf.file_extension(), "pb");
            assert_eq!(protobuf.mime_type(), "application/x-protobuf");
            assert!(protobuf.is_binary());
            assert!(!protobuf.is_text());
        }
    }
}

/// 改进方案 3：混合模式（内置 + 自定义）
/// 
/// 优点：
/// - 内置格式性能优秀（枚举）
/// - 支持自定义扩展（trait）
/// - 平衡性能和扩展性
/// 
/// 缺点：
/// - 设计复杂度较高
/// - 需要维护两套机制
pub mod solution_3_hybrid {
    use super::*;
    use std::sync::Arc;
    
    /// 内置编解码器（使用枚举，性能优先）
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum BuiltinCodec {
        Json,
        Protobuf,
    }
    
    impl BuiltinCodec {
        pub fn encode<T: serde::Serialize>(&self, data: &T) -> Result<Vec<u8>, FlareError> {
            match self {
                BuiltinCodec::Json => {
                    serde_json::to_vec(data)
                        .map_err(|e| FlareError::serialization_error(format!("JSON encoding failed: {}", e)))
                }
                BuiltinCodec::Protobuf => {
                    serde_json::to_vec(data)
                        .map_err(|e| FlareError::serialization_error(format!("Protobuf encoding failed: {}", e)))
                }
            }
        }
        
        pub fn decode<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, FlareError> {
            match self {
                BuiltinCodec::Json => {
                    serde_json::from_slice(bytes)
                        .map_err(|e| FlareError::general_error(format!("JSON decoding failed: {}", e)))
                }
                BuiltinCodec::Protobuf => {
                    serde_json::from_slice(bytes)
                        .map_err(|e| FlareError::general_error(format!("Protobuf decoding failed: {}", e)))
                }
            }
        }
    }
    
    /// 自定义序列化器 trait（用于扩展）
    pub trait CustomSerializer: Send + Sync {
        fn name(&self) -> &str;
        fn encode_bytes(&self, data: &[u8]) -> Result<Vec<u8>, FlareError>;
        fn decode_bytes(&self, bytes: &[u8]) -> Result<Vec<u8>, FlareError>;
    }
    
    /// 统一的 Payload 编解码器
    pub enum PayloadCodec {
        /// 内置编解码器（静态分发，性能优）
        Builtin(BuiltinCodec),
        /// 自定义编解码器（动态分发，扩展性优）
        Custom(Arc<dyn CustomSerializer>),
    }
    
    impl PayloadCodec {
        /// 创建内置 JSON 编解码器
        pub fn json() -> Self {
            PayloadCodec::Builtin(BuiltinCodec::Json)
        }
        
        /// 创建内置 Protobuf 编解码器
        pub fn protobuf() -> Self {
            PayloadCodec::Builtin(BuiltinCodec::Protobuf)
        }
        
        /// 创建自定义编解码器
        pub fn custom<S: CustomSerializer + 'static>(serializer: S) -> Self {
            PayloadCodec::Custom(Arc::new(serializer))
        }
        
        /// 序列化数据
        pub fn encode<T: serde::Serialize>(&self, data: &T) -> Result<Vec<u8>, FlareError> {
            match self {
                PayloadCodec::Builtin(codec) => codec.encode(data),
                PayloadCodec::Custom(serializer) => {
                    // 对于自定义序列化器，先用 JSON 序列化，再调用自定义转换
                    let json_bytes = serde_json::to_vec(data)
                        .map_err(|e| FlareError::serialization_error(format!("Pre-serialization failed: {}", e)))?;
                    serializer.encode_bytes(&json_bytes)
                }
            }
        }
        
        /// 反序列化数据
        pub fn decode<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, FlareError> {
            match self {
                PayloadCodec::Builtin(codec) => codec.decode(bytes),
                PayloadCodec::Custom(serializer) => {
                    // 对于自定义序列化器，先解码为 JSON，再反序列化
                    let json_bytes = serializer.decode_bytes(bytes)?;
                    serde_json::from_slice(&json_bytes)
                        .map_err(|e| FlareError::general_error(format!("Post-deserialization failed: {}", e)))
                }
            }
        }
        
        /// 获取编解码器名称
        pub fn name(&self) -> &str {
            match self {
                PayloadCodec::Builtin(BuiltinCodec::Json) => "json",
                PayloadCodec::Builtin(BuiltinCodec::Protobuf) => "protobuf",
                PayloadCodec::Custom(serializer) => serializer.name(),
            }
        }
    }
    
    // 使用示例：自定义 Base64 序列化器
    #[cfg(test)]
    mod tests {
        use super::*;
        
        struct Base64Serializer;
        
        impl CustomSerializer for Base64Serializer {
            fn name(&self) -> &str {
                "base64"
            }
            
            fn encode_bytes(&self, data: &[u8]) -> Result<Vec<u8>, FlareError> {
                use base64::{Engine as _, engine::general_purpose};
                let encoded = general_purpose::STANDARD.encode(data);
                Ok(encoded.into_bytes())
            }
            
            fn decode_bytes(&self, bytes: &[u8]) -> Result<Vec<u8>, FlareError> {
                use base64::{Engine as _, engine::general_purpose};
                let text = String::from_utf8(bytes.to_vec())
                    .map_err(|e| FlareError::general_error(format!("Invalid UTF-8: {}", e)))?;
                general_purpose::STANDARD.decode(&text)
                    .map_err(|e| FlareError::general_error(format!("Base64 decode failed: {}", e)))
            }
        }
        
        #[test]
        fn test_builtin_codecs() {
            let json_codec = PayloadCodec::json();
            assert_eq!(json_codec.name(), "json");
            
            let protobuf_codec = PayloadCodec::protobuf();
            assert_eq!(protobuf_codec.name(), "protobuf");
        }
        
        #[test]
        fn test_custom_codec() {
            let base64_codec = PayloadCodec::custom(Base64Serializer);
            assert_eq!(base64_codec.name(), "base64");
            
            // 可以正常编解码（通过 JSON 中转）
            #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
            struct TestData {
                value: String,
            }
            
            let data = TestData { value: "test".to_string() };
            let bytes = base64_codec.encode(&data).unwrap();
            let decoded: TestData = base64_codec.decode(&bytes).unwrap();
            assert_eq!(data, decoded);
        }
    }
}

/// 改进方案 4：完全的注册表模式（最大扩展性）
/// 
/// 优点：
/// - 完全的运行时扩展
/// - 支持第三方插件
/// - 符合开闭原则
/// 
/// 缺点：
/// - 性能开销（动态分发）
/// - 复杂度高
/// - 可能的运行时错误
pub mod solution_4_registry {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    
    /// 序列化器 trait（使用类型擦除技术）
    pub trait Serializer: Send + Sync {
        fn name(&self) -> &str;
        
        /// 序列化为字节（类型擦除版本）
        fn serialize_erased(&self, data: &dyn erased_serde::Serialize) -> Result<Vec<u8>, FlareError>;
        
        /// 从字节反序列化（需要外部提供类型信息）
        fn deserialize_bytes(&self, bytes: &[u8]) -> Result<serde_json::Value, FlareError>;
    }
    
    /// JSON 序列化器实现
    pub struct JsonSerializer;
    
    impl Serializer for JsonSerializer {
        fn name(&self) -> &str {
            "json"
        }
        
        fn serialize_erased(&self, data: &dyn erased_serde::Serialize) -> Result<Vec<u8>, FlareError> {
            serde_json::to_vec(data)
                .map_err(|e| FlareError::serialization_error(format!("JSON encoding failed: {}", e)))
        }
        
        fn deserialize_bytes(&self, bytes: &[u8]) -> Result<serde_json::Value, FlareError> {
            serde_json::from_slice(bytes)
                .map_err(|e| FlareError::general_error(format!("JSON decoding failed: {}", e)))
        }
    }
    
    /// 序列化器注册表
    pub struct SerializerRegistry {
        serializers: Arc<RwLock<HashMap<String, Arc<dyn Serializer>>>>,
    }
    
    impl SerializerRegistry {
        pub fn new() -> Self {
            let mut registry = Self {
                serializers: Arc::new(RwLock::new(HashMap::new())),
            };
            
            // 注册默认序列化器
            registry.register(Arc::new(JsonSerializer));
            
            registry
        }
        
        /// 注册序列化器
        pub fn register(&mut self, serializer: Arc<dyn Serializer>) {
            let name = serializer.name().to_string();
            self.serializers.write().unwrap().insert(name, serializer);
        }
        
        /// 获取序列化器
        pub fn get(&self, name: &str) -> Option<Arc<dyn Serializer>> {
            self.serializers.read().unwrap().get(name).cloned()
        }
        
        /// 列出所有已注册的序列化器
        pub fn list_serializers(&self) -> Vec<String> {
            self.serializers.read().unwrap().keys().cloned().collect()
        }
    }
    
    impl Default for SerializerRegistry {
        fn default() -> Self {
            Self::new()
        }
    }
    
    #[cfg(test)]
    mod tests {
        use super::*;
        
        #[test]
        fn test_registry() {
            let registry = SerializerRegistry::new();
            
            // 默认注册了 JSON
            assert!(registry.get("json").is_some());
            
            // 列出序列化器
            let list = registry.list_serializers();
            assert!(list.contains(&"json".to_string()));
        }
    }
}

// 注意：此文件仅用于示例，不会被编译到最终项目中
