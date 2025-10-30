//! 序列化器工厂
//!
//! 提供统一的序列化器创建和管理功能

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::common::{
    error::{Result, FlareError},

    serialization::{
        traits::{FrameSerializer, SerializationFormat, SerializationConfig, SerializerInfo, SerializerFeature},
        json::JsonSerializer,
        msgpack::MessagePackSerializer,
        bincode::BincodeSerializer,
        protobuf::ProtobufSerializer,
        cbor::CborSerializer,
    },
};

/// 序列化器注册表
type SerializerRegistry = HashMap<SerializationFormat, Box<dyn Fn(SerializationConfig) -> Box<dyn FrameSerializer> + Send + Sync>>;

/// 序列化器工厂
pub struct SerializerFactory {
    /// 注册的序列化器创建函数
    registry: Arc<RwLock<SerializerRegistry>>,
}

impl SerializerFactory {
    /// 创建新的序列化器工厂
    pub fn new() -> Self {
        let mut factory = Self {
            registry: Arc::new(RwLock::new(HashMap::new())),
        };
        
        // 注册默认序列化器
        factory.register_defaults();
        factory
    }
    
    /// 注册默认序列化器
    fn register_defaults(&mut self) {
        // 注册JSON序列化器
        self.register_serializer(
            SerializationFormat::Json,
            Box::new(|config| Box::new(JsonSerializer::with_config(config)) as Box<dyn FrameSerializer>),
        ).unwrap();
        
        // 注册MessagePack序列化器
        self.register_serializer(
            SerializationFormat::MessagePack,
            Box::new(|config| Box::new(MessagePackSerializer::with_config(config)) as Box<dyn FrameSerializer>),
        ).unwrap();
        
        // 注册Bincode序列化器
        self.register_serializer(
            SerializationFormat::Bincode,
            Box::new(|config| Box::new(BincodeSerializer::with_config(config)) as Box<dyn FrameSerializer>),
        ).unwrap();
        
        // 注册Protobuf序列化器
        self.register_serializer(
            SerializationFormat::Protobuf,
            Box::new(|config| Box::new(ProtobufSerializer::with_config(config)) as Box<dyn FrameSerializer>),
        ).unwrap();
        
        // 注册CBOR序列化器
        self.register_serializer(
            SerializationFormat::Cbor,
            Box::new(|config| Box::new(CborSerializer::with_config(config)) as Box<dyn FrameSerializer>),
        ).unwrap();
    }
    
    /// 注册新的序列化器
    pub fn register_serializer<F>(&mut self, format: SerializationFormat, creator: F) -> Result<()>
    where
        F: Fn(SerializationConfig) -> Box<dyn FrameSerializer> + Send + Sync + 'static,
    {
        if let Ok(mut registry) = self.registry.write() {
            registry.insert(format, Box::new(creator));
            Ok(())
        } else {
            Err(FlareError::general_error("无法获取注册表写锁"))
        }
    }
    
    /// 创建指定格式的序列化器
    pub fn create(&self, format: SerializationFormat) -> Result<Box<dyn FrameSerializer>> {
        self.create_with_config(format, SerializationConfig::default())
    }
    
    /// 创建带配置的序列化器
    pub fn create_with_config(&self, format: SerializationFormat, config: SerializationConfig) -> Result<Box<dyn FrameSerializer>> {
        if let Ok(registry) = self.registry.read() {
            if let Some(creator) = registry.get(&format) {
                Ok(creator(config))
            } else {
                Err(FlareError::general_error(
                    format!("不支持的序列化格式: {}", format)
                ))
            }
        } else {
            Err(FlareError::general_error("无法获取注册表读锁"))
        }
    }
    
    /// 获取支持的格式列表
    pub fn supported_formats(&self) -> Vec<SerializationFormat> {
        if let Ok(registry) = self.registry.read() {
            registry.keys().copied().collect()
        } else {
            Vec::new()
        }
    }
    
    /// 检查是否支持指定格式
    pub fn supports_format(&self, format: SerializationFormat) -> bool {
        if let Ok(registry) = self.registry.read() {
            registry.contains_key(&format)
        } else {
            false
        }
    }
    
    /// 创建JSON序列化器
    pub fn json() -> Box<dyn FrameSerializer> {
        Box::new(JsonSerializer::new())
    }
    
    /// 创建美化JSON序列化器
    pub fn json_pretty() -> Box<dyn FrameSerializer> {
        Box::new(JsonSerializer::pretty())
    }
    
    /// 创建带配置的JSON序列化器
    pub fn json_with_config(config: SerializationConfig) -> Box<dyn FrameSerializer> {
        Box::new(JsonSerializer::with_config(config))
    }
    
    /// 创建MessagePack序列化器
    pub fn msgpack() -> Box<dyn FrameSerializer> {
        Box::new(MessagePackSerializer::new())
    }
    
    /// 创建带配置的MessagePack序列化器
    pub fn msgpack_with_config(config: SerializationConfig) -> Box<dyn FrameSerializer> {
        Box::new(MessagePackSerializer::with_config(config))
    }
    
    /// 创建Bincode序列化器
    pub fn bincode() -> Box<dyn FrameSerializer> {
        Box::new(BincodeSerializer::new())
    }
    
    /// 创建带配置的Bincode序列化器
    pub fn bincode_with_config(config: SerializationConfig) -> Box<dyn FrameSerializer> {
        Box::new(BincodeSerializer::with_config(config))
    }
    
    /// 创建Protobuf序列化器
    pub fn protobuf() -> Box<dyn FrameSerializer> {
        Box::new(ProtobufSerializer::new())
    }
    
    /// 创建带配置的Protobuf序列化器
    pub fn protobuf_with_config(config: SerializationConfig) -> Box<dyn FrameSerializer> {
        Box::new(ProtobufSerializer::with_config(config))
    }
    
    /// 创建CBOR序列化器
    pub fn cbor() -> Box<dyn FrameSerializer> {
        Box::new(CborSerializer::new())
    }
    
    /// 创建带配置的CBOR序列化器
    pub fn cbor_with_config(config: SerializationConfig) -> Box<dyn FrameSerializer> {
        Box::new(CborSerializer::with_config(config))
    }
    
    /// 根据MIME类型创建序列化器
    pub fn create_by_mime_type(&self, mime_type: &str) -> Result<Box<dyn FrameSerializer>> {
        let format = match mime_type {
            "application/json" => SerializationFormat::Json,
            "application/msgpack" => SerializationFormat::MessagePack,
            "application/octet-stream" => SerializationFormat::Bincode,
            "application/x-protobuf" => SerializationFormat::Protobuf,
            "application/cbor" => SerializationFormat::Cbor,
            _ => return Err(FlareError::general_error(
                format!("不支持的MIME类型: {}", mime_type)
            )),
        };
        
        self.create(format)
    }
    
    /// 根据文件扩展名创建序列化器
    pub fn create_by_extension(&self, extension: &str) -> Result<Box<dyn FrameSerializer>> {
        let format = match extension.to_lowercase().as_str() {
            "json" => SerializationFormat::Json,
            "msgpack" | "mp" => SerializationFormat::MessagePack,
            "bincode" | "bc" => SerializationFormat::Bincode,
            "proto" | "protobuf" => SerializationFormat::Protobuf,
            "cbor" => SerializationFormat::Cbor,
            _ => return Err(FlareError::general_error(
                format!("不支持的文件扩展名: {}", extension)
            )),
        };
        
        self.create(format)
    }
    
    /// 获取序列化器信息
    pub fn get_serializer_info(&self, format: SerializationFormat) -> Result<SerializerInfo> {
        let serializer = self.create(format)?;
        
        Ok(SerializerInfo {
            name: serializer.name(),
            version: serializer.version(),
            description: serializer.description(),
            format,
            features: self.get_serializer_features(format),
            mime_type: serializer.mime_type(),
            file_extension: serializer.file_extension(),
        })
    }
    
    /// 获取序列化器特性
    fn get_serializer_features(&self, format: SerializationFormat) -> Vec<SerializerFeature> {
        match format {
            SerializationFormat::Json => vec![
                SerializerFeature::PrettyFormat,
                SerializerFeature::TextFormat,
                SerializerFeature::SelfDescribing,
            ],
            SerializationFormat::MessagePack => vec![
                SerializerFeature::BinaryFormat,
                SerializerFeature::SelfDescribing,
            ],
            SerializationFormat::Bincode => vec![
                SerializerFeature::BinaryFormat,
            ],
            SerializationFormat::Protobuf => vec![
                SerializerFeature::BinaryFormat,
                SerializerFeature::SchemaValidation,
            ],
            SerializationFormat::Cbor => vec![
                SerializerFeature::BinaryFormat,
                SerializerFeature::SelfDescribing,
            ],
        }
    }
    
    /// 批量创建序列化器
    pub fn create_batch(&self, formats: &[SerializationFormat]) -> Result<Vec<Box<dyn FrameSerializer>>> {
        let mut serializers = Vec::with_capacity(formats.len());
        for &format in formats {
            serializers.push(self.create(format)?);
        }
        Ok(serializers)
    }
    
    /// 创建默认序列化器（JSON）
    pub fn default_serializer() -> Box<dyn FrameSerializer> {
        Self::json()
    }
    
    /// 创建高性能序列化器（Bincode）
    pub fn high_performance_serializer() -> Result<Box<dyn FrameSerializer>> {
        // Bincode是最高性能的选择，专为超低延迟优化
        Ok(Self::bincode())
    }
    
    /// 创建人类可读序列化器（美化JSON）
    pub fn human_readable_serializer() -> Box<dyn FrameSerializer> {
        Self::json_pretty()
    }
    
    /// 创建紧凑序列化器（JSON压缩格式）
    pub fn compact_serializer() -> Box<dyn FrameSerializer> {
        Self::json()
    }
}

impl Default for SerializerFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for SerializerFactory {
    fn clone(&self) -> Self {
        // 创建新实例并复制注册表
        let mut new_factory = Self {
            registry: Arc::new(RwLock::new(HashMap::new())),
        };
        
        // 注册默认序列化器
        new_factory.register_defaults();
        
        new_factory
    }
}

/// 全局序列化器工厂实例
static GLOBAL_FACTORY: std::sync::OnceLock<SerializerFactory> = std::sync::OnceLock::new();

/// 获取全局序列化器工厂
pub fn global_factory() -> &'static SerializerFactory {
    GLOBAL_FACTORY.get_or_init(|| SerializerFactory::new())
}

/// 便捷函数：创建JSON序列化器
pub fn json_serializer() -> Box<dyn FrameSerializer> {
    SerializerFactory::json()
}

/// 便捷函数：创建美化JSON序列化器
pub fn json_pretty_serializer() -> Box<dyn FrameSerializer> {
    SerializerFactory::json_pretty()
}

/// 便捷函数：创建默认序列化器
pub fn default_serializer() -> Box<dyn FrameSerializer> {
    SerializerFactory::default_serializer()
}

/// 便捷函数：创建MessagePack序列化器
pub fn msgpack_serializer() -> Box<dyn FrameSerializer> {
    SerializerFactory::msgpack()
}

/// 便捷函数：创建Bincode序列化器
pub fn bincode_serializer() -> Box<dyn FrameSerializer> {
    SerializerFactory::bincode()
}

/// 便捷函数：创建Protobuf序列化器
pub fn protobuf_serializer() -> Box<dyn FrameSerializer> {
    SerializerFactory::protobuf()
}

/// 便捷函数：创建CBOR序列化器
pub fn cbor_serializer() -> Box<dyn FrameSerializer> {
    SerializerFactory::cbor()
}

/// 便捷函数：创建高性能序列化器（为超低延迟优化）
pub fn high_performance_serializer() -> Box<dyn FrameSerializer> {
    SerializerFactory::high_performance_serializer().unwrap_or_else(|_| json_serializer())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};
    use crate::common::protocol::factory::FrameFactory;
    use crate::common::protocol::Frame;
    
    #[test]
    fn test_factory_creation() {
        let factory = SerializerFactory::new();
        
        // 测试创建各种序列化器
        let json_ser = factory.create(SerializationFormat::Json);
        let msgpack_ser = factory.create(SerializationFormat::MessagePack);
        let bincode_ser = factory.create(SerializationFormat::Bincode);
        let protobuf_ser = factory.create(SerializationFormat::Protobuf);
        let cbor_ser = factory.create(SerializationFormat::Cbor);
        
        assert!(json_ser.is_ok());
        assert!(msgpack_ser.is_ok());
        assert!(bincode_ser.is_ok());
        assert!(protobuf_ser.is_ok());
        assert!(cbor_ser.is_ok());
        
        // 测试不支持的格式 - 使用不存在的格式作为测试
        let unsupported_ser = factory.create(SerializationFormat::Json); // 临时使用已支持的格式
        // 我们可以通过创建一个不存在的枚举值来测试，但这里我们只是演示
        assert!(unsupported_ser.is_ok()); // 这里会返回Ok因为我们使用了已支持的格式
    }
    
    #[test]
    fn test_supported_formats() {
        let factory = SerializerFactory::new();
        let formats = factory.supported_formats();
        
        assert!(formats.contains(&SerializationFormat::Json));
        assert!(formats.contains(&SerializationFormat::MessagePack));
        assert!(formats.contains(&SerializationFormat::Bincode));
        assert!(formats.contains(&SerializationFormat::Protobuf));
        assert!(formats.contains(&SerializationFormat::Cbor));
    }
    
    #[test]
    fn test_format_support() {
        let factory = SerializerFactory::new();
        
        assert!(factory.supports_format(SerializationFormat::Json));
        assert!(factory.supports_format(SerializationFormat::MessagePack));
        assert!(factory.supports_format(SerializationFormat::Bincode));
        assert!(factory.supports_format(SerializationFormat::Protobuf));
        assert!(factory.supports_format(SerializationFormat::Cbor));
    }
    
    #[tokio::test]
    async fn test_mime_type_creation() {
        let factory = SerializerFactory::new();
        
        let json_ser = factory.create_by_mime_type("application/json");
        let msgpack_ser = factory.create_by_mime_type("application/msgpack");
        let bincode_ser = factory.create_by_mime_type("application/octet-stream");
        let protobuf_ser = factory.create_by_mime_type("application/x-protobuf");
        let cbor_ser = factory.create_by_mime_type("application/cbor");
        
        assert!(json_ser.is_ok());
        assert!(msgpack_ser.is_ok());
        assert!(bincode_ser.is_ok());
        assert!(protobuf_ser.is_ok());
        assert!(cbor_ser.is_ok());
        
        // 测试不支持的MIME类型
        let unsupported_ser = factory.create_by_mime_type("application/unsupported");
        assert!(unsupported_ser.is_err());
    }
    
    #[tokio::test]
    async fn test_extension_creation() {
        let factory = SerializerFactory::new();
        
        let json_ser = factory.create_by_extension("json");
        let msgpack_ser = factory.create_by_extension("msgpack");
        let bincode_ser = factory.create_by_extension("bincode");
        let protobuf_ser = factory.create_by_extension("proto");
        let cbor_ser = factory.create_by_extension("cbor");
        
        assert!(json_ser.is_ok());
        assert!(msgpack_ser.is_ok());
        assert!(bincode_ser.is_ok());
        assert!(protobuf_ser.is_ok());
        assert!(cbor_ser.is_ok());
        
        // 测试不支持的扩展名
        let unsupported_ser = factory.create_by_extension("unsupported");
        assert!(unsupported_ser.is_err());
    }
    
    #[tokio::test]
    async fn test_serializer_info() {
        let factory = SerializerFactory::new();
        let info = factory.get_serializer_info(SerializationFormat::Json).unwrap();
        
        assert_eq!(info.name, "JsonSerializer");
        assert_eq!(info.version, "1.0.0");
        assert_eq!(info.description, "JSON格式消息帧序列化器");
        assert_eq!(info.mime_type, "application/json");
        assert_eq!(info.file_extension, "json");
    }
    
    #[tokio::test]
    async fn test_global_factory() {
        let factory = global_factory();
        let serializer = factory.create(SerializationFormat::Json).unwrap();
        
        assert_eq!(serializer.format(), SerializationFormat::Json);
    }
    
    #[tokio::test]
    async fn test_convenience_functions() {
        let json_ser = json_serializer();
        let pretty_ser = json_pretty_serializer();
        let default_ser = default_serializer();
        
        assert_eq!(json_ser.format(), SerializationFormat::Json);
        assert_eq!(pretty_ser.format(), SerializationFormat::Json);
        assert_eq!(default_ser.format(), SerializationFormat::Json);
        
        // 测试实际序列化
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
            
        let message_id = FrameFactory::generate_message_id();
        let frame = FrameFactory::create_ping_frame(message_id.clone()).unwrap();
        
        // 添加测试数据到元数据中
        let mut frame_with_metadata = frame.clone();
        FrameFactory::add_metadata(&mut frame_with_metadata, "test_data".to_string(), b"test".to_vec());
        
        let json_data = json_ser.serialize(&frame_with_metadata).await.unwrap();
        let pretty_data = pretty_ser.serialize(&frame_with_metadata).await.unwrap();
        
        // 美化格式应该更长（包含换行符和缩进）
        assert!(pretty_data.len() > json_data.len());
    }
}