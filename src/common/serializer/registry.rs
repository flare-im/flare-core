//! 序列化器注册表
//! 
//! 管理序列化器的注册和查找，支持用户注册自定义序列化器

use crate::common::protocol::SerializationFormat;
use super::traits::Serializer;
use super::formats::{ProtobufSerializer, JsonSerializer};
use std::sync::Arc;
use std::collections::HashMap;
use std::sync::RwLock;

lazy_static::lazy_static! {
    /// 全局序列化器注册表
    static ref SERIALIZATION_REGISTRY: SerializationRegistry = {
        let registry = SerializationRegistry::new();
        // 注册内置序列化器
        registry.register_defaults();
        registry
    };
}

/// 序列化器注册表
/// 
/// 管理序列化器的注册和查找
pub struct SerializationRegistry {
    serializers: Arc<RwLock<HashMap<String, Arc<dyn Serializer>>>>,
}

impl SerializationRegistry {
    /// 创建新的注册表
    pub fn new() -> Self {
        Self {
            serializers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 注册内置序列化器
    pub fn register_defaults(&self) {
        self.register("protobuf", Arc::new(ProtobufSerializer));
        self.register("json", Arc::new(JsonSerializer));
    }

    /// 注册序列化器
    /// 
    /// # 参数
    /// - `name`: 序列化器名称（用于查找）
    /// - `serializer`: 序列化器实例
    /// 
    /// # 示例
    /// 
    /// ```rust
    /// use flare_core::common::serializer::{SerializationRegistry, Serializer};
    /// use std::sync::Arc;
    /// 
    /// struct MySerializer;
    /// impl Serializer for MySerializer { /* ... */ }
    /// 
    /// let registry = SerializationRegistry::new();
    /// registry.register("my_custom", Arc::new(MySerializer));
    /// ```
    pub fn register(&self, name: &str, serializer: Arc<dyn Serializer>) {
        if let Ok(mut serializers) = self.serializers.write() {
            serializers.insert(name.to_string(), serializer);
        }
    }

    /// 查找序列化器
    /// 
    /// # 参数
    /// - `name`: 序列化器名称
    /// 
    /// # 返回
    /// 找到的序列化器，如果不存在则返回 None
    pub fn find(&self, name: &str) -> Option<Arc<dyn Serializer>> {
        self.serializers
            .read()
            .ok()
            .and_then(|serializers| serializers.get(name).map(|s| Arc::clone(s)))
    }

    /// 根据格式类型查找序列化器
    pub fn find_by_format(&self, format: SerializationFormat) -> Option<Arc<dyn Serializer>> {
        let name = match format {
            SerializationFormat::Protobuf => "protobuf",
            SerializationFormat::Json => "json",
        };
        self.find(name)
    }

    /// 尝试自动检测序列化格式
    /// 
    /// 遍历所有注册的序列化器，使用 `can_detect` 方法检测
    pub fn auto_detect(&self, data: &[u8]) -> Vec<Arc<dyn Serializer>> {
        let mut detected = Vec::new();
        if let Ok(serializers) = self.serializers.read() {
            for serializer in serializers.values() {
                if serializer.can_detect(data) {
                    detected.push(Arc::clone(serializer));
                }
            }
        }
        detected
    }

    /// 获取全局注册表实例
    pub fn global() -> &'static SerializationRegistry {
        &SERIALIZATION_REGISTRY
    }
}

/// 序列化工具类
/// 
/// 提供便捷的序列化/反序列化方法，使用全局注册表
pub struct SerializationUtil;

impl SerializationUtil {
    /// 根据格式类型获取序列化器
    pub fn get_serializer(format: SerializationFormat) -> Option<Arc<dyn Serializer>> {
        SerializationRegistry::global().find_by_format(format)
    }

    /// 根据名称获取序列化器
    pub fn get_serializer_by_name(name: &str) -> Option<Arc<dyn Serializer>> {
        SerializationRegistry::global().find(name)
    }

    /// 尝试自动检测序列化格式
    /// 
    /// 返回所有可能匹配的序列化器（按检测顺序）
    pub fn auto_detect(data: &[u8]) -> Vec<Arc<dyn Serializer>> {
        SerializationRegistry::global().auto_detect(data)
    }

    /// 注册自定义序列化器到全局注册表
    /// 
    /// # 示例
    /// 
    /// ```rust
    /// use flare_core::common::serializer::{SerializationUtil, Serializer};
    /// use std::sync::Arc;
    /// 
    /// struct MySerializer;
    /// impl Serializer for MySerializer { /* ... */ }
    /// 
    /// SerializationUtil::register_custom(Arc::new(MySerializer));
    /// ```
    pub fn register_custom(serializer: Arc<dyn Serializer>) {
        SerializationRegistry::global().register(serializer.name(), serializer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization_registry() {
        let registry = SerializationRegistry::new();
        registry.register_defaults();

        assert!(registry.find("protobuf").is_some());
        assert!(registry.find("json").is_some());
        assert!(registry.find("unknown").is_none());
    }

    #[test]
    fn test_auto_detect_json() {
        let data = b"{\"message_id\":\"test\"}";
        let registry = SerializationRegistry::new();
        registry.register_defaults();

        let serializers = registry.auto_detect(data);
        // JSON 应该能被检测到
        assert!(serializers.iter().any(|s| s.name() == "json"));
    }
}

