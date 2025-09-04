//! 序列化器特性定义
//!
//! 定义统一的序列化接口，支持扩展不同的序列化格式

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::common::{
    error::{Result, FlareError},
    protocol::Frame,
};

/// 序列化格式类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SerializationFormat {
    /// JSON格式
    Json,
    /// MessagePack格式
    MessagePack,
    /// Bincode格式
    Bincode,
    /// Protocol Buffers格式
    Protobuf,
    /// CBOR格式
    Cbor,
}

impl fmt::Display for SerializationFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SerializationFormat::Json => write!(f, "JSON"),
            SerializationFormat::MessagePack => write!(f, "MessagePack"),
            SerializationFormat::Bincode => write!(f, "Bincode"),
            SerializationFormat::Protobuf => write!(f, "Protobuf"),
            SerializationFormat::Cbor => write!(f, "CBOR"),
        }
    }
}

/// 序列化配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializationConfig {
    /// 是否启用压缩
    pub enable_compression: bool,
    /// 压缩级别（如果支持）
    pub compression_level: Option<u32>,
    /// 是否使用美化格式（如果支持）
    pub pretty_format: bool,
    /// 最大消息大小限制
    pub max_message_size: Option<usize>,
    /// 自定义配置参数
    pub custom_params: std::collections::HashMap<String, String>,
}

impl Default for SerializationConfig {
    fn default() -> Self {
        Self {
            enable_compression: false,
            compression_level: None,
            pretty_format: false,
            max_message_size: Some(16 * 1024 * 1024), // 16MB默认限制
            custom_params: std::collections::HashMap::new(),
        }
    }
}

impl SerializationConfig {
    /// 创建新的序列化配置
    pub fn new() -> Self {
        Self::default()
    }
    
    /// 启用压缩
    pub fn with_compression(mut self, level: Option<u32>) -> Self {
        self.enable_compression = true;
        self.compression_level = level;
        self
    }
    
    /// 启用美化格式
    pub fn with_pretty_format(mut self) -> Self {
        self.pretty_format = true;
        self
    }
    
    /// 设置最大消息大小
    pub fn with_max_size(mut self, size: usize) -> Self {
        self.max_message_size = Some(size);
        self
    }
    
    /// 添加自定义参数
    pub fn with_param(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.custom_params.insert(key.into(), value.into());
        self
    }
}

/// 序列化统计信息
#[derive(Debug, Clone, Default)]
pub struct SerializationStats {
    /// 序列化操作次数
    pub serialize_count: u64,
    /// 反序列化操作次数
    pub deserialize_count: u64,
    /// 序列化总字节数
    pub serialized_bytes: u64,
    /// 反序列化总字节数
    pub deserialized_bytes: u64,
    /// 序列化错误次数
    pub serialize_errors: u64,
    /// 反序列化错误次数
    pub deserialize_errors: u64,
    /// 平均序列化时间（微秒）
    pub avg_serialize_time_us: u64,
    /// 平均反序列化时间（微秒）
    pub avg_deserialize_time_us: u64,
}

impl SerializationStats {
    /// 重置统计信息
    pub fn reset(&mut self) {
        *self = Self::default();
    }
    
    /// 获取序列化成功率
    pub fn serialize_success_rate(&self) -> f64 {
        if self.serialize_count == 0 {
            1.0
        } else {
            (self.serialize_count - self.serialize_errors) as f64 / self.serialize_count as f64
        }
    }
    
    /// 获取反序列化成功率
    pub fn deserialize_success_rate(&self) -> f64 {
        if self.deserialize_count == 0 {
            1.0
        } else {
            (self.deserialize_count - self.deserialize_errors) as f64 / self.deserialize_count as f64
        }
    }
    
    /// 获取平均序列化数据大小
    pub fn avg_serialized_size(&self) -> f64 {
        if self.serialize_count == 0 {
            0.0
        } else {
            self.serialized_bytes as f64 / self.serialize_count as f64
        }
    }
    
    /// 获取平均反序列化数据大小
    pub fn avg_deserialized_size(&self) -> f64 {
        if self.deserialize_count == 0 {
            0.0
        } else {
            self.deserialized_bytes as f64 / self.deserialize_count as f64
        }
    }
}

/// 消息帧序列化器trait
/// 
/// 提供统一的序列化接口，支持不同的序列化格式
#[async_trait]
pub trait FrameSerializer: Send + Sync {
    /// 获取序列化格式
    fn format(&self) -> SerializationFormat;
    
    /// 序列化消息帧为字节数组
    async fn serialize(&self, frame: &Frame) -> Result<Vec<u8>>;
    
    /// 从字节数组反序列化消息帧
    async fn deserialize(&self, data: &[u8]) -> Result<Frame>;
    
    /// 获取序列化器名称
    fn name(&self) -> &'static str;
    
    /// 获取序列化器版本
    fn version(&self) -> &'static str {
        "1.0.0"
    }
    
    /// 获取序列化器描述
    fn description(&self) -> &'static str {
        "消息帧序列化器"
    }
    
    /// 检查是否支持指定格式
    fn supports_format(&self, format: SerializationFormat) -> bool {
        self.format() == format
    }
    
    /// 获取序列化配置
    fn config(&self) -> SerializationConfig {
        SerializationConfig::default()
    }
    
    /// 设置序列化配置
    fn set_config(&mut self, _config: SerializationConfig) -> Result<()> {
        // 默认实现不支持配置更新
        Err(FlareError::general_error("此序列化器不支持配置更新"))
    }
    
    /// 获取序列化统计信息
    fn stats(&self) -> SerializationStats {
        SerializationStats::default()
    }
    
    /// 重置统计信息
    fn reset_stats(&mut self) {
        // 默认实现为空
    }
    
    /// 验证数据是否可以反序列化
    async fn validate(&self, data: &[u8]) -> Result<bool> {
        match self.deserialize(data).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
    
    /// 估算序列化后的大小
    async fn estimate_size(&self, frame: &Frame) -> Result<usize> {
        // 默认实现：实际序列化并返回大小
        let data = self.serialize(frame).await?;
        Ok(data.len())
    }
    
    /// 批量序列化
    async fn serialize_batch(&self, frames: &[Frame]) -> Result<Vec<Vec<u8>>> {
        let mut results = Vec::with_capacity(frames.len());
        for frame in frames {
            results.push(self.serialize(frame).await?);
        }
        Ok(results)
    }
    
    /// 批量反序列化
    async fn deserialize_batch(&self, data_vec: &[Vec<u8>]) -> Result<Vec<Frame>> {
        let mut results = Vec::with_capacity(data_vec.len());
        for data in data_vec {
            results.push(self.deserialize(data).await?);
        }
        Ok(results)
    }
    
    /// 克隆序列化器
    fn clone_box(&self) -> Box<dyn FrameSerializer>;
    
    /// 检查是否支持流式处理
    fn supports_streaming(&self) -> bool {
        false
    }
    
    /// 检查是否支持压缩
    fn supports_compression(&self) -> bool {
        false
    }
    
    /// 获取支持的压缩算法
    fn supported_compression_algorithms(&self) -> Vec<&'static str> {
        Vec::new()
    }
    
    /// 获取MIME类型
    fn mime_type(&self) -> &'static str {
        match self.format() {
            SerializationFormat::Json => "application/json",
            SerializationFormat::MessagePack => "application/msgpack",
            SerializationFormat::Bincode => "application/octet-stream",
            SerializationFormat::Protobuf => "application/x-protobuf",
            SerializationFormat::Cbor => "application/cbor",
        }
    }
    
    /// 获取文件扩展名
    fn file_extension(&self) -> &'static str {
        match self.format() {
            SerializationFormat::Json => "json",
            SerializationFormat::MessagePack => "msgpack",
            SerializationFormat::Bincode => "bincode",
            SerializationFormat::Protobuf => "proto",
            SerializationFormat::Cbor => "cbor",
        }
    }
}

/// 序列化器特性
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializerFeature {
    /// 支持压缩
    Compression,
    /// 支持流式处理
    Streaming,
    /// 支持美化格式
    PrettyFormat,
    /// 支持模式验证
    SchemaValidation,
    /// 支持增量序列化
    IncrementalSerialization,
    /// 支持二进制格式
    BinaryFormat,
    /// 支持文本格式
    TextFormat,
    /// 支持自描述格式
    SelfDescribing,
}

/// 序列化器信息
#[derive(Debug, Clone)]
pub struct SerializerInfo {
    /// 序列化器名称
    pub name: &'static str,
    /// 序列化器版本
    pub version: &'static str,
    /// 序列化器描述
    pub description: &'static str,
    /// 支持的格式
    pub format: SerializationFormat,
    /// 支持的特性
    pub features: Vec<SerializerFeature>,
    /// MIME类型
    pub mime_type: &'static str,
    /// 文件扩展名
    pub file_extension: &'static str,
}

/// 可配置的序列化器trait
pub trait ConfigurableSerializer: FrameSerializer {
    /// 更新配置
    fn update_config(&mut self, config: SerializationConfig) -> Result<()>;
    
    /// 获取可配置的参数列表
    fn configurable_params(&self) -> Vec<&'static str>;
    
    /// 验证配置
    fn validate_config(&self, config: &SerializationConfig) -> Result<()>;
}