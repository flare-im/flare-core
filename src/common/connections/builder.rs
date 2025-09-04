//! 连接构建器
//!
//! 提供更灵活的连接创建方式，支持直接传入序列化器实例

use std::sync::Arc;
use crate::common::{
    connections::{ConnectionConfig, ConnectionType, ConnectionRole},
    serialization::{FrameSerializer, SerializationFormat, SerializationConfig},
};

/// 连接构建器
pub struct ConnectionBuilder {
    /// 基础配置
    config: ConnectionConfig,
    /// 自定义序列化器（可选）
    custom_serializer: Option<Arc<Box<dyn FrameSerializer>>>,
}

impl ConnectionBuilder {
    /// 创建新的连接构建器
    pub fn new(id: String) -> Self {
        Self {
            config: ConnectionConfig {
                id,
                ..Default::default()
            },
            custom_serializer: None,
        }
    }
    
    /// 创建客户端连接构建器
    pub fn client(id: String, remote_addr: String) -> Self {
        Self {
            config: ConnectionConfig::client(id, remote_addr),
            custom_serializer: None,
        }
    }
    
    /// 创建服务端连接构建器
    pub fn server(id: String, local_addr: String) -> Self {
        Self {
            config: ConnectionConfig::server(id, local_addr),
            custom_serializer: None,
        }
    }
    
    /// 设置连接类型
    pub fn with_type(mut self, connection_type: ConnectionType) -> Self {
        self.config.connection_type = connection_type;
        self
    }
    
    /// 设置远程地址
    pub fn with_remote_addr(mut self, addr: String) -> Self {
        self.config.remote_addr = addr;
        self
    }
    
    /// 设置超时时间
    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.config.timeout_ms = timeout_ms;
        self
    }
    
    /// 设置心跳间隔
    pub fn with_heartbeat_interval(mut self, interval_ms: u64) -> Self {
        self.config.heartbeat_interval_ms = interval_ms;
        self
    }
    
    /// 启用TLS
    pub fn with_tls(mut self) -> Self {
        self.config.enable_tls = true;
        self
    }
    
    /// 设置缓冲区大小
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.config.buffer_size = size;
        self
    }
    
    /// 启用自动重连
    pub fn with_auto_reconnect(mut self, max_attempts: u32, delay_ms: u64) -> Self {
        self.config.auto_reconnect = true;
        self.config.max_reconnect_attempts = max_attempts;
        self.config.reconnect_delay_ms = delay_ms;
        self
    }
    
    // ========== 序列化器配置方法（原有方式） ==========
    
    /// 使用序列化格式（通过工厂创建）
    pub fn with_serialization_format(mut self, format: SerializationFormat) -> Self {
        self.config.serialization_format = Some(format);
        self.custom_serializer = None; // 清除自定义序列化器
        self
    }
    
    /// 使用序列化配置（通过工厂创建）
    pub fn with_serialization_config(mut self, config: SerializationConfig) -> Self {
        self.config.serialization_config = Some(config);
        self.custom_serializer = None; // 清除自定义序列化器
        self
    }
    
    /// 使用JSON序列化
    pub fn with_json_serialization(mut self) -> Self {
        self.config.serialization_format = Some(SerializationFormat::Json);
        self.config.serialization_config = Some(SerializationConfig::default());
        self.custom_serializer = None; // 清除自定义序列化器
        self
    }
    
    /// 使用美化JSON序列化
    pub fn with_pretty_json_serialization(mut self) -> Self {
        self.config.serialization_format = Some(SerializationFormat::Json);
        self.config.serialization_config = Some(SerializationConfig::default().with_pretty_format());
        self.custom_serializer = None; // 清除自定义序列化器
        self
    }
    
    /// 使用高性能序列化（Bincode）
    pub fn with_high_performance_serialization(mut self) -> Self {
        self.config.serialization_format = Some(SerializationFormat::Bincode);
        self.config.serialization_config = Some(SerializationConfig::default());
        self.custom_serializer = None; // 清除自定义序列化器
        self
    }
    
    // ========== 直接序列化器设置方法（新方式） ==========
    
    /// 设置自定义序列化器（最灵活的方式）
    pub fn with_serializer(mut self, serializer: Arc<Box<dyn FrameSerializer>>) -> Self {
        self.custom_serializer = Some(serializer);
        // 清除基于配置的序列化设置
        self.config.serialization_format = None;
        self.config.serialization_config = None;
        self
    }
    
    /// 设置自定义序列化器（便捷方法，自动包装）
    pub fn with_custom_serializer<T: FrameSerializer + 'static>(mut self, serializer: T) -> Self {
        self.custom_serializer = Some(Arc::new(Box::new(serializer)));
        // 清除基于配置的序列化设置
        self.config.serialization_format = None;
        self.config.serialization_config = None;
        self
    }
    
    // ========== 预定义配置方法 ==========
    
    /// 超低延迟配置（游戏、交易场景）
    pub fn ultra_low_latency(mut self) -> Self {
        self.config.buffer_size = 16384; // 16KB
        self.config.max_message_size = 512 * 1024; // 512KB
        self.config.heartbeat_interval_ms = 5000; // 5秒
        self.config.heartbeat_timeout_ms = 2000; // 2秒
        self.config.connection_type = ConnectionType::Quic; // QUIC更适合低延迟
        
        // 使用紧凑JSON序列化，带严格大小限制
        self.config.serialization_format = Some(SerializationFormat::Json);
        self.config.serialization_config = Some(
            SerializationConfig::default()
                .with_max_size(32 * 1024) // 32KB限制
        );
        self.custom_serializer = None;
        self
    }
    
    /// 高吞吐量配置（批量处理场景）
    pub fn high_throughput(mut self) -> Self {
        self.config.buffer_size = 1024 * 1024; // 1MB
        self.config.max_message_size = 64 * 1024 * 1024; // 64MB
        self.config.heartbeat_interval_ms = 30000; // 30秒
        self.config.connection_type = ConnectionType::WebSocket; // WebSocket更适合大数据传输
        
        // 使用高性能序列化
        self.config.serialization_format = Some(SerializationFormat::Bincode);
        self.config.serialization_config = Some(SerializationConfig::default());
        self.custom_serializer = None;
        self
    }
    
    /// 调试友好配置
    pub fn debug_friendly(mut self) -> Self {
        self.config.connection_type = ConnectionType::WebSocket; // 便于抓包分析
        
        // 使用美化JSON，便于调试
        self.config.serialization_format = Some(SerializationFormat::Json);
        self.config.serialization_config = Some(
            SerializationConfig::default()
                .with_pretty_format()
        );
        self.custom_serializer = None;
        self
    }
    
    /// 获取配置
    pub fn get_config(&self) -> &ConnectionConfig {
        &self.config
    }
    
    /// 获取自定义序列化器
    pub fn get_custom_serializer(&self) -> Option<&Arc<Box<dyn FrameSerializer>>> {
        self.custom_serializer.as_ref()
    }
    
    /// 构建最终配置（内部使用）
    pub fn build_config(self) -> ConnectionConfig {
        self.config
    }
    
    /// 构建连接（返回配置和可选的序列化器）
    pub fn build(self) -> (ConnectionConfig, Option<Arc<Box<dyn FrameSerializer>>>) {
        (self.config, self.custom_serializer)
    }
    
    /// 检查是否使用自定义序列化器
    pub fn uses_custom_serializer(&self) -> bool {
        self.custom_serializer.is_some()
    }
    
    /// 获取序列化器描述（用于日志）
    pub fn serializer_description(&self) -> String {
        if let Some(serializer) = &self.custom_serializer {
            format!("Custom({})", serializer.name())
        } else {
            let format = self.config.get_serialization_format();
            let config = self.config.get_serialization_config();
            if config.pretty_format {
                format!("{}(Pretty)", format)
            } else {
                format.to_string()
            }
        }
    }
}

impl Default for ConnectionBuilder {
    fn default() -> Self {
        Self::new("default".to_string())
    }
}

/// 便捷的连接构建器创建函数
pub fn connection() -> ConnectionBuilder {
    ConnectionBuilder::new("auto_generated".to_string())
}

/// 便捷的客户端连接构建器创建函数
pub fn client_connection(id: String, remote_addr: String) -> ConnectionBuilder {
    ConnectionBuilder::client(id, remote_addr)
}

/// 便捷的服务端连接构建器创建函数
pub fn server_connection(id: String, local_addr: String) -> ConnectionBuilder {
    ConnectionBuilder::server(id, local_addr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::serialization::JsonSerializer;
    
    #[test]
    fn test_connection_builder_basic() {
        let builder = ConnectionBuilder::client("test".to_string(), "ws://localhost:8080".to_string());
        let config = builder.build_config();
        
        assert_eq!(config.id, "test");
        assert_eq!(config.remote_addr, "ws://localhost:8080");
        assert_eq!(config.role, ConnectionRole::Client);
    }
    
    #[test]
    fn test_connection_builder_with_serialization() {
        let builder = ConnectionBuilder::client("test".to_string(), "ws://localhost:8080".to_string())
            .with_json_serialization();
        
        let (config, custom_serializer) = builder.build();
        
        assert_eq!(config.get_serialization_format(), SerializationFormat::Json);
        assert!(custom_serializer.is_none());
    }
    
    #[test]
    fn test_connection_builder_with_custom_serializer() {
        let custom_serializer = JsonSerializer::new();
        let builder = ConnectionBuilder::client("test".to_string(), "ws://localhost:8080".to_string())
            .with_custom_serializer(custom_serializer);
        
        let (config, serializer) = builder.build();
        
        assert!(serializer.is_some());
        assert!(config.serialization_format.is_none());
    }
    
    #[test]
    fn test_ultra_low_latency_preset() {
        let builder = ConnectionBuilder::client("game".to_string(), "quic://game-server:4433".to_string())
            .ultra_low_latency();
        
        let config = builder.build_config();
        
        assert_eq!(config.connection_type, ConnectionType::Quic);
        assert_eq!(config.buffer_size, 16384);
        assert_eq!(config.heartbeat_interval_ms, 5000);
        assert_eq!(config.get_serialization_format(), SerializationFormat::Json);
    }
}