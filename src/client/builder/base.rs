//! 客户端构建器基类
//!
//! 提供所有构建器共享的配置方法，减少代码重复

use crate::client::config::ClientConfig;
use crate::common::compression::CompressionAlgorithm;
use crate::common::config_types::{HeartbeatConfig, TlsConfig, TransportProtocol};
use crate::common::device::DeviceInfo;
use crate::common::protocol::SerializationFormat;
use std::time::Duration;

/// 客户端构建器基类配置
///
/// 提供所有构建器共享的配置方法
/// 使用组合模式，让各个构建器可以复用这些配置逻辑
pub struct BaseClientBuilderConfig {
    pub config: ClientConfig,
}

impl BaseClientBuilderConfig {
    /// 创建新的构建器配置
    pub fn new(server_url: impl Into<String>) -> Self {
        Self {
            config: ClientConfig::new(server_url.into()),
        }
    }

    /// 设置传输协议
    #[must_use]
    pub fn with_protocol(mut self, protocol: TransportProtocol) -> Self {
        self.config.transport = protocol;
        self
    }

    /// 启用多协议竞速
    #[must_use]
    pub fn with_protocol_race(mut self, protocols: Vec<TransportProtocol>) -> Self {
        self.config = self.config.with_protocol_race(protocols);
        self
    }

    /// 为特定协议设置服务器地址
    #[must_use]
    pub fn with_protocol_url(mut self, protocol: TransportProtocol, url: String) -> Self {
        self.config = self.config.with_protocol_url(protocol, url);
        self
    }

    /// 设置用户 ID
    #[must_use]
    pub fn with_user_id(mut self, user_id: String) -> Self {
        self.config = self.config.with_user_id(user_id);
        self
    }

    /// 设置 Token（用于认证）
    #[must_use]
    pub fn with_token(mut self, token: String) -> Self {
        self.config = self.config.with_token(token);
        self
    }

    /// 设置序列化格式（用于协商，默认 JSON）
    #[must_use]
    pub fn with_format(mut self, format: SerializationFormat) -> Self {
        self.config = self.config.with_format(format);
        self
    }

    /// 设置压缩算法（用于协商，默认 None）
    #[must_use]
    pub fn with_compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.config = self.config.with_compression(compression);
        self
    }

    /// 强制指定序列化格式（不进行协商）
    #[must_use]
    pub fn force_format(mut self, format: SerializationFormat) -> Self {
        self.config = self.config.force_format(format);
        self
    }

    /// 强制指定压缩算法（不进行协商）
    #[must_use]
    pub fn force_compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.config = self.config.force_compression(compression);
        self
    }

    /// 设置设备信息
    #[must_use]
    pub fn with_device_info(mut self, device_info: DeviceInfo) -> Self {
        self.config = self.config.with_device_info(device_info);
        self
    }

    /// 设置心跳配置
    #[must_use]
    pub fn with_heartbeat(mut self, heartbeat: HeartbeatConfig) -> Self {
        self.config = self.config.with_heartbeat(heartbeat);
        self
    }

    /// 设置 TLS 配置
    #[must_use]
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.config = self.config.with_tls(tls);
        self
    }

    /// 设置连接超时
    #[must_use]
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.config = self.config.with_connect_timeout(timeout);
        self
    }

    /// 设置协议竞速超时
    #[must_use]
    pub fn with_race_timeout(mut self, timeout: Duration) -> Self {
        self.config = self.config.with_race_timeout(timeout);
        self
    }

    /// 设置重连间隔
    #[must_use]
    pub fn with_reconnect_interval(mut self, interval: Duration) -> Self {
        self.config = self.config.with_reconnect_interval(interval);
        self
    }

    /// 设置最大重连次数
    #[must_use]
    pub fn with_max_reconnect_attempts(mut self, max: Option<u32>) -> Self {
        self.config = self.config.with_max_reconnect_attempts(max);
        self
    }

    /// 启用消息路由
    #[must_use]
    pub fn enable_router(mut self) -> Self {
        self.config = self.config.enable_router();
        self
    }
}
