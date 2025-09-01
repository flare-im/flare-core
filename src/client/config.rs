//! 客户端配置 - 专注于长连接可靠性和协议竞速

use serde::{Deserialize, Serialize};
use std::time::Duration;
use crate::common::protocol::ProtocolSelection;

/// 客户端配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    /// 服务器地址
    pub server_url: String,
    /// 协议选择模式
    pub protocol_selection: ProtocolSelection,
    /// 连接配置
    pub connection: ConnectionConfig,
    /// 协议竞速配置
    pub protocol_racing: ProtocolRacingConfig,
    /// 可靠性配置
    pub reliability: ReliabilityConfig,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_url: "ws://localhost:8080".to_string(),
            protocol_selection: ProtocolSelection::Auto,
            connection: ConnectionConfig::default(),
            protocol_racing: ProtocolRacingConfig::default(),
            reliability: ReliabilityConfig::default(),
        }
    }
}

/// 连接配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    /// 远程服务器地址
    pub remote_addr: String,
    /// 连接超时（毫秒）
    pub connection_timeout_ms: u32,
    /// 心跳间隔（毫秒）
    pub heartbeat_interval_ms: u32,
    /// 心跳超时（毫秒）
    pub heartbeat_timeout_ms: u32,
    /// 最大心跳丢失次数
    pub max_missed_heartbeats: u32,
    /// 自动重连
    pub auto_reconnect: bool,
    /// 最大重连次数
    pub max_reconnect_attempts: u32,
    /// 重连延迟（毫秒）
    pub reconnect_delay_ms: u32,
    /// 启用TLS
    pub enable_tls: bool,
    /// 启用压缩
    pub enable_compression: bool,
    /// 自定义CA证书路径
    pub custom_ca_cert: Option<String>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            remote_addr: "127.0.0.1:8080".to_string(),
            connection_timeout_ms: 10000,
            heartbeat_interval_ms: 30000,
            heartbeat_timeout_ms: 10000,
            max_missed_heartbeats: 3,
            auto_reconnect: true,
            max_reconnect_attempts: 5,
            reconnect_delay_ms: 1000,
            enable_tls: true,
            enable_compression: true,
            custom_ca_cert: None,
        }
    }
}

impl ConnectionConfig {
    /// 检查是否真正启用了TLS（启用TLS且有证书）
    pub fn is_tls_enabled(&self) -> bool {
        self.enable_tls && self.custom_ca_cert.is_some()
    }

    /// 验证TLS配置
    pub fn validate_tls_config(&self) -> Result<(), String> {
        if self.enable_tls {
            if self.custom_ca_cert.is_none() {
                return Err("启用TLS时必须指定CA证书路径".to_string());
            }
            
            let ca_cert_path = self.custom_ca_cert.as_ref().unwrap();
            if !std::path::Path::new(ca_cert_path).exists() {
                return Err(format!("CA证书文件不存在: {}", ca_cert_path));
            }
        }
        Ok(())
    }
}

/// 协议竞速配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolRacingConfig {
    /// 启用协议竞速
    pub enabled: bool,
    /// 竞速测试间隔（毫秒）
    pub test_interval_ms: u32,
    /// 竞速测试超时（毫秒）
    pub test_timeout_ms: u32,
    /// 质量评估权重
    pub quality_weights: QualityWeights,
    /// 协议切换阈值
    pub switch_threshold: f32,
}

impl Default for ProtocolRacingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            test_interval_ms: 60000, // 1分钟
            test_timeout_ms: 5000,   // 5秒
            quality_weights: QualityWeights::default(),
            switch_threshold: 0.1,   // 10%的性能差异
        }
    }
}

/// 质量评估权重
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityWeights {
    /// 延迟权重
    pub latency_weight: f32,
    /// 稳定性权重
    pub stability_weight: f32,
    /// 连接时间权重
    pub connection_time_weight: f32,
}

impl Default for QualityWeights {
    fn default() -> Self {
        Self {
            latency_weight: 0.4,
            stability_weight: 0.4,
            connection_time_weight: 0.2,
        }
    }
}

/// 可靠性配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReliabilityConfig {
    /// 启用消息确认
    pub enable_message_ack: bool,
    /// 消息确认超时（毫秒）
    pub message_ack_timeout_ms: u32,
    /// 启用消息重传
    pub enable_retransmission: bool,
    /// 最大重传次数
    pub max_retransmission_attempts: u32,
    /// 重传延迟（毫秒）
    pub retransmission_delay_ms: u32,
    /// 启用消息排序
    pub enable_message_ordering: bool,
}

impl Default for ReliabilityConfig {
    fn default() -> Self {
        Self {
            enable_message_ack: true,
            message_ack_timeout_ms: 5000,
            enable_retransmission: true,
            max_retransmission_attempts: 3,
            retransmission_delay_ms: 1000,
            enable_message_ordering: false,
        }
    }
}

/// 协议选择模式扩展
impl ProtocolSelection {
    /// 是否启用QUIC
    pub fn is_quic_enabled(&self) -> bool {
        matches!(self, ProtocolSelection::QuicOnly | ProtocolSelection::Auto)
    }

    /// 是否启用WebSocket
    pub fn is_websocket_enabled(&self) -> bool {
        matches!(self, ProtocolSelection::WebSocketOnly | ProtocolSelection::Auto)
    }

    /// 获取协议名称
    pub fn as_str(&self) -> &'static str {
        match self {
            ProtocolSelection::QuicOnly => "QUIC",
            ProtocolSelection::WebSocketOnly => "WebSocket",
            ProtocolSelection::Auto => "Auto",
        }
    }
}

/// 配置构建器
pub struct ClientConfigBuilder {
    config: ClientConfig,
}

impl ClientConfigBuilder {
    /// 创建新的配置构建器
    pub fn new() -> Self {
        Self {
            config: ClientConfig::default(),
        }
    }

    /// 设置服务器URL
    pub fn server_url(mut self, url: String) -> Self {
        self.config.server_url = url.clone();
        self.config.connection.remote_addr = url;
        self
    }

    /// 设置协议选择模式
    pub fn protocol_selection(mut self, selection: ProtocolSelection) -> Self {
        self.config.protocol_selection = selection;
        self
    }

    /// 设置连接超时
    pub fn connection_timeout(mut self, timeout_ms: u32) -> Self {
        self.config.connection.connection_timeout_ms = timeout_ms;
        self
    }

    /// 设置心跳间隔
    pub fn heartbeat_interval(mut self, interval_ms: u32) -> Self {
        self.config.connection.heartbeat_interval_ms = interval_ms;
        self
    }

    /// 启用/禁用自动重连
    pub fn auto_reconnect(mut self, enabled: bool) -> Self {
        self.config.connection.auto_reconnect = enabled;
        self
    }

    /// 设置最大重连次数
    pub fn max_reconnect_attempts(mut self, attempts: u32) -> Self {
        self.config.connection.max_reconnect_attempts = attempts;
        self
    }

    /// 启用/禁用协议竞速
    pub fn protocol_racing(mut self, enabled: bool) -> Self {
        self.config.protocol_racing.enabled = enabled;
        self
    }

    /// 设置竞速测试间隔
    pub fn racing_test_interval(mut self, interval_ms: u32) -> Self {
        self.config.protocol_racing.test_interval_ms = interval_ms;
        self
    }

    /// 启用/禁用TLS
    pub fn tls(mut self, enabled: bool) -> Self {
        self.config.connection.enable_tls = enabled;
        self
    }

    /// 启用/禁用压缩
    pub fn compression(mut self, enabled: bool) -> Self {
        self.config.connection.enable_compression = enabled;
        self
    }

    /// 构建配置
    pub fn build(self) -> ClientConfig {
        self.config
    }
}

impl Default for ClientConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// 配置验证
impl ClientConfig {
    /// 验证配置的有效性
    pub fn validate(&self) -> Result<(), String> {
        if self.server_url.is_empty() {
            return Err("服务器URL不能为空".to_string());
        }

        if self.connection.connection_timeout_ms == 0 {
            return Err("连接超时必须大于0".to_string());
        }

        if self.connection.heartbeat_interval_ms == 0 {
            return Err("心跳间隔必须大于0".to_string());
        }

        if self.connection.heartbeat_timeout_ms == 0 {
            return Err("心跳超时必须大于0".to_string());
        }

        if self.connection.heartbeat_timeout_ms >= self.connection.heartbeat_interval_ms {
            return Err("心跳超时必须小于心跳间隔".to_string());
        }

        if self.protocol_racing.enabled && self.protocol_racing.test_interval_ms == 0 {
            return Err("协议竞速测试间隔必须大于0".to_string());
        }

        if self.reliability.message_ack_timeout_ms == 0 {
            return Err("消息确认超时必须大于0".to_string());
        }

        Ok(())
    }

    /// 获取连接超时Duration
    pub fn connection_timeout_duration(&self) -> Duration {
        Duration::from_millis(self.connection.connection_timeout_ms as u64)
    }

    /// 获取心跳间隔Duration
    pub fn heartbeat_interval_duration(&self) -> Duration {
        Duration::from_millis(self.connection.heartbeat_interval_ms as u64)
    }

    /// 获取心跳超时Duration
    pub fn heartbeat_timeout_duration(&self) -> Duration {
        Duration::from_millis(self.connection.heartbeat_timeout_ms as u64)
    }

    /// 获取重连延迟Duration
    pub fn reconnect_delay_duration(&self) -> Duration {
        Duration::from_millis(self.connection.reconnect_delay_ms as u64)
    }

    /// 获取消息确认超时Duration
    pub fn message_ack_timeout_duration(&self) -> Duration {
        Duration::from_millis(self.reliability.message_ack_timeout_ms as u64)
    }

    /// 获取重传延迟Duration
    pub fn retransmission_delay_duration(&self) -> Duration {
        Duration::from_millis(self.reliability.retransmission_delay_ms as u64)
    }
}