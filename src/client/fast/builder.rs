//! FastClient 构建器
//!
//! 提供链式 API 来构建 FastClient 实例

use std::sync::Arc;

use crate::client::{
    config::{ClientConfig, ProtocolSelection},
};

use super::auth::AuthConfig;

use super::{
    client::FastClient,
    event::FastEvent,
};

/// FastClient构建器
pub struct FastClientBuilder {
    config: ClientConfig,
    auth_config: AuthConfig,
    event_handler: Option<Arc<dyn FastEvent>>,
}

impl FastClientBuilder {
    /// 创建新的构建器
    pub fn new() -> Self {
        Self {
            config: ClientConfig::default(),
            auth_config: AuthConfig::default(),
            event_handler: None,
        }
    }
    
    /// 设置事件处理器
    pub fn with_event_handler(mut self, event_handler: Arc<dyn FastEvent>) -> Self {
        self.event_handler = Some(event_handler);
        self
    }
    
    /// 设置服务器地址
    pub fn with_server_address(mut self, transport: crate::common::connections::types::Transport, address: String) -> Self {
        self.config = self.config.with_server_address(transport, address);
        self
    }
    
    /// 设置协议选择模式
    pub fn with_protocol_selection(mut self, selection: ProtocolSelection) -> Self {
        self.config = self.config.with_protocol_selection(selection);
        self
    }
    
    /// 设置仅使用 QUIC 协议
    pub fn with_quic_only(mut self) -> Self {
        self.config = self.config.with_quic_only();
        self
    }
    
    /// 设置仅使用 WebSocket 协议
    pub fn with_websocket_only(mut self) -> Self {
        self.config = self.config.with_websocket_only();
        self
    }
    
    /// 设置心跳间隔和超时
    pub fn with_heartbeat(mut self, interval_ms: u64, timeout_ms: u64) -> Self {
        self.config = self.config.with_heartbeat(interval_ms, timeout_ms);
        self
    }
    
    /// 启用或禁用自动重连
    pub fn with_auto_reconnect(mut self, enabled: bool) -> Self {
        if enabled {
            self.config.max_reconnect_attempts = 5; // 默认重连5次
        } else {
            self.config.max_reconnect_attempts = 0; // 禁用重连
        }
        self
    }
    
    /// 设置重连参数
    pub fn with_reconnect_params(mut self, max_attempts: u32, delay_ms: u64) -> Self {
        self.config.max_reconnect_attempts = max_attempts;
        self.config.reconnect_delay_ms = delay_ms;
        self
    }
    
    /// 启用认证
    pub fn with_auth_enabled(mut self, enabled: bool) -> Self {
        self.auth_config.enabled = enabled;
        self
    }
    
    /// 设置认证用户ID
    pub fn with_auth_user_id(mut self, user_id: String) -> Self {
        self.auth_config.user_id = Some(user_id);
        self
    }
    
    /// 设置认证平台
    pub fn with_auth_platform(mut self, platform: String) -> Self {
        self.auth_config.platform = Some(platform);
        self
    }
    
    /// 设置认证令牌
    pub fn with_auth_token(mut self, token: String) -> Self {
        self.auth_config.token = Some(token);
        self
    }
    
    /// 设置认证超时时间
    pub fn with_auth_timeout(mut self, timeout_ms: u64) -> Self {
        self.auth_config.timeout_ms = timeout_ms;
        self
    }
    
    /// 设置完整的认证配置
    pub fn with_auth_config(mut self, auth_config: AuthConfig) -> Self {
        self.auth_config = auth_config;
        self
    }
    
    /// 设置序列化格式
    pub fn with_serialization(mut self, config: crate::common::serialization::SerializationConfig) -> Self {
        self.config = self.config.with_serialization(config);
        self
    }
    
    /// 设置请求超时时间
    pub fn with_request_timeout(mut self, timeout_ms: u64) -> Self {
        self.config.request_timeout_ms = timeout_ms;
        self
    }
    
    /// 构建FastClient实例
    pub fn build(self) -> FastClient {
        if let Some(event_handler) = self.event_handler {
            FastClient::new(self.config, self.auth_config, event_handler)
        } else {
            FastClient::with_default_handler(self.config, self.auth_config)
        }
    }
}

impl Default for FastClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}
