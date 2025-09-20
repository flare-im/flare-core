//! 客户端配置模块
//! 
//! 提供客户端连接配置和协议选择功能

use std::collections::HashMap;
use crate::common::{
    connections::types::Transport,
    serialization::{SerializationFormat, SerializationConfig},
};
use serde::{Deserialize, Serialize};

// 添加认证配置的引用
use crate::client::auth::AuthConfig;

/// 协议选择模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProtocolSelection {
    /// 仅使用 QUIC
    QuicOnly,
    /// 仅使用 WebSocket
    WebSocketOnly,
    /// 自动选择（协议竞速）
    Auto,
}

impl Default for ProtocolSelection {
    fn default() -> Self {
        ProtocolSelection::Auto
    }
}

/// 客户端配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    /// 服务器地址映射（传输类型 -> 地址）
    pub server_addresses: HashMap<Transport, String>,
    /// 传输类型
    pub transport: Transport,
    /// 协议选择模式
    pub protocol_selection: ProtocolSelection,
    /// 是否启用自动重连
    pub enable_auto_reconnect: bool,
    /// 最大重连尝试次数
    pub max_reconnect_attempts: u32,
    /// 重连延迟（毫秒）
    pub reconnect_delay_ms: u64,
    /// 心跳间隔（毫秒）
    pub heartbeat_interval_ms: u64,
    /// 心跳监控超时（毫秒）
    pub heartbeat_monitor_timeout_ms: u64,
    /// 是否启用自动心跳响应
    pub enable_auto_heartbeat_response: bool,
    /// 序列化配置
    pub serialization_config: SerializationConfig,
    /// 请求超时时间（毫秒）
    pub request_timeout_ms: u64,
    /// 认证配置
    pub auth_config: AuthConfig,
}

impl Default for ClientConfig {
    fn default() -> Self {
        let mut server_addresses = HashMap::new();
        server_addresses.insert(Transport::WebSocket, "ws://127.0.0.1:8080".to_string());
        server_addresses.insert(Transport::Quic, "127.0.0.1:8081".to_string());
        
        Self {
            server_addresses,
            transport: Transport::WebSocket,
            protocol_selection: ProtocolSelection::Auto,
            enable_auto_reconnect: true,
            max_reconnect_attempts: 5,
            reconnect_delay_ms: 1000,
            heartbeat_interval_ms: 10000,
            heartbeat_monitor_timeout_ms: 30000,
            enable_auto_heartbeat_response: true,
            serialization_config: SerializationConfig::default(),
            request_timeout_ms: 5000, // 默认5秒超时
            auth_config: AuthConfig::default(),
        }
    }
}

impl ClientConfig {
    /// 创建新的客户端配置，指定WebSocket和QUIC地址
    pub fn new(websocket_addr: String, quic_addr: String) -> Self {
        let mut server_addresses = HashMap::new();
        server_addresses.insert(Transport::WebSocket, websocket_addr);
        server_addresses.insert(Transport::Quic, quic_addr);
        
        Self {
            server_addresses,
            transport: Transport::WebSocket,
            protocol_selection: ProtocolSelection::Auto,
            enable_auto_reconnect: true,
            max_reconnect_attempts: 5,
            reconnect_delay_ms: 1000,
            heartbeat_interval_ms: 10000,
            heartbeat_monitor_timeout_ms: 30000,
            enable_auto_heartbeat_response: true,
            serialization_config: SerializationConfig::default(),
            request_timeout_ms: 5000, // 默认5秒超时
            auth_config: AuthConfig::default(),
        }
    }
    
    /// 为特定传输设置服务器地址
    pub fn with_server_address(mut self, transport: Transport, address: String) -> Self {
        self.server_addresses.insert(transport, address);
        self
    }
    
    /// 获取指定传输的服务器地址
    pub fn get_server_address(&self, transport: Transport) -> Option<&String> {
        self.server_addresses.get(&transport)
    }
    
    /// 获取所有配置的传输地址
    pub fn get_all_server_addresses(&self) -> &HashMap<Transport, String> {
        &self.server_addresses
    }

    /// 设置协议选择模式
    pub fn with_protocol_selection(mut self, selection: ProtocolSelection) -> Self {
        self.protocol_selection = selection;
        self
    }

    /// 设置仅使用 QUIC 传输
    pub fn with_quic_only(mut self) -> Self {
        self.protocol_selection = ProtocolSelection::QuicOnly;
        self.transport = Transport::Quic;
        self
    }

    /// 设置仅使用 WebSocket 传输
    pub fn with_websocket_only(mut self) -> Self {
        self.protocol_selection = ProtocolSelection::WebSocketOnly;
        self.transport = Transport::WebSocket;
        self
    }

    /// 设置心跳间隔和超时
    pub fn with_heartbeat(mut self, interval_ms: u64, timeout_ms: u64) -> Self {
        self.heartbeat_interval_ms = interval_ms;
        self.heartbeat_monitor_timeout_ms = timeout_ms;
        self
    }

    /// 设置序列化格式
    pub fn with_serialization(mut self,config: SerializationConfig) -> Self {
        self.serialization_config = config;
        self
    }
    
    /// 设置请求超时时间
    pub fn with_request_timeout(mut self, timeout_ms: u64) -> Self {
        self.request_timeout_ms = timeout_ms;
        self
    }
    
    /// 设置认证配置
    pub fn with_auth_config(mut self, auth_config: AuthConfig) -> Self {
        self.auth_config = auth_config;
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
}