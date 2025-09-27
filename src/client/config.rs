//! 客户端配置模块
//! 
//! 提供客户端连接配置和协议选择功能

use std::collections::HashMap;
use crate::common::{
    connections::types::Transport,
    serialization::SerializationConfig,
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
    
    /// 设置重连参数
    pub fn with_reconnect_params(mut self, max_attempts: u32, delay_ms: u64) -> Self {
        self.max_reconnect_attempts = max_attempts;
        self.reconnect_delay_ms = delay_ms;
        self
    }
    
    /// 转换为连接配置
    /// 
    /// 根据客户端配置创建对应的连接配置
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// * `transport` - 传输类型（可选，如果不指定则使用配置中的默认传输）
    /// 
    /// # 返回值
    /// 返回对应的连接配置
    pub fn to_connection_config(&self, connection_id: String, transport: Option<Transport>) -> crate::common::connections::config::ConnectionConfig {
        use crate::common::connections::config::ConnectionConfig;
        
        // 确定使用的传输类型
        let target_transport = transport.unwrap_or(self.transport);
        
        // 获取对应的服务器地址
        let remote_addr = self.get_server_address(target_transport)
            .cloned()
            .unwrap_or_else(|| {
                match target_transport {
                    Transport::WebSocket => "ws://127.0.0.1:8080".to_string(),
                    Transport::Quic => "127.0.0.1:8081".to_string(),
                    _ => "127.0.0.1:8080".to_string(),
                }
            });
        
        // 创建基础连接配置
        let mut conn_config = ConnectionConfig::client(connection_id, remote_addr);
        
        // 设置传输类型
        conn_config.transport = target_transport;
        
        // 设置心跳配置
        conn_config.heartbeat_interval_ms = self.heartbeat_interval_ms;
        conn_config.heartbeat_timeout_ms = self.heartbeat_monitor_timeout_ms / 3; // 心跳超时为监控超时的1/3
        
        // 设置序列化配置
        conn_config.serialization_config = Some(self.serialization_config.clone());
        
        // 设置客户端特有配置
        if let Some(client_config) = &mut conn_config.client_config {
            client_config.enable_tls = false; // 默认不启用TLS，可根据需要调整
            client_config.auto_reconnect = self.enable_auto_reconnect;
            client_config.max_reconnect_attempts = self.max_reconnect_attempts;
            client_config.reconnect_delay_ms = self.reconnect_delay_ms;
            client_config.user_id = self.auth_config.user_id.clone();
            client_config.token = self.auth_config.token.clone();
            // 设置平台信息
            if let Some(platform_str) = &self.auth_config.platform {
                client_config.platform = Some(crate::common::connections::types::Platform::from_str(platform_str));
            }
        }
        
        conn_config
    }
    
    /// 转换为WebSocket连接配置
    /// 
    /// 专门用于创建WebSocket连接配置
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// 
    /// # 返回值
    /// 返回WebSocket连接配置，如果未配置WebSocket地址则返回None
    pub fn to_websocket_connection_config(&self, connection_id: String) -> Option<crate::common::connections::config::ConnectionConfig> {
        if self.get_server_address(Transport::WebSocket).is_some() || self.transport == Transport::WebSocket {
            Some(self.to_connection_config(connection_id, Some(Transport::WebSocket)))
        } else {
            None
        }
    }
    
    /// 转换为QUIC连接配置
    /// 
    /// 专门用于创建QUIC连接配置
    /// 
    /// # 参数
    /// * `connection_id` - 连接ID
    /// 
    /// # 返回值
    /// 返回QUIC连接配置，如果未配置QUIC地址则返回None
    pub fn to_quic_connection_config(&self, connection_id: String) -> Option<crate::common::connections::config::ConnectionConfig> {
        if self.get_server_address(Transport::Quic).is_some() || self.transport == Transport::Quic {
            Some(self.to_connection_config(connection_id, Some(Transport::Quic)))
        } else {
            None
        }
    }
    
    /// 验证配置的有效性
    /// 
    /// # 返回值
    /// 如果配置有效返回Ok，否则返回错误信息
    pub fn validate(&self) -> Result<(), String> {
        // 检查是否至少配置了一个服务器地址
        if self.server_addresses.is_empty() {
            return Err("至少需要配置一个服务器地址".to_string());
        }
        
        // 检查心跳配置的合理性
        if self.heartbeat_interval_ms == 0 {
            return Err("心跳间隔必须大于0".to_string());
        }
        
        if self.heartbeat_monitor_timeout_ms <= self.heartbeat_interval_ms {
            return Err("心跳监控超时必须大于心跳间隔".to_string());
        }
        
        // 检查重连配置的合理性
        if self.enable_auto_reconnect && self.max_reconnect_attempts == 0 {
            return Err("启用自动重连时，最大重连次数必须大于0".to_string());
        }
        
        if self.reconnect_delay_ms == 0 {
            return Err("重连延迟必须大于0".to_string());
        }
        
        // 检查认证配置的合理性
        if self.auth_config.enabled {
            if self.auth_config.user_id.is_none() {
                return Err("启用认证时，用户ID不能为空".to_string());
            }
            if self.auth_config.timeout_ms == 0 {
                return Err("认证超时必须大于0".to_string());
            }
        }
        
        // 检查请求超时的合理性
        if self.request_timeout_ms == 0 {
            return Err("请求超时必须大于0".to_string());
        }
        
        Ok(())
    }
    
    /// 创建高性能客户端配置
    /// 
    /// 针对高吞吐量场景优化的配置
    pub fn high_performance() -> Self {
        Self::default()
            .with_heartbeat(30000, 90000) // 30秒心跳间隔，90秒监控超时
            .with_request_timeout(10000) // 10秒请求超时
            .with_serialization(crate::common::serialization::SerializationConfig {
                format: crate::common::serialization::SerializationFormat::Protobuf,
                ..Default::default()
            })
    }
    
    /// 创建低延迟客户端配置
    /// 
    /// 针对实时通信场景优化的配置
    pub fn low_latency() -> Self {
        Self::default()
            .with_heartbeat(5000, 15000) // 5秒心跳间隔，15秒监控超时
            .with_request_timeout(3000) // 3秒请求超时
            .with_serialization(crate::common::serialization::SerializationConfig {
                format: crate::common::serialization::SerializationFormat::Cbor,
                ..Default::default()
            })
    }
    
    /// 创建稳定连接客户端配置
    /// 
    /// 针对长时间连接场景优化的配置
    pub fn stable() -> Self {
        Self::default()
            .with_heartbeat(60000, 180000) // 1分钟心跳间隔，3分钟监控超时
            .with_request_timeout(30000) // 30秒请求超时
            .with_reconnect_params(10, 5000) // 最多重连10次，5秒延迟
            .with_serialization(crate::common::serialization::SerializationConfig {
                format: crate::common::serialization::SerializationFormat::Json,
                ..Default::default()
            })
    }
    
    /// 创建生产环境客户端配置
    /// 
    /// 针对生产环境优化的综合配置
    pub fn production() -> Self {
        Self::default()
            .with_heartbeat(30000, 90000) // 30秒心跳间隔，90秒监控超时
            .with_request_timeout(15000) // 15秒请求超时
            .with_reconnect_params(5, 3000) // 最多重连5次，3秒延迟
            .with_auth_enabled(true) // 启用认证
            .with_serialization(crate::common::serialization::SerializationConfig {
                format: crate::common::serialization::SerializationFormat::Protobuf,
                ..Default::default()
            })
    }
}