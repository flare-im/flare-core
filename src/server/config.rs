use crate::common::serialization::{SerializationConfig, SerializationFormat};

/// 服务器类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerType {
    /// WebSocket服务器
    WebSocket,
    /// QUIC服务器
    Quic,
    /// 双协议服务器
    Dual,
}

/// TLS配置
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// 证书文件路径
    pub cert_path: String,
    /// 私钥文件路径
    pub key_path: String,
}

impl TlsConfig {
    /// 创建新的TLS配置
    pub fn new(cert_path: String, key_path: String) -> Self {
        Self {
            cert_path,
            key_path,
        }
    }
}

/// 协议配置
#[derive(Debug, Clone)]
pub struct ProtocolConfig {
    /// 监听地址
    pub listen_addr: String,
    /// 最大连接数
    pub max_connections: usize,
    /// 是否启用TLS
    pub enable_tls: bool,
    /// TLS配置（启用TLS时必须提供）
    pub tls_config: Option<TlsConfig>,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:0".to_string(),
            max_connections: 1000,
            enable_tls: false,
            tls_config: None,
        }
    }
}

impl ProtocolConfig {
    /// 创建新的协议配置
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置监听地址
    pub fn with_listen_addr(mut self, addr: String) -> Self {
        self.listen_addr = addr;
        self
    }

    /// 设置最大连接数
    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// 启用TLS
    pub fn enable_tls(mut self) -> Self {
        self.enable_tls = true;
        self
    }
    
    /// 设置TLS配置
    pub fn with_tls_config(mut self, tls_config: TlsConfig) -> Self {
        self.tls_config = Some(tls_config);
        self.enable_tls = true;
        self
    }
}

/// 服务器配置
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// 服务器类型
    pub server_type: ServerType,
    /// WebSocket配置
    pub websocket_config: Option<ProtocolConfig>,
    /// QUIC配置
    pub quic_config: Option<ProtocolConfig>,
    /// 连接超时时间（毫秒）
    pub connection_timeout_ms: u64,
    /// 心跳间隔（毫秒）
    pub heartbeat_interval_ms: u64,
    /// 最大连接数
    pub max_connections: usize,
    /// 认证超时时间（毫秒）
    pub auth_timeout_ms: u64,
    /// 序列化配置
    pub serialization_config: SerializationConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self::default_websocket()
    }
}

impl ServerConfig {
    /// 创建新的服务器配置
    pub fn new() -> Self {
        Self::default()
    }
    
    /// 创建默认的WebSocket服务器配置
    /// 
    /// WebSocket服务器监听在127.0.0.1:4320，不启用TLS
    pub fn default_websocket() -> Self {
        let ws_config = ProtocolConfig::new()
            .with_listen_addr("127.0.0.1:4320".to_string())
            .with_max_connections(1000);
        
        Self {
            server_type: ServerType::WebSocket,
            websocket_config: Some(ws_config),
            quic_config: None,
            connection_timeout_ms: 30000,
            heartbeat_interval_ms: 10000,
            max_connections: 1000,
            auth_timeout_ms: 30000, // 默认认证超时时间30秒
            serialization_config: SerializationConfig::default(),
        }
    }
    
    /// 创建默认的QUIC服务器配置
    /// 
    /// QUIC服务器监听在127.0.0.1:4321，必须启用TLS
    /// 注意：使用此配置时，必须提供有效的TLS证书和私钥路径
    pub fn default_quic(cert_path: String, key_path: String) -> Self {
        let tls_config = TlsConfig::new(cert_path, key_path);
        let quic_config = ProtocolConfig::new()
            .with_listen_addr("127.0.0.1:4321".to_string())
            .with_max_connections(1000)
            .with_tls_config(tls_config);
        
        Self {
            server_type: ServerType::Quic,
            websocket_config: None,
            quic_config: Some(quic_config),
            connection_timeout_ms: 30000,
            heartbeat_interval_ms: 10000,
            max_connections: 1000,
            auth_timeout_ms: 30000, // 默认认证超时时间30秒
            serialization_config: SerializationConfig::default(),
        }
    }
    
    /// 创建默认的双协议服务器配置
    /// 
    /// WebSocket服务器监听在127.0.0.1:4320，不启用TLS
    /// QUIC服务器监听在127.0.0.1:4321，必须启用TLS
    /// 注意：使用此配置时，必须提供有效的QUIC TLS证书和私钥路径
    pub fn default_dual_protocol(quic_cert_path: String, quic_key_path: String) -> Self {
        let ws_config = ProtocolConfig::new()
            .with_listen_addr("127.0.0.1:4320".to_string())
            .with_max_connections(1000);
            
        let tls_config = TlsConfig::new(quic_cert_path, quic_key_path);
        let quic_config = ProtocolConfig::new()
            .with_listen_addr("127.0.0.1:4321".to_string())
            .with_max_connections(1000)
            .with_tls_config(tls_config);
        
        Self {
            server_type: ServerType::Dual,
            websocket_config: Some(ws_config),
            quic_config: Some(quic_config),
            connection_timeout_ms: 30000,
            heartbeat_interval_ms: 10000,
            max_connections: 1000,
            auth_timeout_ms: 30000, // 默认认证超时时间30秒
            serialization_config: SerializationConfig::default(),
        }
    }
    
    /// 设置服务器类型
    pub fn with_server_type(mut self, server_type: ServerType) -> Self {
        self.server_type = server_type;
        self
    }
    
    /// 设置WebSocket配置
    pub fn with_websocket_config(mut self, config: ProtocolConfig) -> Self {
        self.websocket_config = Some(config);
        self
    }
    
    /// 设置QUIC配置
    pub fn with_quic_config(mut self, config: ProtocolConfig) -> Self {
        self.quic_config = Some(config);
        self
    }
    
    /// 同时设置WebSocket和QUIC配置（双协议模式）
    pub fn with_dual_protocol_config(mut self, ws_config: ProtocolConfig, quic_config: ProtocolConfig) -> Self {
        self.websocket_config = Some(ws_config);
        self.quic_config = Some(quic_config);
        self.server_type = ServerType::Dual;
        self
    }
    
    /// 设置连接超时时间
    pub fn with_connection_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.connection_timeout_ms = timeout_ms;
        self
    }
    
    /// 设置心跳间隔
    pub fn with_heartbeat_interval_ms(mut self, interval_ms: u64) -> Self {
        self.heartbeat_interval_ms = interval_ms;
        self
    }
    
    /// 设置最大连接数
    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }
    
    /// 设置认证超时时间
    pub fn with_auth_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.auth_timeout_ms = timeout_ms;
        self
    }
    
    /// 设置序列化配置
    pub fn with_serialization_config(mut self, config: SerializationConfig) -> Self {
        self.serialization_config = config;
        self
    }
    
    /// 设置序列化格式
    pub fn with_serialization_format(mut self, format: SerializationFormat) -> Self {
        self.serialization_config.format = format;
        self
    }
    
    /// 检查是否配置了WebSocket
    pub fn has_websocket(&self) -> bool {
        self.websocket_config.is_some()
    }
    
    /// 检查是否配置了QUIC
    pub fn has_quic(&self) -> bool {
        self.quic_config.is_some()
    }
    
    /// 检查是否为双协议模式
    pub fn is_dual_protocol(&self) -> bool {
        self.has_websocket() && self.has_quic()
    }
    
    /// 获取WebSocket配置（如果存在）
    pub fn get_websocket_config(&self) -> Option<&ProtocolConfig> {
        self.websocket_config.as_ref()
    }
    
    /// 获取QUIC配置（如果存在）
    pub fn get_quic_config(&self) -> Option<&ProtocolConfig> {
        self.quic_config.as_ref()
    }
    
    /// 获取服务器类型
    pub fn get_server_type(&self) -> &ServerType {
        &self.server_type
    }
    
    /// 获取认证超时时间（毫秒）
    pub fn get_auth_timeout_ms(&self) -> u64 {
        self.auth_timeout_ms
    }
    
    /// 获取序列化配置
    pub fn get_serialization_config(&self) -> &SerializationConfig {
        &self.serialization_config
    }
}