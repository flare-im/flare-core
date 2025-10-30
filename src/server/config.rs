use crate::common::parsing::PayloadCodec;
use crate::common::connections::config::ConnectionConfig;
use crate::common::connections::enums::Transport;

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
    /// 是否要求客户端证书（双向TLS）
    pub require_client_auth: bool,
    /// 客户端CA证书路径（用于验证客户端证书）
    pub client_ca_cert_path: Option<String>,
}

impl TlsConfig {
    /// 创建新的TLS配置
    pub fn new(cert_path: String, key_path: String) -> Self {
        Self {
            cert_path,
            key_path,
            require_client_auth: false,
            client_ca_cert_path: None,
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

/// 服务端性能优化配置
#[derive(Debug, Clone)]
pub struct ServerPerformanceConfig {
    /// 工作线程数（0表示使用CPU核心数）
    pub worker_threads: usize,
    /// 是否启用CPU亲和性
    pub enable_cpu_affinity: bool,
    /// 是否启用NUMA感知
    pub enable_numa_awareness: bool,
    /// 内存池大小（字节）
    pub memory_pool_size: usize,
    /// 是否启用零拷贝优化
    pub enable_zero_copy: bool,
    /// 批量处理大小
    pub batch_size: usize,
    /// 是否启用连接池
    pub enable_connection_pool: bool,
    /// 连接池大小
    pub connection_pool_size: usize,
}

impl Default for ServerPerformanceConfig {
    fn default() -> Self {
        Self {
            worker_threads: 0, // 0表示自动检测CPU核心数
            enable_cpu_affinity: false,
            enable_numa_awareness: false,
            memory_pool_size: 64 * 1024 * 1024, // 64MB
            enable_zero_copy: false,
            batch_size: 100,
            enable_connection_pool: true,
            connection_pool_size: 1000,
        }
    }
}

/// 服务端安全配置
#[derive(Debug, Clone)]
pub struct ServerSecurityConfig {
    /// 是否启用速率限制
    pub enable_rate_limiting: bool,
    /// 每IP最大连接数
    pub max_connections_per_ip: usize,
    /// 请求速率限制（每秒请求数）
    pub rate_limit_per_second: u32,
    /// 是否启用黑名单
    pub enable_blacklist: bool,
    /// 黑名单文件路径
    pub blacklist_file_path: Option<String>,
    /// 是否启用白名单
    pub enable_whitelist: bool,
    /// 白名单文件路径
    pub whitelist_file_path: Option<String>,
    /// 最大消息大小（字节）
    pub max_message_size: usize,
    /// 是否启用消息加密
    pub enable_message_encryption: bool,
}

impl Default for ServerSecurityConfig {
    fn default() -> Self {
        Self {
            enable_rate_limiting: true,
            max_connections_per_ip: 10,
            rate_limit_per_second: 100,
            enable_blacklist: false,
            blacklist_file_path: None,
            enable_whitelist: false,
            whitelist_file_path: None,
            max_message_size: 10 * 1024 * 1024, // 10MB
            enable_message_encryption: false,
        }
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
    /// 心跳超时时间（毫秒）
    pub heartbeat_timeout_ms: u64,
    /// 最大心跳丢失次数
    pub max_missed_heartbeats: u32,
    /// 最大连接数
    pub max_connections: usize,
    /// 认证超时时间（毫秒）
    pub auth_timeout_ms: u64,
    /// 缓冲区大小（字节）
    pub buffer_size: usize,
    /// 是否启用自动心跳响应
    pub auto_heartbeat_response: bool,
    /// 心跳监控超时（毫秒）
    pub heartbeat_monitor_timeout_ms: u64,
    /// 连接清理间隔（毫秒）
    pub cleanup_interval_ms: u64,
    /// 序列化配置
    /// 序列化编解码器
    pub serialization_codec: PayloadCodec,
    /// 性能优化配置
    pub performance_config: ServerPerformanceConfig,
    /// 安全配置
    pub security_config: ServerSecurityConfig,
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
            heartbeat_timeout_ms: 5000,
            max_missed_heartbeats: 3,
            max_connections: 1000,
            auth_timeout_ms: 30000, // 默认认证超时时间30秒
            buffer_size: 65536, // 64KB
            auto_heartbeat_response: true,
            heartbeat_monitor_timeout_ms: 20000,
            cleanup_interval_ms: 60000,
            serialization_codec: PayloadCodec::default(),
            performance_config: ServerPerformanceConfig::default(),
            security_config: ServerSecurityConfig::default(),
        }
    }
    
    /// 创建默认的QUIC服务器配置
    /// 
    /// QUIC服务器监听在127.0.0.1:8081，必须启用TLS
    /// 注意：使用此配置时，必须提供有效的TLS证书和私钥路径
    pub fn default_quic(cert_path: String, key_path: String) -> Self {
        let tls_config = TlsConfig::new(cert_path, key_path);
        let quic_config = ProtocolConfig::new()
            .with_listen_addr("127.0.0.1:8081".to_string())
            .with_max_connections(1000)
            .with_tls_config(tls_config);
        
        Self {
            server_type: ServerType::Quic,
            websocket_config: None,
            quic_config: Some(quic_config),
            connection_timeout_ms: 30000,
            heartbeat_interval_ms: 10000,
            heartbeat_timeout_ms: 5000,
            max_missed_heartbeats: 3,
            max_connections: 1000,
            auth_timeout_ms: 30000, // 默认认证超时时间30秒
            buffer_size: 65536, // 64KB
            auto_heartbeat_response: true,
            heartbeat_monitor_timeout_ms: 20000,
            cleanup_interval_ms: 60000,
            serialization_codec: PayloadCodec::default(),
            performance_config: ServerPerformanceConfig::default(),
            security_config: ServerSecurityConfig::default(),
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
            heartbeat_timeout_ms: 5000,
            max_missed_heartbeats: 3,
            max_connections: 1000,
            auth_timeout_ms: 30000, // 默认认证超时时间30秒
            buffer_size: 65536, // 64KB
            auto_heartbeat_response: true,
            heartbeat_monitor_timeout_ms: 20000,
            cleanup_interval_ms: 60000,
            serialization_codec: PayloadCodec::default(),
            performance_config: ServerPerformanceConfig::default(),
            security_config: ServerSecurityConfig::default(),
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
    
    /// 设置心跳超时时间
    pub fn with_heartbeat_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.heartbeat_timeout_ms = timeout_ms;
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
    
    /// 设置序列化编觧码器
    pub fn with_serialization_codec(mut self, codec: PayloadCodec) -> Self {
        self.serialization_codec = codec;
        self
    }
    
    /// 设置心跳配置
    pub fn with_heartbeat_config(mut self, interval_ms: u64, timeout_ms: u64, max_missed: u32) -> Self {
        self.heartbeat_interval_ms = interval_ms;
        self.heartbeat_timeout_ms = timeout_ms;
        self.max_missed_heartbeats = max_missed;
        self
    }
    
    /// 设置缓冲区大小
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }
    
    /// 设置自动心跳响应
    pub fn with_auto_heartbeat_response(mut self, auto_response: bool) -> Self {
        self.auto_heartbeat_response = auto_response;
        self
    }
    
    /// 设置心跳监控配置
    pub fn with_heartbeat_monitoring(mut self, monitor_timeout_ms: u64, cleanup_interval_ms: u64) -> Self {
        self.heartbeat_monitor_timeout_ms = monitor_timeout_ms;
        self.cleanup_interval_ms = cleanup_interval_ms;
        self
    }
    
    /// 设置性能配置
    pub fn with_performance_config(mut self, config: ServerPerformanceConfig) -> Self {
        self.performance_config = config;
        self
    }
    
    /// 设置安全配置
    pub fn with_security_config(mut self, config: ServerSecurityConfig) -> Self {
        self.security_config = config;
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
    /// 获取序列化编解码器
    pub fn get_serialization_codec(&self) -> PayloadCodec {
        self.serialization_codec
    }
    
    /// 转换为连接配置
    /// 
    /// 根据服务器类型和配置创建对应的连接配置
    /// 对于双协议服务器，会创建WebSocket连接配置（默认）
    pub fn to_connection_config(&self, connection_id: String) -> ConnectionConfig {
        match self.server_type {
            ServerType::WebSocket => {
                if let Some(ws_config) = &self.websocket_config {
                    self.create_websocket_connection_config(connection_id, ws_config.listen_addr.clone())
                } else {
                    // 如果没有WebSocket配置，使用默认配置
                    self.create_websocket_connection_config(connection_id, "127.0.0.1:4320".to_string())
                }
            }
            ServerType::Quic => {
                if let Some(quic_config) = &self.quic_config {
                    self.create_quic_connection_config(connection_id, quic_config)
                } else {
                    // 如果没有QUIC配置，使用默认配置
                    let default_quic_config = ProtocolConfig::new()
                        .with_listen_addr("127.0.0.1:8081".to_string());
                    self.create_quic_connection_config(connection_id, &default_quic_config)
                }
            }
            ServerType::Dual => {
                // 双协议模式默认创建WebSocket连接配置
                // 可以通过其他方法创建QUIC连接配置
                if let Some(ws_config) = &self.websocket_config {
                    self.create_websocket_connection_config(connection_id, ws_config.listen_addr.clone())
                } else {
                    self.create_websocket_connection_config(connection_id, "127.0.0.1:4320".to_string())
                }
            }
        }
    }
    
    /// 创建WebSocket连接配置
    fn create_websocket_connection_config(&self, connection_id: String, listen_addr: String) -> ConnectionConfig {
        let mut conn_config = ConnectionConfig::default();
        conn_config.id = Some(connection_id);
        conn_config.transport = Transport::WebSocket;
        conn_config.remote_addr = Some(listen_addr);
        conn_config.heartbeat_interval_ms = Some(self.heartbeat_interval_ms);
        conn_config.heartbeat_timeout_ms = Some(self.heartbeat_interval_ms / 2); // 使用心跳间隔的一半作为超时
        conn_config.max_missed_heartbeats = Some(self.max_missed_heartbeats);
        conn_config
    }
    
    /// 创建QUIC连接配置
    fn create_quic_connection_config(&self, connection_id: String, quic_cfg: &ProtocolConfig) -> ConnectionConfig {
        let mut conn_config = ConnectionConfig::default();
        conn_config.id = Some(connection_id);
        conn_config.transport = Transport::Quic;
        conn_config.remote_addr = Some(quic_cfg.listen_addr.clone());
        conn_config.heartbeat_interval_ms = Some(self.heartbeat_interval_ms);
        conn_config.heartbeat_timeout_ms = Some(self.heartbeat_interval_ms / 2); // 使用心跳间隔的一半作为超时
        conn_config.max_missed_heartbeats = Some(self.max_missed_heartbeats);

        // 配置 QUIC 子配置（仅服务端部分）
        let mut quic_server = crate::common::connections::config::QuicServerConfig::default();
        if quic_cfg.enable_tls {
            if let Some(tls) = &quic_cfg.tls_config {
                quic_server.cert_path = Some(tls.cert_path.clone());
                quic_server.key_path = Some(tls.key_path.clone());
                quic_server.require_client_auth = tls.require_client_auth;
                quic_server.client_ca_cert_path = tls.client_ca_cert_path.clone();
            }
        }
        let quic_config = crate::common::connections::config::QuicConfig { client: None, server: Some(quic_server) };
        conn_config.protocol_config = Some(crate::common::connections::config::ProtocolConfig { websocket: None, quic: Some(quic_config) });
        conn_config
    }
    
    /// 转换为WebSocket连接配置
    /// 
    /// 专门用于创建WebSocket连接配置，即使服务器类型不是WebSocket
    pub fn to_websocket_connection_config(&self, connection_id: String) -> Option<ConnectionConfig> {
        self.websocket_config.as_ref().map(|ws_config| self.create_websocket_connection_config(connection_id, ws_config.listen_addr.clone()))
    }
    
    /// 转换为QUIC连接配置
    /// 
    /// 专门用于创建QUIC连接配置，即使服务器类型不是QUIC
    pub fn to_quic_connection_config(&self, connection_id: String) -> Option<ConnectionConfig> {
        self.quic_config.as_ref().map(|quic_config| self.create_quic_connection_config(connection_id, quic_config))
    }
    
    /// 创建高性能服务器配置
    /// 
    /// 针对高并发、高吞吐量场景优化的配置
    pub fn high_performance_websocket() -> Self {
        let mut config = Self::default_websocket();
        
        // 性能优化配置
        config.performance_config = ServerPerformanceConfig {
            worker_threads: 0, // 自动检测CPU核心数
            enable_cpu_affinity: true,
            enable_numa_awareness: true,
            memory_pool_size: 256 * 1024 * 1024, // 256MB
            enable_zero_copy: true,
            batch_size: 500,
            enable_connection_pool: true,
            connection_pool_size: 5000,
        };
        
        // 连接配置优化
        config.max_connections = 10000;
        config.buffer_size = 262144; // 256KB
        config.heartbeat_interval_ms = 30000; // 30秒
        config.heartbeat_timeout_ms = 15000; // 15秒
        config.max_missed_heartbeats = 5;
        
        // 安全配置
        config.security_config.max_connections_per_ip = 50;
        config.security_config.rate_limit_per_second = 1000;
        config.security_config.max_message_size = 50 * 1024 * 1024; // 50MB
        
        config
    }
    
    /// 创建低延迟服务器配置
    /// 
    /// 针对实时通信场景优化的配置
    pub fn low_latency_websocket() -> Self {
        let mut config = Self::default_websocket();
        
        // 性能优化配置
        config.performance_config = ServerPerformanceConfig {
            worker_threads: 0,
            enable_cpu_affinity: true,
            enable_numa_awareness: false, // 低延迟场景下NUMA可能增加延迟
            memory_pool_size: 64 * 1024 * 1024, // 64MB
            enable_zero_copy: true,
            batch_size: 10, // 小批量处理以降低延迟
            enable_connection_pool: false, // 连接池可能增加延迟
            connection_pool_size: 0,
        };
        
        // 连接配置优化
        config.max_connections = 5000;
        config.buffer_size = 32768; // 32KB
        config.heartbeat_interval_ms = 5000; // 5秒
        config.heartbeat_timeout_ms = 2000; // 2秒
        config.max_missed_heartbeats = 2;
        config.connection_timeout_ms = 10000; // 10秒
        
        // 安全配置
        config.security_config.max_connections_per_ip = 20;
        config.security_config.rate_limit_per_second = 500;
        config.security_config.max_message_size = 1024 * 1024; // 1MB
        
        config
    }
    
    /// 创建稳定连接服务器配置
    /// 
    /// 针对长时间连接、高可靠性场景优化的配置
    pub fn stable_websocket() -> Self {
        let mut config = Self::default_websocket();
        
        // 性能优化配置
        config.performance_config = ServerPerformanceConfig {
            worker_threads: 0,
            enable_cpu_affinity: false, // 稳定场景下不需要CPU亲和性
            enable_numa_awareness: false,
            memory_pool_size: 128 * 1024 * 1024, // 128MB
            enable_zero_copy: false, // 稳定性优于性能
            batch_size: 100,
            enable_connection_pool: true,
            connection_pool_size: 2000,
        };
        
        // 连接配置优化
        config.max_connections = 5000;
        config.buffer_size = 131072; // 128KB
        config.heartbeat_interval_ms = 60000; // 1分钟
        config.heartbeat_timeout_ms = 30000; // 30秒
        config.max_missed_heartbeats = 5;
        config.connection_timeout_ms = 120000; // 2分钟
        config.heartbeat_monitor_timeout_ms = 180000; // 3分钟
        config.cleanup_interval_ms = 300000; // 5分钟
        
        // 安全配置
        config.security_config.max_connections_per_ip = 30;
        config.security_config.rate_limit_per_second = 200;
        config.security_config.max_message_size = 20 * 1024 * 1024; // 20MB
        
        config
    }
    
    /// 创建生产环境服务器配置
    /// 
    /// 针对生产环境优化的综合配置
    pub fn production_websocket() -> Self {
        let mut config = Self::default_websocket();
        
        // 性能优化配置
        config.performance_config = ServerPerformanceConfig {
            worker_threads: 0,
            enable_cpu_affinity: true,
            enable_numa_awareness: true,
            memory_pool_size: 512 * 1024 * 1024, // 512MB
            enable_zero_copy: true,
            batch_size: 200,
            enable_connection_pool: true,
            connection_pool_size: 8000,
        };
        
        // 连接配置优化
        config.max_connections = 20000;
        config.buffer_size = 524288; // 512KB
        config.heartbeat_interval_ms = 30000; // 30秒
        config.heartbeat_timeout_ms = 15000; // 15秒
        config.max_missed_heartbeats = 3;
        config.connection_timeout_ms = 60000; // 1分钟
        config.heartbeat_monitor_timeout_ms = 90000; // 1.5分钟
        config.cleanup_interval_ms = 300000; // 5分钟
        
        // 安全配置
        config.security_config = ServerSecurityConfig {
            enable_rate_limiting: true,
            max_connections_per_ip: 100,
            rate_limit_per_second: 2000,
            enable_blacklist: true,
            blacklist_file_path: Some("/etc/flare/blacklist.txt".to_string()),
            enable_whitelist: false,
            whitelist_file_path: None,
            max_message_size: 100 * 1024 * 1024, // 100MB
            enable_message_encryption: true,
        };
        
        
        config
    }
    
    /// 验证配置的有效性
    pub fn validate(&self) -> Result<(), String> {
        if self.heartbeat_timeout_ms >= self.heartbeat_interval_ms {
            return Err("心跳超时必须小于心跳间隔".to_string());
        }
        
        if self.connection_timeout_ms == 0 {
            return Err("连接超时必须大于0".to_string());
        }
        
        if self.heartbeat_interval_ms == 0 {
            return Err("心跳间隔必须大于0".to_string());
        }
        
        if self.buffer_size == 0 {
            return Err("缓冲区大小必须大于0".to_string());
        }
        
        if self.max_connections == 0 {
            return Err("最大连接数必须大于0".to_string());
        }
        
        if self.max_missed_heartbeats == 0 {
            return Err("最大心跳丢失次数必须大于0".to_string());
        }
        
        // 验证协议配置
        match self.server_type {
            ServerType::WebSocket => {
                if self.websocket_config.is_none() {
                    return Err("WebSocket服务器类型必须配置WebSocket协议".to_string());
                }
            }
            ServerType::Quic => {
                if self.quic_config.is_none() {
                    return Err("QUIC服务器类型必须配置QUIC协议".to_string());
                }
                if let Some(quic_config) = &self.quic_config {
                    if quic_config.enable_tls && quic_config.tls_config.is_none() {
                        return Err("启用TLS时必须提供TLS配置".to_string());
                    }
                }
            }
            ServerType::Dual => {
                if self.websocket_config.is_none() && self.quic_config.is_none() {
                    return Err("双协议服务器必须至少配置一种协议".to_string());
                }
                if let Some(quic_config) = &self.quic_config {
                    if quic_config.enable_tls && quic_config.tls_config.is_none() {
                        return Err("启用TLS时必须提供TLS配置".to_string());
                    }
                }
            }
        }
        
        Ok(())
    }
}