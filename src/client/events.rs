//! 客户端事件模块
//!
//! 定义客户端事件类型和相关的回调处理逻辑
//! 专门处理客户端特有的事件，不包含消息接收相关事件

use crate::common::{
    callback::{
        EventCallback, ConnectEvent, DisconnectEvent, HeartbeatEvent, CustomEvent,
        create_metadata_from_protocol_message,
    },
    TransportProtocol,
};
use async_trait::async_trait;
use chrono::Utc;
use std::sync::Arc;

/// 客户端事件回调trait
/// 
/// 专门处理客户端特有的事件类型，专注于连接管理、协议管理、性能监控等
/// 不包含消息接收相关事件，因为消息接收有专门的处理机制
#[async_trait]
pub trait ClientEventCallback: Send + Sync {
    // ==================== 连接管理事件 ====================
    
    /// 处理连接建立事件
    async fn on_connected(
        &self, 
        protocol: TransportProtocol, 
        session_id: String, 
        user_id: String
    ) -> Result<(), String>;
    
    /// 处理连接断开事件
    async fn on_disconnected(
        &self, 
        reason: String, 
        session_id: String, 
        user_id: String
    ) -> Result<(), String>;
    
    /// 处理重连开始事件
    async fn on_reconnecting(
        &self, 
        attempt: u32, 
        max_attempts: u32, 
        session_id: String, 
        user_id: String,
        reason: String
    ) -> Result<(), String>;
    
    /// 处理重连成功事件
    async fn on_reconnected(
        &self, 
        protocol: TransportProtocol, 
        session_id: String, 
        user_id: String,
        attempt_count: u32
    ) -> Result<(), String>;
    
    /// 处理重连失败事件
    async fn on_reconnect_failed(
        &self, 
        reason: String, 
        attempt: u32, 
        max_attempts: u32,
        session_id: String, 
        user_id: String
    ) -> Result<(), String>;
    
    /// 处理连接超时事件
    async fn on_connection_timeout(
        &self,
        session_id: String,
        user_id: String,
        timeout_duration_ms: u64
    ) -> Result<(), String>;
    
    // ==================== 协议管理事件 ====================
    
    /// 处理协议切换事件
    async fn on_protocol_switched(
        &self, 
        new_protocol: TransportProtocol, 
        old_protocol: TransportProtocol, 
        session_id: String, 
        user_id: String,
        reason: String
    ) -> Result<(), String>;
    
    /// 处理协议竞速开始事件
    async fn on_protocol_racing_started(
        &self,
        protocols: Vec<TransportProtocol>,
        session_id: String,
        user_id: String
    ) -> Result<(), String>;
    
    /// 处理协议竞速完成事件
    async fn on_protocol_racing_completed(
        &self,
        winner: TransportProtocol,
        results: Vec<ProtocolTestResult>,
        session_id: String,
        user_id: String
    ) -> Result<(), String>;
    
    /// 处理协议降级事件
    async fn on_protocol_fallback(
        &self,
        from_protocol: TransportProtocol,
        to_protocol: TransportProtocol,
        reason: String,
        session_id: String,
        user_id: String
    ) -> Result<(), String>;
    
    // ==================== 心跳和健康检查事件 ====================
    
    /// 处理心跳发送事件
    async fn on_heartbeat_sent(
        &self, 
        session_id: String, 
        user_id: String,
        sequence: u64
    ) -> Result<(), String>;
    
    /// 处理心跳确认事件
    async fn on_heartbeat_ack(
        &self, 
        session_id: String, 
        user_id: String, 
        latency_ms: u64,
        sequence: u64
    ) -> Result<(), String>;
    
    /// 处理心跳超时事件
    async fn on_heartbeat_timeout(
        &self,
        session_id: String,
        user_id: String,
        sequence: u64,
        timeout_duration_ms: u64
    ) -> Result<(), String>;
    
    /// 处理健康检查事件
    async fn on_health_check(
        &self,
        session_id: String,
        user_id: String,
        health_status: HealthStatus
    ) -> Result<(), String>;
    
    // ==================== 性能监控事件 ====================
    
    /// 处理连接质量变化事件
    async fn on_connection_quality_changed(
        &self, 
        quality: f64, 
        session_id: String, 
        user_id: String,
        metrics: ConnectionMetrics
    ) -> Result<(), String>;
    
    /// 处理性能指标事件
    async fn on_performance_metrics(
        &self,
        session_id: String,
        user_id: String,
        metrics: PerformanceMetrics
    ) -> Result<(), String>;
    
    /// 处理带宽使用事件
    async fn on_bandwidth_usage(
        &self,
        session_id: String,
        user_id: String,
        upload_bytes: u64,
        download_bytes: u64,
        duration_ms: u64
    ) -> Result<(), String>;
    
    // ==================== 认证和安全事件 ====================
    
    /// 处理认证开始事件
    async fn on_authentication_started(
        &self,
        method: String,
        session_id: String,
        user_id: String
    ) -> Result<(), String>;
    
    /// 处理认证成功事件
    async fn on_authentication_success(
        &self,
        method: String,
        session_id: String,
        user_id: String,
        auth_info: AuthInfo
    ) -> Result<(), String>;
    
    /// 处理认证失败事件
    async fn on_authentication_failed(
        &self,
        method: String,
        reason: String,
        session_id: String,
        user_id: String
    ) -> Result<(), String>;
    
    /// 处理证书验证事件
    async fn on_certificate_validation(
        &self,
        success: bool,
        certificate_info: CertificateInfo,
        session_id: String,
        user_id: String
    ) -> Result<(), String>;
    
    // ==================== 错误和异常事件 ====================
    
    /// 处理一般错误事件
    async fn on_error(
        &self, 
        code: String, 
        message: String, 
        session_id: String, 
        user_id: String,
        error_details: ErrorDetails
    ) -> Result<(), String>;
    
    /// 处理网络错误事件
    async fn on_network_error(
        &self,
        error_type: NetworkErrorType,
        message: String,
        session_id: String,
        user_id: String
    ) -> Result<(), String>;
    
    /// 处理协议错误事件
    async fn on_protocol_error(
        &self,
        protocol: TransportProtocol,
        error_code: String,
        message: String,
        session_id: String,
        user_id: String
    ) -> Result<(), String>;
    
    // ==================== 状态变化事件 ====================
    
    /// 处理客户端状态变化事件
    async fn on_client_state_changed(
        &self,
        old_state: ClientState,
        new_state: ClientState,
        session_id: String,
        user_id: String,
        reason: Option<String>
    ) -> Result<(), String>;
    
    /// 处理连接状态变化事件
    async fn on_connection_state_changed(
        &self,
        old_state: ConnectionState,
        new_state: ConnectionState,
        session_id: String,
        user_id: String,
        reason: Option<String>
    ) -> Result<(), String>;
    
    // ==================== 配置和设置事件 ====================
    
    /// 处理配置更新事件
    async fn on_config_updated(
        &self,
        config_changes: Vec<ConfigChange>,
        session_id: String,
        user_id: String
    ) -> Result<(), String>;
    
    /// 处理设置变更事件
    async fn on_settings_changed(
        &self,
        setting_name: String,
        old_value: String,
        new_value: String,
        session_id: String,
        user_id: String
    ) -> Result<(), String>;
}

// ==================== 支持的数据结构 ====================

/// 连接信息
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub remote_address: String,
    pub local_address: String,
    pub protocol: TransportProtocol,
    pub connection_time: chrono::DateTime<Utc>,
    pub tls_enabled: bool,
    pub compression_enabled: bool,
    pub encryption_enabled: bool,
}

/// 协议测试结果
#[derive(Debug, Clone)]
pub struct ProtocolTestResult {
    pub protocol: TransportProtocol,
    pub latency_ms: u64,
    pub success: bool,
    pub error_message: Option<String>,
}

/// 健康状态
#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub overall_health: f64, // 0.0 - 1.0
    pub connection_quality: f64,
    pub latency_ms: u64,
    pub packet_loss_rate: f64,
    pub bandwidth_mbps: f64,
}

/// 连接指标
#[derive(Debug, Clone)]
pub struct ConnectionMetrics {
    pub rtt_ms: u64,
    pub jitter_ms: u64,
    pub packet_loss_rate: f64,
    pub bandwidth_mbps: f64,
    pub congestion_window_size: u32,
}

/// 性能指标
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub cpu_usage_percent: f64,
    pub memory_usage_mb: u64,
    pub network_io_mbps: f64,
    pub response_time_ms: u64,
}

/// 认证信息
#[derive(Debug, Clone)]
pub struct AuthInfo {
    pub auth_method: String,
    pub auth_time: chrono::DateTime<Utc>,
    pub expires_at: Option<chrono::DateTime<Utc>>,
    pub permissions: Vec<String>,
}

/// 证书信息
#[derive(Debug, Clone)]
pub struct CertificateInfo {
    pub subject: String,
    pub issuer: String,
    pub valid_from: chrono::DateTime<Utc>,
    pub valid_until: chrono::DateTime<Utc>,
    pub serial_number: String,
}

/// 错误详情
#[derive(Debug, Clone)]
pub struct ErrorDetails {
    pub error_type: String,
    pub stack_trace: Option<String>,
    pub context: std::collections::HashMap<String, String>,
    pub timestamp: chrono::DateTime<Utc>,
}

/// 网络错误类型
#[derive(Debug, Clone)]
pub enum NetworkErrorType {
    Timeout,
    ConnectionRefused,
    HostUnreachable,
    NetworkUnreachable,
    ConnectionReset,
    BrokenPipe,
    Other(String),
}

/// 客户端状态
#[derive(Debug, Clone, PartialEq)]
pub enum ClientState {
    Initializing,
    Ready,
    Connecting,
    Connected,
    Disconnected,
    Reconnecting,
    Error,
    ShuttingDown,
}

/// 连接状态
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
    Failed,
    Timeout,
}

/// 配置变更
#[derive(Debug, Clone)]
pub struct ConfigChange {
    pub key: String,
    pub old_value: String,
    pub new_value: String,
    pub change_time: chrono::DateTime<Utc>,
}


/// 统一客户端事件处理器
/// 
/// 整合了ClientEventCallback和EventCallback的功能，提供统一的事件处理接口
/// 这样在消息处理器中就可以统一使用这个处理器
pub struct UnifiedClientEventHandler {
    /// 客户端事件回调
    client_event_callback: Arc<dyn ClientEventCallback>,
    /// common事件回调（用于兼容性和消息处理器）
    common_event_callback: Arc<dyn EventCallback>,
}

impl UnifiedClientEventHandler {
    /// 创建新的事件处理器
    pub fn new(
        client_event_callback: Arc<dyn ClientEventCallback>,
        common_event_callback: Arc<dyn EventCallback>,
    ) -> Self {
        Self {
            client_event_callback,
            common_event_callback,
        }
    }

    /// 处理连接建立事件
    pub async fn handle_connected(
        &self, 
        protocol: TransportProtocol, 
        session_id: String, 
        user_id: String
    ) -> Result<(), String> {
        // 处理客户端特有的事件
        let client_result = self.client_event_callback.on_connected(
            protocol.clone(), 
            session_id.clone(), 
            user_id.clone()
        ).await;

        // 转换为common事件并处理
        let common_result = self.handle_connected_as_common_event(
            protocol, 
            session_id.clone(), 
            user_id.clone()
        ).await;

        // 如果客户端事件处理失败，返回错误
        client_result?;
        
        // 如果common事件处理失败，记录但不中断
        if let Err(e) = common_result {
            eprintln!("Common事件处理失败: {}", e);
        }

        Ok(())
    }

    /// 处理连接断开事件
    pub async fn handle_disconnected(
        &self, 
        reason: String, 
        session_id: String, 
        user_id: String
    ) -> Result<(), String> {
        // 处理客户端特有的事件
        let client_result = self.client_event_callback.on_disconnected(
            reason.clone(), 
            session_id.clone(), 
            user_id.clone()
        ).await;

        // 转换为common事件并处理
        let common_result = self.handle_disconnected_as_common_event(
            reason, 
            session_id.clone(), 
            user_id.clone()
        ).await;

        // 如果客户端事件处理失败，返回错误
        client_result?;
        
        // 如果common事件处理失败，记录但不中断
        if let Err(e) = common_result {
            eprintln!("Common事件处理失败: {}", e);
        }

        Ok(())
    }

    /// 处理重连开始事件
    pub async fn handle_reconnecting(
        &self, 
        attempt: u32, 
        max_attempts: u32, 
        session_id: String, 
        user_id: String
    ) -> Result<(), String> {
        self.client_event_callback.on_reconnecting(attempt, max_attempts, session_id, user_id, "".to_string()).await
    }

    /// 处理重连成功事件
    pub async fn handle_reconnected(
        &self, 
        protocol: TransportProtocol, 
        session_id: String, 
        user_id: String
    ) -> Result<(), String> {
        self.client_event_callback.on_reconnected(protocol, session_id, user_id, 0).await
    }

    /// 处理重连失败事件
    pub async fn handle_reconnect_failed(
        &self, 
        reason: String, 
        attempt: u32, 
        max_attempts: u32,
        session_id: String, 
        user_id: String
    ) -> Result<(), String> {
        self.client_event_callback.on_reconnect_failed(reason, attempt, max_attempts, session_id, user_id).await
    }



    /// 处理心跳发送事件
    pub async fn handle_heartbeat_sent(
        &self, 
        session_id: String, 
        user_id: String,
        sequence: u64
    ) -> Result<(), String> {
        self.client_event_callback.on_heartbeat_sent(session_id, user_id, sequence).await
    }

    /// 处理心跳事件
    pub async fn handle_heartbeat(
        &self, 
        session_id: String, 
        user_id: String
    ) -> Result<(), String> {
        // 转换为common事件并处理
        let common_result = self.handle_heartbeat_as_common_event(
            session_id.clone(), 
            user_id.clone()
        ).await;

        // 如果common事件处理失败，记录但不中断
        if let Err(e) = common_result {
            eprintln!("Common事件处理失败: {}", e);
        }

        Ok(())
    }

    /// 处理心跳确认事件
    pub async fn handle_heartbeat_ack(
        &self, 
        session_id: String, 
        user_id: String, 
        latency_ms: u64
    ) -> Result<(), String> {
        self.client_event_callback.on_heartbeat_ack(session_id, user_id, latency_ms, 0).await
    }

    /// 处理错误事件
    pub async fn handle_error(
        &self, 
        code: String, 
        message: String, 
        session_id: String, 
        user_id: String
    ) -> Result<(), String> {
        self.client_event_callback.on_error(code, message, session_id, user_id, ErrorDetails {
            error_type: "".to_string(), // Placeholder
            stack_trace: None, // Placeholder
            context: std::collections::HashMap::new(), // Placeholder
            timestamp: Utc::now(),
        }).await
    }

    /// 处理协议切换事件
    pub async fn handle_protocol_switched(
        &self, 
        new_protocol: TransportProtocol, 
        old_protocol: TransportProtocol, 
        session_id: String, 
        user_id: String
    ) -> Result<(), String> {
        // 处理客户端特有的事件
        let client_result = self.client_event_callback.on_protocol_switched(
            new_protocol.clone(), 
            old_protocol.clone(), 
            session_id.clone(), 
            user_id.clone(),
            "".to_string()
        ).await;

        // 转换为common事件并处理
        let common_result = self.handle_protocol_switched_as_common_event(
            new_protocol, 
            old_protocol, 
            session_id.clone(), 
            user_id.clone()
        ).await;

        // 如果客户端事件处理失败，返回错误
        client_result?;
        
        // 如果common事件处理失败，记录但不中断
        if let Err(e) = common_result {
            eprintln!("Common事件处理失败: {}", e);
        }

        Ok(())
    }

    /// 处理连接质量变化事件
    pub async fn handle_connection_quality_changed(
        &self, 
        quality: f64, 
        session_id: String, 
        user_id: String
    ) -> Result<(), String> {
        // 处理客户端特有的事件
        let client_result = self.client_event_callback.on_connection_quality_changed(
            quality, 
            session_id.clone(), 
            user_id.clone(),
            ConnectionMetrics {
                rtt_ms: 0, // Placeholder
                jitter_ms: 0, // Placeholder
                packet_loss_rate: 0.0, // Placeholder
                bandwidth_mbps: 0.0, // Placeholder
                congestion_window_size: 0, // Placeholder
            }
        ).await;

        // 转换为common事件并处理
        let common_result = self.handle_connection_quality_changed_as_common_event(
            quality, 
            session_id.clone(), 
            user_id.clone()
        ).await;

        // 如果客户端事件处理失败，返回错误
        client_result?;
        
        // 如果common事件处理失败，记录但不中断
        if let Err(e) = common_result {
            eprintln!("Common事件处理失败: {}", e);
        }

        Ok(())
    }



    /// 将连接建立事件转换为common事件并处理
    async fn handle_connected_as_common_event(
        &self, 
        protocol: TransportProtocol, 
        session_id: String, 
        user_id: String
    ) -> Result<(), String> {
        let metadata = create_metadata_from_protocol_message(
            "connect",
            Some(user_id),
            None,
            Some(Utc::now()),
            session_id,
        );
        let connect_event = ConnectEvent { metadata };
        self.common_event_callback.on_connect(&connect_event).await
    }

    /// 将连接断开事件转换为common事件并处理
    async fn handle_disconnected_as_common_event(
        &self, 
        reason: String, 
        session_id: String, 
        user_id: String
    ) -> Result<(), String> {
        let metadata = create_metadata_from_protocol_message(
            "disconnect",
            Some(user_id),
            None,
            Some(Utc::now()),
            session_id,
        );
        let disconnect_event = DisconnectEvent { reason, metadata };
        self.common_event_callback.on_disconnect(&disconnect_event).await
    }

    /// 将心跳事件转换为common事件并处理
    async fn handle_heartbeat_as_common_event(
        &self, 
        session_id: String, 
        user_id: String
    ) -> Result<(), String> {
        let metadata = create_metadata_from_protocol_message(
            "heartbeat",
            Some(user_id),
            None,
            Some(Utc::now()),
            session_id,
        );
        let heartbeat_event = HeartbeatEvent { metadata };
        self.common_event_callback.on_heartbeat(&heartbeat_event).await
    }

    /// 将协议切换事件转换为common事件并处理
    async fn handle_protocol_switched_as_common_event(
        &self, 
        new_protocol: TransportProtocol, 
        old_protocol: TransportProtocol, 
        session_id: String, 
        user_id: String
    ) -> Result<(), String> {
        let event_name = format!("protocol_switched:{}->{}", old_protocol, new_protocol);
        let data = serde_json::json!({
            "old_protocol": old_protocol,
            "new_protocol": new_protocol
        }).to_string().into_bytes();
        
        let metadata = create_metadata_from_protocol_message(
            "custom_event",
            Some(user_id),
            None,
            Some(Utc::now()),
            session_id,
        );
        let custom_event = CustomEvent { event_name, data, metadata };
        self.common_event_callback.on_custom_event(&custom_event).await
    }

    /// 将连接质量变化事件转换为common事件并处理
    async fn handle_connection_quality_changed_as_common_event(
        &self, 
        quality: f64, 
        session_id: String, 
        user_id: String
    ) -> Result<(), String> {
        let event_name = "connection_quality_changed".to_string();
        let data = serde_json::json!({ "quality": quality }).to_string().into_bytes();
        
        let metadata = create_metadata_from_protocol_message(
            "custom_event",
            Some(user_id),
            None,
            Some(Utc::now()),
            session_id,
        );
        let custom_event = CustomEvent { event_name, data, metadata };
        self.common_event_callback.on_custom_event(&custom_event).await
    }

    /// 获取common事件回调的引用
    /// 这样消息处理器就可以直接使用
    pub fn get_common_event_callback(&self) -> Arc<dyn EventCallback> {
        self.common_event_callback.clone()
    }

    /// 获取客户端事件回调的引用
    pub fn get_client_event_callback(&self) -> Arc<dyn ClientEventCallback> {
        self.client_event_callback.clone()
    }
}

impl Default for UnifiedClientEventHandler {
    fn default() -> Self {
        Self::new(
            Arc::new(DefaultClientEventCallback),
            Arc::new(crate::common::callback::DefaultEventCallback),
        )
    }
}

// 为了向后兼容，保留原有的ClientEventHandler
pub type ClientEventHandler = UnifiedClientEventHandler;
/// 默认客户端事件回调实现
pub struct DefaultClientEventCallback;

#[async_trait]
impl ClientEventCallback for DefaultClientEventCallback {
    async fn on_connected(
        &self,
        protocol: TransportProtocol,
        session_id: String,
        user_id: String
    ) -> Result<(), String> {
        println!("客户端连接建立: 协议={:?}, 会话={}, 用户={}", protocol, session_id, user_id);
        Ok(())
    }

    async fn on_disconnected(
        &self,
        reason: String,
        session_id: String,
        user_id: String
    ) -> Result<(), String> {
        println!("客户端连接断开: 原因={}, 会话={}, 用户={}", reason, session_id, user_id);
        Ok(())
    }

    async fn on_reconnecting(
        &self,
        attempt: u32,
        max_attempts: u32,
        session_id: String,
        user_id: String,
        reason: String
    ) -> Result<(), String> {
        println!("客户端重连中: 尝试{}/{}次, 会话={}, 用户={}, 原因={}", attempt, max_attempts, session_id, user_id, reason);
        Ok(())
    }

    async fn on_reconnected(
        &self,
        protocol: TransportProtocol,
        session_id: String,
        user_id: String,
        attempt_count: u32
    ) -> Result<(), String> {
        println!("客户端重连成功: 协议={:?}, 会话={}, 用户={}", protocol, session_id, user_id);
        Ok(())
    }

    async fn on_reconnect_failed(
        &self,
        reason: String,
        attempt: u32,
        max_attempts: u32,
        session_id: String,
        user_id: String
    ) -> Result<(), String> {
        println!("客户端重连失败: 原因={}, 尝试{}次, 会话={}, 用户={}", reason, attempt, session_id, user_id);
        Ok(())
    }

    async fn on_connection_timeout(
        &self,
        session_id: String,
        user_id: String,
        timeout_duration_ms: u64
    ) -> Result<(), String> {
        println!("客户端连接超时: 会话={}, 用户={}, 超时={}ms", session_id, user_id, timeout_duration_ms);
        Ok(())
    }

    async fn on_protocol_switched(
        &self,
        new_protocol: TransportProtocol,
        old_protocol: TransportProtocol,
        session_id: String,
        user_id: String,
        reason: String
    ) -> Result<(), String> {
        println!("客户端协议切换: {}->{}, 会话={}, 用户={}, 原因={}", old_protocol, new_protocol, session_id, user_id, reason);
        Ok(())
    }

    async fn on_protocol_racing_started(
        &self,
        protocols: Vec<TransportProtocol>,
        session_id: String,
        user_id: String
    ) -> Result<(), String> {
        println!("客户端协议竞速开始: 协议={:?}, 会话={}, 用户={}", protocols, session_id, user_id);
        Ok(())
    }

    async fn on_protocol_racing_completed(
        &self,
        winner: TransportProtocol,
        results: Vec<ProtocolTestResult>,
        session_id: String,
        user_id: String
    ) -> Result<(), String> {
        println!("客户端协议竞速完成: 胜者={:?}, 结果={:?}, 会话={}, 用户={}", winner, results, session_id, user_id);
        Ok(())
    }

    async fn on_protocol_fallback(
        &self,
        from_protocol: TransportProtocol,
        to_protocol: TransportProtocol,
        reason: String,
        session_id: String,
        user_id: String
    ) -> Result<(), String> {
        println!("客户端协议降级: 从={:?}, 到={:?}, 原因={}, 会话={}, 用户={}", from_protocol, to_protocol, reason, session_id, user_id);
        Ok(())
    }

    async fn on_heartbeat_sent(
        &self, 
        session_id: String, 
        user_id: String,
        sequence: u64
    ) -> Result<(), String> {
        println!("客户端心跳发送: 会话={}, 用户={}, 序列号={}", session_id, user_id, sequence);
        Ok(())
    }

    async fn on_heartbeat_ack(
        &self, 
        session_id: String, 
        user_id: String, 
        latency_ms: u64,
        sequence: u64
    ) -> Result<(), String> {
        println!("客户端心跳确认: 会话={}, 用户={}, 延迟={}ms, 序列号={}", session_id, user_id, latency_ms, sequence);
        Ok(())
    }

    async fn on_heartbeat_timeout(
        &self,
        session_id: String,
        user_id: String,
        sequence: u64,
        timeout_duration_ms: u64
    ) -> Result<(), String> {
        println!("客户端心跳超时: 会话={}, 用户={}, 序列号={}, 超时={}ms", session_id, user_id, sequence, timeout_duration_ms);
        Ok(())
    }

    async fn on_health_check(
        &self,
        session_id: String,
        user_id: String,
        health_status: HealthStatus
    ) -> Result<(), String> {
        println!("客户端健康检查: 会话={}, 用户={}, 健康状态={:?}", session_id, user_id, health_status);
        Ok(())
    }

    async fn on_connection_quality_changed(
        &self, 
        quality: f64, 
        session_id: String, 
        user_id: String,
        metrics: ConnectionMetrics
    ) -> Result<(), String> {
        println!("客户端连接质量变化: 质量={:.2}, 会话={}, 用户={}, 指标={:?}", quality, session_id, user_id, metrics);
        Ok(())
    }

    async fn on_performance_metrics(
        &self,
        session_id: String,
        user_id: String,
        metrics: PerformanceMetrics
    ) -> Result<(), String> {
        println!("客户端性能指标: 会话={}, 用户={}, 指标={:?}", session_id, user_id, metrics);
        Ok(())
    }

    async fn on_bandwidth_usage(
        &self,
        session_id: String,
        user_id: String,
        upload_bytes: u64,
        download_bytes: u64,
        duration_ms: u64
    ) -> Result<(), String> {
        println!("客户端带宽使用: 会话={}, 用户={}, 上传={}B, 下载={}B, 持续={}ms", session_id, user_id, upload_bytes, download_bytes, duration_ms);
        Ok(())
    }

    async fn on_authentication_started(
        &self,
        method: String,
        session_id: String,
        user_id: String
    ) -> Result<(), String> {
        println!("客户端认证开始: 方法={}, 会话={}, 用户={}", method, session_id, user_id);
        Ok(())
    }

    async fn on_authentication_success(
        &self,
        method: String,
        session_id: String,
        user_id: String,
        auth_info: AuthInfo
    ) -> Result<(), String> {
        println!("客户端认证成功: 方法={}, 会话={}, 用户={}, 认证信息={:?}", method, session_id, user_id, auth_info);
        Ok(())
    }

    async fn on_authentication_failed(
        &self,
        method: String,
        reason: String,
        session_id: String,
        user_id: String
    ) -> Result<(), String> {
        println!("客户端认证失败: 方法={}, 原因={}, 会话={}, 用户={}", method, reason, session_id, user_id);
        Ok(())
    }

    async fn on_certificate_validation(
        &self,
        success: bool,
        certificate_info: CertificateInfo,
        session_id: String,
        user_id: String
    ) -> Result<(), String> {
        println!("客户端证书验证: 成功={}, 证书信息={:?}, 会话={}, 用户={}", success, certificate_info, session_id, user_id);
        Ok(())
    }

    async fn on_error(
        &self, 
        code: String, 
        message: String, 
        session_id: String, 
        user_id: String,
        error_details: ErrorDetails
    ) -> Result<(), String> {
        println!("客户端错误: 代码={}, 消息={}, 会话={}, 用户={}, 错误详情={:?}", code, message, session_id, user_id, error_details);
        Ok(())
    }

    async fn on_network_error(
        &self,
        error_type: NetworkErrorType,
        message: String,
        session_id: String,
        user_id: String
    ) -> Result<(), String> {
        println!("客户端网络错误: 类型={:?}, 消息={}, 会话={}, 用户={}", error_type, message, session_id, user_id);
        Ok(())
    }

    async fn on_protocol_error(
        &self,
        protocol: TransportProtocol,
        error_code: String,
        message: String,
        session_id: String,
        user_id: String
    ) -> Result<(), String> {
        println!("客户端协议错误: 协议={:?}, 错误码={}, 消息={}, 会话={}, 用户={}", protocol, error_code, message, session_id, user_id);
        Ok(())
    }

    async fn on_client_state_changed(
        &self,
        old_state: ClientState,
        new_state: ClientState,
        session_id: String,
        user_id: String,
        reason: Option<String>
    ) -> Result<(), String> {
        println!("客户端状态变化: 旧状态={:?}, 新状态={:?}, 会话={}, 用户={}, 原因={:?}", old_state, new_state, session_id, user_id, reason);
        Ok(())
    }

    async fn on_connection_state_changed(
        &self,
        old_state: ConnectionState,
        new_state: ConnectionState,
        session_id: String,
        user_id: String,
        reason: Option<String>
    ) -> Result<(), String> {
        println!("客户端连接状态变化: 旧状态={:?}, 新状态={:?}, 会话={}, 用户={}, 原因={:?}", old_state, new_state, session_id, user_id, reason);
        Ok(())
    }

    async fn on_config_updated(
        &self,
        config_changes: Vec<ConfigChange>,
        session_id: String,
        user_id: String
    ) -> Result<(), String> {
        println!("客户端配置更新: 会话={}, 用户={}, 变更={:?}", session_id, user_id, config_changes);
        Ok(())
    }

    async fn on_settings_changed(
        &self,
        setting_name: String,
        old_value: String,
        new_value: String,
        session_id: String,
        user_id: String
    ) -> Result<(), String> {
        println!("客户端设置变更: 名称={}, 旧值={}, 新值={}, 会话={}, 用户={}", setting_name, old_value, new_value, session_id, user_id);
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_default_callback() {
        let callback = DefaultClientEventCallback;
        let result = callback.on_connected(
            TransportProtocol::WebSocket,
            "test_session".to_string(),
            "test_user".to_string()
        ).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_event_handler() {
        let handler = UnifiedClientEventHandler::default();
        let result = handler.handle_connected(
            TransportProtocol::WebSocket,
            "test_session".to_string(),
            "test_user".to_string()
        ).await;
        assert!(result.is_ok());
    }
}
