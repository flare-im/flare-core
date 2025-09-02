//! 连接抽象定义
//! 
//! 提供统一的连接接口，支持客户端和服务端的差异化需求

use std::sync::Arc;
use async_trait::async_trait;

use crate::common::{
    error::Result,
    protocol::Frame,
    connections::types::{ConnectionState, ConnectionConfig},
};

// 重新导出事件处理相关定义，保持对外路径稳定
pub use super::event::{ConnectionEventHandler, DefaultConnectionEventHandler, EchoConnectionEventHandler, HeartbeatConnectionEventHandler};

/// 心跳响应处理器类型
pub type HeartbeatResponseHandler = Box<dyn Fn(Vec<u8>) -> Result<()> + Send + Sync>;

/// 基础连接接口
/// 
/// 所有连接类型都必须实现这个接口，提供基本的连接状态和配置信息
#[async_trait]
pub trait Connection: Send + Sync {
    /// 获取连接ID
    fn get_id(&self) -> &str;
    
    /// 获取连接状态
    async fn get_state(&self) -> ConnectionState;
    
    /// 检查连接是否活跃
    async fn is_active(&self) -> bool;
    
    /// 获取连接配置
    fn get_config(&self) -> &ConnectionConfig;
    
    /// 获取最后活跃时间
    async fn get_last_activity(&self) -> std::time::Instant;
    
    /// 更新最后活跃时间
    async fn update_last_activity(&self);
    
    /// 发送心跳消息
    async fn send_heartbeat(&self) -> Result<()>;
    
    /// 发送心跳响应
    async fn send_heartbeat_response(&self, data: Option<Vec<u8>>) -> Result<()>;
    
    /// 设置心跳响应处理器
    async fn set_heartbeat_response_handler(&mut self, handler: Option<HeartbeatResponseHandler>);
    
    /// 检查是否收到心跳消息
    async fn has_received_heartbeat(&self) -> bool;
    
    /// 重置心跳状态
    async fn reset_heartbeat_state(&self);
    
    /// 设置事件处理器（新增方法）
    async fn set_connection_event_handler(&mut self, handler: Arc<dyn ConnectionEventHandler>);
}

/// 客户端连接接口
/// 
/// 客户端连接负责主动建立连接、处理重连等
#[async_trait]
pub trait ClientConnection: Connection + Send + Sync {
    /// 建立连接
    async fn connect(&mut self) -> Result<()>;
    
    /// 断开连接
    async fn disconnect(&mut self) -> Result<()>;
    
    /// 发送消息
    async fn send_message(&mut self, message: Frame) -> Result<()>;
    
    /// 尝试重连
    async fn try_reconnect(&mut self) -> Result<()>;
    
    /// 检查是否需要重连
    async fn needs_reconnect(&self) -> bool;
    
    /// 获取重连次数
    async fn get_reconnect_attempts(&self) -> u32;
    
    /// 重置重连次数
    async fn reset_reconnect_attempts(&mut self);
}

/// 服务端连接接口
/// 
/// 服务端连接负责接受连接、管理连接生命周期、处理客户端消息等
#[async_trait]
pub trait ServerConnection: Connection + Send + Sync {
    /// 接受连接（从原始连接创建服务端连接）
    async fn accept(&mut self) -> Result<()>;
    
    /// 关闭连接
    async fn close(&mut self) -> Result<()>;
    
    /// 发送消息
    async fn send_message(&mut self, message: Frame) -> Result<()>;
    
    /// 接收消息
    async fn receive_message(&mut self) -> Result<Option<Frame>>;
    
    /// 检查连接健康状态
    async fn is_healthy(&self) -> bool;
    
    /// 获取客户端信息
    fn get_client_info(&self) -> Option<String>;
    
    /// 获取连接统计信息
    async fn get_connection_stats(&self) -> ConnectionStats;
}

/// 连接统计信息
#[derive(Debug, Clone)]
pub struct ConnectionStats {
    /// 连接建立时间
    pub established_at: std::time::Instant,
    /// 最后活跃时间
    pub last_activity: std::time::Instant,
    /// 接收消息数量
    pub messages_received: u64,
    /// 发送消息数量
    pub messages_sent: u64,
    /// 心跳响应次数
    pub heartbeat_responses: u64,
    /// 连接质量评分 (0-100)
    pub quality_score: u8,
}

impl Default for ConnectionStats {
    fn default() -> Self {
        Self {
            established_at: std::time::Instant::now(),
            last_activity: std::time::Instant::now(),
            messages_received: 0,
            messages_sent: 0,
            heartbeat_responses: 0,
            quality_score: 100,
        }
    }
}

/// 连接工厂接口
/// 
/// 负责创建不同类型的连接实例
#[async_trait]
pub trait ConnectionFactory: Send + Sync {
    /// 创建客户端连接
    async fn create_client_connection(&self, config: ConnectionConfig) -> Result<Box<dyn ClientConnection>>;
    
    /// 创建服务端连接
    async fn create_server_connection(&self, config: ConnectionConfig) -> Result<Box<dyn ServerConnection>>;
    
    /// 获取支持的类型
    fn supported_types(&self) -> Vec<crate::common::connections::types::ConnectionType>;
    
    /// 检查配置是否支持
    fn supports_config(&self, config: &ConnectionConfig) -> bool;
    
    /// 克隆工厂
    fn clone_box(&self) -> Box<dyn ConnectionFactory>;
}

// 原有 ConnectionEventHandler 与 DefaultConnectionEventHandler 已迁移至 event.rs

/// 服务端连接管理器
/// 
/// 管理服务端的所有连接，提供统一的连接管理接口
#[async_trait]
pub trait ServerConnectionManager: Send + Sync {
    /// 添加新连接
    async fn add_connection(&mut self, connection: Arc<dyn ServerConnection>) -> Result<()>;
    
    /// 移除连接
    async fn remove_connection(&mut self, connection_id: &str) -> Result<()>;
    
    /// 获取连接
    async fn get_connection(&self, connection_id: &str) -> Option<Arc<dyn ServerConnection>>;
    
    /// 获取所有连接
    async fn get_all_connections(&self) -> Vec<Arc<dyn ServerConnection>>;
    
    /// 获取连接数量
    async fn get_connection_count(&self) -> usize;
    
    /// 广播消息到所有连接
    async fn broadcast_message(&self, message: Frame) -> Result<usize>;
    
    /// 清理不活跃的连接
    async fn cleanup_inactive_connections(&mut self, timeout: std::time::Duration) -> usize;
    
    /// 获取连接统计信息
    async fn get_connection_stats(&self) -> ServerStats;
}

/// 服务端统计信息
#[derive(Debug, Clone)]
pub struct ServerStats {
    /// 总连接数
    pub total_connections: usize,
    /// 活跃连接数
    pub active_connections: usize,
    /// 总消息数
    pub total_messages: u64,
    /// 平均连接质量
    pub average_quality: u8,
    /// 服务器启动时间
    pub uptime: std::time::Duration,
}
