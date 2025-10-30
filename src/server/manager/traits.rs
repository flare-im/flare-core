//! 服务端连接管理器trait定义

use crate::common::connections::traits::ServerConnection;
use crate::common::connections::enums::ConnectionState;
use crate::common::error::FlareError;
use crate::common::protocol::frame::Frame;
use crate::server::events::handler::EventHandlerAdapter;
use std::sync::Arc;

/// 连接管理器trait
/// 
/// 定义连接管理器的核心接口，提供连接的添加、移除、查询等基本功能
#[async_trait::async_trait]
pub trait ConnectionManager: Send + Sync {
    /// 添加连接（不关联用户ID，适用于无需认证的场景）
    /// 
    /// # 参数
    /// * `conn` - 要添加的连接
    /// 
    /// # 返回值
    /// 如果是新连接返回 true，如果已存在返回 false
    fn add_connection(&self, conn: Arc<dyn ServerConnection>) -> bool;

    /// 移除连接
    /// 
    /// # 参数
    /// * `id` - 要移除的连接ID
    /// 
    /// # 返回值
    /// 如果找到并移除连接则返回该连接，否则返回 None
    fn remove_connection(&self, id: &str) -> Option<Arc<dyn ServerConnection>>;

    /// 获取连接
    /// 
    /// # 参数
    /// * `id` - 连接ID
    /// 
    /// # 返回值
    /// 如果找到连接则返回该连接的克隆，否则返回 None
    fn get_connection(&self, id: &str) -> Option<Arc<dyn ServerConnection>>;

    /// 获取聚合统计信息
    /// 
    /// # 返回值
    /// 当前所有连接的聚合统计信息
    fn stats_snapshot(&self) -> AggregatedStats;

    /// 清理过期连接
    /// 
    /// # 参数
    /// * `heartbeat_monitor_timeout_ms` - 心跳监控超时时间（毫秒）
    /// 
    /// # 返回值
    /// 操作结果
    fn cleanup(&self, heartbeat_monitor_timeout_ms: u64) -> Result<(), FlareError>;
    
    /// 获取连接总数
    /// 
    /// # 返回值
    /// 当前管理的连接总数
    fn connection_count(&self) -> usize;
    
    /// 获取所有连接ID列表
    /// 
    /// # 返回值
    /// 所有连接ID的列表
    fn all_connection_ids(&self) -> Vec<String>;
    
    /// 广播消息给所有连接
    /// 
    /// # 参数
    /// * `frame` - 要广播的消息帧
    /// 
    /// # 返回值
    /// 发送结果的统计信息
    fn broadcast_message(&self, frame: Frame) -> Result<BroadcastStats, FlareError>;
    
    /// 按连接状态过滤连接
    /// 
    /// # 参数
    /// * `state` - 连接状态
    /// 
    /// # 返回值
    /// 指定状态的连接ID列表
    fn get_connections_by_state(&self, state: ConnectionState) -> Vec<String>;
    
    /// 获取连接的详细信息
    /// 
    /// # 参数
    /// * `id` - 连接ID
    /// 
    /// # 返回值
    /// 连接的详细信息，如果连接不存在则返回None
    fn get_connection_info(&self, id: &str) -> Option<ConnectionInfo>;
    
    /// 获取所有连接的详细信息
    /// 
    /// # 返回值
    /// 所有连接的详细信息列表
    fn all_connection_info(&self) -> Vec<ConnectionInfo>;
    
    /// 获取事件处理器适配器
    async fn get_event_handler_adapter(&self) -> EventHandlerAdapter;
    
    /// 设置事件处理器
    async fn set_event_handler(&self, handler: Arc<dyn crate::server::events::handler::EnhancedEventHandler>);
}

/// 聚合统计信息
#[derive(Debug, Default)]
pub struct AggregatedStats {
    /// 总连接数
    pub total: usize,
    /// 活跃连接数
    pub active: usize,
    /// 失败连接数
    pub failed: usize,
    /// 重连中的连接数
    pub reconnecting: usize,
    /// 消息发送速率（每秒）
    pub msg_send_rate: Option<f32>,
    /// 消息接收速率（每秒）
    pub msg_recv_rate: Option<f32>,
    /// 平均连接质量
    pub avg_quality: Option<u8>,
    /// 按状态分组的连接数
    pub by_state: std::collections::HashMap<ConnectionState, usize>,
}

/// 广播统计信息
#[derive(Debug, Default)]
pub struct BroadcastStats {
    /// 成功发送的连接数
    pub success: usize,
    /// 发送失败的连接数
    pub failed: usize,
}

/// 连接详细信息
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    /// 连接ID
    pub id: String,
    /// 连接状态
    pub state: ConnectionState,
    /// 用户ID（如果已认证）
    pub user_id: Option<String>,
    /// 连接建立时间
    pub established_at: u64,
    /// 最后活动时间
    pub last_activity_at: u64,
    /// 连接质量
    pub quality: Option<u8>,
    /// 发送的消息数
    pub messages_sent: u64,
    /// 接收的消息数
    pub messages_received: u64,
}