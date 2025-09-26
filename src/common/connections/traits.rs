//! 连接抽象定义
//! 
//! 提供统一的连接接口，支持客户端和服务端的差异化需求

use std::sync::Arc;
use async_trait::async_trait;

use crate::{common::{
    connections::types::{ClientInfo, ConnectionConfig, ConnectionState}, error::Result, protocol::Frame
}};
use crate::common::connections::enums::Platform;
// 重新导出事件处理相关定义，保持对外路径稳定
pub use super::event::{ConnectionEvent, DefConnectionEventHandler};

/// 心跳响应处理器类型
pub type HeartbeatResponseHandler = Box<dyn Fn(Vec<u8>) -> Result<()> + Send + Sync>;

/// 基础连接接口
/// 
/// 所有连接类型都必须实现这个接口，提供基本的连接状态和配置信息
#[async_trait]
pub trait Connection: Send + Sync {
    /// 获取连接ID
    fn id(&self) -> String;
    
    /// 获取连接状态
    fn state(&self) -> ConnectionState;

    /// 获取连接配置
    fn config(&self) -> Arc<ConnectionConfig>;

    /// 检查连接是否活跃
    fn stats(&self) -> ConnectionStats;
    
    /// 获取最后活跃时间
    fn last_activity_epoch_ms(&self) -> i64;
    
    /// 更新最后活跃时间
    fn status(&self) -> ConnectionState;
    
    /// 发送消息
    async fn send_message(&self, message: Frame) -> Result<()>;

    /// 关闭通道
    async fn close(&self,reason: Option<String>) -> Result<()>;

    /// 设置事件处理
    async fn set_event_handler(&mut self, handler: Arc<dyn ConnectionEvent>);
    
    /// 发送错误通知消息
    async fn send_error_notification(&self, error_code: u32, error_message: &str) -> Result<()>;
    
}

/// 客户端连接接口
/// 
/// 客户端连接负责主动建立连接、处理重连等
#[async_trait]
pub trait ClientConnection: Connection + Send + Sync {
    /// 建立连接
    async fn connect(&self) -> Result<()>;

    /// 标记认证完成
    async fn authenticated_complete(&self) -> Result<()>;

    /// 断开连接
    async fn disconnect(&self,reason: Option<String>) -> Result<()>;

    /// 尝试重连
    async fn try_reconnect(&self) -> Result<()>;
    
    /// 检查是否需要重连
    fn needs_reconnect(&self) -> bool;
    
    /// 获取重连次数
    fn get_reconnect_attempts(&self) -> u32;
    
    /// 重置重连次数
    fn reset_reconnect_attempts(&self);

    
}

/// 服务端连接接口
/// 
/// 服务端连接负责接受连接、管理连接生命周期、处理客户端消息等
#[async_trait]
pub trait ServerConnection: Connection + Send + Sync {
    /// 接受连接（从原始连接创建服务端连接）
    async fn accept(&self) -> Result<()>;

    /// 认证
    async fn authenticate(&self,success:bool,platform: Platform, user_id: String, info: Option<Vec<u8>>,reason: Option<String>) -> Result<()>;
    
    /// 获取客户端信息
    fn get_client_info(&self) -> Result<ClientInfo>;
    
    /// 获取用户ID（如果已认证）
    async fn get_user_id(&self) -> Option<String> {
        // 默认实现返回None
        None
    }
    
    /// 设置用户ID
    async fn set_user_id(&self, user_id: String) {
        // 默认实现为空，具体实现应该在连接中重写
        let _ = user_id;
    }
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

