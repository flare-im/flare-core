//! 服务端连接事件处理
//!
//! 定义服务端连接事件处理相关的 trait 与默认实现
//! 设计理念：扩展基础连接事件，提供服务端特有的事件回调

use async_trait::async_trait;

use crate::common::{
    protocol::Frame,
    connections::{
        traits::ConnectionStats,
        event::ConnectionEvent,
    },
    error::Result
};
use crate::Platform;

/// 服务端连接事件处理器
/// 
/// 扩展基础连接事件，提供服务端特有的事件回调
/// 设计目标：在基础连接事件基础上，增加服务端管理功能所需的事件回调
#[async_trait::async_trait]
pub trait ServerEvent: ConnectionEvent + Send + Sync {
    /// 心跳响应
    async fn on_heartbeat_response(&self, connection_id: &str);
    
    /// 用户认证完成
    async fn on_user_authenticated(&self, connection_id: &str, user_id: &str, platform: &Platform);
    
    /// 用户上线
    async fn on_user_online(&self, user_id: &str, platform: &Platform, connection_id: &str);
    
    /// 用户下线
    async fn on_user_offline(&self, user_id: &str, platform: &Platform, reason: &str);
    
    /// 连接数量变化
    async fn on_connection_count_changed(&self, total_connections: usize, authenticated_users: usize);
    
    /// 认证失败
    async fn on_authentication_failed(&self, connection_id: &str, error: &str);
    
    /// 认证超时
    async fn on_authentication_timeout(&self, connection_id: &str);
    
    /// 用户消息
    async fn on_user_message(&self, connection_id: &str, user_id: &str, message: &Frame) ->  Result<()>;

    /// 收到请求
    async fn on_request(&self, connection_id: &str, user_id: &str, message: &Frame) -> Result<()>;
    
    async fn on_authentication_request(&self, connection_id: &str, user_id: &str, platform: &str, token: &str);
    
    async fn on_authentication_response(&self, connection_id: &str, success: bool, user_info: Option<Vec<u8>>, error_message: Option<String>);
}

/// 默认服务端连接事件处理器
#[derive(Debug)]
pub struct DefServerEventHandler;

#[async_trait]
impl ConnectionEvent for DefServerEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        tracing::info!("服务端: 连接已建立: {}", connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        tracing::info!("服务端: 连接已断开: {} - 原因: {}", connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        tracing::info!("服务端: 连接错误: {} - 错误: {}", connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        tracing::info!("服务端: 收到消息: {} - 类型: {}", connection_id, message.get_command_type_str());
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        tracing::info!("服务端: 发送消息: {} - 类型: {}", connection_id, message.get_command_type_str());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        tracing::info!("服务端: 心跳超时: {}", connection_id);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        tracing::info!("服务端: 收到心跳的ping: {}", connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        tracing::info!("服务端: 收到心跳的pong: {}", connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        tracing::info!("服务端: 连接质量变化: {} - 评分: {}", connection_id, quality_score);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        tracing::info!("服务端: 开始重连: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        tracing::info!("服务端: 重连成功: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        tracing::info!("服务端: 重连失败: {} - 尝试次数: {} - 错误: {}", connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &ConnectionStats) {
        tracing::info!("服务端: 统计信息更新: {} - 收到: {} - 发送: {} - 质量: {}", 
                     connection_id, stats.messages_received, stats.messages_sent, stats.quality_score);
    }
}

#[async_trait::async_trait]
impl ServerEvent for DefServerEventHandler {
    async fn on_heartbeat_response(&self, connection_id: &str) {
        tracing::info!("服务端: 收到心跳响应: {}", connection_id);
    }
    
    async fn on_user_authenticated(&self, connection_id: &str, user_id: &str, platform: &Platform) {
        tracing::info!("服务端: 用户认证成功: 连接={} 用户={} 平台={:?}", connection_id, user_id, platform);
    }
    
    async fn on_user_online(&self, user_id: &str, platform: &Platform, connection_id: &str) {
        tracing::info!("服务端: 用户上线: 用户={} 平台={:?} 连接={}", user_id, platform, connection_id);
    }
    
    async fn on_user_offline(&self, user_id: &str, platform: &Platform, reason: &str) {
        tracing::info!("服务端: 用户下线: 用户={} 平台={:?} 原因={}", user_id, platform, reason);
    }

    
    async fn on_connection_count_changed(&self, total_connections: usize, authenticated_users: usize) {
        tracing::info!("服务端: 连接数量变化: 总连接数={} 已认证用户数={}", total_connections, authenticated_users);
    }

    
    async fn on_authentication_failed(&self, connection_id: &str, error: &str) {
        tracing::info!("服务端: 连接认证失败: 连接={} 错误={}", connection_id, error);
    }
    
    async fn on_authentication_timeout(&self, connection_id: &str) {
        tracing::info!("服务端: 连接认证超时: 连接={}", connection_id);
    }
    
    async fn on_user_message(&self, connection_id: &str, user_id: &str, message: &Frame) -> Result<()>  {
        tracing::info!("服务端: 收到用户消息: 连接={} 用户={} 类型={}", connection_id, user_id, message.get_command_type_str());
        // 默认返回true，表示继续处理消息
        Ok(())
    }
    
    async fn on_request(&self, connection_id: &str, user_id: &str, message: &Frame) -> Result<()> {
        tracing::info!("服务端: 收到请求: 连接={} 用户={} 类型={}", connection_id, user_id, message.get_command_type_str());
        Ok(())
    }
    
    async fn on_authentication_request(&self, connection_id: &str, user_id: &str, platform: &str, token: &str) {
        tracing::info!("服务端: 收到认证请求: 连接={} 用户={} 平台={} Token长度={}", 
                      connection_id, user_id, platform, token.len());
    }
    
    async fn on_authentication_response(&self, connection_id: &str, success: bool, user_info: Option<Vec<u8>>, error_message: Option<String>) {
        if success {
            tracing::info!("服务端: 客户端认证成功: {} - 用户信息长度: {:?}", connection_id, user_info.as_ref().map(|v| v.len()));
        } else {
            tracing::info!("服务端: 客户端认证失败: {} - 错误信息: {:?}", connection_id, error_message);
        }
    }
}

impl Default for DefServerEventHandler {
    fn default() -> Self {
        Self
    }
}