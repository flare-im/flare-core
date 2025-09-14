//! 认证事件处理器
//!
//! 处理服务器端连接的认证相关事件

use std::sync::Arc;
use tracing::{info, warn, debug};

use crate::common::{
    protocol::Frame,
    connections::{
        event::ConnectionEvent,
        traits::ConnectionStats,
    },
};
use crate::server::{
    event::ServerEvent,
    manager::user_connection_manager::UserConnectionManager,
    ServerConnectionManager,
};

use crate::common::error::Result;

/// 认证事件处理器
pub struct AuthEventHandler {
    /// 用户连接管理器
    user_connection_manager: Arc<UserConnectionManager>,
}

impl AuthEventHandler {
    /// 创建新的认证事件处理器
    pub fn new(user_connection_manager: Arc<UserConnectionManager>) -> Self {
        Self {
            user_connection_manager,
        }
    }
}

#[async_trait::async_trait]
impl ConnectionEvent for AuthEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("连接已建立: {}", connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("连接已断开: {} - 原因: {}", connection_id, reason);
        
        // 从用户连接管理器中移除连接
        if let Err(e) = self.user_connection_manager.remove_connection(connection_id, Some(reason.to_string())).await {
            warn!("移除连接失败: {} - 错误: {}", connection_id, e);
        }
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        warn!("连接错误: {} - 错误: {}", connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        debug!("收到消息: {} - 类型: {:?}", connection_id, message.get_message_type());
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        debug!("发送消息: {} - 类型: {:?}", connection_id, message.get_message_type());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("心跳超时: {}", connection_id);
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        debug!("收到心跳的ping: {}", connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        debug!("收到心跳的pong: {}", connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        debug!("连接质量变化: {} - 评分: {}", connection_id, quality_score);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        info!("开始重连: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        info!("重连成功: {} - 尝试次数: {}", connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        warn!("重连失败: {} - 尝试次数: {} - 错误: {}", connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &ConnectionStats) {
        debug!("统计信息更新: {} - 收到: {} - 发送: {} - 质量: {}", 
               connection_id, stats.messages_received, stats.messages_sent, stats.quality_score);
    }
}

#[async_trait::async_trait]
impl ServerEvent for AuthEventHandler {
    async fn on_heartbeat_response(&self, connection_id: &str) {
        debug!("收到心跳响应: {}", connection_id);
    }
    
    async fn on_user_authenticated(&self, connection_id: &str, user_id: &str, platform: &crate::common::connections::enums::Platform) {
        info!("用户认证成功: 连接={} 用户={} 平台={}", connection_id, user_id, platform);
    }
    
    async fn on_user_online(&self, user_id: &str, platform: &crate::common::connections::enums::Platform, connection_id: &str) {
        info!("用户上线: 用户={} 平台={} 连接={}", user_id, platform, connection_id);
    }
    
    async fn on_user_offline(&self, user_id: &str, platform: &crate::common::connections::enums::Platform, _reason: &str) {
        info!("用户下线: 用户={} 平台={}", user_id, platform);
    }
    
    async fn on_connection_count_changed(&self, total_connections: usize, authenticated_users: usize) {
        debug!("连接数量变化: 总连接数={} 已认证用户数={}", total_connections, authenticated_users);
    }
    

    
    async fn on_authentication_failed(&self, connection_id: &str, error: &str) {
        warn!("连接认证失败: 连接={} 错误={}", connection_id, error);
    }
    
    async fn on_authentication_timeout(&self, connection_id: &str) {
        warn!("连接认证超时: 连接={}", connection_id);
    }
    
    async fn on_user_message(&self, connection_id: &str, user_id: &str, message: &Frame) -> Result<()> {
        debug!("收到用户消息: 连接={} 用户={} 类型={:?}", connection_id, user_id, message.get_message_type());
        // 默认返回Ok(())，表示继续处理消息
        Ok(())
    }
    
    async fn on_authentication_request(&self, connection_id: &str, user_id: &str, platform: &str, token: &str) {
        info!("收到认证请求: {} - 用户ID: {} - 平台: {} - Token长度: {}", 
              connection_id, user_id, platform, token.len());
        
        // 处理认证请求
        if let Err(e) = self.user_connection_manager
            .handle_authentication_request(
                connection_id.to_string(),
                user_id.to_string(),
                platform.to_string(),
                token.to_string(),
            )
            .await
        {
            warn!("处理认证请求失败: {} - 错误: {}", connection_id, e);
            
            // 发送认证失败响应
            if let Some(connection) = self.user_connection_manager
                .get_connection(connection_id)
                .await
            {
                let failure_response = Frame::auth_response(
                    false,
                    None,
                    Some(format!("认证处理失败: {}", e)),
                );
                
                if let Err(send_err) = connection.send_message(failure_response).await {
                    warn!("发送认证失败响应失败: {} - 错误: {}", connection_id, send_err);
                }
            }
            
            // 通知认证失败
            self.on_authentication_failed(connection_id, &format!("处理认证请求失败: {}", e)).await;
        }
    }
    
    async fn on_authentication_response(&self, connection_id: &str, success: bool, user_info: Option<Vec<u8>>, error_message: Option<String>) {
        if success {
            info!("客户端认证成功: {} - 用户信息长度: {:?}", connection_id, user_info.as_ref().map(|v| v.len()));
        } else {
            warn!("客户端认证失败: {} - 错误信息: {:?}", connection_id, error_message);
        }
    }
    
    async fn on_request(&self, connection_id: &str, user_id: &str, message: &Frame) -> Result<()> {
        debug!("收到请求: 连接={} 用户={} 类型={:?}", connection_id, user_id, message.get_message_type());
        Ok(())
    }
}