//! 认证处理器实现
//!
//! 处理客户端连接的认证请求

use std::sync::Arc;
use tracing::{info, warn, debug};

use crate::common::{
    error::Result,
    protocol::Frame,
    connections::enums::Platform,
};
use crate::server::{
    manager::user_connection_manager::UserConnectionManager,
    manager::traits::ServerConnectionManager,
    auth::AuthHandler,
};

/// 认证处理器
pub struct ServerAuthHandler {
    /// 用户连接管理器
    user_connection_manager: Arc<UserConnectionManager>,
    /// 认证处理器
    auth_handler: Arc<dyn AuthHandler>,
}

impl ServerAuthHandler {
    /// 创建新的认证处理器
    pub fn new(
        user_connection_manager: Arc<UserConnectionManager>,
        auth_handler: Arc<dyn AuthHandler>,
    ) -> Self {
        Self {
            user_connection_manager,
            auth_handler,
        }
    }

    /// 处理认证请求
    pub async fn handle_auth_request(
        &self,
        connection_id: String,
        user_id: String,
        platform_str: String,
        token: String,
    ) -> Result<()> {
        debug!("处理认证请求: 连接={} 用户={} 平台={} Token长度={}", 
               connection_id, user_id, platform_str, token.len());

        // 解析平台信息
        let platform = Platform::from_str(&platform_str);
        
        // 准备认证数据
        let auth_data = token.into_bytes();
        
        // 尝试认证
        let auth_result = self.auth_handler.authenticate_with_platform(
            auth_data,
            Some(platform.clone()),
            None, // device_id
            None, // app_version
        ).await;

        match auth_result {
            Ok(authenticated_user_id) => {
                // 认证成功
                info!("用户认证成功: 连接={} 用户={} 平台={:?}", connection_id, authenticated_user_id, platform);
                
                // 完成连接认证并绑定用户
                self.user_connection_manager
                    .complete_authentication(
                        connection_id.clone(),
                        authenticated_user_id.clone(),
                        platform.clone(),
                        None, // device_id
                    )
                    .await?;

                // 发送认证成功响应
                let success_response = Frame::auth_response(
                    true,
                    None, // user_info
                    None, // error_message
                );
                
                // 获取连接并发送响应
                if let Some(connection) = self.user_connection_manager
                    .get_connection(&connection_id)
                    .await
                {
                    if let Err(e) = connection.send_message(success_response).await {
                        warn!("发送认证成功响应失败: {} - 错误: {}", connection_id, e);
                    }
                }

                // 触发用户认证成功事件
                // 这里应该通知相关的事件处理器，但在当前架构中可能需要通过其他方式处理
                
                Ok(())
            }
            Err(e) => {
                // 认证失败
                warn!("用户认证失败: 连接={} 用户={} 错误={}", connection_id, user_id, e);
                
                // 发送认证失败响应
                let failure_response = Frame::auth_response(
                    false,
                    None, // user_info
                    Some(format!("认证失败: {}", e)),
                );
                
                // 获取连接并发送响应
                if let Some(connection) = self.user_connection_manager
                    .get_connection(&connection_id)
                    .await
                {
                    if let Err(send_err) = connection.send_message(failure_response).await {
                        warn!("发送认证失败响应失败: {} - 错误: {}", connection_id, send_err);
                    }
                }

                // 断开未认证的连接
                if let Err(disconnect_err) = self.user_connection_manager
                    .disconnect_unauthenticated_connection(&connection_id, Some(format!("认证失败: {}", e)))
                    .await
                {
                    warn!("断开未认证连接失败: {} - 错误: {}", connection_id, disconnect_err);
                }

                Ok(())
            }
        }
    }
}