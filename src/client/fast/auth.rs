//! FastClient 认证管理
//!
//! 提供 FastClient 的认证功能，包括自动认证、认证状态管理等

use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{timeout, Duration};
use tracing::{info, warn};

use crate::common::{
    error::{Result, FlareError},
    protocol::{
        Frame,
        commands::ControlCmd,
    },
};

use super::{
    event::FastEvent,
};

use crate::client::Client;
use serde::{Deserialize, Serialize};

/// 认证配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// 是否启用认证
    pub enabled: bool,
    /// 用户ID
    pub user_id: Option<String>,
    /// 平台信息
    pub platform: Option<String>,
    /// 认证令牌
    pub token: Option<String>,
    /// 认证超时时间（毫秒）
    pub timeout_ms: u64,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            user_id: None,
            platform: None,
            token: None,
            timeout_ms: 5000,
        }
    }
}

/// 认证状态
#[derive(Debug, Clone, PartialEq)]
pub enum AuthState {
    /// 未认证
    NotAuthenticated,
    /// 认证中
    Authenticating,
    /// 认证成功
    Authenticated,
    /// 认证失败
    AuthenticationFailed(String),
}

/// FastClient 认证管理器
pub struct FastAuthManager {
    /// 认证状态
    state: Arc<RwLock<AuthState>>,
    /// 认证配置
    auth_config: AuthConfig,
    /// 认证超时时间（毫秒）
    auth_timeout_ms: u64,
    /// 认证响应通道
    auth_response_tx: Arc<RwLock<Option<tokio::sync::oneshot::Sender<Result<bool>>>>>,
}

impl FastAuthManager {
    /// 创建新的认证管理器
    pub fn new(auth_config: AuthConfig, auth_timeout_ms: u64) -> Self {
        Self {
            state: Arc::new(RwLock::new(AuthState::NotAuthenticated)),
            auth_config,
            auth_timeout_ms,
            auth_response_tx: Arc::new(RwLock::new(None)),
        }
    }
    
    /// 获取认证状态
    pub async fn get_auth_state(&self) -> AuthState {
        self.state.read().await.clone()
    }
    
    /// 检查是否已认证
    pub async fn is_authenticated(&self) -> bool {
        matches!(*self.state.read().await, AuthState::Authenticated)
    }
    
    /// 执行认证流程
    pub async fn authenticate(&self, client: &Client, event_handler: Option<Arc<dyn FastEvent>>) -> Result<()> {
        if !self.auth_config.enabled {
            info!("认证未启用，跳过认证流程");
            *self.state.write().await = AuthState::Authenticated;
            return Ok(());
        }
        
        info!("开始执行认证流程");
        
        // 更新认证状态
        *self.state.write().await = AuthState::Authenticating;
        
        // 创建认证请求
        let auth_request = crate::common::protocol::commands::AuthRequestCommand {
            user_id: self.auth_config.user_id.clone().unwrap_or_default(),
            platform: self.auth_config.platform.clone().unwrap_or_default(),
            token: self.auth_config.token.clone().unwrap_or_default(),
        };
        
        // 创建认证响应通道
        let (tx, rx) = tokio::sync::oneshot::channel();
        *self.auth_response_tx.write().await = Some(tx);
        
        // 发送认证请求
        // 通过消息处理器发送认证请求
        let message_handler = client.get_message_handler();
        message_handler.send_control(ControlCmd::AuthRequest(auth_request)).await?;
        
        // 等待认证响应
        let timeout_duration = Duration::from_millis(self.auth_timeout_ms);
        match timeout(timeout_duration, rx).await {
            Ok(Ok(Ok(true))) => {
                // 认证成功
                *self.state.write().await = AuthState::Authenticated;
                if let Some(handler) = event_handler {
                    handler.on_authenticated().await;
                }
                info!("认证成功");
                Ok(())
            }
            Ok(Ok(Ok(false))) => {
                // 认证失败
                let error_msg = "认证失败".to_string();
                *self.state.write().await = AuthState::AuthenticationFailed(error_msg.clone());
                if let Some(handler) = event_handler {
                    handler.on_authentication_failed(&error_msg).await;
                }
                Err(FlareError::authentication_failed(error_msg))
            }
            Ok(Ok(Err(e))) => {
                // 认证过程中发生错误
                let error_msg = format!("认证过程错误: {}", e);
                *self.state.write().await = AuthState::AuthenticationFailed(error_msg.clone());
                if let Some(handler) = event_handler {
                    handler.on_authentication_failed(&error_msg).await;
                }
                Err(e)
            }
            Ok(Err(e)) => {
                // 通道错误
                let error_msg = format!("认证通道错误: {}", e);
                *self.state.write().await = AuthState::AuthenticationFailed(error_msg.clone());
                if let Some(handler) = event_handler {
                    handler.on_authentication_failed(&error_msg).await;
                }
                Err(FlareError::general_error(error_msg))
            }
            Err(_) => {
                // 认证超时
                let error_msg = "认证超时".to_string();
                *self.state.write().await = AuthState::AuthenticationFailed(error_msg.clone());
                if let Some(handler) = event_handler {
                    handler.on_authentication_failed(&error_msg).await;
                }
                Err(FlareError::timeout(error_msg))
            }
        }
    }
    
    /// 处理认证响应
    pub async fn handle_auth_response(&self, response: &Frame) -> Result<()> {
        // 检查是否是认证响应
        if let crate::common::protocol::commands::Command::Control(
            crate::common::protocol::commands::ControlCmd::AuthResponse(auth_response)
        ) = &response.command {
            // 获取认证响应通道
            if let Some(tx) = self.auth_response_tx.write().await.take() {
                let success = auth_response.success;
                if let Err(_) = tx.send(Ok(success)) {
                    warn!("认证响应通道已关闭");
                }
            }
        }
        Ok(())
    }
    
    /// 创建认证请求
    fn create_auth_request(&self) -> Result<Frame> {
        let message_id = crate::common::protocol::factory::FrameFactory::generate_message_id();
        
        let auth_request = crate::common::protocol::commands::ControlCmd::AuthRequest(
            crate::common::protocol::commands::AuthRequestCommand {
                user_id: self.auth_config.user_id.clone().unwrap_or_default(),
                platform: self.auth_config.platform.clone().unwrap_or_default(),
                token: self.auth_config.token.clone().unwrap_or_default(),
            }
        );
        
        let frame = Frame::new(
            crate::common::protocol::commands::Command::Control(auth_request),
            message_id,
            crate::common::protocol::Reliability::BestEffort,
        );
        
        Ok(frame)
    }
    
    /// 重置认证状态
    pub async fn reset(&self) {
        *self.state.write().await = AuthState::NotAuthenticated;
        *self.auth_response_tx.write().await = None;
    }
}
