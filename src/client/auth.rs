//! 客户端认证模块
//! 
//! 提供客户端认证功能，支持可配置的认证流程

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn, error};
use crate::common::{
    error::Result,
    protocol::{Frame, factory::FrameFactory},
};
use crate::common::protocol::commands::AuthRequestCommand;
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
    Unauthenticated,
    /// 认证中
    Authenticating,
    /// 已认证
    Authenticated,
    /// 认证失败
    Failed,
}

/// 客户端认证管理器
pub struct ClientAuthManager {
    /// 认证配置
    config: AuthConfig,
    /// 认证状态
    state: Arc<RwLock<AuthState>>,
    /// 认证响应回调
    auth_callback: Arc<RwLock<Option<tokio::sync::oneshot::Sender<Result<bool>>>>>,
}

impl ClientAuthManager {
    /// 创建新的认证管理器
    pub fn new(config: AuthConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(AuthState::Unauthenticated)),
            auth_callback: Arc::new(RwLock::new(None)),
        }
    }

    /// 获取认证配置
    pub fn get_config(&self) -> &AuthConfig {
        &self.config
    }

    /// 获取认证状态
    pub async fn get_state(&self) -> AuthState {
        let state = self.state.read().await;
        state.clone()
    }

    /// 设置认证状态
    async fn set_state(&self, new_state: AuthState) {
        let mut state = self.state.write().await;
        *state = new_state;
    }

    /// 创建认证请求消息
    pub fn create_auth_request(&self) -> Result<Frame> {
        if !self.config.enabled {
            return Err(crate::common::error::FlareError::general_error(
                "认证未启用".to_string()
            ));
        }

        let user_id = self.config.user_id.as_ref()
            .ok_or_else(|| crate::common::error::FlareError::general_error(
                "用户ID未设置".to_string()
            ))?;

        let platform = self.config.platform.as_ref()
            .ok_or_else(|| crate::common::error::FlareError::general_error(
                "平台信息未设置".to_string()
            ))?;

        let token = self.config.token.as_ref()
            .ok_or_else(|| crate::common::error::FlareError::general_error(
                "认证令牌未设置".to_string()
            ))?;

        let message_id = FrameFactory::generate_message_id();
        let _auth_request = AuthRequestCommand::new(
            user_id.clone(),
            platform.clone(),
            token.clone(),
        );

        let frame = FrameFactory::create_auth_request_frame(
            message_id,
            user_id.clone(),
            platform.clone(),
            token.clone(),
        )?;

        Ok(frame)
    }

    /// 处理认证响应
    pub async fn handle_auth_response(&self, response: &Frame) -> Result<bool> {
        debug!("处理认证响应");

        // 检查消息类型
        if let crate::common::protocol::commands::Command::Control(
            crate::common::protocol::commands::ControlCmd::AuthResponse(auth_response)
        ) = &response.command {
            // 检查认证是否成功
            if auth_response.success {
                info!("客户端认证成功");
                self.set_state(AuthState::Authenticated).await;
                
                // 通知等待的回调
                if let Some(callback) = self.auth_callback.write().await.take() {
                    let _ = callback.send(Ok(true));
                }
                
                Ok(true)
            } else {
                warn!("客户端认证失败: {:?}", auth_response.error_message);
                self.set_state(AuthState::Failed).await;
                
                // 通知等待的回调
                if let Some(callback) = self.auth_callback.write().await.take() {
                    let error_msg = auth_response.error_message.clone().unwrap_or_else(|| "认证失败".to_string());
                    let _ = callback.send(Err(crate::common::error::FlareError::authentication_failed(error_msg)));
                }
                
                Ok(false)
            }
        } else {
            error!("收到非认证响应消息");
            Err(crate::common::error::FlareError::general_error(
                "收到非认证响应消息".to_string()
            ))
        }
    }

    /// 等待认证完成
    pub async fn wait_for_authentication(&self) -> Result<bool> {
        if !self.config.enabled {
            return Ok(true);
        }

        // 创建一次性通道用于接收认证结果
        let (sender, receiver) = tokio::sync::oneshot::channel();
        
        // 保存回调
        {
            let mut callback = self.auth_callback.write().await;
            *callback = Some(sender);
        }

        // 等待认证结果或超时
        let timeout_duration = tokio::time::Duration::from_millis(self.config.timeout_ms);
        match tokio::time::timeout(timeout_duration, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                Err(crate::common::error::FlareError::connection_failed(
                    "等待认证响应时通道关闭".to_string()
                ))
            },
            Err(_) => {
                // 超时
                self.set_state(AuthState::Failed).await;
                Err(crate::common::error::FlareError::timeout(
                    "认证超时".to_string()
                ))
            }
        }
    }

    /// 重置认证状态
    pub async fn reset(&self) {
        self.set_state(AuthState::Unauthenticated).await;
        let mut callback = self.auth_callback.write().await;
        *callback = None;
    }
}