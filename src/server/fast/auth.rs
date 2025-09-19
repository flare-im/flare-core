//! 认证trait定义
//!
//! 定义认证相关的trait，用于处理用户认证逻辑

use async_trait::async_trait;
use crate::common::error::Result;

/// 认证提供者trait
/// 
/// 用户需要实现此trait来提供自定义的认证逻辑
#[async_trait]
pub trait AuthProvider: Send + Sync {
    /// 验证用户令牌
    /// 
    /// # 参数
    /// * `user_id` - 用户ID
    /// * `platform` - 平台信息
    /// * `token` - 认证令牌
    /// 
    /// # 返回值
    /// * `Ok(true)` - 认证成功
    /// * `Ok(false)` - 认证失败
    /// * `Err(Error)` - 验证过程中发生错误
    async fn validate_token(&self, user_id: &str, platform: &str, token: &str) -> Result<bool>;
    
    /// 获取用户信息
    /// 
    /// # 参数
    /// * `user_id` - 用户ID
    /// 
    /// # 返回值
    /// * `Ok(Some(Vec<u8>))` - 用户信息数据
    /// * `Ok(None)` - 用户不存在
    /// * `Err(Error)` - 获取过程中发生错误
    async fn get_user_info(&self, user_id: &str) -> Result<Option<Vec<u8>>>;
}

/// 默认认证提供者实现
/// 
/// 提供一个默认的认证实现，始终返回认证成功
#[derive(Debug)]
pub struct DefaultAuthProvider;

#[async_trait]
impl AuthProvider for DefaultAuthProvider {
    async fn validate_token(&self, _user_id: &str, _platform: &str, _token: &str) -> Result<bool> {
        // 默认实现始终返回认证成功
        Ok(true)
    }
    
    async fn get_user_info(&self, user_id: &str) -> Result<Option<Vec<u8>>> {
        // 默认实现返回简单的用户信息
        Ok(Some(user_id.as_bytes().to_vec()))
    }
}

impl Default for DefaultAuthProvider {
    fn default() -> Self {
        Self
    }
}