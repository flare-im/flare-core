//! 认证提供者

use crate::common::error::FlareError;

/// 认证提供者trait
pub trait AuthProvider: Send + Sync {
    /// 获取认证令牌
    fn get_token(&self) -> Result<String, FlareError>;
}

/// 默认认证提供者
pub struct DefaultAuthProvider;

impl AuthProvider for DefaultAuthProvider {
    fn get_token(&self) -> Result<String, FlareError> {
        // 默认实现：返回空令牌
        Ok("".to_string())
    }
}

impl Default for DefaultAuthProvider {
    fn default() -> Self {
        Self
    }
}