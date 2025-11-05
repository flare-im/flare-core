//! 默认认证器
//! 
//! 提供一个简单的默认实现，允许所有连接（不验证）

use crate::server::auth::authenticator::{Authenticator, AuthResult};
use crate::common::error::Result;
use crate::common::device::DeviceInfo;
use async_trait::async_trait;

/// 默认认证器
/// 
/// 允许所有连接，不进行验证
/// 适用于不需要认证的场景
pub struct DefaultAuthenticator;

#[async_trait]
impl Authenticator for DefaultAuthenticator {
    async fn authenticate(
        &self,
        _token: &str,
        _connection_id: &str,
        _device_info: Option<&DeviceInfo>,
        _metadata: Option<&std::collections::HashMap<String, Vec<u8>>>,
    ) -> Result<AuthResult> {
        // 默认允许所有连接
        Ok(AuthResult::success(None))
    }
}

