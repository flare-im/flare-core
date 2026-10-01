//! 默认认证器
//!
//! 提供一个简单的默认实现，允许所有连接（不验证）

use crate::common::device::DeviceInfo;
use crate::common::error::Result;
use crate::server::auth::authenticator::{AuthResult, Authenticator};
use async_trait::async_trait;

/// CONNECT 元数据里客户端自报的用户 ID。
const DECLARED_USER_ID_KEY: &str = "user_id";

/// 默认认证器
///
/// 不校验令牌，直接采用客户端在 CONNECT 元数据 `user_id` 里自报的用户。
/// 只适用于不需要认证的场景（本机调试、受信内网）：任何客户端都能自称任何用户。
///
/// 认证通过的结果必须带用户，所以客户端没报 `user_id` 时按认证失败拒绝。
pub struct DefaultAuthenticator;

#[async_trait]
impl Authenticator for DefaultAuthenticator {
    async fn authenticate(
        &self,
        _token: &str,
        _connection_id: &str,
        _device_info: Option<&DeviceInfo>,
        metadata: Option<&std::collections::HashMap<String, Vec<u8>>>,
    ) -> Result<AuthResult> {
        let declared_user_id = metadata
            .and_then(|metadata| metadata.get(DECLARED_USER_ID_KEY))
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .map(str::trim)
            .filter(|user_id| !user_id.is_empty())
            .map(str::to_owned);
        Ok(match declared_user_id {
            Some(user_id) => AuthResult::success(Some(user_id)),
            None => AuthResult::failure("CONNECT 未带 user_id".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn adopts_the_declared_user_and_nothing_else() {
        let metadata = HashMap::from([(DECLARED_USER_ID_KEY.to_string(), b" alice ".to_vec())]);
        let result = DefaultAuthenticator
            .authenticate("any", "c1", None, Some(&metadata))
            .await
            .unwrap();
        assert!(result.authenticated);
        assert_eq!(result.user_id.as_deref(), Some("alice"));
        assert_eq!(result.tenant_id(), None);

        for metadata in [
            None,
            Some(HashMap::from([(
                DECLARED_USER_ID_KEY.to_string(),
                b"  ".to_vec(),
            )])),
        ] {
            let result = DefaultAuthenticator
                .authenticate("any", "c1", None, metadata.as_ref())
                .await
                .unwrap();
            assert!(!result.authenticated, "no declared user, no connection");
            assert_eq!(result.user_id, None);
        }
    }
}
