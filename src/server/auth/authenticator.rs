//! 认证器 Trait
//!
//! 定义认证接口，允许用户实现自定义的 token 验证逻辑

use crate::common::device::DeviceInfo;
use crate::common::error::Result;
use async_trait::async_trait;

/// 认证结果
#[derive(Debug, Clone)]
pub struct AuthResult {
    /// 是否验证通过
    pub authenticated: bool,
    /// 用户 ID（如果验证通过）
    pub user_id: Option<String>,
    /// 错误消息（如果验证失败）
    pub error_message: Option<String>,
    /// 用户元数据（可选，用于存储额外的用户信息）
    pub user_metadata: Option<std::collections::HashMap<String, String>>,
}

impl AuthResult {
    /// 创建成功的认证结果
    pub fn success(user_id: Option<String>) -> Self {
        Self {
            authenticated: true,
            user_id,
            error_message: None,
            user_metadata: None,
        }
    }

    /// 创建成功的认证结果（带元数据）
    pub fn success_with_metadata(
        user_id: Option<String>,
        metadata: std::collections::HashMap<String, String>,
    ) -> Self {
        Self {
            authenticated: true,
            user_id,
            error_message: None,
            user_metadata: Some(metadata),
        }
    }

    /// 创建失败的认证结果
    pub fn failure(error_message: String) -> Self {
        Self {
            authenticated: false,
            user_id: None,
            error_message: Some(error_message),
            user_metadata: None,
        }
    }
}

/// 认证器 Trait
///
/// 实现此 trait 以提供自定义的 token 验证逻辑
/// 例如：JWT 验证、数据库查询、Redis 验证等
#[async_trait]
pub trait Authenticator: Send + Sync {
    /// 验证 token
    ///
    /// # 参数
    /// - `token`: 客户端提供的 token（从 CONNECT 消息的 metadata 中提取）
    /// - `connection_id`: 连接 ID
    /// - `device_info`: 设备信息（可选，客户端可能提供）
    /// - `metadata`: CONNECT 消息的其他元数据（可选）
    ///
    /// # 返回
    /// - `Ok(AuthResult)`: 认证结果
    /// - `Err`: 认证过程出错（非验证失败，而是系统错误）
    async fn authenticate(
        &self,
        token: &str,
        connection_id: &str,
        device_info: Option<&DeviceInfo>,
        metadata: Option<&std::collections::HashMap<String, Vec<u8>>>,
    ) -> Result<AuthResult>;

    /// 检查连接是否已验证（可选，用于验证状态管理）
    ///
    /// 默认实现返回 true（假设验证通过后连接状态由 ConnectionManager 管理）
    /// 如果需要自定义验证状态管理（如 Redis、数据库），可以覆盖此方法
    async fn is_authenticated(&self, connection_id: &str) -> Result<bool> {
        let _ = connection_id;
        Ok(true)
    }

    /// 使连接失效（可选，用于主动撤销认证）
    ///
    /// 默认实现不做任何操作
    /// 如果需要支持主动撤销认证（如用户登出、token 撤销），可以覆盖此方法
    async fn revoke_authentication(&self, connection_id: &str) -> Result<()> {
        let _ = connection_id;
        Ok(())
    }
}
