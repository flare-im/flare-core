//! 服务端认证模块
//!
//! 提供 token 验证功能，支持用户自定义验证逻辑

pub mod authenticator;
pub mod default;

pub use authenticator::{AuthResult, Authenticator};
pub use default::DefaultAuthenticator;
