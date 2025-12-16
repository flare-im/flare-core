//! Flare 错误处理模块
//!
//! 提供统一的错误处理机制，支持国际化、错误代码分类和错误转换
//!
//! ## 模块结构
//!
//! - `code` - 错误代码和错误类别定义
//! - `localized` - 国际化错误信息结构
//! - `flare_error` - Flare IM 统一错误类型
//! - `builder` - 错误构建器（链式 API）
//! - `conversions` - 错误类型转换实现

pub mod builder;
pub mod code;
mod conversions;
pub mod flare_error;
pub mod localized;

// 重新导出公共类型和函数
pub use builder::ErrorBuilder;
pub use code::{ErrorCategory, ErrorCode};
pub use flare_error::{ClientError, FlareError, Result, ServerError};
pub use localized::LocalizedError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code() {
        assert_eq!(ErrorCode::ConnectionFailed.as_u32(), 1000);
        assert_eq!(ErrorCode::AuthenticationFailed.as_u32(), 2000);
        assert_eq!(ErrorCode::ConnectionFailed.as_str(), "CONNECTION_FAILED");
    }

    #[test]
    fn test_error_code_from_u32() {
        assert_eq!(ErrorCode::from_u32(1000), Some(ErrorCode::ConnectionFailed));
        assert_eq!(ErrorCode::from_u32(9999), Some(ErrorCode::UnknownError));
        assert_eq!(ErrorCode::from_u32(99999), None);
    }

    #[test]
    fn test_error_category() {
        assert_eq!(
            ErrorCode::ConnectionFailed.category(),
            ErrorCategory::Connection
        );
        assert_eq!(
            ErrorCode::AuthenticationFailed.category(),
            ErrorCategory::Authentication
        );
        assert_eq!(
            ErrorCode::SerializationError.category(),
            ErrorCategory::Serialization
        );
    }

    #[test]
    fn test_error_retryable() {
        assert!(ErrorCode::ConnectionTimeout.is_retryable());
        assert!(ErrorCode::NetworkTimeout.is_retryable());
        assert!(!ErrorCode::AuthenticationFailed.is_retryable());
    }

    #[test]
    fn test_localized_error() {
        let error = LocalizedError::new(ErrorCode::UserNotFound, "用户不存在")
            .with_param("user_id", "user123")
            .with_details("详细错误信息");

        assert_eq!(error.code, ErrorCode::UserNotFound);
        assert_eq!(error.reason, "用户不存在");
        assert_eq!(error.details, Some("详细错误信息".to_string()));
        assert_eq!(
            error.params.as_ref().unwrap().get("user_id"),
            Some(&"user123".to_string())
        );
        assert!(error.is_retryable() == false);
    }

    #[test]
    fn test_flare_error() {
        let error = FlareError::user_not_found("user123");

        assert_eq!(error.code(), Some(ErrorCode::UserNotFound));
        assert_eq!(error.reason(), "用户不存在");
        assert!(!error.is_retryable());
    }

    #[test]
    fn test_error_builder() {
        let error = ErrorBuilder::new(ErrorCode::MessageSendFailed, "消息发送失败")
            .param("message_id", "msg123")
            .param("user_id", "user456")
            .details("网络连接中断")
            .build();

        assert_eq!(error.code(), Some(ErrorCode::MessageSendFailed));
        assert_eq!(error.reason(), "消息发送失败");
    }

    #[test]
    fn test_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "连接被拒绝");
        let flare_err: FlareError = io_err.into();
        assert!(flare_err.reason().contains("连接被拒绝"));
    }
}
