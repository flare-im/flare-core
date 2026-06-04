//! 错误类型转换

use super::flare_error::FlareError;

// ============================================================
// 从其他错误类型转换
// ============================================================

impl From<&str> for FlareError {
    fn from(err: &str) -> Self {
        FlareError::general_error(err)
    }
}

impl From<String> for FlareError {
    fn from(err: String) -> Self {
        FlareError::general_error(err)
    }
}

impl From<std::io::Error> for FlareError {
    fn from(err: std::io::Error) -> Self {
        FlareError::io(err.to_string())
    }
}

impl From<serde_json::Error> for FlareError {
    fn from(err: serde_json::Error) -> Self {
        FlareError::deserialization_error(err.to_string())
    }
}

impl From<prost::DecodeError> for FlareError {
    fn from(err: prost::DecodeError) -> Self {
        FlareError::deserialization_error(err.to_string())
    }
}

impl From<prost::EncodeError> for FlareError {
    fn from(err: prost::EncodeError) -> Self {
        FlareError::encoding_error(err.to_string())
    }
}

impl From<tokio::time::error::Elapsed> for FlareError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        FlareError::timeout("操作超时")
    }
}
