use thiserror::Error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 错误代码枚举 - 用于国际化
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ErrorCode {
    // 连接相关错误 (1000-1999)
    ConnectionFailed = 1000,
    ConnectionTimeout = 1001,
    ConnectionClosed = 1002,
    ConnectionRefused = 1003,
    ConnectionLimitExceeded = 1004,
    NotConnected = 1005,
    
    // 认证相关错误 (2000-2999)
    AuthenticationFailed = 2000,
    AuthenticationExpired = 2001,
    AuthenticationInvalid = 2002,
    AuthenticationRequired = 2003,
    PermissionDenied = 2004,
    
    // 协议相关错误 (3000-3999)
    ProtocolError = 3000,
    ProtocolVersionMismatch = 3001,
    ProtocolNotSupported = 3002,
    MessageFormatError = 3003,
    MessageTooLarge = 3004,
    
    // 消息相关错误 (4000-4999)
    MessageSendFailed = 4000,
    MessageDeliveryFailed = 4001,
    MessageNotFound = 4002,
    MessageExpired = 4003,
    MessageRateLimitExceeded = 4004,
    
    // 用户相关错误 (5000-5999)
    UserNotFound = 5000,
    UserOffline = 5001,
    UserBlocked = 5002,
    UserQuotaExceeded = 5003,
    UserSessionLimitExceeded = 5004,
    
    // 系统相关错误 (6000-6999)
    InternalError = 6000,
    ServiceUnavailable = 6001,
    ResourceExhausted = 6002,
    ConfigurationError = 6003,
    DatabaseError = 6004,
    
    // 网络相关错误 (7000-7999)
    NetworkError = 7000,
    NetworkTimeout = 7001,
    NetworkUnreachable = 7002,
    NetworkConnectionLost = 7003,
    
    // 序列化相关错误 (8000-8999)
    SerializationError = 8000,
    DeserializationError = 8001,
    EncodingError = 8002,
    
    // 通用错误 (9000-9999)
    GeneralError = 9000,
    InvalidParameter = 9001,
    OperationNotSupported = 9002,
    OperationFailed = 9003,
    OperationTimeout = 9004,
}

impl ErrorCode {
    /// 获取错误代码的数字值
    pub fn as_u32(&self) -> u32 {
        *self as u32
    }
    
    /// 从数字值创建错误代码
    pub fn from_u32(code: u32) -> Option<Self> {
        match code {
            1000 => Some(ErrorCode::ConnectionFailed),
            1001 => Some(ErrorCode::ConnectionTimeout),
            1002 => Some(ErrorCode::ConnectionClosed),
            1003 => Some(ErrorCode::ConnectionRefused),
            1004 => Some(ErrorCode::ConnectionLimitExceeded),
            1005 => Some(ErrorCode::NotConnected),
            2000 => Some(ErrorCode::AuthenticationFailed),
            2001 => Some(ErrorCode::AuthenticationExpired),
            2002 => Some(ErrorCode::AuthenticationInvalid),
            2003 => Some(ErrorCode::AuthenticationRequired),
            2004 => Some(ErrorCode::PermissionDenied),
            3000 => Some(ErrorCode::ProtocolError),
            3001 => Some(ErrorCode::ProtocolVersionMismatch),
            3002 => Some(ErrorCode::ProtocolNotSupported),
            3003 => Some(ErrorCode::MessageFormatError),
            3004 => Some(ErrorCode::MessageTooLarge),
            4000 => Some(ErrorCode::MessageSendFailed),
            4001 => Some(ErrorCode::MessageDeliveryFailed),
            4002 => Some(ErrorCode::MessageNotFound),
            4003 => Some(ErrorCode::MessageExpired),
            4004 => Some(ErrorCode::MessageRateLimitExceeded),
            5000 => Some(ErrorCode::UserNotFound),
            5001 => Some(ErrorCode::UserOffline),
            5002 => Some(ErrorCode::UserBlocked),
            5003 => Some(ErrorCode::UserQuotaExceeded),
            5004 => Some(ErrorCode::UserSessionLimitExceeded),
            6000 => Some(ErrorCode::InternalError),
            6001 => Some(ErrorCode::ServiceUnavailable),
            6002 => Some(ErrorCode::ResourceExhausted),
            6003 => Some(ErrorCode::ConfigurationError),
            6004 => Some(ErrorCode::DatabaseError),
            7000 => Some(ErrorCode::NetworkError),
            7001 => Some(ErrorCode::NetworkTimeout),
            7002 => Some(ErrorCode::NetworkUnreachable),
            7003 => Some(ErrorCode::NetworkConnectionLost),
            8000 => Some(ErrorCode::SerializationError),
            8001 => Some(ErrorCode::DeserializationError),
            8002 => Some(ErrorCode::EncodingError),
            8003 => Some(ErrorCode::DeserializationError),
            9000 => Some(ErrorCode::GeneralError),
            9001 => Some(ErrorCode::InvalidParameter),
            9002 => Some(ErrorCode::OperationNotSupported),
            9003 => Some(ErrorCode::OperationFailed),
            9004 => Some(ErrorCode::OperationTimeout),
            _ => None,
        }
    }
    
    /// 获取错误代码的英文标识符
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCode::ConnectionFailed => "CONNECTION_FAILED",
            ErrorCode::ConnectionTimeout => "CONNECTION_TIMEOUT",
            ErrorCode::ConnectionClosed => "CONNECTION_CLOSED",
            ErrorCode::ConnectionRefused => "CONNECTION_REFUSED",
            ErrorCode::ConnectionLimitExceeded => "CONNECTION_LIMIT_EXCEEDED",
            ErrorCode::NotConnected => "NOT_CONNECTED",
            ErrorCode::AuthenticationFailed => "AUTHENTICATION_FAILED",
            ErrorCode::AuthenticationExpired => "AUTHENTICATION_EXPIRED",
            ErrorCode::AuthenticationInvalid => "AUTHENTICATION_INVALID",
            ErrorCode::AuthenticationRequired => "AUTHENTICATION_REQUIRED",
            ErrorCode::PermissionDenied => "PERMISSION_DENIED",
            ErrorCode::ProtocolError => "PROTOCOL_ERROR",
            ErrorCode::ProtocolVersionMismatch => "PROTOCOL_VERSION_MISMATCH",
            ErrorCode::ProtocolNotSupported => "PROTOCOL_NOT_SUPPORTED",
            ErrorCode::MessageFormatError => "MESSAGE_FORMAT_ERROR",
            ErrorCode::MessageTooLarge => "MESSAGE_TOO_LARGE",
            ErrorCode::MessageSendFailed => "MESSAGE_SEND_FAILED",
            ErrorCode::MessageDeliveryFailed => "MESSAGE_DELIVERY_FAILED",
            ErrorCode::MessageNotFound => "MESSAGE_NOT_FOUND",
            ErrorCode::MessageExpired => "MESSAGE_EXPIRED",
            ErrorCode::MessageRateLimitExceeded => "MESSAGE_RATE_LIMIT_EXCEEDED",
            ErrorCode::UserNotFound => "USER_NOT_FOUND",
            ErrorCode::UserOffline => "USER_OFFLINE",
            ErrorCode::UserBlocked => "USER_BLOCKED",
            ErrorCode::UserQuotaExceeded => "USER_QUOTA_EXCEEDED",
            ErrorCode::UserSessionLimitExceeded => "USER_SESSION_LIMIT_EXCEEDED",
            ErrorCode::InternalError => "INTERNAL_ERROR",
            ErrorCode::ServiceUnavailable => "SERVICE_UNAVAILABLE",
            ErrorCode::ResourceExhausted => "RESOURCE_EXHAUSTED",
            ErrorCode::ConfigurationError => "CONFIGURATION_ERROR",
            ErrorCode::DatabaseError => "DATABASE_ERROR",
            ErrorCode::NetworkError => "NETWORK_ERROR",
            ErrorCode::NetworkTimeout => "NETWORK_TIMEOUT",
            ErrorCode::NetworkUnreachable => "NETWORK_UNREACHABLE",
            ErrorCode::NetworkConnectionLost => "NETWORK_CONNECTION_LOST",
            ErrorCode::SerializationError => "SERIALIZATION_ERROR",
            ErrorCode::DeserializationError => "FRAME_DESERIALIZATION_ERROR",
            ErrorCode::EncodingError => "ENCODING_ERROR",
            ErrorCode::GeneralError => "GENERAL_ERROR",
            ErrorCode::InvalidParameter => "INVALID_PARAMETER",
            ErrorCode::OperationNotSupported => "OPERATION_NOT_SUPPORTED",
            ErrorCode::OperationFailed => "OPERATION_FAILED",
            ErrorCode::OperationTimeout => "OPERATION_TIMEOUT",
        }
    }
}

/// 国际化错误信息结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalizedError {
    /// 错误代码
    pub code: ErrorCode,
    /// 错误原因（用于国际化）
    pub reason: String,
    /// 错误详情（可选，用于调试）
    pub details: Option<String>,
    /// 错误参数（用于国际化插值）
    pub params: Option<HashMap<String, String>>,
    /// 错误时间戳
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl LocalizedError {
    /// 创建新的本地化错误
    pub fn new(code: ErrorCode, reason: impl Into<String>) -> Self {
        Self {
            code,
            reason: reason.into(),
            details: None,
            params: None,
            timestamp: chrono::Utc::now(),
        }
    }
    
    /// 添加错误详情
    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }
    
    /// 添加错误参数
    pub fn with_params(mut self, params: HashMap<String, String>) -> Self {
        self.params = Some(params);
        self
    }
    
    /// 添加单个参数
    pub fn with_param(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        if self.params.is_none() {
            self.params = Some(HashMap::new());
        }
        if let Some(ref mut params) = self.params {
            params.insert(key.into(), value.into());
        }
        self
    }
    
    /// 获取错误代码的数字值
    pub fn code_value(&self) -> u32 {
        self.code.as_u32()
    }
    
    /// 获取错误代码的字符串标识符
    pub fn code_str(&self) -> &'static str {
        self.code.as_str()
    }
}

impl std::fmt::Display for LocalizedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.reason)
    }
}

/// Flare IM 统一错误类型
#[derive(Error, Debug, Clone)]
pub enum FlareError {
    /// 本地化错误
    #[error("错误: {0}")]
    Localized(LocalizedError),
    
    /// 系统错误（用于内部错误，不暴露给用户）
    #[error("系统错误: {0}")]
    System(String),
    
    /// 配置错误
    #[error("配置错误: {0}")]
    InvalidConfiguration(String),
    
    /// 网络错误
    #[error("网络错误: {0}")]
    NetworkError(String),
    
    /// 协议错误
    #[error("协议错误: {0}")]
    ProtocolError(String),
    
    /// 连接失败
    #[error("连接失败: {0}")]
    ConnectionFailed(String),
    
    /// 认证错误
    #[error("认证错误: {0}")]
    AuthenticationError(String),
    
    /// 不支持的格式
    #[error("不支持的格式: {0}")]
    UnsupportedFormat(String),
    
    /// 解码错误
    #[error("解码错误: {0}")]
    DecodeError(String),
}

impl FlareError {
    /// 创建连接失败错误
    pub fn connection_failed(reason: impl Into<String>) -> Self {
        FlareError::Localized(LocalizedError::new(ErrorCode::ConnectionFailed, reason))
    }
    
    /// 创建连接超时错误
    pub fn connection_timeout(reason: impl Into<String>) -> Self {
        FlareError::Localized(LocalizedError::new(ErrorCode::ConnectionTimeout, reason))
    }
    
    /// 创建认证失败错误
    pub fn authentication_failed(reason: impl Into<String>) -> Self {
        FlareError::Localized(LocalizedError::new(ErrorCode::AuthenticationFailed, reason))
    }
    
    /// 创建认证过期错误
    pub fn authentication_expired(reason: impl Into<String>) -> Self {
        FlareError::Localized(LocalizedError::new(ErrorCode::AuthenticationExpired, reason))
    }
    
    /// 创建协议错误
    pub fn protocol_error(reason: impl Into<String>) -> Self {
        FlareError::Localized(LocalizedError::new(ErrorCode::ProtocolError, reason))
    }
    
    /// 创建消息发送失败错误
    pub fn message_send_failed(reason: impl Into<String>) -> Self {
        FlareError::Localized(LocalizedError::new(ErrorCode::MessageSendFailed, reason))
    }
    
    /// 创建用户不存在错误
    pub fn user_not_found(user_id: impl Into<String>) -> Self {
        let mut params = HashMap::new();
        params.insert("user_id".to_string(), user_id.into());
        FlareError::Localized(
            LocalizedError::new(ErrorCode::UserNotFound, "用户不存在")
                .with_params(params)
        )
    }
    
    /// 创建用户离线错误
    pub fn user_offline(user_id: impl Into<String>) -> Self {
        let mut params = HashMap::new();
        params.insert("user_id".to_string(), user_id.into());
        FlareError::Localized(
            LocalizedError::new(ErrorCode::UserOffline, "用户离线")
                .with_params(params)
        )
    }
    
    /// 创建内部错误
    pub fn internal_error(reason: impl Into<String>) -> Self {
        FlareError::Localized(LocalizedError::new(ErrorCode::InternalError, reason))
    }
    
    /// 创建网络错误
    pub fn network_error(reason: impl Into<String>) -> Self {
        FlareError::Localized(LocalizedError::new(ErrorCode::NetworkError, reason))
    }
    
    /// 创建序列化错误
    pub fn serialization_error(reason: impl Into<String>) -> Self {
        FlareError::Localized(LocalizedError::new(ErrorCode::SerializationError, reason))
    }
    
    /// 创建反序列化错误
    pub fn deserialization_failed(reason: impl Into<String>) -> Self {
        FlareError::Localized(LocalizedError::new(ErrorCode::DeserializationError, reason))
    }
    
    /// 创建不支持的格式错误
    pub fn unsupported_format(reason: impl Into<String>) -> Self {
        FlareError::UnsupportedFormat(reason.into())
    }
    
    /// 创建解码错误
    pub fn decode_error(reason: impl Into<String>) -> Self {
        FlareError::DecodeError(reason.into())
    }
    
    /// 创建通用错误
    pub fn general_error(reason: impl Into<String>) -> Self {
        FlareError::Localized(LocalizedError::new(ErrorCode::GeneralError, reason))
    }
    
    /// 获取本地化错误信息
    pub fn localized(&self) -> Option<&LocalizedError> {
        match self {
            FlareError::Localized(localized) => Some(localized),
            FlareError::System(_) => None,
            FlareError::InvalidConfiguration(_) => None,
            FlareError::NetworkError(_) => None,
            FlareError::ProtocolError(_) => None,
            FlareError::ConnectionFailed(_) => None,
            FlareError::AuthenticationError(_) => None,
            FlareError::UnsupportedFormat(_) => None,
            FlareError::DecodeError(_) => None,
        }
    }
    
    /// 获取错误代码
    pub fn code(&self) -> Option<ErrorCode> {
        match self {
            FlareError::Localized(localized) => Some(localized.code),
            FlareError::System(_) => None,
            FlareError::InvalidConfiguration(_) => None,
            FlareError::NetworkError(_) => None,
            FlareError::ProtocolError(_) => None,
            FlareError::ConnectionFailed(_) => None,
            FlareError::AuthenticationError(_) => None,
            FlareError::UnsupportedFormat(_) => None,
            FlareError::DecodeError(_) => None,
        }
    }
    
    /// 获取错误原因
    pub fn reason(&self) -> &str {
        match self {
            FlareError::Localized(localized) => &localized.reason,
            FlareError::System(msg) => msg,
            FlareError::InvalidConfiguration(msg) => msg,
            FlareError::NetworkError(err) => err,
            FlareError::ProtocolError(msg) => msg,
            FlareError::ConnectionFailed(msg) => msg,
            FlareError::AuthenticationError(msg) => msg,
            FlareError::UnsupportedFormat(msg) => msg,
            FlareError::DecodeError(msg) => msg,
        }
    }
    
    /// 转换为本地化错误
    pub fn to_localized(self) -> LocalizedError {
        match self {
            FlareError::Localized(localized) => localized,
            FlareError::System(msg) => LocalizedError::new(ErrorCode::GeneralError, msg),
            FlareError::InvalidConfiguration(msg) => LocalizedError::new(ErrorCode::ConfigurationError, msg),
            FlareError::NetworkError(err) => LocalizedError::new(ErrorCode::NetworkError, err),
            FlareError::ProtocolError(msg) => LocalizedError::new(ErrorCode::ProtocolError, msg),
            FlareError::ConnectionFailed(msg) => LocalizedError::new(ErrorCode::ConnectionFailed, msg),
            FlareError::AuthenticationError(msg) => LocalizedError::new(ErrorCode::AuthenticationFailed, msg),
            FlareError::UnsupportedFormat(msg) => LocalizedError::new(ErrorCode::GeneralError, msg),
            FlareError::DecodeError(msg) => LocalizedError::new(ErrorCode::DeserializationError, msg),
        }
    }
}

/// 客户端错误类型
pub type ClientError = FlareError;

/// 服务端错误类型
pub type ServerError = FlareError;

/// 结果类型别名
pub type Result<T> = std::result::Result<T, FlareError>;

// 从其他错误类型转换
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
        FlareError::NetworkError(err.to_string())
    }
}

impl From<serde_json::Error> for FlareError {
    fn from(err: serde_json::Error) -> Self {
        FlareError::serialization_error(err.to_string())
    }
}

impl From<tokio::time::error::Elapsed> for FlareError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        FlareError::connection_timeout("操作超时")
    }
}

// 移除对不存在依赖的 From 实现
// impl From<quinn::ConnectionError> for FlareError {
//     fn from(err: quinn::ConnectionError) -> Self {
//         FlareError::ConnectionFailed(err.to_string())
//     }
// }

// impl From<tokio_tungstenite::tungstenite::Error> for FlareError {
//     fn from(err: tokio_tungstenite::tungstenite::Error) -> Self {
//         FlareError::ConnectionFailed(err.to_string())
//     }
// }

// 错误构建器
pub struct ErrorBuilder {
    code: ErrorCode,
    reason: String,
    details: Option<String>,
    params: Option<HashMap<String, String>>,
}

impl ErrorBuilder {
    /// 创建新的错误构建器
    pub fn new(code: ErrorCode, reason: impl Into<String>) -> Self {
        Self {
            code,
            reason: reason.into(),
            details: None,
            params: None,
        }
    }
    
    /// 添加错误详情
    pub fn details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }
    
    /// 添加错误参数
    pub fn params(mut self, params: HashMap<String, String>) -> Self {
        self.params = Some(params);
        self
    }
    
    /// 添加单个参数
    pub fn param(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        if self.params.is_none() {
            self.params = Some(HashMap::new());
        }
        if let Some(ref mut params) = self.params {
            params.insert(key.into(), value.into());
        }
        self
    }
    
    /// 构建错误
    pub fn build(self) -> FlareError {
        let mut localized = LocalizedError::new(self.code, self.reason);
        if let Some(details) = self.details {
            localized = localized.with_details(details);
        }
        if let Some(params) = self.params {
            localized = localized.with_params(params);
        }
        FlareError::Localized(localized)
    }
}

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
        assert_eq!(ErrorCode::from_u32(9999), None);
    }
    
    #[test]
    fn test_localized_error() {
        let error = LocalizedError::new(ErrorCode::UserNotFound, "用户不存在")
            .with_param("user_id", "user123")
            .with_details("详细错误信息");
        
        assert_eq!(error.code, ErrorCode::UserNotFound);
        assert_eq!(error.reason, "用户不存在");
        assert_eq!(error.details, Some("详细错误信息".to_string()));
        assert_eq!(error.params.as_ref().unwrap().get("user_id"), Some(&"user123".to_string()));
    }
    
    #[test]
    fn test_flare_error() {
        let error = FlareError::user_not_found("user123");
        let localized = error.localized().unwrap();
        
        assert_eq!(localized.code, ErrorCode::UserNotFound);
        assert_eq!(localized.reason, "用户不存在");
        assert_eq!(localized.params.as_ref().unwrap().get("user_id"), Some(&"user123".to_string()));
    }
    
    #[test]
    fn test_error_builder() {
        let error = ErrorBuilder::new(ErrorCode::MessageSendFailed, "消息发送失败")
            .param("message_id", "msg123")
            .param("user_id", "user456")
            .details("网络连接中断")
            .build();
        
        let localized = error.localized().unwrap();
        assert_eq!(localized.code, ErrorCode::MessageSendFailed);
        assert_eq!(localized.reason, "消息发送失败");
        assert_eq!(localized.details, Some("网络连接中断".to_string()));
        assert_eq!(localized.params.as_ref().unwrap().get("message_id"), Some(&"msg123".to_string()));
        assert_eq!(localized.params.as_ref().unwrap().get("user_id"), Some(&"user456".to_string()));
    }
}
