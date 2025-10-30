//! 统一错误类型定义
//!
//! 设计原则：
//! 1. **细粒度分类**：错误类型足够具体，便于上层精准处理
//! 2. **上下文丰富**：包含详细的错误信息和可选的源错误
//! 3. **易于使用**：提供便捷的构造方法
//! 4. **标准兼容**：实现 std::error::Error trait
//!
//! # 使用示例
//!
//! ```rust
//! use flare_core::common::error::FlareError;
//!
//! // 简单错误
//! let err = FlareError::connection_failed("Connection timeout");
//!
//! // 带源错误的错误
//! let io_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout");
//! let err = FlareError::connection_failed_with_source("Failed to connect", io_err);
//!
//! // 匹配错误类型
//! match err {
//!     FlareError::ConnectionFailed { .. } => { /* 处理连接错误 */ },
//!     FlareError::Timeout { .. } => { /* 处理超时 */ },
//!     _ => { /* 其他错误 */ }
//! }
//! ```

use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::io;

/// Flare 统一错误类型
///
/// 所有错误都携带详细的上下文信息，便于调试和错误处理。
#[derive(Debug, Clone)]
pub enum FlareError {
    /// 连接失败
    ///
    /// 包括建立连接失败、握手失败、TLS 错误等
    ConnectionFailed {
        message: String,
        // 注意：source 不实现 Clone，但我们可以在 Clone 时丢弃它
        #[allow(dead_code)]
        source: Option<String>,  // 改为 String 以支持 Clone
    },

    /// 连接超时
    ///
    /// 连接建立、读取、写入等操作超时
    Timeout {
        operation: String,
        timeout_ms: u64,
    },

    /// 序列化/反序列化错误
    ///
    /// JSON、Protobuf、MsgPack 等格式的编解码错误
    SerializationError {
        message: String,
        #[allow(dead_code)]
        source: Option<String>,  // 改为 String 以支持 Clone
    },

    /// 消息发送失败
    ///
    /// 消息队列已满、连接已关闭等
    MessageSendFailed {
        message: String,
        reason: Option<String>,
    },

    /// 心跳超时
    ///
    /// 连续多次心跳未响应
    HeartbeatTimeout {
        missed_count: u32,
        threshold: u32,
    },

    /// 认证失败
    ///
    /// 用户名密码错误、Token 无效等
    AuthenticationFailed {
        message: String,
    },

    /// 限流错误
    ///
    /// 超过速率限制
    RateLimitExceeded {
        limit: u64,
        window_ms: u64,
    },

    /// 配置错误
    ///
    /// 配置参数非法、缺少必要配置等
    ConfigError {
        message: String,
    },

    /// I/O 错误
    ///
    /// 底层 I/O 操作失败
    IoError {
        message: String,
        kind: String,  // io::ErrorKind 的字符串表示
    },

    /// 协议错误
    ///
    /// 协议格式错误、版本不匹配等
    ProtocolError {
        message: String,
    },

    /// 状态错误
    ///
    /// 操作与当前状态不兼容（如断开状态下发送消息）
    InvalidState {
        current_state: String,
        operation: String,
    },

    /// 其他未分类错误
    Other {
        message: String,
    },
}

impl FlareError {
    // ============================================================================
    // 便捷构造方法
    // ============================================================================

    /// 创建连接失败错误
    pub fn connection_failed<S: Into<String>>(message: S) -> Self {
        FlareError::ConnectionFailed {
            message: message.into(),
            source: None,
        }
    }

    /// 创建带源错误的连接失败错误
    pub fn connection_failed_with_source<S: Into<String>, E: Error + Send + Sync + 'static>(
        message: S,
        source: E,
    ) -> Self {
        FlareError::ConnectionFailed {
            message: message.into(),
            source: Some(source.to_string()),  // 转换为 String
        }
    }

    /// 创建超时错误
    pub fn timeout<S: Into<String>>(operation: S, timeout_ms: u64) -> Self {
        FlareError::Timeout {
            operation: operation.into(),
            timeout_ms,
        }
    }

    /// 创建序列化错误
    pub fn serialization_error<S: Into<String>>(message: S) -> Self {
        FlareError::SerializationError {
            message: message.into(),
            source: None,
        }
    }

    /// 创建带源错误的序列化错误
    pub fn serialization_error_with_source<S: Into<String>, E: Error + Send + Sync + 'static>(
        message: S,
        source: E,
    ) -> Self {
        FlareError::SerializationError {
            message: message.into(),
            source: Some(source.to_string()),  // 转换为 String
        }
    }

    /// 创建消息发送失败错误
    pub fn message_send_failed<S: Into<String>>(message: S) -> Self {
        FlareError::MessageSendFailed {
            message: message.into(),
            reason: None,
        }
    }

    /// 创建心跳超时错误
    pub fn heartbeat_timeout(missed_count: u32, threshold: u32) -> Self {
        FlareError::HeartbeatTimeout {
            missed_count,
            threshold,
        }
    }

    /// 创建认证失败错误
    pub fn authentication_failed<S: Into<String>>(message: S) -> Self {
        FlareError::AuthenticationFailed {
            message: message.into(),
        }
    }

    /// 创建限流错误
    pub fn rate_limit_exceeded(limit: u64, window_ms: u64) -> Self {
        FlareError::RateLimitExceeded { limit, window_ms }
    }

    /// 创建配置错误
    pub fn config_error<S: Into<String>>(message: S) -> Self {
        FlareError::ConfigError {
            message: message.into(),
        }
    }

    /// 创建 I/O 错误
    pub fn io_error<S: Into<String>>(message: S, source: io::Error) -> Self {
        FlareError::IoError {
            message: message.into(),
            kind: format!("{:?}", source.kind()),
        }
    }

    /// 创建协议错误
    pub fn protocol_error<S: Into<String>>(message: S) -> Self {
        FlareError::ProtocolError {
            message: message.into(),
        }
    }

    /// 创建状态错误
    pub fn invalid_state<S1: Into<String>, S2: Into<String>>(
        current_state: S1,
        operation: S2,
    ) -> Self {
        FlareError::InvalidState {
            current_state: current_state.into(),
            operation: operation.into(),
        }
    }

    /// 创建其他错误
    pub fn other<S: Into<String>>(message: S) -> Self {
        FlareError::Other {
            message: message.into(),
        }
    }

    /// 创建压缩错误
    pub fn compression_error<S: Into<String>>(message: S) -> Self {
        FlareError::SerializationError {
            message: format!("压缩错误: {}", message.into()),
            source: None,
        }
    }

    /// 创建输入无效错误
    pub fn invalid_input<S: Into<String>>(message: S) -> Self {
        FlareError::Other {
            message: format!("输入无效: {}", message.into()),
        }
    }

    // ============================================================================
    // 兼容性方法（保持向后兼容）
    // ============================================================================

    /// 兼容旧代码：general_error
    pub fn general_error<S: Into<String>>(s: S) -> Self {
        FlareError::Other {
            message: s.into(),
        }
    }

    /// 兼容旧代码：deserialization_failed
    pub fn deserialization_failed<S: Into<String>>(s: S) -> Self {
        FlareError::SerializationError {
            message: s.into(),
            source: None,
        }
    }

    // ============================================================================
    // 辅助方法
    // ============================================================================

    /// 判断是否为可重试错误
    ///
    /// # 返回
    /// - `true`: 错误可能是暂时性的，可以重试
    /// - `false`: 错误是永久性的，重试无意义
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            FlareError::ConnectionFailed { .. }  // 连接失败通常可以重试
                | FlareError::Timeout { .. }
                | FlareError::HeartbeatTimeout { .. }
                | FlareError::IoError { .. }
                | FlareError::RateLimitExceeded { .. }
        )
    }

    /// 判断是否为网络相关错误
    pub fn is_network_error(&self) -> bool {
        matches!(
            self,
            FlareError::ConnectionFailed { .. }
                | FlareError::Timeout { .. }
                | FlareError::IoError { .. }
        )
    }

    /// 判断是否为认证相关错误
    pub fn is_auth_error(&self) -> bool {
        matches!(self, FlareError::AuthenticationFailed { .. })
    }
}

impl Display for FlareError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            FlareError::ConnectionFailed { message, .. } => {
                write!(f, "Connection failed: {}", message)
            }
            FlareError::Timeout {
                operation,
                timeout_ms,
            } => {
                write!(f, "Operation '{}' timed out after {}ms", operation, timeout_ms)
            }
            FlareError::SerializationError { message, .. } => {
                write!(f, "Serialization error: {}", message)
            }
            FlareError::MessageSendFailed { message, reason } => {
                if let Some(r) = reason {
                    write!(f, "Message send failed: {} (reason: {})", message, r)
                } else {
                    write!(f, "Message send failed: {}", message)
                }
            }
            FlareError::HeartbeatTimeout {
                missed_count,
                threshold,
            } => {
                write!(
                    f,
                    "Heartbeat timeout: {}/{} missed",
                    missed_count, threshold
                )
            }
            FlareError::AuthenticationFailed { message } => {
                write!(f, "Authentication failed: {}", message)
            }
            FlareError::RateLimitExceeded { limit, window_ms } => {
                write!(
                    f,
                    "Rate limit exceeded: {} requests per {}ms",
                    limit, window_ms
                )
            }
            FlareError::ConfigError { message } => {
                write!(f, "Configuration error: {}", message)
            }
            FlareError::IoError { message, .. } => {
                write!(f, "I/O error: {}", message)
            }
            FlareError::ProtocolError { message } => {
                write!(f, "Protocol error: {}", message)
            }
            FlareError::InvalidState {
                current_state,
                operation,
            } => {
                write!(
                    f,
                    "Invalid state: cannot '{}' in state '{}'",
                    operation, current_state
                )
            }
            FlareError::Other { message } => {
                write!(f, "Error: {}", message)
            }
        }
    }
}

impl Error for FlareError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // 由于我们不再存储原始 source，这里返回 None
        None
    }
}

// ============================================================================
// 类型转换
// ============================================================================

impl From<io::Error> for FlareError {
    fn from(err: io::Error) -> Self {
        FlareError::IoError {
            message: err.to_string(),
            kind: format!("{:?}", err.kind()),
        }
    }
}

impl From<serde_json::Error> for FlareError {
    fn from(err: serde_json::Error) -> Self {
        FlareError::serialization_error_with_source("JSON error", err)
    }
}

// ============================================================================
// 单元测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_failed() {
        let err = FlareError::connection_failed("Connection refused");
        assert!(err.to_string().contains("Connection failed"));
        assert!(err.is_network_error());
        assert!(err.is_retryable());
    }

    #[test]
    fn test_timeout() {
        let err = FlareError::timeout("connect", 5000);
        assert!(err.to_string().contains("timed out"));
        assert!(err.to_string().contains("5000ms"));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_heartbeat_timeout() {
        let err = FlareError::heartbeat_timeout(3, 3);
        assert!(err.to_string().contains("3/3"));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_invalid_state() {
        let err = FlareError::invalid_state("Disconnected", "send_message");
        assert!(err.to_string().contains("Invalid state"));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_with_source() {
        let io_err = io::Error::new(io::ErrorKind::TimedOut, "timeout");
        let err = FlareError::connection_failed_with_source("Failed", io_err);
        // source 现在是 String，可以 clone
        let _cloned = err.clone();
    }

    #[test]
    fn test_from_io_error() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "not found");
        let err: FlareError = io_err.into();
        assert!(matches!(err, FlareError::IoError { .. }));
    }

    #[test]
    fn test_retryable() {
        assert!(FlareError::timeout("op", 1000).is_retryable());
        assert!(!FlareError::protocol_error("bad format").is_retryable());
    }

    #[test]
    fn test_auth_error() {
        let err = FlareError::authentication_failed("invalid token");
        assert!(err.is_auth_error());
        assert!(!err.is_network_error());
    }
}
