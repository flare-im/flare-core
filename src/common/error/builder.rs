//! 错误构建器

use super::code::ErrorCode;
use super::flare_error::FlareError;
use super::localized::LocalizedError;
use std::collections::HashMap;

/// 错误构建器 - 提供链式 API 构建复杂错误
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
    #[must_use]
    pub fn details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }

    /// 添加错误参数
    #[must_use]
    pub fn params(mut self, params: HashMap<String, String>) -> Self {
        self.params = Some(params);
        self
    }

    /// 添加单个参数
    #[must_use]
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
        FlareError::Localized {
            code: self.code,
            reason: self.reason,
            details: self.details,
            params: self.params,
            timestamp: chrono::Utc::now(),
        }
    }

    /// 构建为 LocalizedError
    pub fn build_localized(self) -> LocalizedError {
        let mut localized = LocalizedError::new(self.code, self.reason);
        if let Some(details) = self.details {
            localized = localized.with_details(details);
        }
        if let Some(params) = self.params {
            localized = localized.with_params(params);
        }
        localized
    }
}
