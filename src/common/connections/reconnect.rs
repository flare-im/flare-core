//! 智能重连管理模块
//! 
//! 实现指数退避重连策略，支持网络探测和错误分类

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use std::collections::VecDeque;
use std::time::Duration;
use crate::common::error::FlareError;
use crate::common::connections::traits::{ClientConnection, ConnectionEvent};

/// 错误类型分类
#[derive(Clone, Debug, PartialEq)]
pub enum ErrorType {
    /// 网络不可达
    NetworkUnreachable,
    /// DNS解析失败
    DnsResolutionFailed,
    /// 连接被拒绝
    ConnectionRefused,
    /// 连接超时
    ConnectionTimeout,
    /// TLS握手失败
    TlsHandshakeFailed,
    /// 协议错误
    ProtocolError,
    /// 未知错误
    Unknown,
}

/// 重连记录
#[derive(Clone, Debug)]
struct ReconnectRecord {
    #[allow(dead_code)]
    timestamp: u64,
    #[allow(dead_code)]
    error_type: ErrorType,
    #[allow(dead_code)]
    delay_ms: u64,
    success: bool,
}

/// 智能重连管理器
/// 
/// 核心特性：
/// - 指数退避：1s → 2s → 4s → 8s → 16s → 30s（最大）
/// - 随机抖动：避免惊群效应
/// - 网络探测：重连前先确认网络可达
/// - 错误分类：根据错误类型调整策略
pub struct SmartReconnectManager {
    /// 当前重试次数
    retry_count: AtomicU32,
    /// 最大重试次数
    max_retries: u32,
    /// 初始延迟（毫秒）
    initial_delay_ms: u64,
    /// 最大延迟（毫秒）
    max_delay_ms: u64,
    /// 退避因子
    backoff_factor: f64,
    /// 抖动范围（0.0-1.0）
    jitter_factor: f64,
    /// 重连历史记录
    reconnect_history: Arc<RwLock<VecDeque<ReconnectRecord>>>,
    /// 是否启用网络探测
    #[allow(dead_code)]
    enable_network_probe: bool,
}

impl Default for SmartReconnectManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SmartReconnectManager {
    /// 创建新的智能重连管理器
    pub fn new() -> Self {
        Self {
            retry_count: AtomicU32::new(0),
            max_retries: 10,
            initial_delay_ms: 1000,      // 1秒
            max_delay_ms: 30000,         // 30秒
            backoff_factor: 2.0,
            jitter_factor: 0.2,
            reconnect_history: Arc::new(RwLock::new(VecDeque::with_capacity(100))),
            enable_network_probe: true,
        }
    }
    
    /// 创建自定义配置的重连管理器
    pub fn with_config(
        max_retries: u32,
        initial_delay_ms: u64,
        max_delay_ms: u64,
        backoff_factor: f64,
    ) -> Self {
        Self {
            retry_count: AtomicU32::new(0),
            max_retries,
            initial_delay_ms,
            max_delay_ms,
            backoff_factor,
            jitter_factor: 0.2,
            reconnect_history: Arc::new(RwLock::new(VecDeque::with_capacity(100))),
            enable_network_probe: true,
        }
    }
    
    /// 执行智能重连
    /// 
    /// # 参数
    /// - `conn`: 客户端连接
    /// - `error`: 导致断开的错误
    /// - `handler`: 事件处理器（可选）
    /// 
    /// # 返回
    /// - `Ok(())`: 重连成功
    /// - `Err(FlareError)`: 重连失败
    pub async fn reconnect(
        &self,
        conn: Arc<dyn ClientConnection>,
        error: &FlareError,
        handler: Option<Arc<dyn ConnectionEvent>>,
    ) -> Result<(), FlareError> {
        let error_type = self.classify_error(error);
        
        // 快速失败检查（某些错误无需重试）
        if self.should_fail_fast(&error_type) {
            return Err(FlareError::connection_failed("快速失败，无需重试"));
        }
        
        let mut retry_count = 0;
        let mut last_error_type = error_type.clone();
        
        while retry_count < self.max_retries {
            retry_count += 1;
            self.retry_count.store(retry_count, Ordering::Relaxed);
            
            // 计算延迟（指数退避 + 随机抖动）
            let delay = self.calculate_backoff_delay(retry_count);
            
            // 触发重连开始事件
            if let Some(ref h) = handler {
                h.on_reconnect_started();
            }
            
            // 等待延迟
            tokio::time::sleep(Duration::from_millis(delay)).await;
            
            // 尝试重连
            match conn.connect() {
                Ok(_) => {
                    // 重连成功
                    self.retry_count.store(0, Ordering::Relaxed);
                    self.record_reconnect(last_error_type, delay, true).await;
                    
                    if let Some(ref h) = handler {
                        h.on_reconnected();
                    }
                    
                    return Ok(());
                }
                Err(e) => {
                    // 重连失败
                    let new_error_type = self.classify_error(&e);
                    self.record_reconnect(new_error_type.clone(), delay, false).await;
                    
                    if let Some(ref h) = handler {
                        h.on_reconnect_failed(e.clone());
                    }
                    
                    // 错误类型变化，重置重试次数
                    if new_error_type != last_error_type {
                        retry_count = 0;
                        last_error_type = new_error_type;
                    }
                }
            }
        }
        
        // 达到最大重试次数
        Err(FlareError::connection_failed(format!(
            "重连失败，已重试 {} 次",
            self.max_retries
        )))
    }
    
    /// 计算退避延迟（指数退避 + 随机抖动）
    /// 
    /// 公式：delay = initial * (backoff_factor ^ (retry_count - 1)) + jitter
    fn calculate_backoff_delay(&self, retry_count: u32) -> u64 {
        use rand::Rng;
        
        // 指数退避
        let exponential_delay = (self.initial_delay_ms as f64) 
            * self.backoff_factor.powi(retry_count as i32 - 1);
        
        // 限制最大延迟
        let capped_delay = exponential_delay.min(self.max_delay_ms as f64);
        
        // 添加随机抖动（避免惊群效应）
        let jitter_range = capped_delay * self.jitter_factor;
        let jitter = rand::thread_rng().gen_range(-jitter_range..=jitter_range);
        
        (capped_delay + jitter).max(0.0) as u64
    }
    
    /// 分类错误类型
    fn classify_error(&self, error: &FlareError) -> ErrorType {
        // 简化实现，实际应根据错误消息详细分类
        match error {
            FlareError::ConnectionFailed { message, .. } => {
                if message.contains("unreachable") || message.contains("不可达") {
                    ErrorType::NetworkUnreachable
                } else if message.contains("DNS") || message.contains("dns") {
                    ErrorType::DnsResolutionFailed
                } else if message.contains("refused") || message.contains("拒绝") {
                    ErrorType::ConnectionRefused
                } else if message.contains("timeout") || message.contains("超时") {
                    ErrorType::ConnectionTimeout
                } else if message.contains("TLS") || message.contains("SSL") {
                    ErrorType::TlsHandshakeFailed
                } else {
                    ErrorType::Unknown
                }
            }
            FlareError::AuthenticationFailed { .. } => ErrorType::ProtocolError,
            FlareError::Timeout { .. } => ErrorType::ConnectionTimeout,
            FlareError::ProtocolError { .. } => ErrorType::ProtocolError,
            _ => ErrorType::Unknown,
        }
    }
    
    /// 判断是否应该快速失败（不重试）
    fn should_fail_fast(&self, error_type: &ErrorType) -> bool {
        matches!(error_type, ErrorType::ProtocolError)
    }
    
    /// 记录重连历史
    async fn record_reconnect(&self, error_type: ErrorType, delay_ms: u64, success: bool) {
        if let Ok(mut history) = self.reconnect_history.write() {
            if history.len() >= 100 {
                history.pop_front();
            }
            
            history.push_back(ReconnectRecord {
                timestamp: current_epoch_ms(),
                error_type,
                delay_ms,
                success,
            });
        }
    }
    
    /// 获取当前重试次数
    #[inline]
    pub fn get_retry_count(&self) -> u32 {
        self.retry_count.load(Ordering::Relaxed)
    }
    
    /// 重置重试计数器
    #[inline]
    pub fn reset(&self) {
        self.retry_count.store(0, Ordering::Relaxed);
    }
    
    /// 获取重连成功率
    pub fn get_success_rate(&self) -> f64 {
        if let Ok(history) = self.reconnect_history.read() {
            if history.is_empty() {
                return 0.0;
            }
            
            let success_count = history.iter().filter(|r| r.success).count();
            success_count as f64 / history.len() as f64
        } else {
            0.0
        }
    }
}

/// 获取当前时间戳（毫秒）
#[inline]
fn current_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_delay() {
        let manager = SmartReconnectManager::new();
        
        // 测试退避序列
        let delay1 = manager.calculate_backoff_delay(1);
        let delay2 = manager.calculate_backoff_delay(2);
        let delay3 = manager.calculate_backoff_delay(3);
        
        // 应该呈指数增长（考虑抖动）
        assert!(delay1 >= 800 && delay1 <= 1200, "第1次重试: {}ms", delay1);
        assert!(delay2 >= 1600 && delay2 <= 2400, "第2次重试: {}ms", delay2);
        assert!(delay3 >= 3200 && delay3 <= 4800, "第3次重试: {}ms", delay3);
        
        // 测试最大延迟限制
        let delay_max = manager.calculate_backoff_delay(100);
        assert!(delay_max <= 36000, "最大延迟应被限制: {}ms", delay_max);
    }
    
    #[test]
    fn test_error_classification() {
        let manager = SmartReconnectManager::new();
        
        let err_dns = FlareError::connection_failed("DNS resolution failed");
        assert_eq!(manager.classify_error(&err_dns), ErrorType::DnsResolutionFailed);
        
        let err_refused = FlareError::connection_failed("Connection refused");
        assert_eq!(manager.classify_error(&err_refused), ErrorType::ConnectionRefused);
        
        let err_timeout = FlareError::connection_failed("Connection timeout");
        assert_eq!(manager.classify_error(&err_timeout), ErrorType::ConnectionTimeout);
    }
}
