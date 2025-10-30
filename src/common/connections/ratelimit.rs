// 流量控制模块 - 令牌桶算法
// 用于防止连接过载，支持动态速率调整

use std::sync::atomic::{AtomicU64, Ordering};

/// 令牌桶限流器（线程安全）
#[derive(Debug)]
pub struct TokenBucket {
    /// 当前令牌数（使用浮点数的整数表示，实际值需除以 PRECISION）
    tokens: AtomicU64,
    /// 桶容量
    capacity: u64,
    /// 令牌补充速率（tokens/秒）
    refill_rate: u64,
    /// 上次补充时间（纳秒）
    last_refill_ns: AtomicU64,
    /// 精度因子（避免浮点运算）
    precision: u64,
}

const PRECISION: u64 = 1000;

impl TokenBucket {
    /// 创建新的令牌桶
    /// 
    /// # 参数
    /// - `capacity`: 桶容量（最大令牌数）
    /// - `refill_rate`: 补充速率（tokens/秒）
    pub fn new(capacity: u64, refill_rate: u64) -> Self {
        Self {
            tokens: AtomicU64::new(capacity * PRECISION),
            capacity,
            refill_rate,
            last_refill_ns: AtomicU64::new(now_nanos()),
            precision: PRECISION,
        }
    }

    /// 尝试获取指定数量的令牌
    /// 
    /// # 返回
    /// - `true`: 成功获取令牌
    /// - `false`: 令牌不足，请求被限流
    pub fn try_acquire(&self, tokens: u64) -> bool {
        self.refill();
        
        let required = tokens * self.precision;
        let mut current = self.tokens.load(Ordering::Relaxed);
        
        loop {
            if current < required {
                return false; // 令牌不足
            }
            
            match self.tokens.compare_exchange_weak(
                current,
                current - required,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// 补充令牌（根据时间流逝自动补充）
    fn refill(&self) {
        let now = now_nanos();
        let last = self.last_refill_ns.load(Ordering::Relaxed);
        
        if now <= last {
            return; // 时间未流逝
        }
        
        let elapsed_ns = now - last;
        let elapsed_secs = elapsed_ns as f64 / 1_000_000_000.0;
        
        // 计算应补充的令牌数
        let tokens_to_add = (self.refill_rate as f64 * elapsed_secs * self.precision as f64) as u64;
        
        if tokens_to_add > 0 {
            let max_tokens = self.capacity * self.precision;
            let mut current = self.tokens.load(Ordering::Relaxed);
            
            loop {
                let new_tokens = (current + tokens_to_add).min(max_tokens);
                
                match self.tokens.compare_exchange_weak(
                    current,
                    new_tokens,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        self.last_refill_ns.store(now, Ordering::Relaxed);
                        break;
                    }
                    Err(actual) => current = actual,
                }
            }
        }
    }

    /// 获取当前可用令牌数
    pub fn available(&self) -> u64 {
        self.refill();
        self.tokens.load(Ordering::Relaxed) / self.precision
    }

    /// 重置令牌桶（填满）
    pub fn reset(&self) {
        self.tokens.store(self.capacity * self.precision, Ordering::Relaxed);
        self.last_refill_ns.store(now_nanos(), Ordering::Relaxed);
    }
}

/// 获取当前时间（纳秒）
#[inline]
fn now_nanos() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// 分层限流器（连接级 + 全局级）
#[derive(Debug)]
pub struct HierarchicalRateLimiter {
    /// 单连接限流器
    per_connection: TokenBucket,
    /// 全局限流器（所有连接共享）
    global: Option<&'static TokenBucket>,
}

impl HierarchicalRateLimiter {
    /// 创建分层限流器
    pub fn new(per_conn_rate: u64, global_limiter: Option<&'static TokenBucket>) -> Self {
        Self {
            per_connection: TokenBucket::new(per_conn_rate * 2, per_conn_rate),
            global: global_limiter,
        }
    }

    /// 尝试通过限流检查
    pub fn try_acquire(&self, tokens: u64) -> bool {
        // 先检查连接级限流
        if !self.per_connection.try_acquire(tokens) {
            return false;
        }
        
        // 再检查全局限流
        if let Some(global) = self.global {
            if !global.try_acquire(tokens) {
                // 全局限流失败，归还连接级令牌
                // （简化实现，实际可能需要更精细的补偿机制）
                return false;
            }
        }
        
        true
    }
}

/// 背压控制器
#[derive(Debug)]
pub struct BackpressureController {
    /// 当前负载水平 (0-100)
    load_level: AtomicU64,
    /// 高水位线（触发背压）
    high_watermark: u64,
    /// 低水位线（解除背压）
    low_watermark: u64,
}

impl BackpressureController {
    pub fn new(high: u64, low: u64) -> Self {
        Self {
            load_level: AtomicU64::new(0),
            high_watermark: high,
            low_watermark: low,
        }
    }

    /// 更新负载水平
    pub fn update_load(&self, current: u64, capacity: u64) {
        let level = if capacity > 0 {
            (current * 100 / capacity).min(100)
        } else {
            0
        };
        self.load_level.store(level, Ordering::Relaxed);
    }

    /// 是否应该应用背压
    pub fn should_apply(&self) -> bool {
        self.load_level.load(Ordering::Relaxed) > self.high_watermark
    }

    /// 是否可以解除背压
    pub fn can_release(&self) -> bool {
        self.load_level.load(Ordering::Relaxed) < self.low_watermark
    }

    /// 获取当前负载水平
    pub fn get_load(&self) -> u64 {
        self.load_level.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_token_bucket() {
        let bucket = TokenBucket::new(10, 10); // 10个令牌容量，10 tokens/秒
        
        // 初始应该有 10 个令牌
        assert_eq!(bucket.available(), 10);
        
        // 获取 5 个令牌
        assert!(bucket.try_acquire(5));
        assert_eq!(bucket.available(), 5);
        
        // 再获取 6 个令牌应该失败
        assert!(!bucket.try_acquire(6));
        assert_eq!(bucket.available(), 5);
    }

    #[test]
    fn test_refill() {
        let bucket = TokenBucket::new(10, 10); // 10 tokens/秒
        
        // 消耗所有令牌
        assert!(bucket.try_acquire(10));
        assert_eq!(bucket.available(), 0);
        
        // 等待 1 秒
        thread::sleep(Duration::from_secs(1));
        
        // 应该补充了约 10 个令牌
        let available = bucket.available();
        assert!(available >= 9 && available <= 10);
    }

    #[test]
    fn test_backpressure() {
        let bp = BackpressureController::new(80, 20);
        
        // 低负载，不应该触发背压
        bp.update_load(10, 100);
        assert!(!bp.should_apply());
        assert!(bp.can_release());
        
        // 高负载，应该触发背压
        bp.update_load(90, 100);
        assert!(bp.should_apply());
        assert!(!bp.can_release());
    }
}
