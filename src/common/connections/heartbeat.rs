//! 通用心跳管理模块
//! 
//! 提供灵活、可配置的心跳检测与自适应调节能力，适用于各种长连接场景。
//! 
//! # 核心功能
//! 
//! - **心跳间隔管理**：支持固定间隔或自适应调节
//! - **超时检测**：连续超时次数统计与阈值触发
//! - **RTT测量**：往返时延统计与抖动分析
//! - **网络感知**：可选的网络类型感知与动态调整
//! 
//! # 设计原则
//! 
//! 1. **通用性**：不绑定特定业务场景，所有参数可配置
//! 2. **灵活性**：支持多种调节策略，可插拔扩展
//! 3. **高性能**：原子操作，无锁设计，低开销
//! 4. **易测试**：行为可预测，便于单元测试
//! 
//! # 使用示例
//! 
//! ```rust
//! use flare_core::common::connections::heartbeat::{HeartbeatConfig, HeartbeatManager};
//! 
//! // 1. 创建配置（使用默认值）
//! let config = HeartbeatConfig::default();
//! 
//! // 2. 或自定义配置
//! let custom_config = HeartbeatConfig {
//!     initial_interval_ms: 30000,  // 初始30秒
//!     min_interval_ms: 5000,       // 最小5秒
//!     max_interval_ms: 120000,     // 最大2分钟
//!     timeout_threshold: 3,        // 连续3次超时触发重连
//!     enable_adaptive: true,       // 启用自适应调节
//!     rtt_window_size: 30,         // RTT滑动窗口30个样本
//!     ..Default::default()
//! };
//! 
//! // 3. 创建心跳管理器
//! let manager = HeartbeatManager::new(custom_config);
//! 
//! // 4. 使用
//! let interval = manager.get_interval();
//! manager.on_heartbeat_success();
//! manager.record_rtt(50);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

/// 心跳配置
/// 
/// 所有参数均可配置，业务层可根据需求定制
#[derive(Clone, Debug)]
pub struct HeartbeatConfig {
    /// 初始心跳间隔（毫秒）
    pub initial_interval_ms: u64,
    
    /// 最小心跳间隔（毫秒）
    pub min_interval_ms: u64,
    
    /// 最大心跳间隔（毫秒）
    pub max_interval_ms: u64,
    
    /// 连续超时阈值（超过此值触发重连）
    pub timeout_threshold: u32,
    
    /// 是否启用自适应调节
    pub enable_adaptive: bool,
    
    /// RTT滑动窗口大小（用于计算平均值和抖动）
    pub rtt_window_size: usize,
    
    /// 高延迟阈值（毫秒，超过此值认为网络质量差）
    pub high_rtt_threshold_ms: u32,
    
    /// 低延迟阈值（毫秒，低于此值认为网络质量好）
    pub low_rtt_threshold_ms: u32,
    
    /// 高抖动阈值（毫秒，超过此值认为网络不稳定）
    pub high_jitter_threshold_ms: f64,
    
    /// 低抖动阈值（毫秒，低于此值认为网络稳定）
    pub low_jitter_threshold_ms: f64,
    
    /// 自适应调节系数（网络质量差时的缩短系数）
    pub adaptive_decrease_factor: f64,
    
    /// 自适应调节系数（网络质量好时的延长系数）
    pub adaptive_increase_factor: f64,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            initial_interval_ms: 30000,      // 默认30秒
            min_interval_ms: 10000,          // 最小10秒
            max_interval_ms: 60000,          // 最大60秒
            timeout_threshold: 3,            // 连续3次超时触发重连
            enable_adaptive: true,           // 默认启用自适应
            rtt_window_size: 30,             // 窗口30个样本
            high_rtt_threshold_ms: 500,      // >500ms认为高延迟
            low_rtt_threshold_ms: 100,       // <100ms认为低延迟
            high_jitter_threshold_ms: 100.0, // >100ms认为高抖动
            low_jitter_threshold_ms: 20.0,   // <20ms认为低抖动
            adaptive_decrease_factor: 0.8,   // 质量差时缩短20%
            adaptive_increase_factor: 1.2,   // 质量好时延长20%
        }
    }
}

/// 心跳管理器
/// 
/// 线程安全的心跳管理器，提供心跳间隔管理、超时检测、RTT统计等功能
pub struct HeartbeatManager {
    /// 配置
    config: HeartbeatConfig,
    
    /// 当前心跳间隔（毫秒）
    current_interval_ms: AtomicU64,
    
    /// RTT历史记录（滑动窗口）
    rtt_history: Arc<RwLock<VecDeque<u32>>>,
    
    /// 连续心跳超时次数
    consecutive_timeouts: AtomicU64,
    
    /// 总心跳发送次数
    total_heartbeats_sent: AtomicU64,
    
    /// 总心跳成功次数
    total_heartbeats_success: AtomicU64,
}

impl HeartbeatManager {
    /// 创建新的心跳管理器
    /// 
    /// # 参数
    /// 
    /// - `config`: 心跳配置
    /// 
    /// # 示例
    /// 
    /// ```
    /// use flare_core::common::connections::heartbeat::{HeartbeatConfig, HeartbeatManager};
    /// 
    /// let config = HeartbeatConfig::default();
    /// let manager = HeartbeatManager::new(config);
    /// ```
    pub fn new(config: HeartbeatConfig) -> Self {
        let initial_interval = config.initial_interval_ms;
        
        Self {
            config,
            current_interval_ms: AtomicU64::new(initial_interval),
            rtt_history: Arc::new(RwLock::new(VecDeque::new())),
            consecutive_timeouts: AtomicU64::new(0),
            total_heartbeats_sent: AtomicU64::new(0),
            total_heartbeats_success: AtomicU64::new(0),
        }
    }
    
    /// 获取当前心跳间隔（毫秒）
    #[inline]
    pub fn get_interval(&self) -> u64 {
        self.current_interval_ms.load(Ordering::Relaxed)
    }
    
    /// 手动设置心跳间隔
    /// 
    /// # 参数
    /// 
    /// - `interval_ms`: 新的心跳间隔（毫秒），会被限制在 [min, max] 范围内
    /// 
    /// # 返回
    /// 
    /// 实际设置的间隔值
    pub fn set_interval(&self, interval_ms: u64) -> u64 {
        let clamped = interval_ms
            .max(self.config.min_interval_ms)
            .min(self.config.max_interval_ms);
        
        self.current_interval_ms.store(clamped, Ordering::Relaxed);
        clamped
    }
    
    /// 记录心跳发送
    #[inline]
    pub fn on_heartbeat_sent(&self) {
        self.total_heartbeats_sent.fetch_add(1, Ordering::Relaxed);
    }
    
    /// 记录心跳成功（收到响应）
    /// 
    /// 重置连续超时计数器，增加成功计数
    #[inline]
    pub fn on_heartbeat_success(&self) {
        self.consecutive_timeouts.store(0, Ordering::Relaxed);
        self.total_heartbeats_success.fetch_add(1, Ordering::Relaxed);
    }
    
    /// 记录心跳超时
    /// 
    /// # 返回
    /// 
    /// - `true`: 连续超时次数达到阈值，建议触发重连
    /// - `false`: 尚未达到阈值
    #[inline]
    pub fn on_heartbeat_timeout(&self) -> bool {
        let count = self.consecutive_timeouts.fetch_add(1, Ordering::Relaxed) + 1;
        count >= self.config.timeout_threshold as u64
    }
    
    /// 记录RTT（往返时延）
    /// 
    /// 如果启用了自适应调节，会自动触发间隔调整
    /// 
    /// # 参数
    /// 
    /// - `rtt_ms`: 本次测量的RTT（毫秒）
    pub fn record_rtt(&self, rtt_ms: u32) {
        // 1. 添加到历史记录
        if let Ok(mut history) = self.rtt_history.write() {
            if history.len() >= self.config.rtt_window_size {
                history.pop_front();
            }
            history.push_back(rtt_ms);
        }
        
        // 2. 如果启用自适应，触发调节
        if self.config.enable_adaptive {
            self.adjust_interval();
        }
    }
    
    /// 自适应调节心跳间隔
    /// 
    /// 根据RTT统计结果动态调整间隔：
    /// - 高延迟或高抖动 → 缩短间隔（提高检测频率）
    /// - 低延迟低抖动 → 延长间隔（节省资源）
    fn adjust_interval(&self) {
        if let Ok(history) = self.rtt_history.read() {
            if history.is_empty() {
                return;
            }
            
            // 计算平均RTT
            let avg_rtt: u32 = history.iter().sum::<u32>() / history.len() as u32;
            
            // 计算抖动（标准差）
            let jitter = if history.len() >= 2 {
                let variance: f64 = history.iter()
                    .map(|&x| {
                        let diff = x as f64 - avg_rtt as f64;
                        diff * diff
                    })
                    .sum::<f64>() / history.len() as f64;
                variance.sqrt()
            } else {
                0.0
            };
            
            // 根据RTT和抖动调整间隔
            let current = self.current_interval_ms.load(Ordering::Relaxed);
            let new_interval = if avg_rtt > self.config.high_rtt_threshold_ms 
                || jitter > self.config.high_jitter_threshold_ms {
                // 网络质量差，缩短间隔
                (current as f64 * self.config.adaptive_decrease_factor)
                    .max(self.config.min_interval_ms as f64) as u64
            } else if avg_rtt < self.config.low_rtt_threshold_ms 
                && jitter < self.config.low_jitter_threshold_ms {
                // 网络质量好，延长间隔
                (current as f64 * self.config.adaptive_increase_factor)
                    .min(self.config.max_interval_ms as f64) as u64
            } else {
                current
            };
            
            self.current_interval_ms.store(new_interval, Ordering::Relaxed);
        }
    }
    
    /// 获取连续超时次数
    #[inline]
    pub fn get_consecutive_timeouts(&self) -> u64 {
        self.consecutive_timeouts.load(Ordering::Relaxed)
    }
    
    /// 获取心跳成功率
    /// 
    /// # 返回
    /// 
    /// 成功率 (0.0 - 1.0)，如果尚未发送心跳则返回 0.0
    pub fn get_success_rate(&self) -> f64 {
        let total = self.total_heartbeats_sent.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        
        let success = self.total_heartbeats_success.load(Ordering::Relaxed);
        success as f64 / total as f64
    }
    
    /// 获取平均RTT
    /// 
    /// # 返回
    /// 
    /// 如果有RTT记录则返回平均值，否则返回 None
    pub fn get_avg_rtt(&self) -> Option<u32> {
        if let Ok(history) = self.rtt_history.read() {
            if history.is_empty() {
                None
            } else {
                Some(history.iter().sum::<u32>() / history.len() as u32)
            }
        } else {
            None
        }
    }
    
    /// 获取RTT抖动（标准差）
    /// 
    /// # 返回
    /// 
    /// 如果有足够的RTT记录（>=2）则返回抖动值，否则返回 None
    pub fn get_rtt_jitter(&self) -> Option<f64> {
        if let Ok(history) = self.rtt_history.read() {
            if history.len() < 2 {
                return None;
            }
            
            let avg = history.iter().sum::<u32>() as f64 / history.len() as f64;
            let variance: f64 = history.iter()
                .map(|&x| {
                    let diff = x as f64 - avg;
                    diff * diff
                })
                .sum::<f64>() / history.len() as f64;
            
            Some(variance.sqrt())
        } else {
            None
        }
    }
    
    /// 重置所有统计数据
    pub fn reset_statistics(&self) {
        self.consecutive_timeouts.store(0, Ordering::Relaxed);
        self.total_heartbeats_sent.store(0, Ordering::Relaxed);
        self.total_heartbeats_success.store(0, Ordering::Relaxed);
        
        if let Ok(mut history) = self.rtt_history.write() {
            history.clear();
        }
    }
    
    /// 获取配置的引用
    pub fn config(&self) -> &HeartbeatConfig {
        &self.config
    }
}

/// 获取当前时间戳（毫秒）
#[inline]
pub fn current_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = HeartbeatConfig::default();
        assert_eq!(config.initial_interval_ms, 30000);
        assert_eq!(config.min_interval_ms, 10000);
        assert_eq!(config.max_interval_ms, 60000);
        assert_eq!(config.timeout_threshold, 3);
        assert!(config.enable_adaptive);
    }
    
    #[test]
    fn test_custom_config() {
        let config = HeartbeatConfig {
            initial_interval_ms: 20000,
            min_interval_ms: 5000,
            max_interval_ms: 120000,
            timeout_threshold: 5,
            enable_adaptive: false,
            ..Default::default()
        };
        
        let manager = HeartbeatManager::new(config);
        assert_eq!(manager.get_interval(), 20000);
    }
    
    #[test]
    fn test_interval_clamping() {
        let config = HeartbeatConfig::default();
        let manager = HeartbeatManager::new(config);
        
        // 测试最小值限制
        let actual = manager.set_interval(5000);
        assert_eq!(actual, 10000); // 应被限制为min
        
        // 测试最大值限制
        let actual = manager.set_interval(120000);
        assert_eq!(actual, 60000); // 应被限制为max
        
        // 测试正常值
        let actual = manager.set_interval(30000);
        assert_eq!(actual, 30000);
    }
    
    #[test]
    fn test_timeout_threshold() {
        let config = HeartbeatConfig {
            timeout_threshold: 3,
            ..Default::default()
        };
        let manager = HeartbeatManager::new(config);
        
        // 前2次超时不应触发重连
        assert!(!manager.on_heartbeat_timeout());
        assert!(!manager.on_heartbeat_timeout());
        
        // 第3次超时应触发重连
        assert!(manager.on_heartbeat_timeout());
        
        // 成功后重置
        manager.on_heartbeat_success();
        assert_eq!(manager.get_consecutive_timeouts(), 0);
    }
    
    #[test]
    fn test_rtt_recording() {
        let config = HeartbeatConfig {
            enable_adaptive: false, // 禁用自适应以便测试
            ..Default::default()
        };
        let manager = HeartbeatManager::new(config);
        
        // 记录一些RTT
        manager.record_rtt(50);
        manager.record_rtt(60);
        manager.record_rtt(55);
        
        // 验证平均值
        let avg = manager.get_avg_rtt().unwrap();
        assert_eq!(avg, 55); // (50+60+55)/3 = 55
        
        // 验证抖动
        let jitter = manager.get_rtt_jitter().unwrap();
        assert!(jitter > 0.0 && jitter < 10.0); // 应该有小幅抖动
    }
    
    #[test]
    fn test_adaptive_adjustment() {
        let config = HeartbeatConfig {
            initial_interval_ms: 60000,
            enable_adaptive: true,
            high_rtt_threshold_ms: 500,
            low_rtt_threshold_ms: 100,
            adaptive_decrease_factor: 0.8,
            adaptive_increase_factor: 1.2,
            ..Default::default()
        };
        let manager = HeartbeatManager::new(config);
        let initial = manager.get_interval();
        
        // 模拟高延迟（应缩短间隔）
        for _ in 0..10 {
            manager.record_rtt(600);
        }
        let after_high_rtt = manager.get_interval();
        assert!(after_high_rtt < initial, "高延迟应该缩短心跳间隔");
        
        // 模拟低延迟（应延长间隔）
        let manager2 = HeartbeatManager::new(HeartbeatConfig {
            initial_interval_ms: 30000,
            enable_adaptive: true,
            ..Default::default()
        });
        let initial2 = manager2.get_interval();
        for _ in 0..10 {
            manager2.record_rtt(50);
        }
        let after_low_rtt = manager2.get_interval();
        assert!(after_low_rtt > initial2, "低延迟应该延长心跳间隔");
    }
    
    #[test]
    fn test_success_rate() {
        let config = HeartbeatConfig::default();
        let manager = HeartbeatManager::new(config);
        
        // 初始时成功率为0
        assert_eq!(manager.get_success_rate(), 0.0);
        
        // 模拟心跳
        manager.on_heartbeat_sent();
        manager.on_heartbeat_success();
        
        manager.on_heartbeat_sent();
        manager.on_heartbeat_success();
        
        manager.on_heartbeat_sent();
        // 这次没有成功
        
        // 成功率应该是 2/3
        let rate = manager.get_success_rate();
        assert!((rate - 0.666).abs() < 0.01);
    }
    
    #[test]
    fn test_reset_statistics() {
        let config = HeartbeatConfig::default();
        let manager = HeartbeatManager::new(config);
        
        // 生成一些数据
        manager.on_heartbeat_sent();
        manager.on_heartbeat_success();
        manager.record_rtt(50);
        manager.on_heartbeat_timeout();
        
        // 重置
        manager.reset_statistics();
        
        // 验证重置后的状态
        assert_eq!(manager.get_consecutive_timeouts(), 0);
        assert_eq!(manager.get_success_rate(), 0.0);
        assert!(manager.get_avg_rtt().is_none());
    }
}
