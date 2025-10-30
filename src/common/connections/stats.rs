// 高性能连接统计模块
// 使用原子操作避免锁竞争，支持千万级并发

use std::sync::atomic::{AtomicU64, AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[allow(unused_imports)]
use std::sync::Arc;

/// 高性能统计计数器（无锁原子操作）
#[derive(Debug)]
pub struct AtomicStats {
    pub messages_sent: AtomicU64,
    pub messages_received: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub heartbeat_pings: AtomicU64,
    pub heartbeat_pongs: AtomicU64,
    pub missed_heartbeats: AtomicU32,
    pub last_activity_epoch_ms: AtomicU64,
    pub avg_rtt_ms: AtomicU32,
    pub quality: AtomicU32,
}

impl Default for AtomicStats {
    fn default() -> Self {
        Self {
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            heartbeat_pings: AtomicU64::new(0),
            heartbeat_pongs: AtomicU64::new(0),
            missed_heartbeats: AtomicU32::new(0),
            last_activity_epoch_ms: AtomicU64::new(current_epoch_ms()),
            avg_rtt_ms: AtomicU32::new(0),
            quality: AtomicU32::new(100),
        }
    }
}

impl AtomicStats {
    /// 创建新的统计实例
    pub fn new() -> Self {
        Self::default()
    }

    /// 消息发送计数 +1
    #[inline]
    pub fn inc_messages_sent(&self) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// 消息接收计数 +1
    #[inline]
    pub fn inc_messages_received(&self) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
    }

    /// 增加发送字节数
    #[inline]
    pub fn add_bytes_sent(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    /// 增加接收字节数
    #[inline]
    pub fn add_bytes_received(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// 心跳 Ping 计数 +1
    #[inline]
    pub fn inc_heartbeat_pings(&self) {
        self.heartbeat_pings.fetch_add(1, Ordering::Relaxed);
    }

    /// 心跳 Pong 计数 +1
    #[inline]
    pub fn inc_heartbeat_pongs(&self) {
        self.heartbeat_pongs.fetch_add(1, Ordering::Relaxed);
    }

    /// 更新最后活动时间
    #[inline]
    pub fn update_last_activity(&self) {
        self.last_activity_epoch_ms.store(current_epoch_ms(), Ordering::Relaxed);
    }

    /// 更新平均 RTT
    #[inline]
    pub fn update_avg_rtt(&self, rtt_ms: u32) {
        self.avg_rtt_ms.store(rtt_ms, Ordering::Relaxed);
    }

    /// 更新质量评分
    #[inline]
    pub fn update_quality(&self, quality: u8) {
        self.quality.store(quality as u32, Ordering::Relaxed);
    }

    /// 重置 missed heartbeats
    #[inline]
    pub fn reset_missed_heartbeats(&self) {
        self.missed_heartbeats.store(0, Ordering::Relaxed);
    }

    /// missed heartbeats +1
    #[inline]
    pub fn inc_missed_heartbeats(&self) {
        self.missed_heartbeats.fetch_add(1, Ordering::Relaxed);
    }

    /// 获取当前统计快照（用于监控/日志）
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            heartbeat_pings: self.heartbeat_pings.load(Ordering::Relaxed),
            heartbeat_pongs: self.heartbeat_pongs.load(Ordering::Relaxed),
            missed_heartbeats: self.missed_heartbeats.load(Ordering::Relaxed),
            last_activity_epoch_ms: self.last_activity_epoch_ms.load(Ordering::Relaxed),
            avg_rtt_ms: self.avg_rtt_ms.load(Ordering::Relaxed),
            quality: self.quality.load(Ordering::Relaxed) as u8,
        }
    }

    /// 获取 last_activity_epoch_ms
    #[inline]
    pub fn get_last_activity(&self) -> u64 {
        self.last_activity_epoch_ms.load(Ordering::Relaxed)
    }

    /// 获取 missed_heartbeats
    #[inline]
    pub fn get_missed_heartbeats(&self) -> u32 {
        self.missed_heartbeats.load(Ordering::Relaxed)
    }

    /// 获取 avg_rtt_ms
    #[inline]
    pub fn get_avg_rtt(&self) -> u32 {
        self.avg_rtt_ms.load(Ordering::Relaxed)
    }

    /// 获取 quality
    #[inline]
    pub fn get_quality(&self) -> u8 {
        self.quality.load(Ordering::Relaxed) as u8
    }
}

/// 统计快照（普通结构体，用于序列化/显示）
#[derive(Debug, Clone, Default)]
pub struct StatsSnapshot {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub heartbeat_pings: u64,
    pub heartbeat_pongs: u64,
    pub missed_heartbeats: u32,
    pub last_activity_epoch_ms: u64,
    pub avg_rtt_ms: u32,
    pub quality: u8,
}

impl StatsSnapshot {
    /// 计算消息发送速率（条/秒）
    pub fn send_rate(&self, duration_secs: f64) -> f64 {
        if duration_secs > 0.0 {
            self.messages_sent as f64 / duration_secs
        } else {
            0.0
        }
    }

    /// 计算消息接收速率（条/秒）
    pub fn receive_rate(&self, duration_secs: f64) -> f64 {
        if duration_secs > 0.0 {
            self.messages_received as f64 / duration_secs
        } else {
            0.0
        }
    }

    /// 计算带宽（bytes/秒）
    pub fn bandwidth(&self, duration_secs: f64) -> (f64, f64) {
        if duration_secs > 0.0 {
            let send_bps = self.bytes_sent as f64 / duration_secs;
            let recv_bps = self.bytes_received as f64 / duration_secs;
            (send_bps, recv_bps)
        } else {
            (0.0, 0.0)
        }
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
    fn test_atomic_stats() {
        let stats = AtomicStats::new();
        
        // 测试原子操作
        stats.inc_messages_sent();
        stats.inc_messages_sent();
        stats.add_bytes_sent(100);
        
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.messages_sent, 2);
        assert_eq!(snapshot.bytes_sent, 100);
    }

    #[test]
    fn test_concurrent_updates() {
        use std::thread;
        
        let stats = Arc::new(AtomicStats::new());
        let mut handles = vec![];
        
        // 10 个线程并发更新
        for _ in 0..10 {
            let stats_clone = Arc::clone(&stats);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    stats_clone.inc_messages_sent();
                }
            }));
        }
        
        for h in handles {
            h.join().unwrap();
        }
        
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.messages_sent, 10000);
    }
}
