/// 连接监控与心跳相关的辅助方法

/// 根据平均RTT与连续心跳超时次数计算质量评分（0-100）
pub fn compute_quality(avg_rtt_ms: Option<u32>, missed_heartbeats: u32) -> u8 {
    let base = 100u8;
    let rtt_penalty = avg_rtt_ms.map(|rtt| (rtt / 50).min(50) as u8).unwrap_or(0); // 每50ms扣1分，最多50分
    let miss_penalty = (missed_heartbeats.min(10) * 5) as u8; // 每次超时扣5分，最多10次
    base.saturating_sub(rtt_penalty).saturating_sub(miss_penalty)
}

/// 判断是否心跳超时（基于上次活动时间戳与超时时间）
pub fn is_heartbeat_timeout(last_activity_epoch_ms: u64, now_epoch_ms: u64, timeout_ms: u64) -> bool {
    now_epoch_ms.saturating_sub(last_activity_epoch_ms) > timeout_ms
}
