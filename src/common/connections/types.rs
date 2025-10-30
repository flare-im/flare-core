#[derive(Debug, Default, Clone)]
pub struct ConnectionStats {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub avg_rtt_ms: Option<u32>,
    pub quality: Option<u8>,
    // 新增：心跳与生命周期统计
    pub heartbeat_pings: u64,
    pub heartbeat_pongs: u64,
    pub missed_heartbeats: u32,
    pub established_epoch_ms: u64,
    pub last_activity_epoch_ms: u64,
}
