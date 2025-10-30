use std::sync::Arc;
use std::time::Duration;
use crate::common::error::FlareError;
use crate::common::connections::traits::{ClientConnection, ConnectionEvent};
use tracing::{debug, warn};

/// 自动重连管理（客户端专有逻辑）
pub struct ReconnectManager {
    pub enabled: bool,
    pub max_attempts: u32,
    pub delay_ms: u64,
}

impl Default for ReconnectManager {
    fn default() -> Self {
        Self { enabled: false, max_attempts: 3, delay_ms: 3000 }
    }
}

impl ReconnectManager {
    pub fn new(max_attempts: u32, delay_ms: u64) -> Self {
        Self { 
            enabled: true, 
            max_attempts, 
            delay_ms 
        }
    }
    
    pub async fn try_reconnect(
        &self,
        conn: Arc<dyn ClientConnection>,
        event: Option<Arc<dyn ConnectionEvent>>,
    ) -> Result<(), FlareError> {
        if !self.enabled { return Ok(()); }
        if let Some(h) = &event { h.on_reconnect_started(); }
        let mut attempt: u32 = 0;
        while attempt < self.max_attempts {
            attempt += 1;
            debug!("尝试重连第 {} 次", attempt);
            match conn.connect() {
                Ok(_) => {
                    debug!("重连成功");
                    if let Some(h) = &event { h.on_reconnected(); }
                    return Ok(());
                }
                Err(e) => {
                    warn!("重连失败: {:?}", e);
                    if let Some(h) = &event { h.on_reconnect_failed(e.clone()); }
                    if attempt < self.max_attempts {
                        debug!("等待 {} 毫秒后重试", self.delay_ms);
                        tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
                    }
                }
            }
        }
        Err(FlareError::connection_failed("自动重连失败，达到最大尝试次数"))
    }
}