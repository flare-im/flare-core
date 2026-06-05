//! 跨平台时间工具
//!
//! | API | Native | WASM |
//! |-----|--------|------|
//! | `MonotonicInstant` | `std::time::Instant` | `web_time::Instant` |
//! | `wall_clock_ms` | `SystemTime` | `js_sys::Date::now()` |

use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
pub use std::time::Instant as MonotonicInstant;
#[cfg(target_arch = "wasm32")]
pub use web_time::Instant as MonotonicInstant;

#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};

/// 单调时钟起点（心跳、超时、RTT 等应使用此 API，而非墙钟）
pub fn monotonic_now() -> MonotonicInstant {
    MonotonicInstant::now()
}

/// 墙钟毫秒时间戳（Unix epoch）
#[cfg(not(target_arch = "wasm32"))]
pub fn wall_clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(target_arch = "wasm32")]
pub fn wall_clock_ms() -> u64 {
    js_sys::Date::now() as u64
}

/// 墙钟秒时间戳（Unix epoch）
pub fn wall_clock_secs() -> u64 {
    wall_clock_ms() / 1000
}

/// 自 `start` 起经过的毫秒数
pub fn elapsed_ms(start: MonotonicInstant) -> u64 {
    start.elapsed().as_millis() as u64
}

/// 自 `start` 起是否已超过 `timeout`
pub fn is_elapsed(start: MonotonicInstant, timeout: Duration) -> bool {
    start.elapsed() >= timeout
}

/// 当前 UTC 时间（RFC3339，用于日志/调试）
pub fn format_now_rfc3339() -> String {
    #[cfg(not(target_arch = "wasm32"))]
    {
        chrono::Utc::now().to_rfc3339()
    }
    #[cfg(target_arch = "wasm32")]
    {
        js_sys::Date::new_0().to_iso_string().into()
    }
}
