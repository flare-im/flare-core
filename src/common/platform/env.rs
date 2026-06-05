//! 跨平台环境与实例标识

use crate::common::device::{DeviceInfo, DevicePlatform};

/// 运行时实例 ID（Native：进程 ID；WASM：随机数）
#[cfg(not(target_arch = "wasm32"))]
pub fn runtime_instance_id() -> String {
    format!("native-{}", std::process::id())
}

#[cfg(target_arch = "wasm32")]
pub fn runtime_instance_id() -> String {
    format!("wasm-{:016x}", rand::random::<u64>())
}

/// 读取环境变量（WASM 浏览器中恒为 `None`）
#[cfg(not(target_arch = "wasm32"))]
pub fn optional_env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

#[cfg(target_arch = "wasm32")]
pub fn optional_env(_key: &str) -> Option<String> {
    None
}

/// 本地开发默认 WebSocket 地址（`flare_chat_server` / `simple_server` 均为 8080）
pub fn default_local_ws_url() -> &'static str {
    "ws://127.0.0.1:8080"
}

/// 构建浏览器/WASM 客户端设备信息（平台 `Web`，用于 `flare_chat_server` 设备管理）
pub fn web_device_info(user_id: &str) -> DeviceInfo {
    let instance = runtime_instance_id();
    DeviceInfo::new(format!("web-{instance}"), DevicePlatform::Web)
        .with_model("Browser".to_string())
        .with_app_version(env!("CARGO_PKG_VERSION").to_string())
        .with_system_version(format!("flare-core wasm ({user_id})"))
}
