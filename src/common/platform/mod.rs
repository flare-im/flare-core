//! 跨平台工具（Native / WASM 差异封装）
//!
//! 将浏览器与 Native 在时钟、环境、实例标识等方面的差异集中在此，
//! 业务代码优先使用本模块 API，避免在业务层散落 `cfg(target_arch = "wasm32")`。

pub mod async_time;
pub mod encryption;
pub mod env;
pub mod time;

pub use async_time::*;
pub use encryption::*;
pub use env::*;
pub use time::*;
