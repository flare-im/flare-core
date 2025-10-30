//! Protocol 模块 - 手写定义（当前使用）
//!
//! 包含 Frame、Command、Reliability 等核心协议定义

// 手写定义（当前使用）
pub mod reliability;
pub mod frame;
pub mod commands;
pub mod factory;

// Protobuf 生成的代码（备用，未来迁移使用）
#[path = "flare.core.rs"]
pub mod flare_core;

#[path = "flare.core.commands.rs"]
pub mod flare_core_commands;

// 重新导出常用类型
pub use frame::Frame;
pub use commands::Command;
pub use reliability::Reliability;
