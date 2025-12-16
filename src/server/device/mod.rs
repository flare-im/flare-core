//! 服务端设备管理模块
//!
//! 提供设备冲突检测、设备在线管理等功能

pub mod manager;
pub mod strategy;

pub use manager::DeviceManager;
pub use strategy::{DeviceConflictStrategy, DeviceConflictStrategyBuilder};
