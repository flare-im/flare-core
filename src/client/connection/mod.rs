//! 客户端连接管理模块
//!
//! 提供客户端连接相关的功能，包括：
//! - 连接状态管理：跟踪单个连接的状态变化

pub mod state;

// 重新导出常用类型
pub use state::{ConnectionState, ConnectionStateManager};
