//! 客户端心跳模块
//!
//! 提供客户端心跳功能：
//! - 主动发送 PING 消息
//! - 检测 PONG 响应超时
//! - 在超时时自动断开连接

pub mod manager;

pub use manager::HeartbeatManager;
