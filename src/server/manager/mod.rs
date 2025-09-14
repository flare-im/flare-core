//! 服务端连接管理器模块
//!
//! 提供多种连接管理策略实现
//!
//! # 模块结构
//!
//! - [traits](traits/index.html): 连接管理器接口定义
//! - [connection_manager](connection_manager/index.html): 简单连接管理器实现
//! - [user_connection_manager](user_connection_manager/index.html): 用户连接管理器实现
//! - [message_handler](message_handler/index.html): 消息处理器实现

pub mod traits;
pub mod connection_manager;
pub mod user_connection_manager;
pub mod message_handler;

// 重新导出常用的类型，方便外部使用
pub use connection_manager::ConnectionManager;
pub use connection_manager::HeartbeatConfig;
pub use user_connection_manager::UserConnectionManager;
pub use crate::common::connections::enums::Platform;