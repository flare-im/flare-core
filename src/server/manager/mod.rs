//! 服务端连接管理器模块
//!
//! 提供多种连接管理策略实现
//!
//! # 模块结构
//!
//! - [traits](traits/index.html): 连接管理器接口定义
//! - [connection_based](connection_based/index.html): 基于连接的管理器实现
//! - [user_based](user_based/index.html): 基于用户的管理器实现
//! - [heartbeat_manager](heartbeat_manager/index.html): 心跳管理器实现
//! - [message_handler](message_handler/index.html): 消息处理器实现

pub mod traits;
pub mod connection_based;
pub mod user_based;
pub mod heartbeat_manager;
pub mod message_handler;

pub use connection_based::ConnectionBasedManager;
pub use user_based::UserBasedManager;
pub use heartbeat_manager::HeartbeatManager;