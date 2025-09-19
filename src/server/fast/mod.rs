//! 服务代理模块
//!
//! 提供统一的服务代理接口，简化服务端开发

pub mod server;
pub mod message_handler;
pub mod event_handler;
pub mod message_sender;
pub mod auth;

/// 重新导出常用的类型
pub use server::{FastServer, ServerStats};
pub use message_handler::{MessageHandler, ConnectionEventType};
pub use message_sender::MessageSender;
pub use auth::{AuthProvider, DefaultAuthProvider};