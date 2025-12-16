//! 客户端事件处理模块
//!
//! 提供客户端事件处理接口，支持用户自定义业务逻辑

pub mod handler;
pub mod observer;

pub use handler::ClientEventHandler;
pub use observer::DefaultClientMessageObserver;
