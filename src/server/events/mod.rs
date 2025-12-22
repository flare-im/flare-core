//! 服务端事件处理模块
//!
//! 提供细化的服务端事件处理接口，支持按命令类型处理

pub mod factory;
pub mod handler;
pub mod observer;
// pub mod observer_back;
pub mod event_wrapper;

pub use factory::{
    ChainedObserverFactory, DefaultServerMessageObserverFactory, ServerMessageObserverFactory,
};
pub use handler::ServerEventHandler;
// pub use observer_back::DefaultServerMessageObserver;
pub use event_wrapper::ServerMessageWrapper;
