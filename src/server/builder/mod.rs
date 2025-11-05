//! 服务端构建器模块
//! 
//! 提供两种模式来创建和配置服务端：
//! 1. 观察者模式：使用实现了 ConnectionHandler trait 的处理器
//! 2. 简单模式：使用闭包定义消息处理逻辑

pub mod observer;
pub mod simple;

// 重新导出观察者模式
pub use observer::{ObserverServerBuilder, ObserverServer};

// 重新导出简单模式
pub use simple::{ServerBuilder, SimpleServer, MessageContext};

