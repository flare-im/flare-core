//! 客户端构建器模块
//! 
//! 提供两种模式来创建和配置客户端：
//! 1. 观察者模式：使用实现了 ConnectionObserver trait 的观察者
//! 2. 简单模式：使用闭包定义消息处理逻辑

pub mod observer;
pub mod simple;

// 重新导出观察者模式
pub use observer::{ObserverClientBuilder, ObserverClient};

// 重新导出简单模式
pub use simple::{ClientBuilder, SimpleClient};

