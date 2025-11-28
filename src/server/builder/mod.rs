//! 服务端构建器模块
//! 
//! 提供三种模式来创建和配置服务端：
//! 1. 观察者模式：使用实现了 ConnectionHandler trait 的处理器
//! 2. 简单模式：使用闭包定义消息处理逻辑
//! 3. Flare 模式：使用 MessagePipeline 提供统一的消息处理流程
//! 
//! 所有构建器都基于 HybridServer，提供统一的 ServerHandle 访问接口

pub mod base;
pub mod common;
pub mod observer;
pub mod simple;
pub mod flare;

// 重新导出基类和通用组件
pub use base::BaseServerBuilderConfig;
pub use common::ServerWrapper;

// 重新导出观察者模式
pub use observer::{ObserverServerBuilder, ObserverServer};

// 重新导出简单模式
pub use simple::{ServerBuilder, SimpleServer, MessageContext};

// 重新导出 Flare 服务端构建器
pub use flare::{FlareServerBuilder, FlareServer, MessageListener};

