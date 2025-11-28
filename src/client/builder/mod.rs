//! 客户端构建器模块
//! 
//! 提供三种模式来创建和配置客户端：
//! 1. 简单模式：使用闭包定义消息处理逻辑（毛坯房）
//! 2. 观察者模式：使用实现了 ConnectionObserver trait 的观察者（基本装修）
//! 3. Flare 模式：使用 MessagePipeline 提供统一的消息处理流程（精装修）
//! 
//! 所有构建器都基于 HybridClient，提供统一的客户端访问接口

pub mod base;
pub mod common;
pub mod observer;
pub mod simple;
pub mod flare;

// 重新导出基类和通用组件
pub use base::BaseClientBuilderConfig;
pub use common::ClientWrapper;

// 重新导出简单模式
pub use simple::{ClientBuilder, SimpleClient};

// 重新导出观察者模式
pub use observer::{ObserverClientBuilder, ObserverClient};

// 重新导出 Flare 客户端构建器
pub use flare::{FlareClientBuilder, FlareClient, MessageListener};

