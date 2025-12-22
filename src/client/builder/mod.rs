//! 客户端构建器模块
//!
//! 提供统一的三种构建模式，从简单到复杂，满足不同场景需求：
//!
//! ## 三种模式
//!
//! 1. **简单模式（ClientBuilder）** - 使用闭包定义消息处理逻辑，最小实现，适合快速原型开发
//! 2. **观察者模式（ObserverClientBuilder）** - 使用 `ConnectionObserver` trait，基本功能实现
//! 3. **Flare 模式（FlareClientBuilder）** - 使用 `MessageListener` trait，完整功能实现，推荐用于生产环境
//!
//! ## 架构设计原则
//!
//! - **公共逻辑统一处理**：所有模式共享底层实现（`HybridClient`），避免代码重复
//! - **渐进式增强**：从简单到复杂，按需选择，无需为兼容性保留冗余代码
//! - **类型安全**：充分利用 Rust 类型系统，编译期保证正确性
//! - **零成本抽象**：高级抽象不带来运行时开销
//!
//! 所有构建器都基于 `HybridClient`，提供统一的客户端访问接口

pub mod base;
pub mod common;
pub mod flare;
pub mod observer;
pub mod simple;

// 重新导出基类和通用组件
pub use base::BaseClientBuilderConfig;
pub use common::ClientWrapper;

// 重新导出简单模式
pub use simple::{ClientBuilder, SimpleClient};

// 重新导出观察者模式
pub use observer::{ObserverClient, ObserverClientBuilder};

// 重新导出 Flare 客户端构建器
pub use flare::{FlareClient, FlareClientBuilder, MessageListener};
