//! 客户端构建器模块
//!
//! Native: 简单 / 观察者 / Flare 三种模式
//! WASM: 简单 + Flare（WebSocket-only）

pub mod base;
pub mod common;
pub mod flare;
pub mod simple;

#[cfg(not(target_arch = "wasm32"))]
pub mod observer;

pub use base::BaseClientBuilderConfig;
pub use common::ClientWrapper;
pub use flare::{FlareClient, FlareClientBuilder, MessageListener};
pub use simple::{ClientBuilder, SimpleClient};

#[cfg(not(target_arch = "wasm32"))]
pub use observer::{ObserverClient, ObserverClientBuilder};
