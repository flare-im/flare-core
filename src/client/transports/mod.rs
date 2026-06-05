//! 客户端传输协议模块
//!
//! 提供多种传输协议的客户端实现：
//! - QUIC：基于 QUIC 协议的客户端（native）
//! - WebSocket：基于 WebSocket 协议的客户端
//! - Hybrid：混合客户端，支持多种协议（native）

use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::transport::events::ArcObserver;
use async_trait::async_trait;

/// 客户端标准接口
#[async_trait]
pub trait Client: Send + Sync {
    async fn connect(&mut self) -> Result<()>;
    async fn disconnect(&mut self) -> Result<()>;
    async fn send_frame(&mut self, frame: &Frame) -> Result<()>;
    fn is_connected(&self) -> bool;
    fn add_observer(&mut self, observer: ArcObserver);
    fn remove_observer(&mut self, observer: ArcObserver);
    fn connection_id(&self) -> Option<String> {
        None
    }
    fn set_disconnect_requested(&mut self, _value: bool) {}
}

pub mod client_core;
mod common;
#[cfg(feature = "websocket")]
pub mod websocket;

#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "websocket", feature = "quic", feature = "tcp")
))]
pub mod hybrid;
#[cfg(all(not(target_arch = "wasm32"), feature = "quic"))]
pub mod quic;
#[cfg(all(not(target_arch = "wasm32"), feature = "tcp"))]
pub mod tcp;

pub use client_core::ClientCore;
#[cfg(feature = "websocket")]
pub use websocket::WebSocketClient;

#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "websocket", feature = "quic", feature = "tcp")
))]
pub use hybrid::HybridClient;
#[cfg(all(not(target_arch = "wasm32"), feature = "quic"))]
pub use quic::QUICClient;
#[cfg(all(not(target_arch = "wasm32"), feature = "tcp"))]
pub use tcp::TCPClient;
