//! # flare-core - 高性能长连接框架
//!
//! flare-core 是一个专为 IM 场景设计的高性能长连接框架，
//! 支持 WebSocket 和 QUIC 协议，具备千万级并发连接能力。
//!
//! ## 核心特性
//!
//! - ✅ **多协议支持**: WebSocket, QUIC
//! - ✅ **高性能**: 原子操作统计，无锁设计
//! - ✅ **流量控制**: 令牌桶限流 + 背压控制
//! - ✅ **自动重连**: 智能重连策略
//! - ✅ **协议竞速**: 同时尝试多个协议
//! - ✅ **质量监控**: 实时连接质量评分
//!
//! ## 架构设计
//!
//! ```text
//! ┌─────────────────────────┐
//! │        flare-core        │ (门面层)
//! └─────────┬───────────────┘
//!           │
//!   ┌───────┼────────────┐
//!   │        │              │
//! ┌─┴──────┐ ┌─┴──────┐ ┌─┴──────┐
//! │ common  │ │ client │ │ server │
//! └─────────┘ └────┬───┘ └───┬────┘
//!     │          │          │
//!     │    ┌────┴──────┴─────┐
//!     └────► depends on common ◄────┘
//!          └─────────────────┘
//! ```
//!
//! ## 模块说明
//!
//! ### `common` - 通用抽象与工具
//!
//! 提供跨 client/server 的抽象接口和工具类：
//! - `connections`: 连接抽象 (traits, config, factory)
//! - `protocol`: 协议处理 (frame, commands)
//! - `serialization`: 序列化
//! - `error`: 统一错误
//!
//! ### `client` - 客户端实现
//!
//! 客户端连接实现和特有逻辑：
//! - `connections`: WebSocket/QUIC 客户端
//! - `protocol_racer`: 协议竞速
//! - `reconnect`: 自动重连
//! - `auth`: 认证
//!
//! ### `server` - 服务端实现
//!
//! 服务端监听与连接管理：
//! - `connections`: WebSocket/QUIC 服务端连接
//! - `listener`: 监听器
//! - `manager`: 连接管理器
//! - `config`: 服务端配置
//!
//! ## 快速开始
//!
//! ### 客户端示例
//!
//! ```rust,no_run
//! use flare_core::common::connections::config::ConnectionConfig;
//! use flare_core::common::connections::enums::Transport;
//! use flare_core::common::connections::factory::ConnectionFactory;
//! use flare_core::common::connections::traits::ClientConnection;
//!
//! # fn main() -> Result<(), flare_core::FlareError> {
//! // 创建配置
//! let mut config = ConnectionConfig::default();
//! config.transport = Transport::WebSocket;
//! config.remote_addr = Some("ws://localhost:8080".to_string());
//!
//! // 创建客户端
//! let client = ConnectionFactory::create_client(config)?;
//!
//! // 连接
//! client.connect()?;
//!
//! // 发送消息
//! use flare_core::common::protocol::factory::FrameFactory;
//! use flare_core::common::protocol::reliability::Reliability;
//! let frame = FrameFactory::create_data_frame(
//!     FrameFactory::generate_message_id(),
//!     b"Hello, World!".to_vec(),
//!     Reliability::BestEffort,
//! )?;
//! client.send_message(frame)?;
//! # Ok(())
//! # }
//! ```
//!
//! ### 服务端示例
//!
//! ```rust,no_run
//! use flare_core::server::config::ServerConfig;
//! use flare_core::server::websocket::WebSocketServer;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // 创建服务端配置
//! let config = ServerConfig::default();
//!
//! // 创建服务器
//! let server = WebSocketServer::new(config);
//!
//! // 启动监听
//! server.start().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## 性能指标
//!
//! - **统计性能**: 4200万+ ops/秒 (原子操作)
//! - **限流性能**: 3200万+ ops/秒 (令牌桶)
//! - **并发连接**: 支持千万级连接
//! - **内存占用**: ~16KB/连接
//!
//! ## 文档链接
//!
//! - [架构设计](../docs/IM_Long_Connection_Design.md)
//! - [重构方案](../docs/ARCHITECTURE_REFACTORING_PLAN.md)
//! - [性能优化](../docs/PRODUCTION_OPTIMIZATION_PLAN.md)

pub mod common;
pub mod client;
pub mod server;

// 导出常用类型以简化使用

// 连接相关
pub use common::connections::traits::{ClientConnection, ServerConnection, ConnectionEvent};
pub use common::connections::config::{ConnectionConfig, QuicClientConfig, QuicServerConfig, ProtocolConfig, WebSocketConfig, QuicConfig};
pub use common::connections::enums::{Transport, ConnectionState};
pub use common::connections::types::ConnectionStats;
pub use common::error::FlareError;
pub use common::connections::factory::ConnectionFactory;

// 协议相关
pub use common::protocol::frame::Frame;
pub use common::protocol::reliability::Reliability;
pub use common::protocol::commands::*;
pub use common::protocol::factory::FrameFactory;

// 序列化相关（已迁移到 parsing 模块）
// 请使用: use flare_core::common::parsing::PayloadCodec;
pub use common::parsing::PayloadCodec;
