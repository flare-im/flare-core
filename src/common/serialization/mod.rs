//! 序列化模块
//!
//! **注意**：此模块已简化，核心序列化功能已迁移到 `parsing::PayloadCodec`
//!
//! 保留此模块仅为：
//! 1. 提供基础的 Serializer trait（用于特殊扩展场景）
//! 2. 具体实现（json.rs, protobuf.rs）供内部使用
//!
//! **推荐使用**：直接使用 `crate::common::parsing::PayloadCodec`

pub mod traits;
pub mod json;
pub mod protobuf;
