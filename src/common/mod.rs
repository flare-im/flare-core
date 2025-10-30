//! 通用模块 - 提供跨 client/server 的抽象与工具
//!
//! # 职责
//! - 定义通用接口（traits）
//! - 协议处理（Frame, Command）
//! - 序列化/反序列化
//! - 工具类（统计、限流、监控）
//! - 统一错误处理
//!
//! # 设计原则
//! - ✅ 只包含抽象，不包含具体实现
//! - ✅ client/server 模块依赖 common
//! - ❌ common 不依赖 client/server
//!
//! # 模块组织
//! - `error`: 统一错误类型
//! - `connections`: 连接抽象与工具
//!   - `traits`: ClientConnection, ServerConnection traits
//!   - `config`: 连接配置
//!   - `factory`: 连接工厂
//!   - `stats`: 高性能统计
//!   - `ratelimit`: 流量控制
//!   - `monitor`: 监控工具
//! - `protocol`: 协议处理
//!   - `frame`: 消息帧定义
//!   - `commands`: 命令类型
//!   - `reliability`: 可靠性保障
//!   - `factory`: 帧工厂
//! - `serialization`: 序列化
//! - `parsing`: 统一的消息解析器

pub mod error;           // 统一错误
pub mod connections;     // 连接抽象与工具
pub mod protocol;        // 协议处理
pub mod serialization;   // 序列化
pub mod parsing;         // 消息解析器
pub mod messaging;       // 消息处理
pub mod compression;     // 消息压缩
