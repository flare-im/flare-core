//! 核心协议定义 - 专注于长连接可靠性
//! Author: Flare Core Team
//! Description: Core protocol definitions for reliable long-connection communication
//! This module contains both Serde-based and Protobuf-based message structures
//! for maximum compatibility and performance.

mod frame;
pub mod commands;
pub mod factory;
mod converter;

use serde::{Deserialize, Serialize};

// 引入Protobuf生成的代码
mod flare_proto {
    pub mod core {
        include!("flare.core.rs");
    }
    // Include the generated commands module within flare
    pub mod commands {
        include!("flare.core.commands.rs");
    }
}

// 重新导出Protobuf生成的结构和枚举
pub use flare_proto::core::{Frame as ProtobufFrame, Reliability as ProtobufReliability};
pub use frame::{Frame, Reliability};
pub use converter::{ProtocolConverter, FrameConverter};



/// 协议选择模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProtocolSelection {
    /// 仅使用 QUIC
    QuicOnly,
    /// 仅使用 WebSocket
    WebSocketOnly,
    /// 自动选择（协议竞速）
    Auto,
}

impl Default for ProtocolSelection {
    fn default() -> Self {
        ProtocolSelection::Auto
    }
}

/// 连接质量指标
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionQuality {
    /// 延迟（毫秒）
    pub latency_ms: u32,
    /// 抖动（毫秒）
    pub jitter_ms: u32,
    /// 丢包率（百分比）
    pub packet_loss_percent: f32,
    /// 带宽（字节/秒）
    pub bandwidth_bps: u64,
    /// 稳定性评分（0-100）
    pub stability_score: u8,
}

impl Default for ConnectionQuality {
    fn default() -> Self {
        Self {
            latency_ms: 0,
            jitter_ms: 0,
            packet_loss_percent: 0.0,
            bandwidth_bps: 0,
            stability_score: 100,
        }
    }
}

/// 协议测试结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolTestResult {
    /// 协议类型
    pub protocol: ProtocolSelection,
    /// 连接质量
    pub quality: ConnectionQuality,
    /// 连接时间（毫秒）
    pub connection_time_ms: u32,
    /// 是否成功
    pub success: bool,
}

impl ProtocolTestResult {
    /// 计算综合评分
    pub fn calculate_score(&self) -> f32 {
        if !self.success {
            return 0.0;
        }

        let latency_score = (1000.0 / (self.quality.latency_ms as f32 + 1.0)).min(100.0);
        let stability_score = self.quality.stability_score as f32;
        let connection_score = (1000.0 / (self.connection_time_ms as f32 + 1.0)).min(100.0);

        latency_score * 0.4 + stability_score * 0.4 + connection_score * 0.2
    }
}