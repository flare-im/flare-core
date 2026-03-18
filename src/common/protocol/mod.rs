//! Flare 协议模块
//!
//! 包含 protobuf 生成的消息定义和快速构建工具

// 生成的文件需要通过 prost-build 生成
// 文件结构：
// - flare.core.rs: Frame 和 Reliability 定义
// - flare.core.commands.rs: Command 及其子类型定义

// 手动组织模块结构
// 注意：prost 会根据 package 名称生成模块，但我们需要直接包含生成的文件
pub mod flare {
    pub mod core {
        // 先定义 commands 模块（Frame 需要引用它）
        pub mod commands {
            include!("flare.core.commands.rs");
        }

        // Frame 和 Reliability - 直接从生成的文件中包含
        // build.rs 已经修复了 commands 的引用为 super::commands
        pub mod flare_core {
            include!("flare.core.rs");
        }

        pub use flare_core::{Frame, Reliability};

        // 重新导出常用类型
        pub use commands::{
            Command, CustomCommand, NotificationCommand, PayloadCommand, SystemCommand,
        };
    }
}

// 快速构建方法
pub mod builder;

// 序列化示例（仅用于测试和文档）
#[cfg(test)]
pub mod serde_example;

// 重新导出常用类型和构建器
pub use builder::*;
pub use flare::core::commands::system_command::SerializationFormat;
pub use flare::core::commands::*;
pub use flare::core::{Frame, Reliability};
