//! 服务端连接管理模块
//! 
//! 提供服务端连接管理的完整功能，包括：
//! - 连接管理器 trait：定义连接管理的标准接口
//! - 默认实现：基于内存的连接管理器
//! - 连接信息：连接元数据和统计信息

pub mod manager;
pub mod negotiation;
pub mod r#trait;
pub mod device_handler;

// 重新导出常用类型
pub use manager::{ConnectionManager, ConnectionInfo};
pub use r#trait::{ConnectionManagerTrait, ConnectionInfo as TraitConnectionInfo, ConnectionStats};
pub use device_handler::{handle_device_conflict, DeviceConflictResult};
