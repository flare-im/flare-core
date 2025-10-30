//! 连接类型定义
//! 
//! 定义连接相关的枚举、结构体和配置

// 重新导出枚举和配置类型
pub use crate::common::connections::enums::*;
pub use crate::common::connections::config::*;

pub struct ClientInfo {
    /// 传输方式
    pub transport: Transport,
    /// 地址
    pub address: String,
    /// 平台
    pub platform: Platform,
}

impl Clone for ClientInfo {
    fn clone(&self) -> Self {
        Self {
            transport: self.transport.clone(),
            address: self.address.clone(),
            platform: self.platform.clone(),
        }
    }
}