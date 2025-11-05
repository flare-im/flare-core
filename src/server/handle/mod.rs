//! 服务器操作处理器模块
//! 
//! 提供轻量级的服务器操作接口，用于消息发送和连接管理
//! 避免在需要发送消息的地方注入整个 Server 实例

mod default;

pub use default::DefaultServerHandle;
pub use default::ServerHandle;
