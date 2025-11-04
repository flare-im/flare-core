//! 服务端心跳模块
//! 
//! 提供服务端心跳检测功能：
//! - 定期检查连接的最后活跃时间
//! - 自动清理超时的连接
//! - 在收到消息或 PING 时更新连接活跃时间

pub mod detector;

pub use detector::HeartbeatDetector;
