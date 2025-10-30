//! 服务端 trait 定义

use crate::common::error::FlareError;
use crate::server::manager::traits::ConnectionManager;
use std::sync::Arc;

/// 协议服务 trait
/// 
/// 定义了所有协议服务需要实现的接口，包括启动和停止方法
#[async_trait::async_trait]
pub trait ProtocolService: Send + Sync {
    /// 启动服务
    /// 
    /// # 参数
    /// * `connection_manager` - 连接管理器
    /// 
    /// # 返回值
    /// 启动结果
    async fn start(&self, connection_manager: Arc<dyn ConnectionManager>) -> Result<(), FlareError>;
    
    /// 停止服务
    /// 
    /// # 返回值
    /// 停止结果
    async fn stop(&self) -> Result<(), FlareError>;
    
    /// 获取服务名称
    /// 
    /// # 返回值
    /// 服务名称
    fn name(&self) -> &str;
}