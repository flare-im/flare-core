//! 心跳功能测试
//!
//! 测试连接管理器的心跳检测功能

use flare_core::server::{ConnectionManager, HeartbeatConfig};
use std::time::Duration;

#[tokio::test]
async fn test_heartbeat_functionality() {
    // 创建带快速心跳配置的连接管理器
    let config = HeartbeatConfig {
        check_interval: Duration::from_millis(100),
        connection_timeout: Duration::from_millis(300),
        enable_auto_cleanup: true,
    };
    
    let manager = ConnectionManager::with_heartbeat_config(config);
    
    // 启动心跳任务
    manager.start_heartbeat_task().await;
    
    // 验证心跳任务正在运行
    assert!(manager.is_heartbeat_running().await);
    
    println!("心跳功能测试通过！");
}