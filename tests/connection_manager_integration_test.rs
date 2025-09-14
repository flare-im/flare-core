//! 连接管理器集成测试
//!
//! 测试连接管理器的开箱即用功能和健壮性

use flare_core::server::{ConnectionManager, HeartbeatConfig};
use std::time::Duration;

#[tokio::test]
async fn test_connection_manager_creation() {
    // 创建连接管理器
    let manager = ConnectionManager::new();
    
    // 验证默认配置
    let config = manager.get_heartbeat_config();
    assert_eq!(config.check_interval, Duration::from_secs(10));
    assert_eq!(config.connection_timeout, Duration::from_secs(30));
    assert_eq!(config.enable_auto_cleanup, true);
    
    println!("连接管理器创建测试通过！");
}

#[tokio::test]
async fn test_connection_manager_with_custom_config() {
    // 创建带自定义配置的连接管理器
    let config = HeartbeatConfig {
        check_interval: Duration::from_millis(500),
        connection_timeout: Duration::from_millis(1500),
        enable_auto_cleanup: false, // 禁用自动清理以避免任务创建
    };
    
    let manager = ConnectionManager::with_heartbeat_config(config.clone());
    
    // 验证自定义配置
    let manager_config = manager.get_heartbeat_config();
    assert_eq!(manager_config.check_interval, Duration::from_millis(500));
    assert_eq!(manager_config.connection_timeout, Duration::from_millis(1500));
    assert_eq!(manager_config.enable_auto_cleanup, false);
    
    println!("连接管理器自定义配置测试通过！");
}