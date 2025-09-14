//! 连接管理器测试
//!
//! 测试连接管理器的基本功能和心跳集成

use flare_core::server::{
    ConnectionManager,
    HeartbeatConfig,
};
use std::time::Duration;

#[test]
fn test_connection_manager_creation() {
    // 创建禁用自动清理的配置以避免栈溢出
    let config = HeartbeatConfig {
        check_interval: Duration::from_secs(10),
        connection_timeout: Duration::from_secs(30),
        enable_auto_cleanup: false, // 禁用自动清理以避免任务创建
    };
    
    let manager = ConnectionManager::with_heartbeat_config(config);
    
    // 验证默认配置
    let config = manager.get_heartbeat_config();
    assert_eq!(config.check_interval, Duration::from_secs(10));
    assert_eq!(config.connection_timeout, Duration::from_secs(30));
    assert_eq!(config.enable_auto_cleanup, false);
    
    println!("连接管理器创建测试通过！");
}

#[test]
fn test_heartbeat_config_creation() {
    let config = HeartbeatConfig::default();
    
    assert_eq!(config.check_interval, Duration::from_secs(10));
    assert_eq!(config.connection_timeout, Duration::from_secs(30));
    assert_eq!(config.enable_auto_cleanup, true);
    
    println!("心跳配置创建测试通过！");
}

#[test]
fn test_custom_heartbeat_config() {
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
    
    println!("自定义心跳配置测试通过！");
}