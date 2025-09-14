//! 用户连接管理器测试
//!
//! 测试用户连接管理器的核心功能

use flare_core::server::{
    ConnectionManager,
    UserConnectionManager,
    ServerConnectionManager,
};
use flare_core::common::protocol::Platform;
use flare_core::common::connections::types::{ConnectionState, ConnectionConfig};
use flare_core::common::connections::traits::{Connection, ServerConnection, ConnectionStats};
use std::sync::Arc;
use std::time::Duration;

// 创建一个模拟的服务器连接用于测试
struct MockServerConnection {
    id: String,
    user_id: Arc<tokio::sync::RwLock<Option<String>>>,
    config: ConnectionConfig,
}

#[async_trait::async_trait]
impl Connection for MockServerConnection {
    fn get_id(&self) -> &str {
        &self.id
    }
    
    async fn get_state(&self) -> ConnectionState {
        ConnectionState::Connected
    }
    
    async fn is_active(&self) -> bool {
        true
    }
    
    fn get_config(&self) -> &ConnectionConfig {
        &self.config
    }
    
    async fn get_last_activity(&self) -> std::time::Instant {
        std::time::Instant::now()
    }
    
    async fn update_last_activity(&self) {
        // 空实现
    }
    
    async fn send_heartbeat(&self) -> flare_core::common::error::Result<()> {
        Ok(())
    }
    
    async fn send_heartbeat_response(&self, _data: Option<Vec<u8>>) -> flare_core::common::error::Result<()> {
        Ok(())
    }
    
    async fn set_heartbeat_response_handler(&mut self, _handler: Option<flare_core::common::connections::traits::HeartbeatResponseHandler>) {
        // 空实现
    }
    
    async fn has_received_heartbeat(&self) -> bool {
        false
    }
    
    async fn reset_heartbeat_state(&self) {
        // 空实现
    }
    
    async fn set_connection_event_handler(&mut self, _handler: std::sync::Arc<dyn flare_core::common::connections::traits::ConnectionEvent>) {
        // 空实现
    }
    
    async fn send_error_notification(&self, _error_code: u32, _error_message: &str) -> flare_core::common::error::Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl ServerConnection for MockServerConnection {
    async fn accept(&self) -> flare_core::common::error::Result<()> {
        Ok(())
    }
    
    async fn close(&self) -> flare_core::common::error::Result<()> {
        Ok(())
    }
    
    async fn send_message(&self, _message: flare_core::common::protocol::Frame) -> flare_core::common::error::Result<()> {
        Ok(())
    }
    
    async fn receive_message(&self) -> flare_core::common::error::Result<Option<flare_core::common::protocol::Frame>> {
        Ok(None)
    }
    
    async fn is_healthy(&self) -> bool {
        true
    }
    
    fn get_client_info(&self) -> Option<String> {
        None
    }
    
    async fn get_connection_stats(&self) -> ConnectionStats {
        ConnectionStats::default()
    }
    
    async fn get_user_id(&self) -> Option<String> {
        let user_id = self.user_id.read().await;
        user_id.clone()
    }
    
    async fn set_user_id(&self, user_id: String) {
        let mut uid = self.user_id.write().await;
        *uid = Some(user_id);
    }
}

#[tokio::test]
async fn test_user_connection_manager_creation() {
    // 创建基础连接管理器
    let base_manager = Arc::new(ConnectionManager::new());
    
    // 创建用户连接管理器
    let manager = UserConnectionManager::new(base_manager);
    
    // 验证统计信息
    let stats = manager.get_stats().await;
    assert_eq!(stats.total_users, 0);
    assert_eq!(stats.active_users, 0);
    assert_eq!(stats.total_connections, 0);
    assert_eq!(stats.active_connections, 0);
    assert_eq!(stats.pending_auth_connections, 0);
    
    println!("用户连接管理器创建测试通过！");
}

#[tokio::test]
async fn test_pending_connection_management() {
    // 创建基础连接管理器
    let base_manager = Arc::new(ConnectionManager::new());
    
    // 创建用户连接管理器
    let manager = UserConnectionManager::new(base_manager);
    
    // 添加待验证连接
    let connection_id = "test_connection_1".to_string();
    assert!(manager.add_pending_connection(connection_id.clone()).await.is_ok());
    
    // 验证待验证连接数量
    assert_eq!(manager.get_pending_auth_count().await, 1);
    
    // 设置待验证连接的平台信息
    assert!(manager.set_pending_connection_platform(
        &connection_id, 
        Platform::Web, 
        Some("test_device".to_string())
    ).await.is_ok());
    
    // 验证统计信息
    let stats = manager.get_stats().await;
    assert_eq!(stats.pending_auth_connections, 1);
    
    println!("待验证连接管理测试通过！");
}

#[tokio::test]
async fn test_authentication_completion() {
    // 创建基础连接管理器
    let base_manager = Arc::new(ConnectionManager::new());
    
    // 创建用户连接管理器
    let manager = UserConnectionManager::new(base_manager.clone());
    
    // 创建模拟连接配置
    let config = ConnectionConfig::server(
        "mock_connection".to_string(),
        "127.0.0.1:8080".to_string()
    );
    
    // 创建模拟连接
    let connection_id = "test_connection_1".to_string();
    let mock_connection = Arc::new(MockServerConnection {
        id: connection_id.clone(),
        user_id: Arc::new(tokio::sync::RwLock::new(None)),
        config,
    });
    
    // 添加连接到基础管理器
    assert!(base_manager.add_connection(mock_connection.clone()).await.is_ok());
    
    // 添加待验证连接
    assert!(manager.add_pending_connection(connection_id.clone()).await.is_ok());
    
    // 设置待验证连接的平台信息
    assert!(manager.set_pending_connection_platform(
        &connection_id, 
        Platform::Web, 
        Some("test_device".to_string())
    ).await.is_ok());
    
    // 完成认证
    let user_id = "test_user_1".to_string();
    assert!(manager.complete_authentication(
        connection_id.clone(),
        user_id.clone(),
        Platform::Web,
        Some("test_device".to_string())
    ).await.is_ok());
    
    // 验证统计信息
    let stats = manager.get_stats().await;
    assert_eq!(stats.pending_auth_connections, 0);
    assert_eq!(stats.total_connections, 1);
    assert_eq!(stats.active_connections, 1);
    assert_eq!(stats.total_users, 1);
    assert_eq!(stats.active_users, 1);
    
    // 验证用户信息
    let user_info = manager.get_user_info(&user_id).await;
    assert!(user_info.is_some());
    let user_info = user_info.unwrap();
    assert!(user_info.contains_key(&Platform::Web));
    
    // 验证连接与用户映射
    let user_mapping = manager.get_user_by_connection(&connection_id).await;
    assert!(user_mapping.is_some());
    let (mapped_user_id, platform) = user_mapping.unwrap();
    assert_eq!(mapped_user_id, user_id);
    assert_eq!(platform, Platform::Web);
    
    println!("认证完成测试通过！");
}

#[tokio::test]
async fn test_user_connection_removal() {
    // 创建基础连接管理器
    let base_manager = Arc::new(ConnectionManager::new());
    
    // 创建用户连接管理器
    let manager = UserConnectionManager::new(base_manager.clone());
    
    // 创建模拟连接配置
    let config = ConnectionConfig::server(
        "mock_connection".to_string(),
        "127.0.0.1:8080".to_string()
    );
    
    // 创建模拟连接
    let connection_id = "test_connection_1".to_string();
    let mock_connection = Arc::new(MockServerConnection {
        id: connection_id.clone(),
        user_id: Arc::new(tokio::sync::RwLock::new(None)),
        config,
    });
    
    // 添加连接到基础管理器
    assert!(base_manager.add_connection(mock_connection.clone()).await.is_ok());
    
    // 添加待验证连接
    assert!(manager.add_pending_connection(connection_id.clone()).await.is_ok());
    
    // 设置待验证连接的平台信息
    assert!(manager.set_pending_connection_platform(
        &connection_id, 
        Platform::Web, 
        Some("test_device".to_string())
    ).await.is_ok());
    
    // 完成认证
    let user_id = "test_user_1".to_string();
    assert!(manager.complete_authentication(
        connection_id.clone(),
        user_id.clone(),
        Platform::Web,
        Some("test_device".to_string())
    ).await.is_ok());
    
    // 移除连接
    assert!(manager.remove_connection(&connection_id).await.is_ok());
    
    // 验证统计信息
    let stats = manager.get_stats().await;
    assert_eq!(stats.pending_auth_connections, 0);
    assert_eq!(stats.active_connections, 0);
    assert_eq!(stats.active_users, 0);
    
    // 验证用户信息已被移除
    let user_info = manager.get_user_info(&user_id).await;
    assert!(user_info.is_none());
    
    println!("用户连接移除测试通过！");
}

#[tokio::test]
async fn test_timeout_pending_connections_cleanup() {
    // 创建基础连接管理器
    let base_manager = Arc::new(ConnectionManager::new());
    
    // 创建用户连接管理器，设置较短的认证超时时间
    let manager = UserConnectionManager::with_config(base_manager, Duration::from_millis(100));
    
    // 添加待验证连接
    let connection_id = "test_connection_1".to_string();
    assert!(manager.add_pending_connection(connection_id.clone()).await.is_ok());
    
    // 等待超时
    tokio::time::sleep(Duration::from_millis(150)).await;
    
    // 清理超时的待验证连接
    let removed_count = manager.cleanup_timeout_pending_connections().await;
    assert_eq!(removed_count, 1);
    
    // 验证统计信息
    let stats = manager.get_stats().await;
    assert_eq!(stats.pending_auth_connections, 0);
    
    println!("超时待验证连接清理测试通过！");
}

#[tokio::test]
async fn test_disconnect_unauthenticated_connection() {
    // 创建基础连接管理器
    let base_manager = Arc::new(ConnectionManager::new());
    
    // 创建用户连接管理器
    let manager = UserConnectionManager::new(base_manager.clone());
    
    // 创建模拟连接配置
    let config = ConnectionConfig::server(
        "mock_connection".to_string(),
        "127.0.0.1:8080".to_string()
    );
    
    // 创建模拟连接
    let connection_id = "test_connection_1".to_string();
    let mock_connection = Arc::new(MockServerConnection {
        id: connection_id.clone(),
        user_id: Arc::new(tokio::sync::RwLock::new(None)),
        config,
    });
    
    // 添加连接到基础管理器
    assert!(base_manager.add_connection(mock_connection.clone()).await.is_ok());
    
    // 添加待验证连接
    assert!(manager.add_pending_connection(connection_id.clone()).await.is_ok());
    
    // 验证待验证连接数量
    assert_eq!(manager.get_pending_auth_count().await, 1);
    
    // 主动断开未认证的连接
    assert!(manager.disconnect_unauthenticated_connection(&connection_id, "Authentication failed").await.is_ok());
    
    // 验证统计信息
    let stats = manager.get_stats().await;
    assert_eq!(stats.pending_auth_connections, 0);
    
    // 验证连接已从待验证列表中移除
    assert_eq!(manager.get_pending_auth_count().await, 0);
    
    println!("未认证连接断开测试通过！");
}

#[tokio::test]
async fn test_user_connection_manager_trait_implementation() {
    // 创建基础连接管理器
    let base_manager = Arc::new(ConnectionManager::new());
    
    // 创建用户连接管理器
    let manager = UserConnectionManager::new(base_manager);
    
    // 验证 ServerConnectionManager trait 的实现
    let connection_count = manager.get_connection_count().await;
    assert_eq!(connection_count, 0);
    
    println!("用户连接管理器trait实现测试通过！");
}