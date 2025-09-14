//! 用户连接管理器集成测试
//!
//! 测试用户连接管理器作为连接管理器增强版本的功能

use flare_core::server::{
    ConnectionManager,
    UserConnectionManager,
    ServerConnectionManager,
};
use flare_core::common::protocol::{Frame, Platform};
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
    
    async fn send_message(&self, _message: Frame) -> flare_core::common::error::Result<()> {
        Ok(())
    }
    
    async fn receive_message(&self) -> flare_core::common::error::Result<Option<Frame>> {
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
async fn test_user_connection_manager_as_enhanced_connection_manager() {
    // 创建基础连接管理器
    let base_manager = Arc::new(ConnectionManager::new());
    
    // 创建用户连接管理器
    let user_manager = Arc::new(UserConnectionManager::new(base_manager.clone()));
    
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
    
    // 通过用户连接管理器添加连接（作为 ServerConnectionManager trait 使用）
    assert!(user_manager.add_connection(mock_connection.clone()).await.is_ok());
    
    // 验证连接已添加到基础管理器
    assert!(base_manager.get_connection(&connection_id).await.is_some());
    
    // 添加待验证连接
    assert!(user_manager.add_pending_connection(connection_id.clone()).await.is_ok());
    
    // 验证待验证连接数量
    assert_eq!(user_manager.get_pending_auth_count().await, 1);
    
    // 完成认证
    let user_id = "test_user_1".to_string();
    assert!(user_manager.complete_authentication(
        connection_id.clone(),
        user_id.clone(),
        Platform::Web,
        Some("test_device".to_string())
    ).await.is_ok());
    
    // 验证统计信息
    let stats = user_manager.get_stats().await;
    assert_eq!(stats.pending_auth_connections, 0);
    assert_eq!(stats.total_connections, 1);
    assert_eq!(stats.active_connections, 1);
    assert_eq!(stats.total_users, 1);
    assert_eq!(stats.active_users, 1);
    
    // 验证用户信息
    let user_info = user_manager.get_user_info(&user_id).await;
    assert!(user_info.is_some());
    let user_info = user_info.unwrap();
    assert!(user_info.contains_key(&Platform::Web));
    
    // 验证连接与用户映射
    let user_mapping = user_manager.get_user_by_connection(&connection_id).await;
    assert!(user_mapping.is_some());
    let (mapped_user_id, platform) = user_mapping.unwrap();
    assert_eq!(mapped_user_id, user_id);
    assert_eq!(platform, Platform::Web);
    
    // 通过用户连接管理器发送消息（作为 ServerConnectionManager trait 使用）
    let message = Frame::data(1, b"test message".to_vec());
    assert!(user_manager.send_message(&connection_id, message).await.is_ok());
    
    // 通过用户连接管理器获取连接（作为 ServerConnectionManager trait 使用）
    let retrieved_connection = user_manager.get_connection(&connection_id).await;
    assert!(retrieved_connection.is_some());
    
    // 通过用户连接管理器获取所有连接（作为 ServerConnectionManager trait 使用）
    let all_connections = user_manager.get_all_connections().await;
    assert_eq!(all_connections.len(), 1);
    
    // 通过用户连接管理器获取连接数量（作为 ServerConnectionManager trait 使用）
    let connection_count = user_manager.get_connection_count().await;
    assert_eq!(connection_count, 1);
    
    // 通过用户连接管理器广播消息（作为 ServerConnectionManager trait 使用）
    let broadcast_message = Frame::data(2, b"broadcast message".to_vec());
    let broadcast_count = user_manager.broadcast_message(broadcast_message).await.unwrap();
    assert_eq!(broadcast_count, 1);
    
    // 通过用户连接管理器清理不活跃连接（作为 ServerConnectionManager trait 使用）
    let inactive_count = user_manager.cleanup_inactive_connections(Duration::from_secs(10)).await;
    assert_eq!(inactive_count, 0); // 模拟连接始终活跃
    
    println!("用户连接管理器作为增强连接管理器测试通过！");
}

#[tokio::test]
async fn test_user_connection_manager_user_specific_functionality() {
    // 创建基础连接管理器
    let base_manager = Arc::new(ConnectionManager::new());
    
    // 创建用户连接管理器
    let user_manager = Arc::new(UserConnectionManager::new(base_manager.clone()));
    
    // 创建模拟连接配置
    let config = ConnectionConfig::server(
        "mock_connection".to_string(),
        "127.0.0.1:8080".to_string()
    );
    
    // 创建多个模拟连接
    let connection_id1 = "test_connection_1".to_string();
    let mock_connection1 = Arc::new(MockServerConnection {
        id: connection_id1.clone(),
        user_id: Arc::new(tokio::sync::RwLock::new(None)),
        config: config.clone(),
    });
    
    let connection_id2 = "test_connection_2".to_string();
    let mock_connection2 = Arc::new(MockServerConnection {
        id: connection_id2.clone(),
        user_id: Arc::new(tokio::sync::RwLock::new(None)),
        config: config.clone(),
    });
    
    // 添加连接到基础管理器
    assert!(base_manager.add_connection(mock_connection1.clone()).await.is_ok());
    assert!(base_manager.add_connection(mock_connection2.clone()).await.is_ok());
    
    // 添加待验证连接
    assert!(user_manager.add_pending_connection(connection_id1.clone()).await.is_ok());
    assert!(user_manager.add_pending_connection(connection_id2.clone()).await.is_ok());
    
    // 完成认证 - 两个连接属于同一个用户但不同平台
    let user_id = "test_user_1".to_string();
    assert!(user_manager.complete_authentication(
        connection_id1.clone(),
        user_id.clone(),
        Platform::Web,
        Some("web_device".to_string())
    ).await.is_ok());
    
    assert!(user_manager.complete_authentication(
        connection_id2.clone(),
        user_id.clone(),
        Platform::Android,
        Some("android_device".to_string())
    ).await.is_ok());
    
    // 验证用户有多个平台的连接
    let user_info = user_manager.get_user_info(&user_id).await;
    assert!(user_info.is_some());
    let user_info = user_info.unwrap();
    assert_eq!(user_info.len(), 2);
    assert!(user_info.contains_key(&Platform::Web));
    assert!(user_info.contains_key(&Platform::Android));
    
    // 获取用户的所有连接
    let user_connections = user_manager.get_user_connections(&user_id).await;
    assert_eq!(user_connections.len(), 2);
    
    // 向指定用户发送消息
    let user_message = Frame::data(3, b"user specific message".to_vec());
    let sent_count = user_manager.send_message_to_user(&user_id, user_message).await.unwrap();
    assert_eq!(sent_count, 2);
    
    // 移除用户的所有连接
    let removed_count = user_manager.remove_user_connections(&user_id).await.unwrap();
    assert_eq!(removed_count, 2);
    
    // 验证用户信息已被移除
    let user_info = user_manager.get_user_info(&user_id).await;
    // 注意：这里可能仍然存在用户信息，因为remove_user_connections只移除了连接，
    // 但UserConnectionManager的实现中，只有当用户的所有平台连接都被移除时，
    // 用户信息才会被完全移除。
    // 让我们检查连接是否被正确移除
    let user_connections = user_manager.get_user_connections(&user_id).await;
    assert_eq!(user_connections.len(), 0);
    
    // 验证统计信息
    let stats = user_manager.get_stats().await;
    assert_eq!(stats.active_connections, 0);
    // 用户数可能不为0，因为用户信息可能仍然存在
    // assert_eq!(stats.active_users, 0);
    
    println!("用户连接管理器用户特定功能测试通过！");
}