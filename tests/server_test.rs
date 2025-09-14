//! 服务端测试

use flare_core::{
    server::{
        server::{ServerImpl, ServerConfig},
        ConnectionManager,
        manager::traits::ServerConnectionManager,
    },
    common::{
        protocol::{Frame, MessageType, Reliability},
    },
};
use std::sync::Arc;

#[tokio::test]
async fn test_server_creation() {
    // 创建服务端配置
    let config = ServerConfig {
        local_addr: Some("127.0.0.1:0".to_string()), // 使用端口0让系统分配可用端口
        connection_timeout_ms: 30000,
        heartbeat_interval_ms: 10000,
        max_connections: 10,
        enable_tls: false,
    };
    
    // 创建连接管理器
    let connection_manager = Arc::new(ConnectionManager::new());
    
    // 创建服务端实例
    let server = ServerImpl::new(config, connection_manager);
    
    // 验证服务端创建成功（我们不能直接访问私有字段）
    // 只要能创建成功就不报错
    assert!(true);
}

#[tokio::test]
async fn test_connection_manager() {
    let manager = ConnectionManager::new();
    
    // 获取连接数量
    assert_eq!(manager.get_connection_count().await, 0);
}

#[tokio::test]
async fn test_message_creation() {
    // 创建一个测试消息
    let message = Frame {
        message_type: MessageType::Data,
        message_id: 1,
        reliability: Reliability::AtLeastOnce,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        payload: b"test message".to_vec(),
        session_id: None,
        priority: 0,
        compression: None,
        encrypted: false,
        metadata: None,
    };
    
    // 验证消息创建成功
    assert_eq!(message.message_type, MessageType::Data);
    assert_eq!(message.payload, b"test message");
}