//! 服务端测试

use flare_core::{
    server::{
        Server, ServerConfig, ConnectionBasedManager,
        MessageHandler,
    },
    common::{
        protocol::{Frame, MessageType, Reliability},
    },
};
use std::sync::Arc;

/// 简单的消息处理器
struct TestMessageHandler;

#[async_trait::async_trait]
impl MessageHandler for TestMessageHandler {
    async fn handle_message(&self, connection_id: String, message: Frame) -> flare_core::common::error::Result<Option<Frame>> {
        println!("处理消息 from {}: {:?}", connection_id, message);
        Ok(None)
    }
}

#[tokio::test]
async fn test_server_creation() {
    // 创建服务端配置
    let config = ServerConfig {
        websocket_addr: Some("127.0.0.1:0".to_string()), // 使用端口0让系统分配可用端口
        quic_addr: Some("127.0.0.1:0".to_string()), // 使用端口0让系统分配可用端口
        enable_tls: false,
        tls_cert_path: None,
        tls_key_path: None,
        max_connections: 10,
        connection_timeout_ms: 30000,
        heartbeat_interval_ms: 10000,
        enable_auto_cleanup: true,
    };
    
    // 创建连接管理器
    let connection_manager = Arc::new(ConnectionBasedManager::new());
    
    // 创建服务端实例
    let server = Server::new(config, connection_manager);
    
    // 验证服务端创建成功
    assert!(!server.is_running());
}

#[tokio::test]
async fn test_connection_manager() {
    let manager = ConnectionBasedManager::new();
    
    // 检查是否需要清理
    assert!(!manager.should_cleanup().await);
}

#[tokio::test]
async fn test_message_handler() {
    let handler = Arc::new(TestMessageHandler);
    
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
    
    // 处理消息
    let result = handler.handle_message("test_connection".to_string(), message).await;
    assert!(result.is_ok());
}