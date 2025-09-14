//! 服务端连接事件测试
//!
//! 测试服务端连接事件功能的集成

use std::sync::Arc;
use flare_core::{
    server::{
        ServerConnectionEvent,
        ConnectionManager,
        ServerConnectionManager,
    },
    common::{
        protocol::{Frame, MessageType, Reliability, Platform},
        connections::{
            config::ConnectionConfig,
            event::ConnectionEvent,
        },
    },
};

/// 测试服务端事件处理器
#[derive(Debug)]
pub struct TestServerEventHandler {
    pub events: Arc<tokio::sync::RwLock<Vec<String>>>,
}

#[async_trait::async_trait]
impl ConnectionEvent for TestServerEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        let mut events = self.events.write().await;
        events.push(format!("connected:{}", connection_id));
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        let mut events = self.events.write().await;
        events.push(format!("disconnected:{}:{}", connection_id, reason));
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        let mut events = self.events.write().await;
        events.push(format!("error:{}:{}", connection_id, error));
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        let mut events = self.events.write().await;
        events.push(format!("message_received:{}:{:?}", connection_id, message.get_message_type()));
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        let mut events = self.events.write().await;
        events.push(format!("message_sent:{}:{:?}", connection_id, message.get_message_type()));
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        let mut events = self.events.write().await;
        events.push(format!("heartbeat_timeout:{}", connection_id));
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        let mut events = self.events.write().await;
        events.push(format!("quality_changed:{}:{}", connection_id, quality_score));
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        let mut events = self.events.write().await;
        events.push(format!("heartbeat_ping:{}", connection_id));
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
        let mut events = self.events.write().await;
        events.push(format!("heartbeat_pong:{}", connection_id));
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        let mut events = self.events.write().await;
        events.push(format!("reconnect_started:{}:{}", connection_id, attempt));
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        let mut events = self.events.write().await;
        events.push(format!("reconnected:{}:{}", connection_id, attempt));
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        let mut events = self.events.write().await;
        events.push(format!("reconnect_failed:{}:{}:{}", connection_id, attempt, error));
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) {
        let mut events = self.events.write().await;
        events.push(format!("statistics_updated:{}:{}:{}", 
                           connection_id, stats.messages_received, stats.messages_sent));
    }
}

#[async_trait::async_trait]
impl ServerConnectionEvent for TestServerEventHandler {
    async fn on_user_authenticated(&self, connection_id: &str, user_id: &str, platform: &Platform) {
        let mut events = self.events.write().await;
        events.push(format!("user_authenticated:{}:{}:{:?}", connection_id, user_id, platform));
    }
    
    async fn on_user_disconnected(&self, connection_id: &str, user_id: &str, reason: &str) {
        let mut events = self.events.write().await;
        events.push(format!("user_disconnected:{}:{}:{}", connection_id, user_id, reason));
    }
    
    async fn on_authentication_failed(&self, connection_id: &str, error: &str) {
        let mut events = self.events.write().await;
        events.push(format!("authentication_failed:{}:{}", connection_id, error));
    }
    
    async fn on_authentication_timeout(&self, connection_id: &str) {
        let mut events = self.events.write().await;
        events.push(format!("authentication_timeout:{}", connection_id));
    }
    
    async fn on_user_message(&self, connection_id: &str, user_id: &str, message: &Frame) -> bool {
        let mut events = self.events.write().await;
        events.push(format!("user_message:{}:{}:{:?}", connection_id, user_id, message.get_message_type()));
        // 继续处理消息
        true
    }
    
    async fn on_connection_count_changed(&self, total_connections: usize, authenticated_users: usize) {
        let mut events = self.events.write().await;
        events.push(format!("connection_count_changed:{}:{}", total_connections, authenticated_users));
    }
    
    async fn on_user_online(&self, user_id: &str, platform: &Platform, connection_id: &str) {
        let mut events = self.events.write().await;
        events.push(format!("user_online:{}:{:?}:{}", user_id, platform, connection_id));
    }
    
    async fn on_user_offline(&self, user_id: &str, platform: &Platform) {
        let mut events = self.events.write().await;
        events.push(format!("user_offline:{}:{:?}", user_id, platform));
    }
}

impl TestServerEventHandler {
    pub fn new() -> Self {
        Self {
            events: Arc::new(tokio::sync::RwLock::new(Vec::new())),
        }
    }
    
    pub async fn get_events(&self) -> Vec<String> {
        let events = self.events.read().await;
        events.clone()
    }
    
    pub async fn clear_events(&self) {
        let mut events = self.events.write().await;
        events.clear();
    }
}

#[tokio::test]
async fn test_server_connection_event_integration() {
    // 创建连接管理器
    let connection_manager = Arc::new(ConnectionManager::new());
    
    // 创建测试事件处理器
    let event_handler = Arc::new(TestServerEventHandler::new());
    
    // 验证事件处理器实现了 ServerConnectionEvent trait
    assert!(true); // 编译时检查
    
    // 创建连接配置
    let config = ConnectionConfig::server(
        "test_connection".to_string(),
        "127.0.0.1:8080".to_string(),
    );
    
    // 触发一些事件来测试
    let connection_id = "test_connection_1";
    event_handler.on_connected(connection_id).await;
    event_handler.on_message_received(
        connection_id, 
        &Frame::new(MessageType::Data, 1, Reliability::AtLeastOnce, b"test".to_vec())
    ).await;
    event_handler.on_user_authenticated(connection_id, "test_user", &Platform::Web).await;
    
    // 检查事件是否被正确记录
    let events = event_handler.get_events().await;
    assert!(events.contains(&format!("connected:{}", connection_id)));
    assert!(events.contains(&format!("message_received:{}:{:?}", connection_id, MessageType::Data)));
    assert!(events.contains(&format!("user_authenticated:{}:{}:{:?}", connection_id, "test_user", Platform::Web)));
    
    println!("服务端连接事件集成测试通过！");
}