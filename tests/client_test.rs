//! 客户端测试

use flare_core::{
    client::{
        Client, ClientConfig, ProtocolSelection,
    },
    common::{
        connections::types::ConnectionType,
        protocol::{Frame, MessageType, Reliability},
        serialization::{SerializationFormat, SerializationConfig},
    },
};
use std::collections::HashMap;

#[tokio::test]
async fn test_client_config() {
    // 测试默认配置
    let config = ClientConfig::default();
    assert_eq!(config.server_addresses.get(&ConnectionType::WebSocket), Some(&"ws://127.0.0.1:8080".to_string()));
    assert_eq!(config.server_addresses.get(&ConnectionType::Quic), Some(&"127.0.0.1:8081".to_string()));
    assert_eq!(config.connection_type, ConnectionType::WebSocket);
    assert_eq!(config.protocol_selection, ProtocolSelection::Auto);
    
    // 测试自定义配置
    let config = ClientConfig::new("ws://192.168.1.1:9090".to_string(), "192.168.1.1:9091".to_string())
        .with_quic_only()
        .with_heartbeat(3000, 1000)
        .with_serialization(
            SerializationFormat::Json,
            SerializationConfig::default()
        );
    
    assert_eq!(config.server_addresses.get(&ConnectionType::WebSocket), Some(&"ws://192.168.1.1:9090".to_string()));
    assert_eq!(config.server_addresses.get(&ConnectionType::Quic), Some(&"192.168.1.1:9091".to_string()));
    assert_eq!(config.connection_type, ConnectionType::Quic);
    assert_eq!(config.protocol_selection, ProtocolSelection::QuicOnly);
    assert_eq!(config.heartbeat_interval_ms, 3000);
    assert_eq!(config.heartbeat_monitor_timeout_ms, 1000);
}

#[tokio::test]
async fn test_client_creation() {
    let config = ClientConfig::new("ws://127.0.0.1:8080".to_string(), "127.0.0.1:8081".to_string());
    let client = Client::new(config);
    
    // 验证客户端创建成功
    assert!(!client.is_connected().await);
    assert_eq!(client.get_state().await, flare_core::common::connections::types::ConnectionState::Disconnected);
}

#[tokio::test]
async fn test_protocol_selection() {
    // 测试QUIC only配置
    let config = ClientConfig::new("ws://127.0.0.1:8081".to_string(), "127.0.0.1:8082".to_string())
        .with_quic_only();
    assert_eq!(config.protocol_selection, ProtocolSelection::QuicOnly);
    assert_eq!(config.connection_type, ConnectionType::Quic);
    
    // 测试WebSocket only配置
    let config = ClientConfig::new("ws://127.0.0.1:8080".to_string(), "127.0.0.1:8081".to_string())
        .with_websocket_only();
    assert_eq!(config.protocol_selection, ProtocolSelection::WebSocketOnly);
    assert_eq!(config.connection_type, ConnectionType::WebSocket);
    
    // 测试自动选择配置
    let config = ClientConfig::new("ws://127.0.0.1:8080".to_string(), "127.0.0.1:8081".to_string())
        .with_protocol_selection(ProtocolSelection::Auto);
    assert_eq!(config.protocol_selection, ProtocolSelection::Auto);
}

#[tokio::test]
async fn test_client_clone() {
    let config = ClientConfig::new("ws://127.0.0.1:8080".to_string(), "127.0.0.1:8081".to_string());
    let client = Client::new(config);
    let cloned_client = client.clone();
    
    // 验证克隆成功
    assert!(!cloned_client.is_connected().await);
    assert_eq!(cloned_client.get_state().await, flare_core::common::connections::types::ConnectionState::Disconnected);
}

#[tokio::test]
async fn test_serialization_config() {
    let config = ClientConfig::new("ws://127.0.0.1:8080".to_string(), "127.0.0.1:8081".to_string())
        .with_serialization(
            SerializationFormat::Json,
            SerializationConfig::default()
        );
    
    assert_eq!(config.serialization_format, SerializationFormat::Json);
}

#[tokio::test]
async fn test_message_creation() {
    let message = Frame {
        message_type: MessageType::Data,
        message_id: 1,
        reliability: Reliability::AtLeastOnce,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        payload: b"test payload".to_vec(),
        session_id: None,
        priority: 0,
        compression: None,
        encrypted: false,
        metadata: None,
    };
    
    assert_eq!(message.message_type, MessageType::Data);
    assert_eq!(message.message_id, 1);
    assert_eq!(message.reliability, Reliability::AtLeastOnce);
    assert_eq!(message.payload, b"test payload");
}

#[tokio::test]
async fn test_heartbeat_config() {
    let config = ClientConfig::new("ws://127.0.0.1:8080".to_string(), "127.0.0.1:8081".to_string())
        .with_heartbeat(5000, 2000);
    
    assert_eq!(config.heartbeat_interval_ms, 5000);
    assert_eq!(config.heartbeat_monitor_timeout_ms, 2000);
}

#[tokio::test]
async fn test_server_address_config() {
    let mut server_addresses = HashMap::new();
    server_addresses.insert(ConnectionType::WebSocket, "ws://test:8080".to_string());
    server_addresses.insert(ConnectionType::Quic, "test:8081".to_string());
    
    let config = ClientConfig::new("ws://test:8080".to_string(), "test:8081".to_string())
        .with_server_address(ConnectionType::WebSocket, "ws://custom:9090".to_string());
    
    assert_eq!(config.get_server_address(ConnectionType::WebSocket), Some(&"ws://custom:9090".to_string()));
    assert_eq!(config.get_server_address(ConnectionType::Quic), Some(&"test:8081".to_string()));
}