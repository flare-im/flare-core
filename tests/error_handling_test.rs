//! 错误处理测试

use flare_core::{
    common::{
        error::{FlareError, ErrorCode},
        protocol::{Frame, MessageType},
    },
};
use serde_json::Value;

#[tokio::test]
async fn test_error_creation() {
    // 测试连接失败错误
    let error = FlareError::connection_failed("测试连接失败");
    assert_eq!(error.reason(), "测试连接失败");
    assert_eq!(error.code(), Some(ErrorCode::ConnectionFailed));
    
    // 测试认证失败错误
    let error = FlareError::authentication_failed("测试认证失败");
    assert_eq!(error.reason(), "测试认证失败");
    assert_eq!(error.code(), Some(ErrorCode::AuthenticationFailed));
    
    // 测试超时错误
    let error = FlareError::timeout("测试超时");
    assert_eq!(error.reason(), "测试超时");
    assert_eq!(error.code(), Some(ErrorCode::OperationTimeout));
    
    // 测试消息发送失败错误
    let error = FlareError::message_send_failed("测试消息发送失败");
    assert_eq!(error.reason(), "测试消息发送失败");
    assert_eq!(error.code(), Some(ErrorCode::MessageSendFailed));
}

#[tokio::test]
async fn test_frame_creation() {
    // 测试错误帧创建
    let error_frame = Frame::error(404, "Not Found");
    assert_eq!(error_frame.message_type, MessageType::Error);
    
    // 验证错误帧的payload是JSON格式
    let payload_str = std::str::from_utf8(&error_frame.payload).unwrap();
    let parsed: Value = serde_json::from_str(payload_str).unwrap();
    assert_eq!(parsed["code"], 404);
    assert_eq!(parsed["message"], "Not Found");
    
    // 测试心跳帧创建
    let heartbeat_frame = Frame::heartbeat();
    assert_eq!(heartbeat_frame.message_type, MessageType::Heartbeat);
    
    // 测试心跳确认帧创建
    let heartbeat_ack_frame = Frame::heartbeat_ack();
    assert_eq!(heartbeat_ack_frame.message_type, MessageType::HeartbeatAck);
}

#[tokio::test]
async fn test_error_localization() {
    let error = FlareError::user_not_found("user123");
    let localized = error.localized().unwrap();
    
    assert_eq!(localized.code, ErrorCode::UserNotFound);
    assert_eq!(localized.reason, "用户不存在");
    assert_eq!(localized.params.as_ref().unwrap().get("user_id"), Some(&"user123".to_string()));
}