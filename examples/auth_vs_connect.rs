//! 认证消息与连接消息的区别和使用示例

use flare_core::common::protocol::{Frame, MessageType};

fn main() {
    // 1. CONNECT消息 - 用于建立网络连接
    let connect_frame = Frame::connect("client-123");
    println!("CONNECT消息类型: {:?}", connect_frame.get_message_type());
    println!("CONNECT消息负载: {:?}", String::from_utf8_lossy(connect_frame.get_payload()));
    
    // 2. AUTH_REQUEST消息 - 用于身份认证
    let auth_request_frame = Frame::auth_request("user-456", "web", "token-xyz");
    println!("AUTH_REQUEST消息类型: {:?}", auth_request_frame.get_message_type());
    println!("AUTH_REQUEST消息负载: {:?}", String::from_utf8_lossy(auth_request_frame.get_payload()));
    
    // 3. 展示它们的不同用途
    println!("\n=== 消息类型对比 ===");
    println!("CONNECT消息是控制消息: {}", connect_frame.is_control());
    println!("AUTH_REQUEST消息是控制消息: {}", auth_request_frame.is_control());
    
    // 4. 展示它们的不同数据结构
    if let Some((user_id, platform, token)) = auth_request_frame.get_auth_request_data() {
        println!("\n=== 认证请求数据 ===");
        println!("用户ID: {}", user_id);
        println!("平台: {}", platform);
        println!("令牌: {}", token);
    }
    
    // 5. 展示连接消息的数据结构
    // 注意：连接消息的数据结构不同，包含客户端ID和协议信息
    let connect_payload: serde_json::Value = serde_json::from_slice(connect_frame.get_payload()).unwrap();
    println!("\n=== 连接消息数据 ===");
    println!("客户端ID: {}", connect_payload["client_id"]);
    println!("协议: {}", connect_payload["protocol"]);
    
    println!("\n=== 总结 ===");
    println!("1. CONNECT消息用于建立网络连接，包含客户端标识和协议信息");
    println!("2. AUTH_REQUEST消息用于身份验证，包含用户凭证");
    println!("3. 两者有不同的职责，不应合并");
    println!("4. 通常先发送CONNECT消息建立连接，再发送AUTH_REQUEST进行认证");
}