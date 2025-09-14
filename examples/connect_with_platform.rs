//! 连接帧添加平台信息示例

use flare_core::common::protocol::{Frame, MessageType};

fn main() {
    // 1. 创建带平台信息的连接帧
    let connect_frame = Frame::connect("client-123", Some("web"));
    println!("连接帧消息类型: {:?}", connect_frame.get_message_type());
    println!("连接帧负载: {:?}", String::from_utf8_lossy(connect_frame.get_payload()));
    
    // 2. 检查平台信息是否正确添加到元数据中
    if let Some(metadata) = &connect_frame.metadata {
        println!("\n=== 元数据信息 ===");
        for (key, value) in metadata {
            println!("{}: {}", key, String::from_utf8_lossy(value));
        }
    }
    
    // 3. 创建不带平台信息的连接帧（兼容旧版本）
    let connect_frame_no_platform = Frame::connect("client-456", None);
    println!("\n=== 不带平台信息的连接帧 ===");
    println!("连接帧消息类型: {:?}", connect_frame_no_platform.get_message_type());
    println!("连接帧负载: {:?}", String::from_utf8_lossy(connect_frame_no_platform.get_payload()));
    
    if let Some(metadata) = &connect_frame_no_platform.metadata {
        println!("元数据数量: {}", metadata.len());
    }
    
    println!("\n=== 总结 ===");
    println!("1. connect方法现在支持可选的平台参数");
    println!("2. 平台信息存储在Frame的metadata字段中");
    println!("3. 保持了向后兼容性，可以不传递平台信息");
    println!("4. 这样可以在连接建立时就识别客户端平台类型");
}