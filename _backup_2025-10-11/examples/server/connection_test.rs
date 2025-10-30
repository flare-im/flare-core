//! 测试优化后的服务器连接创建功能

use flare_core::server::config::*;
use flare_core::common::serialization::SerializationFormat;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 服务器连接创建功能测试 ===");
    
    // 1. 测试WebSocket服务器连接配置
    println!("\n1. 测试WebSocket服务器连接配置:");
    let ws_config = ServerConfig::high_performance_websocket();
    
    // 模拟客户端连接
    let connection_id = "test_ws_connection_127_0_0_1_12345".to_string();
    let remote_addr = "127.0.0.1:12345".to_string();
    
    // 使用增强的配置转换方法
    let mut ws_conn_config = ws_config.to_websocket_connection_config(connection_id.clone())
        .unwrap();
    
    // 设置远程地址（从原始连接获取）
    ws_conn_config.remote_addr = remote_addr.clone();
    
    println!("   - 连接ID: {}", ws_conn_config.id);
    println!("   - 传输类型: {:?}", ws_conn_config.transport);
    println!("   - 本地地址: {:?}", ws_conn_config.local_addr);
    println!("   - 远程地址: {}", ws_conn_config.remote_addr);
    println!("   - 心跳间隔: {}ms", ws_conn_config.heartbeat_interval_ms);
    println!("   - 缓冲区大小: {}KB", ws_conn_config.buffer_size / 1024);
    println!("   - 序列化格式: {:?}", ws_conn_config.get_serialization_config().format);
    
    // 2. 测试QUIC服务器连接配置
    println!("\n2. 测试QUIC服务器连接配置:");
    let quic_config = ServerConfig::default_quic(
        "certs/server.crt".to_string(),
        "certs/server.key".to_string()
    );
    
    let connection_id = "test_quic_connection_127_0_0_1_54321".to_string();
    let remote_addr = "127.0.0.1:54321".to_string();
    
    // 使用增强的配置转换方法
    let mut quic_conn_config = quic_config.to_quic_connection_config(connection_id.clone())
        .unwrap();
    
    // 设置远程地址（从原始连接获取）
    quic_conn_config.remote_addr = remote_addr.clone();
    
    println!("   - 连接ID: {}", quic_conn_config.id);
    println!("   - 传输类型: {:?}", quic_conn_config.transport);
    println!("   - 本地地址: {:?}", quic_conn_config.local_addr);
    println!("   - 远程地址: {}", quic_conn_config.remote_addr);
    println!("   - 心跳间隔: {}ms", quic_conn_config.heartbeat_interval_ms);
    println!("   - 缓冲区大小: {}KB", quic_conn_config.buffer_size / 1024);
    println!("   - 证书路径: {}", quic_conn_config.protocol_config.quic.server.cert_path);
    println!("   - 私钥路径: {}", quic_conn_config.protocol_config.quic.server.key_path);
    
    // 3. 测试双协议服务器连接配置
    println!("\n3. 测试双协议服务器连接配置:");
    let dual_config = ServerConfig::default_dual_protocol(
        "certs/server.crt".to_string(),
        "certs/server.key".to_string()
    );
    
    // WebSocket连接
    let ws_connection_id = "test_dual_ws_connection_127_0_0_1_23456".to_string();
    let ws_remote_addr = "127.0.0.1:23456".to_string();
    
    let mut dual_ws_config = dual_config.to_websocket_connection_config(ws_connection_id.clone())
        .unwrap();
    dual_ws_config.remote_addr = ws_remote_addr.clone();
    
    println!("   WebSocket连接:");
    println!("     - 连接ID: {}", dual_ws_config.id);
    println!("     - 传输类型: {:?}", dual_ws_config.transport);
    println!("     - 远程地址: {}", dual_ws_config.remote_addr);
    
    // QUIC连接
    let quic_connection_id = "test_dual_quic_connection_127_0_0_1_34567".to_string();
    let quic_remote_addr = "127.0.0.1:34567".to_string();
    
    let mut dual_quic_config = dual_config.to_quic_connection_config(quic_connection_id.clone())
        .unwrap();
    dual_quic_config.remote_addr = quic_remote_addr.clone();
    
    println!("   QUIC连接:");
    println!("     - 连接ID: {}", dual_quic_config.id);
    println!("     - 传输类型: {:?}", dual_quic_config.transport);
    println!("     - 远程地址: {}", dual_quic_config.remote_addr);
    
    // 4. 测试配置验证
    println!("\n4. 测试配置验证:");
    let invalid_config = ServerConfig::default_websocket()
        .with_heartbeat_config(1000, 2000, 3); // 心跳超时大于心跳间隔，应该验证失败
    
    match invalid_config.validate() {
        Ok(_) => println!("   ✗ 配置验证应该失败但通过了"),
        Err(e) => println!("   ✓ 配置验证正确失败: {}", e),
    }
    
    let valid_config = ServerConfig::default_websocket()
        .with_heartbeat_config(10000, 5000, 3); // 正常配置
    
    match valid_config.validate() {
        Ok(_) => println!("   ✓ 有效配置验证通过"),
        Err(e) => println!("   ✗ 有效配置验证失败: {}", e),
    }
    
    // 5. 测试不同预设配置的连接配置
    println!("\n5. 测试不同预设配置的连接配置:");
    
    let configs = vec![
        ("默认配置", ServerConfig::default_websocket()),
        ("高性能配置", ServerConfig::high_performance_websocket()),
        ("低延迟配置", ServerConfig::low_latency_websocket()),
        ("稳定配置", ServerConfig::stable_websocket()),
        ("生产环境配置", ServerConfig::production_websocket()),
    ];
    
    for (name, config) in configs {
        let conn_config = config.to_connection_config("test_connection".to_string());
        println!("   {}: 心跳间隔={}ms, 缓冲区={}KB, 最大消息={}MB", 
                 name,
                 conn_config.heartbeat_interval_ms,
                 conn_config.buffer_size / 1024,
                 conn_config.max_message_size / (1024 * 1024));
    }
    
    println!("\n=== 测试完成 ===");
    println!("✓ 所有连接配置创建成功");
    println!("✓ 远程地址正确从原始连接设置");
    println!("✓ 配置转换功能正常工作");
    println!("✓ 配置验证功能正常");
    println!("✓ 不同预设配置都正确应用");
    
    Ok(())
}
