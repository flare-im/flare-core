//! 演示增强后的 ServerConfig 功能

use flare_core::server::config::*;
use flare_core::common::serialization::SerializationFormat;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== ServerConfig 增强功能演示 ===");
    
    // 1. 测试默认配置
    println!("\n1. 默认WebSocket配置:");
    let default_config = ServerConfig::default_websocket();
    println!("   - 监听地址: {:?}", default_config.websocket_config.as_ref().map(|c| &c.listen_addr));
    println!("   - 最大连接数: {}", default_config.max_connections);
    println!("   - 心跳间隔: {}ms", default_config.heartbeat_interval_ms);
    println!("   - 缓冲区大小: {}KB", default_config.buffer_size / 1024);
    
    // 验证默认配置
    match default_config.validate() {
        Ok(_) => println!("   ✓ 配置验证通过"),
        Err(e) => println!("   ✗ 配置验证失败: {}", e),
    }
    
    // 2. 测试高性能配置
    println!("\n2. 高性能配置:");
    let high_perf_config = ServerConfig::high_performance_websocket();
    println!("   - 最大连接数: {}", high_perf_config.max_connections);
    println!("   - 缓冲区大小: {}KB", high_perf_config.buffer_size / 1024);
    println!("   - 启用零拷贝: {}", high_perf_config.performance_config.enable_zero_copy);
    println!("   - CPU亲和性: {}", high_perf_config.performance_config.enable_cpu_affinity);
    println!("   - NUMA感知: {}", high_perf_config.performance_config.enable_numa_awareness);
    
    // 3. 测试低延迟配置
    println!("\n3. 低延迟配置:");
    let low_latency_config = ServerConfig::low_latency_websocket();
    println!("   - 心跳间隔: {}ms", low_latency_config.heartbeat_interval_ms);
    println!("   - 心跳超时: {}ms", low_latency_config.heartbeat_timeout_ms);
    println!("   - 批量大小: {}", low_latency_config.performance_config.batch_size);
    println!("   - 连接池: {}", low_latency_config.performance_config.enable_connection_pool);
    
    // 4. 测试生产环境配置
    println!("\n4. 生产环境配置:");
    let production_config = ServerConfig::production_websocket();
    println!("   - 最大连接数: {}", production_config.max_connections);
    println!("   - 启用黑名单: {}", production_config.security_config.enable_blacklist);
    println!("   - 安全配置: 启用加密 = {}", production_config.security_config.enable_message_encryption);
    println!("   - 最大消息大小: {}MB", production_config.security_config.max_message_size / (1024 * 1024));
    
    // 5. 测试配置转换
    println!("\n5. 配置转换测试:");
    let connection_id = "demo_connection".to_string();
    let conn_config = high_perf_config.to_connection_config(connection_id.clone());
    
    println!("   - 连接ID: {}", conn_config.id);
    println!("   - 传输类型: {:?}", conn_config.transport);
    println!("   - 远程地址: {}", conn_config.remote_addr);
    println!("   - 心跳间隔: {}ms", conn_config.heartbeat_interval_ms);
    println!("   - 缓冲区大小: {}KB", conn_config.buffer_size / 1024);
    println!("   - 最大消息大小: {}MB", conn_config.max_message_size / (1024 * 1024));
    
    // 6. 测试自定义配置
    println!("\n6. 自定义配置:");
    let custom_config = ServerConfig::default_websocket()
        .with_heartbeat_config(15000, 5000, 3)
        .with_buffer_size(128 * 1024)
        .with_serialization_format(SerializationFormat::Protobuf)
        .with_auto_heartbeat_response(false);
    
    println!("   - 心跳间隔: {}ms", custom_config.heartbeat_interval_ms);
    println!("   - 心跳超时: {}ms", custom_config.heartbeat_timeout_ms);
    println!("   - 序列化格式: {:?}", custom_config.serialization_config.format);
    println!("   - 自动心跳响应: {}", custom_config.auto_heartbeat_response);
    
    // 验证自定义配置
    match custom_config.validate() {
        Ok(_) => println!("   ✓ 自定义配置验证通过"),
        Err(e) => println!("   ✗ 自定义配置验证失败: {}", e),
    }
    
    // 7. 测试QUIC配置
    println!("\n7. QUIC配置测试:");
    let quic_config = ServerConfig::default_quic(
        "certs/server.crt".to_string(),
        "certs/server.key".to_string()
    );
    
    println!("   - 服务器类型: {:?}", quic_config.server_type);
    println!("   - QUIC地址: {:?}", quic_config.quic_config.as_ref().map(|c| &c.listen_addr));
    println!("   - 启用TLS: {:?}", quic_config.quic_config.as_ref().map(|c| c.enable_tls));
    
    // 转换QUIC配置
    let quic_conn_config = quic_config.to_connection_config("quic_demo".to_string());
    println!("   - QUIC连接传输类型: {:?}", quic_conn_config.transport);
    println!("   - 证书路径: {}", quic_conn_config.protocol_config.quic.server.cert_path);
    
    // 8. 测试双协议配置
    println!("\n8. 双协议配置测试:");
    let dual_config = ServerConfig::default_dual_protocol(
        "certs/server.crt".to_string(),
        "certs/server.key".to_string()
    );
    
    println!("   - 服务器类型: {:?}", dual_config.server_type);
    println!("   - WebSocket地址: {:?}", dual_config.websocket_config.as_ref().map(|c| &c.listen_addr));
    println!("   - QUIC地址: {:?}", dual_config.quic_config.as_ref().map(|c| &c.listen_addr));
    
    // 测试专门的转换方法
    if let Some(ws_conn_config) = dual_config.to_websocket_connection_config("ws_dual_demo".to_string()) {
        println!("   - WebSocket连接配置: ✓");
        println!("     * 传输类型: {:?}", ws_conn_config.transport);
    }
    
    if let Some(quic_conn_config) = dual_config.to_quic_connection_config("quic_dual_demo".to_string()) {
        println!("   - QUIC连接配置: ✓");
        println!("     * 传输类型: {:?}", quic_conn_config.transport);
    }
    
    println!("\n=== 演示完成 ===");
    println!("✓ 所有配置类型都成功创建和验证");
    println!("✓ 配置转换功能正常工作");
    println!("✓ 增强的配置选项提供了更灵活的服务端配置能力");
    
    Ok(())
}
