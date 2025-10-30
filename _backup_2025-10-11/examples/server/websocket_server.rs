//! WebSocket服务端示例
//!
//! 演示如何创建和运行WebSocket服务端


use flare_core::{
    server::{
        config::{ServerConfig, ProtocolConfig},
        server::{AggregationServer, ServerBuilder},
        event::DefServerEventHandler,
    },
    common::serialization::{SerializationConfig, SerializationFormat},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    // 创建服务端配置 - 优化长时间连接稳定性
    let config = ServerConfig::default_websocket()
        .with_websocket_config(
            ProtocolConfig::new()
                .with_listen_addr("127.0.0.1:8080".to_string())
                .with_max_connections(1000)
        )
        .with_connection_timeout_ms(120000)  // 2分钟连接超时
        .with_heartbeat_interval_ms(15000)   // 15秒心跳间隔
        .with_heartbeat_timeout_ms(60000)    // 60秒心跳超时，与客户端匹配
        .with_heartbeat_monitoring(60000, 30000) // 1分钟心跳监控超时，30秒清理间隔
        .with_auth_timeout_ms(30000)
        .with_serialization_format(SerializationFormat::Protobuf);
    
    // 打印配置信息用于调试
    tracing::info!("服务器配置: {:?}", config);
    if let Some(ws_config) = &config.websocket_config {
        tracing::info!("WebSocket配置存在，监听地址: {}", ws_config.listen_addr);
    } else {
        tracing::error!("WebSocket配置不存在！");
    }
    tracing::info!("序列化配置: {:?}", config.serialization_config);
    
    // 创建事件处理器
    let event_handler = std::sync::Arc::new(DefServerEventHandler::default());
    
    // 创建AggregationServer实例
    let server = ServerBuilder::new(config)
        .with_event_handler(event_handler)
        .build()?;
    
    // 启动服务端
    server.start().await?;
    
    println!("WebSocket服务端已启动，监听地址: 127.0.0.1:8080");
    println!("按 Ctrl+C 停止服务端");
    
    // 等待中断信号或长时间运行
    println!("服务端正在运行，按 Ctrl+C 停止...");
    
    // 使用 tokio::signal::ctrl_c() 等待中断信号
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            println!("\n收到停止信号，正在关闭服务端...");
        }
        Err(e) => {
            eprintln!("等待中断信号时出错: {}", e);
        }
    }
    
    // 停止服务端
    let _ = server.stop().await;
    
    Ok(())
}