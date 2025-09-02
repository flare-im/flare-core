//! WebSocket 服务端示例
//! 
//! 演示如何使用 common 模块的 ConnectionFactory 和 RawConnectionHandler
//! 创建 WebSocket 服务端连接

use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{info, error, warn};

use flare_core::{
    ConnectionConfig, ConnectionType,
    ConnectionEvent, Frame,
    DefConnectionEventHandler,
    FlareError,
};
use flare_core::common::connections::{
    WebSocketConfig, RawConnectionHandler,
};

type Result<T> = std::result::Result<T, FlareError>;


/// WebSocket 服务端
#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志，指定 info 级别
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("启动 WebSocket 服务端");
    info!("=== WebSocket 服务端启动 ===");
    
    // 创建服务端配置
    let config = ConnectionConfig::server(
        "websocket_server".to_string(),
        "127.0.0.1:8080".to_string()
    ).with_type(ConnectionType::WebSocket)
     .with_websocket_config(WebSocketConfig {
         subprotocols: vec!["text".to_string()],
         extensions: vec![],
         compression_threshold: None,
     })
     .with_heartbeat_monitoring(30000, 60000);
    
    info!("服务端配置: {:?}", config);
    info!("监听地址: {}", config.local_addr.as_ref().unwrap());
    
    // 注意：事件处理器现在为每个连接单独创建
    
    // 创建 TCP 监听器
    let listener = TcpListener::bind("127.0.0.1:8080").await
        .map_err(|e| FlareError::connection_failed(format!("绑定端口失败: {}", e)))?;
    
    info!("服务端监听地址: 127.0.0.1:8080");
    info!("等待客户端连接...");
    info!("按 Ctrl+C 停止服务端");
    
    // 使用 select! 来同时监听连接和中断信号
    loop {
        tokio::select! {
            // 监听新的客户端连接
            result = listener.accept() => {
                match result {
                    Ok((tcp_stream, addr)) => {
                        info!("客户端已连接: {}", addr);
                        
                        // 使用回显事件处理器来处理和显示消息
                        let connection_event_handler = Arc::new(DefConnectionEventHandler::default());
                        // 如果想要自动回显消息，可以使用 EchoConnectionEventHandler
                        // let connection_event_handler = Arc::new(EchoConnectionEventHandler::new());
                        
                        // 克隆配置用于新连接
                        let connection_config = config.clone();
                        
                        // 为每个连接创建独立的任务
                        tokio::spawn(async move {
                            match RawConnectionHandler::from_websocket_with_handler(
                                tcp_stream, 
                                connection_config, 
                                Arc::clone(&connection_event_handler) as Arc<dyn ConnectionEvent>
                            ).await {
                                Ok(mut server_connection) => {
                                    info!("WebSocket 服务端连接已建立: {}", addr);
                                    
                                    // 接受连接
                                    if let Err(e) = server_connection.accept().await {
                                        error!("接受连接失败: {}", e);
                                        return;
                                    }
                                    
                                    // SimpleEventHandler 不需要设置连接实例，直接保持连接活跃
                                    // 如果使用 EchoConnectionEventHandler 或 HeartbeatConnectionEventHandler，需要设置连接
                                    // let server_conn_arc = Arc::new(tokio::sync::Mutex::new(server_connection));
                                    // connection_event_handler.set_connection(server_conn_arc).await;
                                    
                                    // 保持连接活跃，等待客户端断开
                                    info!("连接已就绪，等待消息...");
                                    loop {
                                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; // 更频繁的检查
                                        // 检查连接是否还活跃
                                        if !server_connection.is_active().await {
                                            info!("连接已断开: {}", addr);
                                            break;
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("创建服务端连接失败: {} - {}", addr, e);
                                }
                            }
                        });
                    }
                    Err(e) => {
                        error!("接受连接失败: {}", e);
                        // 短暂等待后继续监听
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
            }
            
            // 监听 Ctrl+C 信号
            _ = signal::ctrl_c() => {
                warn!("收到中断信号 (Ctrl+C)，正在优雅关闭服务端...");
                info!("服务端已停止");
                break;
            }
        }
    }
    
    Ok(())
}
