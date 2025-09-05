//! QUIC Protobuf服务端测试
//! 
//! 专门用于测试QUIC连接中使用Protobuf序列化的服务端

use std::sync::Arc;
use tokio::signal;
use tracing::{info, error, warn};

use flare_core::{
    ConnectionConfig, ConnectionType,
    ConnectionEvent, Frame,
    FlareError,
};
use flare_core::common::connections::{
    QuicConfig, RawConnectionHandler
};
use flare_core::common::serialization::SerializationFormat;

type Result<T> = std::result::Result<T, FlareError>;

/// 简单事件处理器
#[derive(Debug)]
pub struct SimpleEventHandler {
    pub name: String,
}

#[async_trait::async_trait]
impl ConnectionEvent for SimpleEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        info!("[{}] 连接已建立: {}", self.name, connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        info!("[{}] 连接已断开: {} - 原因: {}", self.name, connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        error!("[{}] 连接错误: {} - 错误: {}", self.name, connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        let payload = message.get_payload();
        if let Ok(text) = String::from_utf8(payload.to_vec()) {
            println!("📨 [客户端消息] {}", text);
            info!("[{}] 📨 收到客户端消息: {} - 内容: '{}'", self.name, connection_id, text);
        } else {
            println!("📦 [客户端消息] 二进制数据 ({} bytes)", payload.len());
            info!("[{}] 📦 收到二进制消息: {} - 长度: {}", self.name, connection_id, payload.len());
        }
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        let payload = message.get_payload();
        if let Ok(text) = String::from_utf8(payload.to_vec()) {
            info!("[{}] 📤 数据消息已发送 (ID: {}): '{}'", self.name, message.get_message_id(), text);
        } else {
            info!("[{}] 📦 二进制消息已发送 (ID: {}): {} bytes", self.name, message.get_message_id(), payload.len());
        }
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("[{}] 心跳超时: {}", self.name, connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] 连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
    }

    async fn on_heartbeat_sent(&self, connection_id: &str) {
        info!("[{}] 心跳已发送: {}", self.name, connection_id);
    }

    async fn on_heartbeat_received(&self, connection_id: &str) {
        info!("[{}] 收到心跳: {}", self.name, connection_id);
    }

    async fn on_reconnect_started(&self, connection_id: &str, attempt: u32) {
        info!("[{}] 开始重连: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnected(&self, connection_id: &str, attempt: u32) {
        info!("[{}] 重连成功: {} - 尝试次数: {}", self.name, connection_id, attempt);
    }

    async fn on_reconnect_failed(&self, connection_id: &str, attempt: u32, error: &str) {
        info!("[{}] 重连失败: {} - 尝试次数: {} - 错误: {}", self.name, connection_id, attempt, error);
    }

    async fn on_statistics_updated(&self, connection_id: &str, stats: &flare_core::common::connections::traits::ConnectionStats) {
        info!("[{}] 统计信息更新: {} - 收到消息: {} - 发送消息: {}", 
             self.name, connection_id, stats.messages_received, stats.messages_sent);
    }
}

impl SimpleEventHandler {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

/// 创建 QUIC 端点
async fn create_quic_endpoint() -> Result<quinn::Endpoint> {
    use quinn::{Endpoint, ServerConfig};
    use rustls::ServerConfig as RustlsServerConfig;
    use rustls_pemfile::{certs, private_key};
    use std::fs::File;
    use std::io::BufReader;
    
    // 读取证书文件
    let cert_file = File::open("certs/server.crt")
        .map_err(|e| FlareError::connection_failed(format!("无法打开证书文件: {}", e)))?;
    let key_file = File::open("certs/server.key")
        .map_err(|e| FlareError::connection_failed(format!("无法打开私钥文件: {}", e)))?;
    
    // 解析证书
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<_> = certs(&mut cert_reader)
        .map(|cert| cert.map_err(|e| FlareError::connection_failed(format!("证书解析失败: {}", e))))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    
    // 解析私钥
    let mut key_reader = BufReader::new(key_file);
    let key = private_key(&mut key_reader)
        .map_err(|e| FlareError::connection_failed(format!("私钥解析失败: {}", e)))?
        .ok_or_else(|| FlareError::connection_failed("未找到私钥".to_string()))?;
    
    // 创建 TLS 服务器配置
    let rustls_config = RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| FlareError::connection_failed(format!("TLS 配置失败: {}", e)))?;
    
    // 创建 QUIC 服务器配置
    let server_config = ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(rustls_config)
            .map_err(|e| FlareError::connection_failed(format!("QUIC 配置失败: {}", e)))?
    ));
    
    // 绑定端点
    let endpoint = Endpoint::server(server_config, "127.0.0.1:4433".parse().unwrap())
        .map_err(|e| FlareError::connection_failed(format!("QUIC 端点创建失败: {}", e)))?;
    
    Ok(endpoint)
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化 TLS 加密提供程序
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("无法初始化 TLS 加密提供程序");
    
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("🚀 启动QUIC Protobuf服务端");
    
    // 创建服务端配置，使用Protobuf序列化
    let mut config = ConnectionConfig::server(
        "quic_protobuf_server".to_string(),
        "127.0.0.1:4433".to_string()
    ).with_type(ConnectionType::Quic)
     .with_quic_config(QuicConfig {
         max_concurrent_streams: 20,
         initial_stream_window: 65536,
         connection_window: 262144,
         congestion_control: "bbr".to_string(),
     })
     .with_heartbeat_monitoring(5000, 10000)
     .with_tls();
     
    // 设置使用Protobuf序列化
    config.serialization_format = Some(SerializationFormat::Protobuf);
    
    info!("服务端配置: {:?}", config);
    
    // 创建 QUIC 端点
    let endpoint = create_quic_endpoint().await?;
    
    info!("QUIC服务端监听地址: 127.0.0.1:4433 (使用Protobuf序列化)");
    info!("等待客户端连接...");
    info!("按 Ctrl+C 停止服务端");
    
    // 使用 select! 来同时监听连接和中断信号
    loop {
        tokio::select! {
            // 监听新的客户端连接
            incoming = endpoint.accept() => {
                if let Some(connecting) = incoming {
                    let connection_config = config.clone();
                    
                    tokio::spawn(async move {
                        match connecting.await {
                            Ok(quic_connection) => {
                                let remote_addr = quic_connection.remote_address();
                                info!("QUIC客户端已连接: {}", remote_addr);
                                
                                // 使用新的SimpleEventHandler来处理和显示消息
                                let connection_event_handler = Arc::new(SimpleEventHandler::new(
                                    format!("QUIC服务端-{}", remote_addr)
                                ));
                                
                                // 为每个连接创建独立的任务
                                match RawConnectionHandler::from_quic_with_handler(
                                    quic_connection, 
                                    connection_config, 
                                    Arc::clone(&connection_event_handler) as Arc<dyn ConnectionEvent>
                                ).await {
                                    Ok(mut server_connection) => {
                                        info!("QUIC 服务端连接已建立: {}", remote_addr);
                                        
                                        // 接受连接
                                        if let Err(e) = server_connection.accept().await {
                                            error!("接受连接失败: {}", e);
                                            return;
                                        }
                                        
                                        // 保持连接活跃，等待客户端断开
                                        info!("连接已就绪，等待消息...");
                                        loop {
                                            tokio::task::yield_now().await;
                                            // 检查连接是否还活跃
                                            if !server_connection.is_active().await {
                                                info!("连接已断开: {}", remote_addr);
                                                break;
                                            }
                                            
                                            // 处理收到的消息 - 发送回显
                                            if let Ok(Some(frame)) = server_connection.receive_message().await {
                                                info!("收到消息，发送回显...");
                                                if let Err(e) = server_connection.send_message(frame).await {
                                                    error!("发送回显消息失败: {}", e);
                                                }
                                            }
                                            
                                            // 给系统一个微小的处理时间
                                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                                        }
                                    }
                                    Err(e) => {
                                        error!("创建QUIC服务端连接失败: {} - {}", remote_addr, e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("QUIC连接失败: {}", e);
                            }
                        }
                    });
                }
            }
            
            // 监听 Ctrl+C 信号
            _ = signal::ctrl_c() => {
                warn!("收到中断信号 (Ctrl+C)，正在优雅关闭服务端...");
                info!("QUIC服务端已停止");
                break;
            }
        }
    }
    
    Ok(())
}