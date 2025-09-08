//! WebSocket 超低延迟客户端示例
//! 
//! 演示使用最新优化技术的WebSocket客户端：
//! - 零拷贝Bincode序列化器
//! - 自适应LZ4压缩器  
//! - 异步消息Pipeline
//! - 微批处理优化
//! - CPU亲和性绑定

use tracing::{info, error};
use std::{sync::Arc, time::Instant};

use flare_core::{
    ConnectionConfig, ConnectionType,
    ConnectionEvent, Frame,
    FlareError,
};
use flare_core::common::{
    connections::{types::WebSocketConfig},
    protocol::{MessageType, Reliability},
    serialization::BincodeSerializer,
    compression::{Lz4Compressor, CompressionConfig},
    pipeline::AsyncMessagePipeline,
    system::CpuAffinityManager,
};
use flare_core::common::connections::traits::ConnectionFactory;

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
        if message.is_heartbeat() {
            let message_type = message.get_message_type();
            match message_type {
                MessageType::HeartbeatAck => {
                    info!("[{}] 💗 收到心跳确认: {}", self.name, connection_id);
                }
                MessageType::Heartbeat => {
                    info!("[{}] ❤️  收到服务端心跳: {}", self.name, connection_id);
                }
                _ => {
                    info!("[{}] 💓 收到其他心跳消息: {} - 类型: {:?}", self.name, connection_id, message_type);
                }
            }
        } else {
            let payload = message.get_payload();
            if let Ok(text) = String::from_utf8(payload.to_vec()) {
                println!("📨 [服务器回复] {}", text);
                info!("[{}] 📨 收到服务器消息: {} - 内容: {}", self.name, connection_id, text);
            } else {
                println!("📦 [服务器回复] 二进制数据 ({} bytes)", payload.len());
                info!("[{}] 📦 收到二进制消息: {} - 长度: {}", self.name, connection_id, payload.len());
            }
        }
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        info!("[{}] 心跳超时: {}", self.name, connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        info!("[{}] 连接质量变化: {} - 评分: {}", self.name, connection_id, quality_score);
    }

    async fn on_message_sent(&self, connection_id: &str, message: &Frame) {
        if message.is_heartbeat() {
            info!("[{}] ❤️  心跳消息已发送: {} - 类型: {:?}", self.name, connection_id, message.get_message_type());
        } else {
            let payload = message.get_payload();
            if let Ok(text) = String::from_utf8(payload.to_vec()) {
                info!("[{}] 📤 数据消息已成功发送 (ID: {}): '{}'", self.name, message.get_message_id(), text);
            } else {
                info!("[{}] 📦 二进制消息已发送 (ID: {}): {} bytes", self.name, message.get_message_id(), payload.len());
            }
        }
    }

    async fn on_heartbeat_ping(&self, connection_id: &str) {
        info!("[{}] 心跳已发送: {}", self.name, connection_id);
    }

    async fn on_heartbeat_pong(&self, connection_id: &str) {
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

/// WebSocket 超低延迟客户端
#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志，指定 info 级别
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("启动 WebSocket 超低延迟客户端");
    info!("=== WebSocket 超低延迟客户端启动 ===");
    
    // CPU亲和性优化 - 绑定到专用核心
    if let Ok(affinity_mgr) = CpuAffinityManager::new() {
        if let Err(e) = affinity_mgr.bind_current_thread(2) {
            info!("CPU亲和性绑定失败: {}, 继续运行", e);
        } else {
            info!("✅ 已绑定到CPU核心2，获得专用计算资源");
        }
    }
    
    // 创建超低延迟客户端配置
    let config = ConnectionConfig::client(
        "websocket_ultra_low_latency_client".to_string(),
        "ws://127.0.0.1:8080".to_string()
    ).with_type(ConnectionType::WebSocket)
     .with_websocket_config(WebSocketConfig {
         subprotocols: vec!["binary".to_string()],  // 使用二进制协议
         extensions: vec![],
         compression_threshold: Some(128),  // 128字节以上启用压缩
     })
     .with_heartbeat(5000, 2000);  // 5s间隔，2s超时
    
    // 创建超低延迟序列化器和压缩器
    let serializer = Arc::new(BincodeSerializer::new());
    let compressor = Arc::new(Lz4Compressor::ultra_fast());
    
    // 创建异步消息Pipeline
    let pipeline = AsyncMessagePipeline::ultra_low_latency(
        serializer.clone() as Arc<dyn flare_core::common::serialization::FrameSerializer>,
        compressor.clone() as Arc<dyn flare_core::common::compression::Compressor>,
    );
    
    info!("客户端配置: {:?}", config);
    info!("连接地址: {}", config.remote_addr);
    
    // 创建连接工厂
    let factory = flare_core::common::connections::ConnectionFactory::new();
    
    // 创建客户端连接
    let mut client_connection = factory.create_client_connection(config).await?;
    
    // 设置事件处理器
    let event_handler = Arc::new(SimpleEventHandler::new("客户端".to_string()));
    client_connection.set_connection_event_handler(event_handler as Arc<dyn ConnectionEvent>).await;
    
    // 建立连接
    info!("正在连接WebSocket服务端...");
    let connect_start = Instant::now();
    client_connection.connect().await?;
    let connect_time = connect_start.elapsed();
    info!("✅ 已连接到WebSocket服务端！连接耗时: {:.2}ms", connect_time.as_secs_f64() * 1000.0);
    
    // 优化：更短的稳定时间
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // 启动优化的心跳任务
    let client_connection_heartbeat = Arc::new(tokio::sync::Mutex::new(client_connection));
    let heartbeat_connection = Arc::clone(&client_connection_heartbeat);
    let heartbeat_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(5000)); // 5秒心跳间隔
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            
            let mut conn = heartbeat_connection.lock().await;
            
            // 发送心跳消息
            let heartbeat_frame = Frame::heartbeat();
            if let Err(e) = conn.send_message(heartbeat_frame).await {
                error!("心跳发送失败: {}", e);
                break;
            } else {
                info!("💗 心跳已发送");
            }
            
            // 调用连接的心跳方法更新活跃状态
            if let Err(e) = conn.send_heartbeat().await {
                error!("心跳状态更新失败: {}", e);
            }
        }
    });
    
    // 启动用户输入处理
    println!("\n==== WebSocket 客户端控制台 ====");
    println!("📝 请输入要发送的消息:");
    println!("💡 命令说明:");
    println!("   - 输入任意文本发送消息");
    println!("   - 输入 'quit' 或 'exit' 退出程序");
    println!("   - 输入 'help' 查看帮助");
    println!("   - 输入 'status' 查看连接状态");
    println!("================================\n");
    
    let mut message_counter = 1u64;
    
    // 使用 tokio::task::spawn_blocking 处理阻塞的输入操作
    loop {
        // 在阻塞任务中处理用户输入
        let input = tokio::task::spawn_blocking(|| {
            let mut input = String::new();
            print!("📩 请输入消息 > ");
            use std::io::{self, Write};
            io::stdout().flush().ok();
            match std::io::stdin().read_line(&mut input) {
                Ok(_) => input.trim().to_string(),
                Err(e) => {
                    eprintln!("❌ 读取输入失败: {}", e);
                    String::new()
                }
            }
        }).await.map_err(|e| FlareError::general_error(format!("输入任务失败: {}", e)))?;
        
        // 处理特殊命令
        match input.as_str() {
            "quit" | "exit" => {
                println!("👋 用户请求退出，正在关闭连接...");
                break;
            }
            "help" => {
                println!("\n📖 帮助信息:");
                println!("   quit/exit - 退出程序");
                println!("   help      - 显示此帮助");
                println!("   status    - 显示连接状态");
                println!("   其他      - 发送文本消息\n");
                continue;
            }
            "status" => {
                let conn = client_connection_heartbeat.lock().await;
                let is_active = conn.is_active().await;
                println!("\n📊 连接状态: {}", if is_active { "✅ 活跃" } else { "❌ 断开" });
                println!("🔗 服务器地址: ws://127.0.0.1:8080");
                println!("📈 已发送消息数: {}\n", message_counter - 1);
                continue;
            }
            "" => {
                println!("⚠️  请输入有效的消息内容");
                continue;
            }
            _ => {}
        }
        
        // 发送用户消息 - 使用优化的Pipeline处理
        if !input.is_empty() {
            println!("📤 正在发送消息: '{}'", input);
            
            // 创建统一协议消息
            let message = Frame::new(
                MessageType::Data,
                message_counter,
                Reliability::AtLeastOnce,
                input.as_bytes().to_vec(),
            );
            
            // 通过异步Pipeline处理消息
            let pipeline_start = Instant::now();
            match pipeline.process_async(message.clone()).await {
                Ok(()) => {
                    println!("✅ Pipeline处理完成 (#{})", message_counter);
                    
                    // 通过Connection trait发送已处理的消息
                    let mut conn = client_connection_heartbeat.lock().await;
                    match conn.send_message(message).await {
                        Ok(_) => {
                            println!("📡 消息已发送 (#{})\n", message_counter);
                            message_counter += 1;
                        }
                        Err(e) => {
                            println!("❌ 消息发送失败: {}\n", e);
                            error!("发送消息失败: {}", e);
                            break;
                        }
                    }
                }
                Err(e) => {
                    println!("❌ Pipeline处理失败: {}\n", e);
                    error!("Pipeline处理失败: {}", e);
                    
                    // 回退到直接发送
                    let mut conn = client_connection_heartbeat.lock().await;
                    match conn.send_message(message).await {
                        Ok(_) => {
                            println!("📡 消息已回退发送 (#{})\n", message_counter);
                            message_counter += 1;
                        }
                        Err(e) => {
                            println!("❌ 回退发送也失败: {}\n", e);
                            error!("回退发送失败: {}", e);
                            break;
                        }
                    }
                }
            }
        }
    }
    
    // 停止心跳任务
    heartbeat_task.abort();
    
    // 断开连接
    info!("正在断开连接...");
    let mut conn = client_connection_heartbeat.lock().await;
    conn.disconnect().await?;
    
    info!("客户端已断开");
    Ok(())
}
