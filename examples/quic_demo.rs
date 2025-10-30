//! QUIC 综合能力演示
//!
//! 本示例展示 flare-core QUIC 的所有核心能力：
//! 
//! **核心功能**：
//! - QUIC 服务端监听和接受连接
//! - QUIC 客户端连接和消息发送
//! - Protobuf 序列化格式 - 高效紧凑，适合生产环境
//! - TLS 加密通信（自签名证书）
//!
//! **高级功能**：
//! - 🗜️  **消息压缩** - Snappy/Gzip 高压缩率
//! - 🚦 **流量控制** - 分层限流器（连接级+全局级）
//! - 📦 **批量处理** - 批量编解码提升性能
//! - 🌊 **流式解析** - 支持大消息分片传输
//! - 📊 **统计监控** - 实时性能和流控数据
//! - 🎯 **错误处理** - 完善的错误分类和恢复

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use tokio::sync::Notify;
use std::net::SocketAddr;
use serde::{Serialize, Deserialize};

use flare_core::common::connections::ratelimit::{TokenBucket, HierarchicalRateLimiter, BackpressureController};
use flare_core::common::error::FlareError;
use flare_core::common::parsing::{MessageParser, PayloadCodec};
use flare_core::common::compression::{
    CompressionConfig, CompressionAlgorithm, CompressionLevel, compress, decompress
};

use quinn::{Endpoint, ServerConfig};

/// 示例消息结构 - 使用 Protobuf 序列化（注：当前使用 JSON 作为 fallback）
#[derive(Debug, Clone, Serialize, Deserialize)]
struct QuicMessage {
    /// 消息 ID
    id: u32,
    /// 消息类型
    msg_type: String,
    /// 消息内容
    content: String,
    /// 时间戳
    timestamp: u64,
    /// 序列号
    sequence: u32,
    /// 消息大小（字节）
    size: usize,
    /// 是否已压缩
    compressed: bool,
}

impl QuicMessage {
    fn new(id: u32, msg_type: String, content: String, sequence: u32) -> Self {
        let size = content.len();
        Self {
            id,
            msg_type,
            content,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            sequence,
            size,
            compressed: false,
        }
    }
    
    fn with_compression(mut self, compressed: bool) -> Self {
        self.compressed = compressed;
        self
    }
}

// 全局限流器（所有连接共享）
static GLOBAL_LIMITER: OnceLock<TokenBucket> = OnceLock::new();

fn get_global_limiter() -> &'static TokenBucket {
    GLOBAL_LIMITER.get_or_init(|| TokenBucket::new(1000, 500))
}

/// QUIC 服务端
async fn run_quic_server(
    shutdown: Arc<Notify>, 
    cert_ready: Arc<Notify>,
    global_limiter: &'static TokenBucket,
) -> Result<(), FlareError> {
    // 使用生成的证书而不是自签名证书
    println!("📜 使用生成的证书...");
    
    // 读取生成的证书和私钥
    let cert_path = "certs/server.crt";
    let key_path = "certs/server.key";
    
    let cert_pem = std::fs::read_to_string(cert_path)
        .map_err(|e| FlareError::connection_failed(format!("读取证书失败: {}", e)))?;
    let key_pem = std::fs::read_to_string(key_path)
        .map_err(|e| FlareError::connection_failed(format!("读取私钥失败: {}", e)))?;
    
    // 解析证书
    let cert = rustls_pemfile::certs(&mut cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| FlareError::connection_failed(format!("解析证书失败: {}", e)))?;

    // 解析私钥
    let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
        .map_err(|e| FlareError::connection_failed(format!("读取私钥失败: {}", e)))?
        .ok_or_else(|| FlareError::connection_failed("私钥为空".to_string()))?;

    // 配置 TLS
    let server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            cert,
            key,
        )
        .map_err(|e| FlareError::connection_failed(format!("配置TLS失败: {}", e)))?;

    // 配置 QUIC 
    let mut server_config = ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)
            .map_err(|e| FlareError::connection_failed(format!("创建QUIC配置失败: {}", e)))?
    ));
    
    let mut transport_config = quinn::TransportConfig::default();
    transport_config.max_concurrent_bidi_streams(100u32.into());
    transport_config.max_concurrent_uni_streams(100u32.into());
    server_config.transport_config(Arc::new(transport_config));

    // 启动服务端
    let addr: SocketAddr = "127.0.0.1:5000".parse()
        .map_err(|e| FlareError::connection_failed(format!("解析地址失败: {}", e)))?;
    
    let endpoint = Endpoint::server(server_config, addr)
        .map_err(|e| FlareError::connection_failed(format!("创建QUIC服务端失败: {}", e)))?;

    println!("🚀 QUIC 服务端启动在 127.0.0.1:5000\n");

    // 保存证书供客户端使用
    std::fs::write("/tmp/flare_quic_cert.pem", cert_pem)
        .map_err(|e| FlareError::connection_failed(format!("保存证书失败: {}", e)))?;
    
    // 通知客户端证书已准备好
    cert_ready.notify_one();
    
    // 接受连接
    let shutdown_clone = Arc::clone(&shutdown);
    let server_task = tokio::spawn(async move {
        // 创建 Protobuf 消息解析器
        let parser = MessageParser::new(PayloadCodec::Protobuf);
        
        // 配置压缩（QUIC 适合使用 Snappy - 平衡速度和压缩率）
        let compression = CompressionConfig::new(CompressionAlgorithm::Snappy)
            .with_level(CompressionLevel::Default)
            .with_min_size(256); // 256字节以上才压缩
        
        loop {
            tokio::select! {
                Some(connecting) = endpoint.accept() => {
                    let parser = parser.clone();
                    let compression = compression.clone();
                    let global_limiter = global_limiter;
                    
                    tokio::spawn(async move {
                        let connection = match connecting.await {
                            Ok(conn) => conn,
                            Err(e) => {
                                eprintln!("QUIC 连接失败: {}", e);
                                return;
                            }
                        };

                        let remote = connection.remote_address();
                        println!("🔗 新的 QUIC 连接: {}", remote);
                        
                        // 创建连接级别的限流器和统计
                        let rate_limiter = Arc::new(HierarchicalRateLimiter::new(100, Some(global_limiter)));
                        let backpressure = Arc::new(BackpressureController::new(80, 20));
                        let msg_count = Arc::new(AtomicU64::new(0));
                        let bytes_saved = Arc::new(AtomicU64::new(0));

                        // 接受双向流
                        loop {
                            match connection.accept_bi().await {
                                Ok((mut send, mut recv)) => {
                                    let parser = parser.clone();
                                    let compression = compression.clone();
                                    let rate_limiter = Arc::clone(&rate_limiter);
                                    let backpressure = Arc::clone(&backpressure);
                                    let msg_count = Arc::clone(&msg_count);
                                    let bytes_saved = Arc::clone(&bytes_saved);
                                    
                                    tokio::spawn(async move {
                                        // 读取消息
                                        match recv.read_to_end(65536).await {
                                            Ok(data) => {
                                                if data.is_empty() {
                                                    return;
                                                }
                                                
                                                // 流量控制
                                                if !rate_limiter.try_acquire(1) {
                                                    println!("⚠️  [服务端] 请求被限流");
                                                    let _ = send.finish();
                                                    return;
                                                }
                                                
                                                // 更新背压
                                                let count = msg_count.fetch_add(1, Ordering::Relaxed) + 1;
                                                backpressure.update_load(count, 100);
                                                
                                                // 尝试解压缩
                                                let payload = if data.len() > 100 {
                                                    decompress(&data, &compression).unwrap_or_else(|_| data.clone())
                                                } else {
                                                    data.clone()
                                                };
                                                
                                                // 使用 Protobuf 解析
                                                match parser.codec().decode::<QuicMessage>(&payload) {
                                                    Ok(msg) => {
                                                        println!(
                                                            "📥 [服务端] 收到 [Protobuf]: #{} - {} (序列: {})",
                                                            msg.id, msg.content, msg.sequence
                                                        );

                                                        // 构造响应消息
                                                        let mut response_msg = QuicMessage::new(
                                                            msg.id + 1000,
                                                            "response".to_string(),
                                                            format!("Echo: {} | Stats: {} msgs processed, {} bytes saved", 
                                                                msg.content,
                                                                msg_count.load(Ordering::Relaxed),
                                                                bytes_saved.load(Ordering::Relaxed)),
                                                            msg.sequence,
                                                        );
                                                        
                                                        // 使用 Protobuf 编码
                                                        match parser.codec().encode(&response_msg) {
                                                            Ok(mut response_bytes) => {
                                                                // 应用压缩
                                                                let original_size = response_bytes.len();
                                                                if compression.should_compress(original_size) {
                                                                    if let Ok(compressed) = compress(&response_bytes, &compression) {
                                                                        let saved = original_size.saturating_sub(compressed.len());
                                                                        bytes_saved.fetch_add(saved as u64, Ordering::Relaxed);
                                                                        println!("🗜️  [服务端] 压缩响应: {} -> {} 字节 ({:.1}%)",
                                                                            original_size, compressed.len(),
                                                                            (compressed.len() as f64 / original_size as f64) * 100.0);
                                                                        response_msg = response_msg.with_compression(true);
                                                                        response_bytes = compressed;
                                                                    }
                                                                }
                                                                
                                                                // 发送响应并等待完成
                                                                match send.write_all(&response_bytes).await {
                                                                    Ok(_) => {
                                                                        // 正确完成发送流
                                                                        match send.finish() {
                                                                            Ok(_) => {
                                                                                println!(
                                                                                    "📤 [服务端] 回复 [Protobuf]: {} ({} 字节)",
                                                                                    response_msg.content, response_bytes.len()
                                                                                );
                                                                            }
                                                                            Err(e) => {
                                                                                eprintln!("⚠️  [服务端] 完成发送失败: {}", e);
                                                                            }
                                                                        }
                                                                    }
                                                                    Err(e) => {
                                                                        eprintln!("❌ [服务端] 发送响应失败: {}", e);
                                                                    }
                                                                }
                                                            }
                                                            Err(e) => {
                                                                eprintln!("❌ [服务端] Protobuf 编码失败: {:?}", e);
                                                            }
                                                        }
                                                    }
                                                    Err(_) => {
                                                        // Fallback 到普通字符串
                                                        let msg = String::from_utf8_lossy(&data);
                                                        println!("📥 [服务端] 收到: {}", msg);
                                                        let response = format!("Echo: {}", msg);
                                                        
                                                        // 正确处理发送和关闭
                                                        if let Ok(_) = send.write_all(response.as_bytes()).await {
                                                            let _ = send.finish();
                                                        }
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                eprintln!("读取数据失败: {}", e);
                                            }
                                        }
                                    });
                                }
                                Err(e) => {
                                    eprintln!("接受流失败: {}", e);
                                    break;
                                }
                            }
                        }
                    });
                }
                _ = shutdown_clone.notified() => {
                    break;
                }
            }
        }
    });

    // 等待关闭信号
    shutdown.notified().await;
    println!("\n🛑 QUIC 服务端关闭");
    
    let _ = server_task.await;
    
    Ok(())
}

/// QUIC 客户端
async fn run_quic_client(
    cert_ready: Arc<Notify>,
    global_limiter: &'static TokenBucket,
) -> Result<(), FlareError> {
    // 等待服务端启动并证书准备好
    cert_ready.notified().await;

    println!("🔌 QUIC 客户端连接中...\n");

    // 创建 Protobuf 消息解析器
    let parser = MessageParser::new(PayloadCodec::Protobuf);

    // 读取服务端证书
    let cert_pem = std::fs::read_to_string("/tmp/flare_quic_cert.pem")
        .map_err(|e| FlareError::connection_failed(format!("读取证书失败: {}", e)))?;
    
    let cert = rustls_pemfile::certs(&mut cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| FlareError::connection_failed(format!("解析证书失败: {}", e)))?;

    // 配置客户端 TLS
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(cert[0].clone())
        .map_err(|e| FlareError::connection_failed(format!("添加证书失败: {}", e)))?;

    let client_crypto = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let mut client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto)
            .map_err(|e| FlareError::connection_failed(format!("创建QUIC配置失败: {}", e)))?
    ));
    
    client_config.transport_config(Arc::new({
        let mut config = quinn::TransportConfig::default();
        config.max_concurrent_bidi_streams(100u32.into());
        config
    }));

    // 创建客户端 Endpoint
    let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())
        .map_err(|e| FlareError::connection_failed(format!("创建 Endpoint 失败: {}", e)))?;
    
    endpoint.set_default_client_config(client_config);

    // 连接到服务端
    let connection = endpoint.connect("127.0.0.1:5000".parse().unwrap(), "localhost")
        .map_err(|e| FlareError::connection_failed(format!("连接失败: {}", e)))?
        .await
        .map_err(|e| FlareError::connection_failed(format!("连接握手失败: {}", e)))?;

    println!("✅ QUIC 连接建立成功\n");
    
    // 创建分层限流器
    let rate_limiter = Arc::new(HierarchicalRateLimiter::new(50, Some(global_limiter)));
    let backpressure = Arc::new(BackpressureController::new(80, 20));
    let msg_count = Arc::new(AtomicU64::new(0));
    let bytes_saved = Arc::new(AtomicU64::new(0));
    
    // 配置压缩
    let compression = CompressionConfig::new(CompressionAlgorithm::Snappy)
        .with_level(CompressionLevel::Default)
        .with_min_size(256);
    
    println!("📝 使用 Protobuf 序列化格式（JSON fallback）");
    println!("🗜️  压缩算法: Snappy (阈值: 256 字节)");
    println!("🚦 限流配置: 连接级(50/s) + 全局级(500/s)\n");

    // 演示1: 发送普通消息（带压缩和流控）
    println!("\n=== 演示1: 发送普通消息（自动压缩） ===");
    for i in 1..=3 {
        let mut quic_msg = QuicMessage::new(
            i,
            "data".to_string(),
            format!("QUIC message #{} with compression and rate limiting features enabled", i),
            i,
        );
        
        // 使用 Protobuf 编码
        let mut message_bytes = parser.codec().encode(&quic_msg)
            .map_err(|e| FlareError::serialization_error(format!("Protobuf 编码失败: {}", e)))?;
        
        // 应用压缩
        let original_size = message_bytes.len();
        if compression.should_compress(original_size) {
            if let Ok(compressed) = compress(&message_bytes, &compression) {
                let saved = original_size.saturating_sub(compressed.len());
                bytes_saved.fetch_add(saved as u64, Ordering::Relaxed);
                println!("🗜️  压缩消息 #{}: {} -> {} 字节 ({:.1}%)",
                    i, original_size, compressed.len(),
                    (compressed.len() as f64 / original_size as f64) * 100.0);
                quic_msg = quic_msg.with_compression(true);
                message_bytes = compressed;
            }
        }
        
        // 流量控制检查
        if !rate_limiter.try_acquire(1) {
            println!("⚠️  消息 #{} 被限流，跳过", i);
            continue;
        }
        
        msg_count.fetch_add(1, Ordering::Relaxed);
        
        // 打开双向流
        let (mut send, mut recv) = connection.open_bi().await
            .map_err(|e| FlareError::connection_failed(format!("打开流失败: {}", e)))?;

        // 发送消息
        send.write_all(&message_bytes).await
            .map_err(|e| FlareError::connection_failed(format!("发送失败: {}", e)))?;
        
        let compressed_marker = if quic_msg.compressed { " 🗜️" } else { "" };
        println!("📤 [客户端] 发送{} [Protobuf]: #{} - {} ({} 字节)", 
            compressed_marker, quic_msg.id, quic_msg.content, message_bytes.len());
        
        // 完成发送（半关闭流）
        send.finish()
            .map_err(|e| FlareError::connection_failed(format!("完成发送失败: {}", e)))?;

        // 接收响应（增加超时保护）
        let response = tokio::time::timeout(
            tokio::time::Duration::from_secs(5),
            recv.read_to_end(65536)
        ).await
            .map_err(|_| FlareError::connection_failed("接收响应超时".to_string()))?
            .map_err(|e| FlareError::connection_failed(format!("接收失败: {}", e)))?;
        
        // 尝试解压缩
        let response_payload = if response.len() > 100 {
            decompress(&response, &compression).unwrap_or_else(|_| response.clone())
        } else {
            response.clone()
        };
        
        // 解析响应
        match parser.codec().decode::<QuicMessage>(&response_payload) {
            Ok(response_msg) => {
                let compressed_marker = if response_msg.compressed { " 🗜️" } else { "" };
                println!(
                    "📥 [客户端] 收到{} [Protobuf]: #{} - {}",
                    compressed_marker, response_msg.id, response_msg.content
                );
            }
            Err(_) => {
                let response_str = String::from_utf8_lossy(&response_payload);
                println!("📥 [客户端] 收到: {}", response_str);
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
    
    // 演示2: 批量消息处理
    println!("\n=== 演示2: 批量消息处理 ===");
    let mut batch_messages = Vec::new();
    for i in 10..=13 {
        let msg = QuicMessage::new(
            i,
            "batch".to_string(),
            format!("Batch message #{}", i),
            i,
        );
        batch_messages.push(msg);
    }
    
    // 批量编码
    let mut batch_bytes = Vec::new();
    for msg in &batch_messages {
        let bytes = parser.codec().encode(msg)?;
        batch_bytes.push(bytes);
    }
    println!("📦 批量编码了 {} 条消息", batch_bytes.len());
    
    // 发送批量消息（每个消息使用独立的流）
    for (i, bytes) in batch_bytes.iter().enumerate() {
        if !rate_limiter.try_acquire(1) {
            println!("⚠️  批量消息 #{} 被限流", i);
            continue;
        }
        
        msg_count.fetch_add(1, Ordering::Relaxed);
        
        // 每个消息使用独立的流，并等待服务端响应
        let (mut send, mut recv) = connection.open_bi().await
            .map_err(|e| FlareError::connection_failed(format!("打开流失败: {}", e)))?;
        send.write_all(bytes).await
            .map_err(|e| FlareError::connection_failed(format!("发送失败: {}", e)))?;
        send.finish()
            .map_err(|e| FlareError::connection_failed(format!("完成发送失败: {}", e)))?;
        println!("📤 发送批量消息 #{} ({} 字节)", i + 10, bytes.len());
        
        // 等待并读取服务端响应（带超时保护）
        match tokio::time::timeout(
            tokio::time::Duration::from_secs(3),
            recv.read_to_end(65536)
        ).await {
            Ok(Ok(response)) if !response.is_empty() => {
                // 尝试解析响应
                let response_payload = if response.len() > 100 {
                    decompress(&response, &compression).unwrap_or_else(|_| response.clone())
                } else {
                    response.clone()
                };
                
                if let Ok(response_msg) = parser.codec().decode::<QuicMessage>(&response_payload) {
                    println!("   ✅ 收到批量响应: {}", response_msg.content.chars().take(50).collect::<String>());
                }
            }
            Ok(Ok(_)) => {
                // 空响应，忽略
            }
            Ok(Err(e)) => {
                eprintln!("   ⚠️  接收批量响应失败: {}", e);
            }
            Err(_) => {
                eprintln!("   ⚠️  接收批量响应超时");
            }
        }
        
        // 给服务端一些处理时间
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    
    // 演示3: 大消息压缩测试
    println!("\n=== 演示3: 大消息压缩效果 ===");
    let large_msg = QuicMessage::new(
        100,
        "large".to_string(),
        "Large message content ".repeat(100), // 重复内容，易压缩
        100,
    );
    
    let mut large_bytes = parser.codec().encode(&large_msg)?;
    let original_size = large_bytes.len();
    
    if let Ok(compressed) = compress(&large_bytes, &compression) {
        let ratio = (compressed.len() as f64 / original_size as f64) * 100.0;
        let saved = original_size.saturating_sub(compressed.len());
        bytes_saved.fetch_add(saved as u64, Ordering::Relaxed);
        println!("🗜️  大消息压缩: {} -> {} 字节 (压缩率: {:.1}%)",
            original_size, compressed.len(), ratio);
        large_bytes = compressed;
    }
    
    msg_count.fetch_add(1, Ordering::Relaxed);
    
    let (mut send, mut recv) = connection.open_bi().await
        .map_err(|e| FlareError::connection_failed(format!("打开流失败: {}", e)))?;
    send.write_all(&large_bytes).await
        .map_err(|e| FlareError::connection_failed(format!("发送失败: {}", e)))?;
    send.finish()
        .map_err(|e| FlareError::connection_failed(format!("完成发送失败: {}", e)))?;
    println!("📤 发送大消息 (压缩后 {} 字节)", large_bytes.len());
    
    // 等待并读取服务端响应（带超时保护）
    match tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        recv.read_to_end(65536)
    ).await {
        Ok(Ok(response)) if !response.is_empty() => {
            let response_payload = if response.len() > 100 {
                decompress(&response, &compression).unwrap_or_else(|_| response.clone())
            } else {
                response.clone()
            };
            
            if let Ok(response_msg) = parser.codec().decode::<QuicMessage>(&response_payload) {
                println!("📥 [客户端] 大消息响应: {}", response_msg.content.chars().take(80).collect::<String>());
            }
        }
        Ok(Ok(_)) => {
            println!("   ℹ️  收到空响应");
        }
        Ok(Err(e)) => {
            eprintln!("   ⚠️  接收大消息响应失败: {}", e);
        }
        Err(_) => {
            eprintln!("   ⚠️  接收大消息响应超时");
        }
    }
    
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // 打印最终统计
    let parser_stats = parser.get_stats();
    
    println!("\n📊 [客户端最终统计]");
    println!("  消息统计:");
    println!("    发送: {} 条消息", msg_count.load(Ordering::Relaxed));
    println!("    解析: {} 条成功, {} 条失败", parser_stats.parsed_count, parser_stats.failed_count);
    println!("  压缩统计:");
    println!("    节省字节: {} bytes", bytes_saved.load(Ordering::Relaxed));
    println!("  流控统计:");
    println!("    全局可用: {} tokens", global_limiter.available());
    println!("    背压状态: {}", if backpressure.should_apply() { "触发" } else { "正常" });
    
    // 关闭连接（给服务端时间处理最后的消息）
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    connection.close(0u32.into(), b"done");
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), FlareError> {
    // 初始化 rustls CryptoProvider
    let _ = rustls::crypto::ring::default_provider().install_default();
    
    println!("╔════════════════════════════════════════════════════════╗");
    println!("║  Flare QUIC 综合能力演示                              ║");
    println!("╚════════════════════════════════════════════════════════╝\n");
    println!("🎯 本演示展示以下能力:");
    println!("   ✅ Protobuf 序列化 - 高效紧凑");
    println!("   ✅ Snappy 压缩 - 平衡速度和压缩率");
    println!("   ✅ 分层限流 - 连接级+全局级");
    println!("   ✅ 背压控制 - 智能流控");
    println!("   ✅ 批量处理 - 提升吞吐");
    println!("   ✅ 统计监控 - 实时观测");
    println!("   ⚠️  注：当前使用 JSON 作为 Protobuf 的 fallback 实现\n");

    let shutdown = Arc::new(Notify::new());
    let shutdown_clone = Arc::clone(&shutdown);
    let cert_ready = Arc::new(Notify::new());
    let cert_ready_clone = Arc::clone(&cert_ready);

    // 启动服务端
    let server_handle = tokio::spawn(async move {
        run_quic_server(shutdown_clone, cert_ready_clone, get_global_limiter()).await
    });

    // 启动客户端
    let client_handle = tokio::spawn(async move {
        run_quic_client(cert_ready, get_global_limiter()).await
    });

    // 等待客户端完成
    match client_handle.await {
        Ok(Ok(_)) => println!("\n✅ 客户端执行成功"),
        Ok(Err(e)) => eprintln!("\n❌ 客户端错误: {:?}", e),
        Err(e) => eprintln!("\n❌ 客户端任务错误: {:?}", e),
    }

    // 等待一段时间后关闭服务端
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    shutdown.notify_one();

    let _ = server_handle.await;

    // 清理临时证书文件
    let _ = std::fs::remove_file("/tmp/flare_quic_cert.pem");

    // 打印全局限流器最终状态
    println!("\n🌐 全局限流器最终状态:");
    println!("   可用令牌: {}", get_global_limiter().available());
    
    println!("\n✅ QUIC 综合演示完成!");
    Ok(())
}
