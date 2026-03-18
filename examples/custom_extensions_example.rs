//! 自定义扩展示例
//!
//! 演示如何实现和使用自定义的加密、压缩和序列化格式
//!
//! ## 功能说明
//!
//! 本示例展示了如何：
//! 1. 实现自定义加密算法（XOR 加密作为示例）
//! 2. 实现自定义压缩算法（简单的 RLE 压缩作为示例）
//! 3. 实现自定义序列化格式（MessagePack 作为示例）
//! 4. 在客户端和服务端注册并使用这些自定义扩展
//!
//! ## 运行示例
//!
//! ```bash
//! # 终端 1：启动服务端
//! RUST_LOG=info cargo run --example custom_extensions_example -- server
//!
//! # 终端 2：启动客户端
//! RUST_LOG=info cargo run --example custom_extensions_example -- client
//! ```

use async_trait::async_trait;
use flare_core::client::{FlareClientBuilder, MessageListener};
use flare_core::common::compression::{CompressionAlgorithm, CompressionUtil, Compressor};
use flare_core::common::encryption::{EncryptionAlgorithm, EncryptionUtil, Encryptor};
use flare_core::common::error::{FlareError, Result};
use flare_core::common::protocol::{Frame, SerializationFormat};
use flare_core::common::serializer::{SerializationUtil, Serializer};
use flare_core::server::*;
use std::sync::Arc;
use tracing::info;

// ============================================================================
// 1. 自定义加密算法：XOR 加密（仅用于演示，不推荐生产使用）
// ============================================================================

/// XOR 加密器（简单示例，不推荐生产使用）
///
/// 使用 XOR 加密算法，密钥为固定字节
pub struct XorEncryptor {
    key: Vec<u8>,
}

impl XorEncryptor {
    pub fn new(key: &[u8]) -> Self {
        Self { key: key.to_vec() }
    }
}

impl Encryptor for XorEncryptor {
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut encrypted = Vec::with_capacity(data.len());
        for (i, byte) in data.iter().enumerate() {
            encrypted.push(byte ^ self.key[i % self.key.len()]);
        }
        Ok(encrypted)
    }

    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        // XOR 加密和解密是相同的操作
        self.encrypt(data)
    }

    fn algorithm(&self) -> EncryptionAlgorithm {
        EncryptionAlgorithm::Custom("xor".to_string())
    }

    fn name(&self) -> &'static str {
        "xor"
    }
}

// ============================================================================
// 2. 自定义压缩算法：RLE (Run-Length Encoding) 压缩
// ============================================================================

/// RLE 压缩器（简单示例）
///
/// 使用简单的游程编码压缩算法
pub struct RleCompressor;

impl Compressor for RleCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(vec![]);
        }

        let mut compressed = Vec::new();
        let mut current_byte = data[0];
        let mut count = 1u8;

        for &byte in &data[1..] {
            if byte == current_byte && count < 255 {
                count += 1;
            } else {
                compressed.push(count);
                compressed.push(current_byte);
                current_byte = byte;
                count = 1;
            }
        }
        compressed.push(count);
        compressed.push(current_byte);

        Ok(compressed)
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(vec![]);
        }

        if data.len() % 2 != 0 {
            return Err(FlareError::deserialization_error(
                "Invalid RLE data: length must be even".to_string(),
            ));
        }

        let mut decompressed = Vec::new();
        for chunk in data.chunks(2) {
            let count = chunk[0] as usize;
            let byte = chunk[1];
            decompressed.extend(std::iter::repeat_n(byte, count));
        }

        Ok(decompressed)
    }

    fn algorithm(&self) -> CompressionAlgorithm {
        CompressionAlgorithm::Custom("rle".to_string())
    }

    fn name(&self) -> &'static str {
        "rle"
    }

    fn can_detect(&self, _data: &[u8]) -> bool {
        // RLE 数据总是偶数长度，且前两个字节通常是 [count, byte]
        // 这里简化处理，实际应该更严格
        false
    }
}

// ============================================================================
// 3. 自定义序列化格式：MessagePack（需要 rmp-serde 依赖）
// ============================================================================

/// MessagePack 序列化器
///
/// 使用 MessagePack 格式进行序列化（需要添加 rmp-serde 依赖）
/// 注意：这里使用 JSON 作为占位符，实际应该使用 rmp-serde
pub struct MessagePackSerializer;

impl Serializer for MessagePackSerializer {
    fn serialize(&self, frame: &Frame) -> Result<Vec<u8>> {
        // 注意：这里使用 JSON 作为占位符
        // 实际实现应该使用 rmp-serde 进行 MessagePack 序列化
        // 为了示例完整性，这里使用 JSON
        let json_serializer = SerializationUtil::get_serializer_by_name("json")
            .ok_or_else(|| FlareError::encoding_error("JSON serializer not found".to_string()))?;
        json_serializer.serialize(frame)
    }

    fn deserialize(&self, data: &[u8]) -> Result<Frame> {
        // 注意：这里使用 JSON 作为占位符
        // 实际实现应该使用 rmp-serde 进行 MessagePack 反序列化
        let json_serializer =
            SerializationUtil::get_serializer_by_name("json").ok_or_else(|| {
                FlareError::deserialization_error("JSON serializer not found".to_string())
            })?;
        json_serializer.deserialize(data)
    }

    fn format(&self) -> SerializationFormat {
        // 注意：由于 SerializationFormat 是 protobuf 生成的枚举，无法直接扩展
        // 这里返回 Json 作为占位符，实际应该通过 metadata 传递自定义格式名称
        SerializationFormat::Json
    }

    fn name(&self) -> &'static str {
        "messagepack"
    }

    fn can_detect(&self, data: &[u8]) -> bool {
        // MessagePack 格式检测：通常以 0x82-0x8F 开头（map）
        // 这里简化处理
        !data.is_empty() && (data[0] >= 0x80 && data[0] <= 0x8F)
    }
}

// ============================================================================
// 4. 服务端实现
// ============================================================================

struct CustomExtensionsServer;

#[async_trait]
impl ServerEventHandler for CustomExtensionsServer {
    async fn handle_message(
        &self,
        command: &flare_core::common::protocol::PayloadCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        // 尝试解析protobuf消息内容
        let payload_str = match String::from_utf8(command.payload.clone()) {
            Ok(text) => text,
            Err(_) => {
                // 如果不是有效的UTF-8，则显示十六进制调试信息
                format!("<protobuf_binary_data: {} bytes>", command.payload.len())
            }
        };
        info!(
            "📨 [服务端] 收到消息: connection_id={}, payload={}",
            connection_id, payload_str
        );
        Ok(None)
    }

    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        info!("✅ [服务端] 新连接: {}", connection_id);
        Ok(())
    }

    async fn on_disconnect(&self, connection_id: &str, _reason: Option<&str>) -> Result<()> {
        info!("❌ [服务端] 连接断开: {}", connection_id);
        Ok(())
    }
}

async fn run_server() -> Result<()> {
    info!("🚀 启动自定义扩展服务端示例");

    // 1. 注册自定义加密器
    let xor_key = b"my_secret_key_1234567890123456"; // 32 bytes
    let xor_encryptor = XorEncryptor::new(xor_key);
    EncryptionUtil::register_custom(Arc::new(xor_encryptor));
    info!("✅ 已注册自定义加密器: XOR");

    // 2. 注册自定义压缩器
    let rle_compressor = RleCompressor;
    CompressionUtil::register_custom(Arc::new(rle_compressor));
    info!("✅ 已注册自定义压缩器: RLE");

    // 3. 注册自定义序列化器
    let msgpack_serializer = MessagePackSerializer;
    SerializationUtil::register_custom(Arc::new(msgpack_serializer));
    info!("✅ 已注册自定义序列化器: MessagePack");

    // 4. 创建服务器
    let server = FlareServerBuilder::new("0.0.0.0:8080", Arc::new(CustomExtensionsServer))
        .with_default_format(SerializationFormat::Json) // 使用 JSON 作为默认格式
        .with_default_compression(CompressionAlgorithm::Custom("rle".to_string())) // 使用自定义 RLE 压缩
        .with_default_encryption(EncryptionAlgorithm::Custom("xor".to_string())) // 使用自定义 XOR 加密
        .build()?;

    info!("📡 服务端监听在: 0.0.0.0:8080");
    info!("💡 使用自定义扩展: RLE 压缩 + XOR 加密");

    server.start().await?;
    Ok(())
}

// ============================================================================
// 5. 客户端实现
// ============================================================================

/// 简单的消息监听器（用于客户端示例）
struct SimpleMessageListener;

#[async_trait]
impl MessageListener for SimpleMessageListener {
    async fn on_message(&self, frame: &Frame) -> Result<Option<Frame>> {
        if let Some(cmd) = &frame.command {
            if let Some(
                flare_core::common::protocol::flare::core::commands::command::Type::Payload(
                    msg_cmd,
                ),
            ) = &cmd.r#type
            {
                // 尝试解析protobuf消息内容
                let payload_str = match String::from_utf8(msg_cmd.payload.clone()) {
                    Ok(text) => text,
                    Err(_) => {
                        // 如果不是有效的UTF-8，则显示十六进制调试信息
                        format!("<protobuf_binary_data: {} bytes>", msg_cmd.payload.len())
                    }
                };
                info!("📨 [客户端] 收到消息: payload={}", payload_str);
            }
        }
        Ok(None)
    }
}

async fn run_client() -> Result<()> {
    info!("🚀 启动自定义扩展客户端示例");

    // 1. 注册自定义加密器（必须与服务端使用相同的密钥）
    let xor_key = b"my_secret_key_1234567890123456"; // 32 bytes，必须与服务端相同
    let xor_encryptor = XorEncryptor::new(xor_key);
    EncryptionUtil::register_custom(Arc::new(xor_encryptor));
    info!("✅ 已注册自定义加密器: XOR");

    // 2. 注册自定义压缩器
    let rle_compressor = RleCompressor;
    CompressionUtil::register_custom(Arc::new(rle_compressor));
    info!("✅ 已注册自定义压缩器: RLE");

    // 3. 注册自定义序列化器
    let msgpack_serializer = MessagePackSerializer;
    SerializationUtil::register_custom(Arc::new(msgpack_serializer));
    info!("✅ 已注册自定义序列化器: MessagePack");

    // 4. 创建客户端
    // 注意：加密算法通过协商时的 metadata 传递，客户端构建器不直接支持设置加密
    // 如果需要指定加密，需要在 CONNECT 消息的 metadata 中添加 "encryption" 字段
    let listener = Arc::new(SimpleMessageListener);
    let client = FlareClientBuilder::new("ws://127.0.0.1:8080")
        .with_listener(listener) // 必须：设置消息监听器
        .with_format(SerializationFormat::Json) // 使用 JSON 格式
        .with_compression(CompressionAlgorithm::Custom("rle".to_string())) // 使用自定义 RLE 压缩
        .build_with_race() // 使用协议竞速构建
        .await?;

    info!("🔗 已连接到服务端: ws://127.0.0.1:8080");
    info!("💡 使用自定义扩展: RLE 压缩 + XOR 加密");

    // 5. 发送测试消息
    use flare_core::common::protocol::*;
    let msg_cmd = send_message(
        generate_message_id(),
        "Hello from custom extensions client!"
            .to_string()
            .into_bytes(),
        None,
        None,
    );
    let message = frame_with_message_command(msg_cmd, Reliability::AtLeastOnce);

    client.send_frame(&message).await?;
    info!("📤 已发送消息");

    // 6. 等待一段时间后断开
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    client.disconnect().await?;
    info!("👋 客户端已断开");

    Ok(())
}

// ============================================================================
// 6. 主函数
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("server") => run_server().await,
        Some("client") => run_client().await,
        _ => {
            eprintln!("用法: {} [server|client]", args[0]);
            eprintln!();
            eprintln!("示例:");
            eprintln!("  {} server  # 启动服务端", args[0]);
            eprintln!("  {} client  # 启动客户端", args[0]);
            Ok(())
        }
    }
}
