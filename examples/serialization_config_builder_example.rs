//! SerializationConfig 构建器模式示例
//!
//! 演示如何使用新的构建器模式创建 SerializationConfig

use flare_core::common::serialization::{SerializationConfig, SerializationFormat};

fn main() {
    // 使用构建器模式创建配置
    let config = SerializationConfig::builder()
        .format(SerializationFormat::Protobuf)
        .enable_compression(true)
        .compression_level(Some(6))
        .max_message_size(Some(32 * 1024 * 1024)) // 32MB
        .add_param("custom_key", "custom_value")
        .build();

    println!("序列化格式: {:?}", config.format);
    println!("启用压缩: {}", config.enable_compression);
    println!("压缩级别: {:?}", config.compression_level);
    println!("最大消息大小: {:?}", config.max_message_size);
    println!("自定义参数: {:?}", config.custom_params);

    // 验证配置是否正确设置
    assert_eq!(config.format, SerializationFormat::Protobuf);
    assert!(config.enable_compression);
    assert_eq!(config.compression_level, Some(6));
    assert_eq!(config.max_message_size, Some(32 * 1024 * 1024));
    assert_eq!(config.custom_params.get("custom_key"), Some(&"custom_value".to_string()));

    println!("✅ 构建器模式测试通过！");
}