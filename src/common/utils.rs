//! 工具函数模块
//! 
//! 提供常用的工具函数和辅助方法

use crate::common::error::Result;
use std::time::{SystemTime, UNIX_EPOCH};

/// 生成唯一 ID（基于时间戳和随机数）
pub fn generate_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{:016x}", timestamp, counter)
}

/// 生成简短 ID（仅用于测试或临时标识）
pub fn generate_short_id() -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:08x}", counter)
}

/// 获取当前时间戳（毫秒）
pub fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// 获取当前时间戳（秒）
pub fn current_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// 验证字符串是否为有效的连接 ID
/// 
/// 连接 ID 应该只包含字母、数字、连字符和下划线
pub fn is_valid_connection_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_')
}

/// 验证字符串是否为有效的用户 ID
/// 
/// 用户 ID 应该只包含字母、数字、连字符、下划线和 @ 符号
pub fn is_valid_user_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '@' || c == '.')
}

/// 截断字符串到指定长度（如果超过）
pub fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

/// 将字节数组转换为十六进制字符串
pub fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

/// 将十六进制字符串转换为字节数组
pub fn hex_to_bytes(hex: &str) -> Result<Vec<u8>> {
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .map_err(|e| crate::common::error::FlareError::protocol_error(format!(
                    "Invalid hex string: {}",
                    e
                )))
        })
        .collect()
}

/// 计算数据的哈希值（简单实现）
/// 
/// 注意：这是一个简单的哈希实现，用于非安全场景
/// 如果需要加密安全的哈希，请使用外部库（如 sha2）
pub fn simple_hash(data: &[u8]) -> u64 {
    // 简单的 FNV-1a 哈希算法
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    
    let mut hash = FNV_OFFSET_BASIS;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// 安全地比较两个字节数组（防止时序攻击）
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| x ^ y)
        .fold(0u8, |acc, x| acc | x) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_id() {
        let id1 = generate_id();
        let id2 = generate_id();
        assert_ne!(id1, id2);
        assert!(!id1.is_empty());
    }

    #[test]
    fn test_is_valid_connection_id() {
        assert!(is_valid_connection_id("conn-123"));
        assert!(is_valid_connection_id("conn_456"));
        assert!(!is_valid_connection_id("conn 123")); // 包含空格
        assert!(!is_valid_connection_id("")); // 空字符串
    }

    #[test]
    fn test_is_valid_user_id() {
        assert!(is_valid_user_id("user-123"));
        assert!(is_valid_user_id("user@example.com"));
        assert!(!is_valid_user_id("user 123")); // 包含空格
        assert!(!is_valid_user_id("")); // 空字符串
    }

    #[test]
    fn test_truncate_string() {
        assert_eq!(truncate_string("hello", 10), "hello");
        assert_eq!(truncate_string("hello world", 5), "he...");
    }

    #[test]
    fn test_bytes_to_hex() {
        assert_eq!(bytes_to_hex(&[0x12, 0x34, 0xAB]), "1234ab");
    }

    #[test]
    fn test_hex_to_bytes() {
        assert_eq!(hex_to_bytes("1234ab").unwrap(), vec![0x12, 0x34, 0xAB]);
        assert!(hex_to_bytes("invalid").is_err());
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hell"));
    }
}

