//! 会话ID生成和验证模块
//!
//! 提供不同会话类型的ID生成和验证功能
//!
//! ## 会话ID统一格式
//!
//! 格式：`<type_prefix>-<type_specific_id>-<optional_suffix>`
//!
//! - **单聊（1）**：`1-{sha256_hash}` - 排序用户ID + SHA256哈希
//! - **群聊（2）**：`2-{group_id}` - 群组ID
//! - **AI助手（3）**：`3-{user_id}-{ai_service}` - 用户ID + AI服务ID
//! - **系统通知（4）**：`4-{system_id}-{timestamp}` - 系统ID + 时间戳
//! - **客服会话（5）**：`5-{customer_id}-{agent_id}` - 客户ID + 客服ID
//! - **临时会话（6）**：`6-{ulid}` - ULID
//!
//! ## 优势
//!
//! - ✅ **多端一致性**：前端和后端都能直接生成一致会话ID
//! - ✅ **高效同步**：消息和会话映射直接用会话ID，推送和缓存统一
//! - ✅ **前端离线支持**：前端可在本地缓存会话列表和消息，快速渲染UI
//! - ✅ **数据库索引友好**：统一前缀 + 固定长度ID，支持高性能查询和排序
//! - ✅ **易扩展**：新增类型只需增加前缀和生成规则，无需改动底层系统

use anyhow::{Context, Result};
use std::cmp::Ordering;
use std::str::FromStr;

/// 会话类型枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionType {
    /// 单聊（一对一）- 前缀：1
    Single = 1,
    /// 群聊 - 前缀：2
    Group = 2,
    /// AI助手会话 - 前缀：3
    Ai = 3,
    /// 系统通知 - 前缀：4
    System = 4,
    /// 客服会话 - 前缀：5
    Customer = 5,
    /// 临时会话 - 前缀：6
    Temp = 6,
}

impl FromStr for SessionType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        // 支持数字前缀和字符串前缀（向后兼容）
        match s {
            "1" | "single" => Ok(SessionType::Single),
            "2" | "group" => Ok(SessionType::Group),
            "3" | "ai" => Ok(SessionType::Ai),
            "4" | "system" => Ok(SessionType::System),
            "5" | "customer" => Ok(SessionType::Customer),
            "6" | "temp" => Ok(SessionType::Temp),
            _ => Err(anyhow::anyhow!("Unknown session type: {}", s)),
        }
    }
}

impl SessionType {
    /// 获取类型前缀（数字格式）
    pub fn prefix(&self) -> &'static str {
        match self {
            SessionType::Single => "1",
            SessionType::Group => "2",
            SessionType::Ai => "3",
            SessionType::System => "4",
            SessionType::Customer => "5",
            SessionType::Temp => "6",
        }
    }
    
    /// 从数字前缀解析会话类型
    pub fn from_prefix(prefix: &str) -> Result<Self> {
        match prefix {
            "1" => Ok(SessionType::Single),
            "2" => Ok(SessionType::Group),
            "3" => Ok(SessionType::Ai),
            "4" => Ok(SessionType::System),
            "5" => Ok(SessionType::Customer),
            "6" => Ok(SessionType::Temp),
            _ => Err(anyhow::anyhow!("Unknown session type prefix: {}", prefix)),
        }
    }
}

/// 生成单聊会话ID
///
/// 规则：
/// 1. 将两个用户ID排序（字典序）
/// 2. 拼接：`{min_user_id}:{max_user_id}`
/// 3. 计算 SHA256 哈希
/// 4. 取前16字节，转为十六进制字符串（32个字符）
/// 5. 添加前缀：`1-{hash}`
///
/// # 参数
/// * `user1` - 第一个用户ID
/// * `user2` - 第二个用户ID
///
/// # 返回
/// 格式化的会话ID：`1-{32位十六进制字符串}`
///
/// # 示例
/// ```
/// use flare_core::common::session_id::generate_single_chat_session_id;
///
/// let id1 = generate_single_chat_session_id("user1", "user2");
/// let id2 = generate_single_chat_session_id("user2", "user1");
/// assert_eq!(id1, id2); // 无论顺序如何，生成的ID相同
/// ```
pub fn generate_single_chat_session_id(user1: &str, user2: &str) -> String {
    use sha2::{Digest, Sha256};

    // 1. 排序用户ID（保证一致性）
    let (min_id, max_id) = match user1.cmp(user2) {
        Ordering::Less | Ordering::Equal => (user1, user2),
        Ordering::Greater => (user2, user1),
    };

    // 2. 拼接
    let combined = format!("{}:{}", min_id, max_id);

    // 3. 计算哈希
    let mut hasher = Sha256::new();
    hasher.update(combined.as_bytes());
    let hash = hasher.finalize();

    // 4. 取前16字节，转为十六进制
    let hash_str = hex::encode(&hash[..16]);

    // 5. 添加前缀（使用数字前缀：1）
    format!("1-{}", hash_str)
}

/// 生成群聊会话ID（前端和后端都能生成）
///
/// 使用业务系统的群组ID + 类型前缀
///
/// # 参数
/// * `group_id` - 业务系统的群组ID
///
/// # 返回
/// 格式化的会话ID：`2-{group_id}`
///
/// # 示例
/// ```
/// use flare_core::common::session_id::generate_group_session_id;
///
/// let id = generate_group_session_id("group_12345");
/// assert_eq!(id, "2-group_12345");
/// ```
pub fn generate_group_session_id(group_id: &str) -> String {
    format!("2-{}", group_id)
}

/// 生成AI助手会话ID（前端和后端都能生成）
///
/// 使用用户ID + AI标识 + 类型前缀，排序保证一致性
///
/// # 参数
/// * `user_id` - 用户ID
/// * `ai_id` - AI助手ID
///
/// # 返回
/// 格式化的会话ID：`3-{user_id}-{ai_service}`
///
/// # 示例
/// ```
/// use flare_core::common::session_id::generate_ai_session_id;
///
/// let id1 = generate_ai_session_id("user_001", "ai_001");
/// // 注意：AI会话ID不排序，顺序会影响结果
/// assert_eq!(id1, "3-user_001-ai_001");
/// ```
pub fn generate_ai_session_id(user_id: &str, ai_service: &str) -> String {
    format!("3-{}-{}", user_id, ai_service)
}

/// 生成客服会话ID（前端和后端都能生成）
///
/// 使用用户ID + 客服ID + 类型前缀，排序保证一致性
///
/// # 参数
/// * `user_id` - 用户ID
/// * `service_id` - 客服ID
///
/// # 返回
/// 格式化的会话ID：`5-{customer_id}-{agent_id}`
pub fn generate_customer_session_id(customer_id: &str, agent_id: &str) -> String {
    format!("5-{}-{}", customer_id, agent_id)
}

/// 生成系统通知会话ID（类型前缀：4）
///
/// 使用系统ID + 时间戳/随机ID
///
/// # 参数
/// * `system_id` - 系统标识（如 `system_notification`、`system_announcement`）
/// * `suffix` - 时间戳或随机ID（可选，如果不提供则使用当前时间戳）
///
/// # 返回
/// 格式化的会话ID：`4-{system_id}-{suffix}`
pub fn generate_system_session_id(system_id: &str, suffix: Option<String>) -> String {
    let suffix_str = suffix.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string()
    });
    format!("4-{}-{}", system_id, suffix_str)
}

/// 生成临时会话ID（类型前缀：6）
///
/// 使用ULID生成唯一ID
///
/// # 返回
/// 格式化的会话ID：`6-{ulid}`
pub fn generate_temp_session_id() -> String {
    use ulid::Ulid;
    format!("6-{}", Ulid::new().to_string())
}

/// 生成服务端会话ID（向后兼容，使用ULID）
///
/// 注意：推荐使用具体的生成函数（如 `generate_temp_session_id`）
#[deprecated(note = "Use specific generation functions like generate_temp_session_id()")]
pub fn generate_server_session_id(session_type: SessionType) -> String {
    use ulid::Ulid;
    format!("{}-{}", session_type.prefix(), Ulid::new().to_string())
}

/// 验证会话ID格式
///
/// 验证会话ID是否符合规范，并返回会话类型
///
/// # 参数
/// * `session_id` - 要验证的会话ID
///
/// # 返回
/// 如果格式正确，返回 `Ok(SessionType)`
/// 如果格式错误，返回 `Err`
///
/// # 示例
/// ```
/// use flare_core::common::session_id::{validate_session_id, SessionType};
///
/// let id = "1-a1b2c3d4e5f6g7h8a1b2c3d4e5f6g7h8";
/// let session_type = validate_session_id(id).unwrap();
/// assert_eq!(session_type, SessionType::Single);
/// ```
pub fn validate_session_id(session_id: &str) -> Result<SessionType> {
    if session_id.is_empty() {
        return Err(anyhow::anyhow!("Session ID cannot be empty"));
    }

    // 支持两种格式：数字前缀（新格式）和字符串前缀（旧格式，向后兼容）
    // 新格式：1-xxx, 2-xxx, 3-xxx-xxx 等
    // 旧格式：single:xxx, group:xxx, ai:xxx:xxx 等（向后兼容）
    
    // 检查是否包含分隔符（- 或 :）
    if !session_id.contains('-') && !session_id.contains(':') {
        if session_id.is_empty() {
            return Err(anyhow::anyhow!("Session ID cannot be empty"));
        }
        // 旧格式：直接返回，不验证
        // 允许向后兼容（假设是单聊）
        return Ok(SessionType::Single);
    }

    // 优先使用新格式（数字前缀 + 连字符）
    if session_id.contains('-') {
        let parts: Vec<&str> = session_id.split('-').collect();
        if parts.is_empty() {
            return Err(anyhow::anyhow!("Invalid session ID format"));
        }
        
        let prefix = parts[0];
        let session_type = SessionType::from_prefix(prefix)
            .with_context(|| format!("Failed to parse session type from prefix: {}", prefix))?;
        
        // 对于单聊（1），需要验证哈希长度
        if session_type == SessionType::Single && parts.len() >= 2 {
            let hash = parts[1];
            if hash.len() != 32 {
                return Err(anyhow::anyhow!(
                    "Invalid single chat session ID format: expected 32 hex characters, got {}",
                    hash.len()
                ));
            }
            if !hash.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(anyhow::anyhow!(
                    "Invalid single chat session ID format: contains non-hex characters"
                ));
            }
        }
        
        return Ok(session_type);
    }
    
    // 向后兼容：旧格式（字符串前缀 + 冒号）
    let parts: Vec<&str> = session_id.split(':').collect();
    if parts.len() < 2 {
        return Err(anyhow::anyhow!(
            "Invalid session ID format: expected 'type:identifier' or 'type-identifier', got '{}'",
            session_id
        ));
    }

    let prefix = parts[0];
    // 对于多段格式（如 ai:user_id:ai_id），identifier 是剩余部分
    let identifier = parts[1..].join(":");

    // 向后兼容：支持字符串前缀
    match prefix {
        "1" | "single" => {
            // 单聊：验证是否为32位十六进制字符串
            if identifier.len() != 32 {
                return Err(anyhow::anyhow!(
                    "Invalid single chat session ID format: expected 32 hex characters, got {}",
                    identifier.len()
                ));
            }
            if !identifier.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(anyhow::anyhow!(
                    "Invalid single chat session ID format: contains non-hex characters"
                ));
            }
            Ok(SessionType::Single)
        }
        "2" | "group" => {
            // 群聊：2-{group_id} 或 group:{group_id}，group_id可以是任意字符串
            if identifier.is_empty() {
                return Err(anyhow::anyhow!("Group ID cannot be empty"));
            }
            Ok(SessionType::Group)
        }
        "3" | "ai" => {
            // AI助手：ai:{user_id}:{ai_id} 或 ai:{ulid}
            // 支持两种格式：带冒号的格式（user_id:ai_id）或ULID格式
            if identifier.contains(':') {
                // 格式：user_id:ai_id
                let sub_parts: Vec<&str> = identifier.split(':').collect();
                if sub_parts.len() != 2 {
                    return Err(anyhow::anyhow!(
                        "Invalid AI session ID format: expected 'ai:user_id:ai_id', got 'ai:{}'",
                        identifier
                    ));
                }
            } else {
                // 格式：ULID（向后兼容）
                if identifier.len() != 26 {
                    return Err(anyhow::anyhow!(
                        "Invalid AI session ID format: expected ULID (26 characters) or 'user_id:ai_id', got {}",
                        identifier.len()
                    ));
                }
            }
            Ok(SessionType::Ai)
        }
        "5" | "customer" => {
            // 客服：customer:{user_id}:{service_id} 或 customer:{ulid}
            // 支持两种格式：带冒号的格式（user_id:service_id）或ULID格式
            if identifier.contains(':') {
                // 格式：user_id:service_id
                let sub_parts: Vec<&str> = identifier.split(':').collect();
                if sub_parts.len() != 2 {
                    return Err(anyhow::anyhow!(
                        "Invalid customer session ID format: expected 'customer:user_id:service_id', got 'customer:{}'",
                        identifier
                    ));
                }
            } else {
                // 格式：ULID（向后兼容）
                if identifier.len() != 26 {
                    return Err(anyhow::anyhow!(
                        "Invalid customer session ID format: expected ULID (26 characters) or 'user_id:service_id', got {}",
                        identifier.len()
                    ));
                }
            }
            Ok(SessionType::Customer)
        }
        "4" | "system" => {
            // 系统通知：4-{system_id}-{suffix} 或 system:{system_id}
            Ok(SessionType::System)
        }
        "6" | "temp" => {
            // 系统/临时：验证是否为ULID格式（26个字符，Base32编码）或其他格式
            if identifier.len() != 26 && !identifier.contains(':') {
                return Err(anyhow::anyhow!(
                    "Invalid session ID format: expected ULID (26 characters) or identifier with colon, got {}",
                    identifier.len()
                ));
            }
            // 验证ULID格式（Base32字符集：0-9, A-Z）
            if identifier.len() == 26 {
                if !identifier.chars().all(|c| c.is_ascii_alphanumeric()) {
                    return Err(anyhow::anyhow!(
                        "Invalid session ID format: ULID contains invalid characters"
                    ));
                }
            }
            SessionType::from_str(prefix)
                .with_context(|| format!("Failed to parse session type from prefix: {}", prefix))
        }
        _ => Err(anyhow::anyhow!(
            "Unknown session type prefix: {}. Valid prefixes: 1/single, 2/group, 3/ai, 4/system, 5/customer, 6/temp",
            prefix
        )),
    }
}

/// 从会话ID中提取会话类型
///
/// 如果会话ID格式正确，返回会话类型；否则返回None
///
/// # 参数
/// * `session_id` - 会话ID
///
/// # 返回
/// 如果格式正确，返回 `Some(SessionType)`；否则返回 `None`
pub fn extract_session_type(session_id: &str) -> Option<SessionType> {
    validate_session_id(session_id).ok()
}

/// 检查会话ID是否为单聊格式
///
/// # 参数
/// * `session_id` - 会话ID
///
/// # 返回
/// 如果是单聊格式，返回 `true`；否则返回 `false`
pub fn is_single_chat(session_id: &str) -> bool {
    matches!(extract_session_type(session_id), Some(SessionType::Single))
}

/// 检查会话ID是否为群聊格式
///
/// # 参数
/// * `session_id` - 会话ID
///
/// # 返回
/// 如果是群聊格式，返回 `true`；否则返回 `false`
pub fn is_group_chat(session_id: &str) -> bool {
    matches!(extract_session_type(session_id), Some(SessionType::Group))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_chat_session_id_consistency() {
        let id1 = generate_single_chat_session_id("user1", "user2");
        let id2 = generate_single_chat_session_id("user2", "user1");

        // 两个用户无论顺序如何，生成的ID应该相同
        assert_eq!(id1, id2);
        assert!(id1.starts_with("1-"));
        assert_eq!(id1.len(), 34); // "1-" (2) + 32 hex chars

        // 不同用户对应该生成不同ID
        let id3 = generate_single_chat_session_id("user1", "user3");
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_group_session_id_generation() {
        let id1 = generate_group_session_id("group_12345");
        let id2 = generate_group_session_id("group_12345");

        assert_eq!(id1, "2-group_12345");
        assert_eq!(id2, "2-group_12345");
        // 相同群组ID应该生成相同的会话ID
        assert_eq!(id1, id2);

        let id3 = generate_group_session_id("group_67890");
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_ai_session_id_generation() {
        let id1 = generate_ai_session_id("user_001", "gpt-4");
        let id2 = generate_ai_session_id("user_001", "gpt-4");

        assert!(id1.starts_with("3-"));
        assert_eq!(id1, "3-user_001-gpt-4");
        // 相同参数应该生成相同的ID
        assert_eq!(id1, id2);

        let id3 = generate_ai_session_id("user_001", "claude-3");
        assert_ne!(id1, id3);
    }
    
    #[test]
    fn test_customer_session_id_generation() {
        let id1 = generate_customer_session_id("customer_001", "agent_001");
        assert_eq!(id1, "5-customer_001-agent_001");
    }
    
    #[test]
    fn test_system_session_id_generation() {
        let id1 = generate_system_session_id("system_notification", None);
        assert!(id1.starts_with("4-system_notification-"));
        
        let id2 = generate_system_session_id("system_announcement", Some("1734567890".to_string()));
        assert_eq!(id2, "4-system_announcement-1734567890");
    }
    
    #[test]
    fn test_temp_session_id_generation() {
        let id1 = generate_temp_session_id();
        let id2 = generate_temp_session_id();
        
        assert!(id1.starts_with("6-"));
        assert!(id2.starts_with("6-"));
        // 应该生成不同的ID
        assert_ne!(id1, id2);
    }



    #[test]
    fn test_validate_session_id() {
        // 有效的单聊ID
        let single_id = generate_single_chat_session_id("user1", "user2");
        assert!(validate_session_id(&single_id).is_ok());
        assert_eq!(
            validate_session_id(&single_id).unwrap(),
            SessionType::Single
        );

        // 有效的群聊ID
        let group_id = generate_group_session_id("group_12345");
        assert!(validate_session_id(&group_id).is_ok());
        assert_eq!(validate_session_id(&group_id).unwrap(), SessionType::Group);

        // 有效的AI助手ID（格式：3-user_id-ai_service）
        let ai_id = generate_ai_session_id("user_001", "gpt-4");
        assert!(validate_session_id(&ai_id).is_ok());
        assert_eq!(validate_session_id(&ai_id).unwrap(), SessionType::Ai);
        
        // 有效的系统通知ID
        let system_id = generate_system_session_id("system_notification", Some("1734567890".to_string()));
        assert!(validate_session_id(&system_id).is_ok());
        assert_eq!(validate_session_id(&system_id).unwrap(), SessionType::System);
        
        // 有效的临时会话ID
        let temp_id = generate_temp_session_id();
        assert!(validate_session_id(&temp_id).is_ok());
        assert_eq!(validate_session_id(&temp_id).unwrap(), SessionType::Temp);

        // 无效格式
        assert!(validate_session_id("").is_err());
        assert!(validate_session_id("1-abc").is_err()); // 单聊哈希太短
        assert!(validate_session_id("0-abc123").is_err()); // 未知前缀
        assert!(validate_session_id("unknown-abc123").is_err()); // 未知前缀（字符串格式）

        // 向后兼容：无前缀的旧格式（允许，默认当作单聊处理）
        assert!(validate_session_id("old_session_id").is_ok());
        assert!(validate_session_id("invalid").is_ok()); // 无前缀的旧格式，向后兼容
    }

    #[test]
    fn test_extract_session_type() {
        let single_id = generate_single_chat_session_id("user1", "user2");
        assert_eq!(extract_session_type(&single_id), Some(SessionType::Single));

        let group_id = generate_group_session_id("group_12345");
        assert_eq!(extract_session_type(&group_id), Some(SessionType::Group));

        let ai_id = generate_ai_session_id("user_001", "ai_001");
        assert_eq!(extract_session_type(&ai_id), Some(SessionType::Ai));

        // 向后兼容：无前缀的旧格式返回Single
        assert_eq!(extract_session_type("old_session_id"), Some(SessionType::Single));

        // 无效格式返回None
        assert_eq!(extract_session_type("invalid:format:too:many:parts"), None);
    }

    #[test]
    fn test_is_single_chat() {
        let single_id = generate_single_chat_session_id("user1", "user2");
        assert!(is_single_chat(&single_id));

        let group_id = generate_group_session_id("group_12345");
        assert!(!is_single_chat(&group_id));
    }

    #[test]
    fn test_is_group_chat() {
        let single_id = generate_single_chat_session_id("user1", "user2");
        assert!(!is_group_chat(&single_id));

        let group_id = generate_group_session_id("group_12345");
        assert!(is_group_chat(&group_id));
    }
}

