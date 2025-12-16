//! Conversation ID（CID）生成与校验模块（Production Grade）
//!
//! 本模块定义 IM 系统中 **会话唯一标识（Conversation ID）** 的统一生成规则。
//!
//! 设计目标：
//! - 保证多端（Client / Server / Gateway）**确定性一致生成**
//! - 避免业务字段泄露，确保 **隐私与安全**
//! - 支持 IM 会话类型的 **长期演进与扩展**
//! - 满足高并发推送、存储与索引场景
//!
//! -----------------------------------------------------------------------------
//! 一、CID 设计原则（强约束）
//! -----------------------------------------------------------------------------
//!
//! 1. CID 是 **纯标识符（Opaque Identifier）**
//!    - 不承载业务语义
//!    - 不允许从 CID 反解析用户 / 群 / 角色信息
//!
//! 2. CID 的职责边界
//!    - CID 只用于唯一标识会话
//!    - 会话成员、权限、路由信息 **必须通过独立的 Membership / Routing 模块获取**
//!
//! 3. 生成规则必须满足
//!    - ✅ 确定性（同一会话生成同一 CID）
//!    - ✅ 不可逆（无法反推出业务字段）
//!    - ✅ 固定长度（索引与分片友好）
//!    - ✅ 前后端独立实现（无 RPC / DB 依赖）
//!
//! -----------------------------------------------------------------------------
//! 二、统一 CID 格式（最终规范）
//! -----------------------------------------------------------------------------
//!
//!     <TypePrefix><Version><OpaqueID>
//!
//! 示例：
//!
//!     1A7K9Q2FZ8M3P4C   // 单聊
//!     2A0ZP9N8Q4J2      // 群聊
//!     3A8F2QZK9M7       // AI 会话
//!     6A01HYRZ4F6T2     // 临时会话
//!
//! -----------------------------------------------------------------------------
//! 三、字段定义
//! -----------------------------------------------------------------------------
//!
//! ### 1️⃣ TypePrefix（1 byte，可读）
//!
//! | 类型 | 值 |
//! |------|----|
//! | 单聊 | 1 |
//! | 群聊 | 2 |
//! | AI 会话 | 3 |
//! | 系统通知 | 4 |
//! | 客服会话 | 5 |
//! | 临时会话 | 6 |
//!
//! > TypePrefix 仅用于粗粒度分类、分流或分片，不作为业务判断依据。
//!
//! -----------------------------------------------------------------------------
//!
//! ### 2️⃣ Version（1 byte）
//!
//! 用于 CID 算法演进与兼容控制：
//!
//! | 版本 | 含义 |
//! |------|------|
//! | A | v1 |
//! | B | v2 |
//!
//! Version 的存在确保：
//! - Hash 算法 / 长度可升级
//! - Salt / 输入规则可演进
//! - 历史会话永不失效
//!
//! -----------------------------------------------------------------------------
//!
//! ### 3️⃣ OpaqueID（主体）
//!
//! - Base32（Crockford）或 Base62 编码
//! - 固定长度（推荐 16～20 字符）
//! - 由 **稳定输入 + Hash 截断** 生成
//!
//! -----------------------------------------------------------------------------
//! 四、各类型 CID 生成规则（v1）
//! -----------------------------------------------------------------------------
//!
//! 统一约定：
//! - Hash 算法：SHA-256
//! - Hash 截断：前 80 bit（10 bytes）
//! - 编码方式：Base32（Crockford）
//!
//! -----------------------------------------------------------------------------
//!
//! ### 1️⃣ 单聊（Direct Message）
//!
//! #### 输入规则：
//!
//!     uids = sort(uid_a, uid_b)
//!     input = "DM:v1:" + uids[0] + ":" + uids[1]
//!
//! #### 生成：
//!
//!     opaque = Base32(SHA256(input)[0..10])
//!     cid = "1A" + opaque
//!
//! 特性：
//! - 双向一致
//! - 不暴露用户关系
//! - 用户 ID 规则变更不影响 CID
//!
//! -----------------------------------------------------------------------------
//!
//! ### 2️⃣ 群聊（Group）
//!
//! #### 输入：
//!
//!     input = "GROUP:v1:" + group_id
//!
//! #### 生成：
//!
//!     cid = "2A" + Base32(SHA256(input)[0..10])
//!
//! -----------------------------------------------------------------------------
//!
//! ### 3️⃣ AI 会话
//!
//! #### 输入：
//!
//!     input = "AI:v1:" + user_id + ":" + ai_scope
//!
//! 示例 ai_scope：
//!
//!     openai:gpt-4
//!     claude:sonnet
//!     local:rag:kb42
//!
//! #### 生成：
//!
//!     cid = "3A" + Base32(SHA256(input)[0..10])
//!
//! -----------------------------------------------------------------------------
//!
//! ### 4️⃣ 系统 / 客服（确定性会话）
//!
//! #### 系统：
//!
//!     input = "SYS:v1:" + system_id + ":" + scope
//!     cid = "4A" + Base32(SHA256(input)[0..10])
//!
//! #### 客服：
//!
//!     input = "CS:v1:" + customer_id + ":" + channel
//!     cid = "5A" + Base32(SHA256(input)[0..10])
//!
//! -----------------------------------------------------------------------------
//!
//! ### 5️⃣ 临时会话（非确定性）
//!
//!     cid = "6A" + ULID()
//!
//! 特性：
//! - 不参与消息漫游
//! - 不参与去重
//! - 生命周期短
//!
//! -----------------------------------------------------------------------------
//! 五、禁止事项（强制）
//! -----------------------------------------------------------------------------
//!
//! ❌ 禁止从 CID 中解析用户 / 群 / 角色信息  
//! ❌ 禁止在 CID 中拼接业务字段  
//! ❌ 禁止使用自增 ID 或 UUID 直接作为 CID  
//! ❌ 禁止将 CID 作为权限或成员判断依据  

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

/// 生成单聊会话ID（CID格式：1A + OpaqueID）
///
/// 规则：
/// 1. 将两个用户ID排序（字典序）
/// 2. 输入：`DM:v1:{min_user_id}:{max_user_id}`
/// 3. 计算 SHA256 哈希，取前 10 字节
/// 4. Base32 编码
/// 5. 格式：`1A{opaque_id}`
///
/// # 参数
/// * `user1` - 第一个用户ID
/// * `user2` - 第二个用户ID
///
/// # 返回
/// 格式化的会话ID：`1A{16字符Base32编码}`
///
/// # 示例
/// ```
/// use flare_core::common::session_id::generate_single_chat_session_id;
///
/// let id1 = generate_single_chat_session_id("user1", "user2");
/// let id2 = generate_single_chat_session_id("user2", "user1");
/// assert_eq!(id1, id2); // 无论顺序如何，生成的ID相同
/// assert!(id1.starts_with("1A"));
/// assert_eq!(id1.len(), 18); // "1A" + 16字符
/// ```
pub fn generate_single_chat_session_id(user1: &str, user2: &str) -> String {
    use sha2::{Digest, Sha256};

    // 排序用户ID（保证一致性）
    let (min_id, max_id) = match user1.cmp(user2) {
        Ordering::Less | Ordering::Equal => (user1, user2),
        Ordering::Greater => (user2, user1),
    };

    // 输入：DM:v1:{min_user_id}:{max_user_id}
    let input = format!("DM:v1:{}:{}", min_id, max_id);

    // 计算 SHA256 哈希
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let hash = hasher.finalize();

    // 取前 10 字节（80 bit）
    let truncated = &hash[..10];
    
    // Base32 编码（Crockford）
    let opaque_id = base32::encode(base32::Alphabet::Crockford, truncated);

    // 格式：1A{opaque_id}
    format!("1A{}", opaque_id)
}

/// 生成群聊会话ID（CID格式：2A + OpaqueID）
///
/// 规则：
/// 1. 输入：`GROUP:v1:{group_id}`
/// 2. 计算 SHA256 哈希，取前 10 字节
/// 3. Base32 编码
/// 4. 格式：`2A{opaque_id}`
///
/// # 参数
/// * `group_id` - 业务系统的群组ID
///
/// # 返回
/// 格式化的会话ID：`2A{16字符Base32编码}`
pub fn generate_group_session_id(group_id: &str) -> String {
    use sha2::{Digest, Sha256};
    
    // 输入：GROUP:v1:{group_id}
    let input = format!("GROUP:v1:{}", group_id);
    
    // 计算 SHA256 哈希
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let hash = hasher.finalize();
    
    // 取前 10 字节（80 bit）
    let truncated = &hash[..10];
    
    // Base32 编码
    let opaque_id = base32::encode(base32::Alphabet::Crockford, truncated);
    
    // 格式：2A{opaque_id}
    format!("2A{}", opaque_id)
}

/// 生成AI助手会话ID（CID格式：3A + OpaqueID）
///
/// 规则：
/// 1. 输入：`AI:v1:{user_id}:{ai_scope}`
/// 2. 计算 SHA256 哈希，取前 10 字节
/// 3. Base32 编码
/// 4. 格式：`3A{opaque_id}`
///
/// # 参数
/// * `user_id` - 用户ID
/// * `ai_scope` - AI服务标识（如 "openai:gpt-4", "claude:sonnet"）
///
/// # 返回
/// 格式化的会话ID：`3A{16字符Base32编码}`
pub fn generate_ai_session_id(user_id: &str, ai_scope: &str) -> String {
    use sha2::{Digest, Sha256};
    
    // 输入：AI:v1:{user_id}:{ai_scope}
    let input = format!("AI:v1:{}:{}", user_id, ai_scope);
    
    // 计算 SHA256 哈希
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let hash = hasher.finalize();
    
    // 取前 10 字节（80 bit）
    let truncated = &hash[..10];
    
    // Base32 编码
    let opaque_id = base32::encode(base32::Alphabet::Crockford, truncated);
    
    // 格式：3A{opaque_id}
    format!("3A{}", opaque_id)
}

/// 生成客服会话ID（CID格式：5A + OpaqueID）
///
/// 规则：
/// 1. 输入：`CS:v1:{customer_id}:{channel}`
/// 2. 计算 SHA256 哈希，取前 10 字节
/// 3. Base32 编码
/// 4. 格式：`5A{opaque_id}`
///
/// # 参数
/// * `customer_id` - 客户ID
/// * `channel` - 客服渠道标识
///
/// # 返回
/// 格式化的会话ID：`5A{16字符Base32编码}`
pub fn generate_customer_session_id(customer_id: &str, channel: &str) -> String {
    use sha2::{Digest, Sha256};
    
    // 输入：CS:v1:{customer_id}:{channel}
    let input = format!("CS:v1:{}:{}", customer_id, channel);
    
    // 计算 SHA256 哈希
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let hash = hasher.finalize();
    
    // 取前 10 字节（80 bit）
    let truncated = &hash[..10];
    
    // Base32 编码
    let opaque_id = base32::encode(base32::Alphabet::Crockford, truncated);
    
    // 格式：5A{opaque_id}
    format!("5A{}", opaque_id)
}

/// 生成系统通知会话ID（CID格式：4A + OpaqueID）
///
/// 规则：
/// 1. 输入：`SYS:v1:{system_id}:{scope}`
/// 2. 计算 SHA256 哈希，取前 10 字节
/// 3. Base32 编码
/// 4. 格式：`4A{opaque_id}`
///
/// # 参数
/// * `system_id` - 系统标识（如 "system_notification", "system_announcement"）
/// * `scope` - 作用域（可选，如果不提供则使用空字符串）
///
/// # 返回
/// 格式化的会话ID：`4A{16字符Base32编码}`
pub fn generate_system_session_id(system_id: &str, scope: Option<String>) -> String {
    use sha2::{Digest, Sha256};
    
    // 输入：SYS:v1:{system_id}:{scope}
    let scope_str = scope.unwrap_or_default();
    let input = format!("SYS:v1:{}:{}", system_id, scope_str);
    
    // 计算 SHA256 哈希
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let hash = hasher.finalize();
    
    // 取前 10 字节（80 bit）
    let truncated = &hash[..10];
    
    // Base32 编码
    let opaque_id = base32::encode(base32::Alphabet::Crockford, truncated);
    
    // 格式：4A{opaque_id}
    format!("4A{}", opaque_id)
}

/// 生成临时会话ID（CID格式：6A + ULID）
///
/// 规则：
/// 1. 生成 ULID
/// 2. 格式：`6A{ulid}`
///
/// # 返回
/// 格式化的会话ID：`6A{26字符ULID}`
#[cfg(not(target_arch = "wasm32"))]
pub fn generate_temp_session_id() -> String {
    use ulid::Ulid;
    format!("6A{}", Ulid::new().to_string())
}

#[cfg(target_arch = "wasm32")]
pub fn generate_temp_session_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let c = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("6A{}-{}", ts, c)
}

/// 生成服务端会话ID（向后兼容，使用ULID）
///
/// 注意：推荐使用具体的生成函数（如 `generate_temp_session_id`）
#[deprecated(note = "Use specific generation functions like generate_temp_session_id()")]
#[cfg(not(target_arch = "wasm32"))]
pub fn generate_server_session_id(session_type: SessionType) -> String {
    use ulid::Ulid;
    format!("{}-{}", session_type.prefix(), Ulid::new().to_string())
}

#[cfg(target_arch = "wasm32")]
pub fn generate_server_session_id(session_type: SessionType) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let c = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{}-{}", session_type.prefix(), ts, c)
}

/// 验证会话ID格式（CID格式：TypePrefix + Version + OpaqueID）
///
/// 验证会话ID是否符合CID规范，并返回会话类型
///
/// CID格式：
/// - 单聊：`1A{16字符Base32}`
/// - 群聊：`2A{16字符Base32}`
/// - AI会话：`3A{16字符Base32}`
/// - 系统通知：`4A{16字符Base32}`
/// - 客服会话：`5A{16字符Base32}`
/// - 临时会话：`6A{26字符ULID}`
///
/// # 参数
/// * `session_id` - 要验证的会话ID
///
/// # 返回
/// 如果格式正确，返回 `Ok(SessionType)`
/// 如果格式错误，返回 `Err`
pub fn validate_session_id(session_id: &str) -> Result<SessionType> {
    if session_id.is_empty() {
        return Err(anyhow::anyhow!("Session ID cannot be empty"));
    }

    // CID格式：TypePrefix(1) + Version(1) + OpaqueID
    if session_id.len() < 3 {
        return Err(anyhow::anyhow!("Session ID too short, expected CID format"));
        }

    let prefix = &session_id[..1];
    let version = &session_id[1..2];
    
    // 验证版本号（当前只支持 A）
    if version != "A" {
        return Err(anyhow::anyhow!("Unsupported CID version: {}, expected 'A'", version));
        }
        
    // 验证类型前缀并获取会话类型
        let session_type = SessionType::from_prefix(prefix)
        .with_context(|| format!("Invalid CID type prefix: {}", prefix))?;
        
    // 验证 OpaqueID 长度
    let opaque_id = &session_id[2..];
    match session_type {
        SessionType::Temp => {
            // 临时会话：6A + ULID（26字符）
            if opaque_id.len() != 26 {
                return Err(anyhow::anyhow!(
                    "Invalid temp session CID: expected 26 characters ULID, got {}",
                    opaque_id.len()
                ));
            }
        }
        _ => {
            // 其他类型：TypePrefix + Version + 16字符Base32
            if opaque_id.len() != 16 {
                return Err(anyhow::anyhow!(
                    "Invalid CID opaque ID length: expected 16 characters, got {}",
                    opaque_id.len()
                ));
            }
            // 验证Base32字符（Crockford字母表：0-9, A-Z，排除I、L、O、U）
            if !opaque_id.chars().all(|c| {
                matches!(c, '0'..='9' | 'A'..='H' | 'J'..='K' | 'M'..='N' | 'P'..='T' | 'V'..='Z')
            }) {
                return Err(anyhow::anyhow!(
                    "Invalid CID opaque ID: contains invalid Base32 characters"
                    ));
                }
            }
    }
    
    Ok(session_type)
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

// 注意：CID 是不可逆的，无法从 CID 中提取用户/群/角色信息
// 所有提取函数已移除，必须通过独立的 Membership / Routing 模块获取

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_chat_session_id_consistency() {
        let id1 = generate_single_chat_session_id("user1", "user2");
        let id2 = generate_single_chat_session_id("user2", "user1");

        // 两个用户无论顺序如何，生成的ID应该相同
        assert_eq!(id1, id2);
        assert!(id1.starts_with("1A"));
        assert_eq!(id1.len(), 18); // "1A" + 16字符Base32

        // 不同用户对应该生成不同ID
        let id3 = generate_single_chat_session_id("user1", "user3");
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_group_session_id_generation() {
        let id1 = generate_group_session_id("group_12345");
        let id2 = generate_group_session_id("group_12345");

        assert!(id1.starts_with("2A"));
        assert_eq!(id1.len(), 18); // "2A" + 16字符Base32
        // 相同群组ID应该生成相同的会话ID
        assert_eq!(id1, id2);

        let id3 = generate_group_session_id("group_67890");
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_ai_session_id_generation() {
        let id1 = generate_ai_session_id("user_001", "openai:gpt-4");
        let id2 = generate_ai_session_id("user_001", "openai:gpt-4");

        assert!(id1.starts_with("3A"));
        assert_eq!(id1.len(), 18); // "3A" + 16字符Base32
        // 相同参数应该生成相同的ID
        assert_eq!(id1, id2);

        let id3 = generate_ai_session_id("user_001", "claude:sonnet");
        assert_ne!(id1, id3);
    }
    
    #[test]
    fn test_customer_session_id_generation() {
        let id1 = generate_customer_session_id("customer_001", "channel_001");
        assert!(id1.starts_with("5A"));
        assert_eq!(id1.len(), 18); // "5A" + 16字符Base32
    }
    
    #[test]
    fn test_system_session_id_generation() {
        let id1 = generate_system_session_id("system_notification", None);
        assert!(id1.starts_with("4A"));
        assert_eq!(id1.len(), 18); // "4A" + 16字符Base32
        
        let id2 = generate_system_session_id("system_announcement", Some("scope1".to_string()));
        assert!(id2.starts_with("4A"));
        assert_eq!(id2.len(), 18);
        assert_ne!(id1, id2);
    }
    
    #[test]
    fn test_temp_session_id_generation() {
        let id1 = generate_temp_session_id();
        let id2 = generate_temp_session_id();
        
        assert!(id1.starts_with("6A"));
        assert!(id2.starts_with("6A"));
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
        assert!(validate_session_id("1A").is_err()); // OpaqueID太短
        assert!(validate_session_id("1B1234567890123456").is_err()); // 不支持的版本
        assert!(validate_session_id("0A1234567890123456").is_err()); // 未知前缀
        assert!(validate_session_id("1A123456789012345").is_err()); // OpaqueID长度错误
    }

    #[test]
    fn test_extract_session_type() {
        let single_id = generate_single_chat_session_id("user1", "user2");
        assert_eq!(extract_session_type(&single_id), Some(SessionType::Single));

        let group_id = generate_group_session_id("group_12345");
        assert_eq!(extract_session_type(&group_id), Some(SessionType::Group));

        let ai_id = generate_ai_session_id("user_001", "ai_001");
        assert_eq!(extract_session_type(&ai_id), Some(SessionType::Ai));

        // 无效格式返回None
        assert_eq!(extract_session_type("invalid"), None);
        assert_eq!(extract_session_type("1B1234567890123456"), None); // 不支持的版本
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
