//! 认证提供者和会话管理

use crate::common::error::FlareError;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// 认证提供者trait
pub trait AuthProvider: Send + Sync {
    /// 验证令牌
    /// 
    /// # 参数
    /// * `token` - 认证令牌
    /// 
    /// # 返回值
    /// 成功时返回用户ID，失败时返回错误
    fn validate_token(&self, token: &str) -> Result<String, FlareError>;
    
    /// 生成令牌
    /// 
    /// # 参数
    /// * `user_id` - 用户ID
    /// 
    /// # 返回值
    /// 成功时返回认证令牌，失败时返回错误
    fn generate_token(&self, user_id: &str) -> Result<String, FlareError>;
}

/// 用户会话信息
#[derive(Debug, Clone)]
pub struct UserSession {
    /// 用户ID
    pub user_id: String,
    /// 会话令牌
    pub token: String,
    /// 创建时间（毫秒时间戳）
    pub created_at: u64,
    /// 最后活动时间（毫秒时间戳）
    pub last_activity: u64,
    /// 会话过期时间（毫秒）
    pub expires_in: u64,
}

impl UserSession {
    /// 创建新的用户会话
    /// 
    /// # 参数
    /// * `user_id` - 用户ID
    /// * `token` - 会话令牌
    /// * `expires_in` - 过期时间（毫秒）
    pub fn new(user_id: String, token: String, expires_in: u64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
            
        Self {
            user_id,
            token,
            created_at: now,
            last_activity: now,
            expires_in,
        }
    }
    
    /// 更新最后活动时间
    pub fn update_activity(&mut self) {
        self.last_activity = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
    }
    
    /// 检查会话是否过期
    /// 
    /// # 返回值
    /// 如果会话已过期返回 true，否则返回 false
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
            
        now > self.created_at + self.expires_in
    }
    
    /// 获取剩余有效时间（毫秒）
    /// 
    /// # 返回值
    /// 剩余有效时间，如果已过期则返回0
    pub fn remaining_time(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
            
        let expire_time = self.created_at + self.expires_in;
        if now >= expire_time {
            0
        } else {
            expire_time - now
        }
    }
}

/// 会话管理器
pub struct SessionManager {
    /// 会话存储（令牌 -> 会话）
    sessions: Arc<RwLock<HashMap<String, UserSession>>>,
    /// 会话存储（用户ID -> 令牌列表）
    user_sessions: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// 默认会话过期时间（毫秒）
    default_expires_in: u64,
}

impl SessionManager {
    /// 创建新的会话管理器
    /// 
    /// # 参数
    /// * `default_expires_in` - 默认会话过期时间（毫秒）
    pub fn new(default_expires_in: u64) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            user_sessions: Arc::new(RwLock::new(HashMap::new())),
            default_expires_in,
        }
    }
    
    /// 创建会话
    /// 
    /// # 参数
    /// * `user_id` - 用户ID
    /// * `token` - 会话令牌
    /// * `expires_in` - 过期时间（毫秒），如果为None则使用默认值
    /// 
    /// # 返回值
    /// 成功时返回会话信息，失败时返回错误
    pub fn create_session(&self, user_id: String, token: String, expires_in: Option<u64>) -> Result<UserSession, FlareError> {
        let expires_in = expires_in.unwrap_or(self.default_expires_in);
        let session = UserSession::new(user_id.clone(), token.clone(), expires_in);
        
        // 存储会话
        {
            let mut sessions = self.sessions.write().map_err(|_| FlareError::general_error("无法获取会话写锁".to_string()))?;
            sessions.insert(token.clone(), session.clone());
        }
        
        // 更新用户会话映射
        {
            let mut user_sessions = self.user_sessions.write().map_err(|_| FlareError::general_error("无法获取用户会话写锁".to_string()))?;
            user_sessions.entry(user_id).or_insert_with(Vec::new).push(token);
        }
        
        Ok(session)
    }
    
    /// 验证会话
    /// 
    /// # 参数
    /// * `token` - 会话令牌
    /// 
    /// # 返回值
    /// 如果会话有效返回用户ID，否则返回错误
    pub fn validate_session(&self, token: &str) -> Result<String, FlareError> {
        // 先检查会话是否存在且未过期
        let is_valid = {
            let sessions = self.sessions.read().map_err(|_| FlareError::general_error("无法获取会话读锁".to_string()))?;
            if let Some(session) = sessions.get(token) {
                !session.is_expired()
            } else {
                false
            }
        };
        
        if is_valid {
            // 更新最后活动时间
            self.update_session_activity(token);
            // 再次读取以获取用户ID
            let sessions = self.sessions.read().map_err(|_| FlareError::general_error("无法获取会话读锁".to_string()))?;
            if let Some(session) = sessions.get(token) {
                Ok(session.user_id.clone())
            } else {
                Err(FlareError::authentication_failed("无效的会话令牌".to_string()))
            }
        } else {
            // 会话已过期或不存在，移除它
            self.remove_session(token);
            Err(FlareError::authentication_failed("会话已过期或无效".to_string()))
        }
    }
    
    /// 更新会话活动时间
    /// 
    /// # 参数
    /// * `token` - 会话令牌
    pub fn update_session_activity(&self, token: &str) {
        if let Ok(mut sessions) = self.sessions.write() {
            if let Some(session) = sessions.get_mut(token) {
                session.update_activity();
            }
        }
    }
    
    /// 移除会话
    /// 
    /// # 参数
    /// * `token` - 会话令牌
    pub fn remove_session(&self, token: &str) {
        // 从会话存储中移除
        let user_id = {
            if let Ok(mut sessions) = self.sessions.write() {
                if let Some(session) = sessions.remove(token) {
                    Some(session.user_id.clone())
                } else {
                    None
                }
            } else {
                None
            }
        };
        
        // 从用户会话映射中移除
        if let Some(user_id) = user_id {
            if let Ok(mut user_sessions) = self.user_sessions.write() {
                if let Some(tokens) = user_sessions.get_mut(&user_id) {
                    tokens.retain(|t| t != token);
                    // 如果用户没有其他会话，移除用户条目
                    if tokens.is_empty() {
                        user_sessions.remove(&user_id);
                    }
                }
            }
        }
    }
    
    /// 获取用户的所有会话令牌
    /// 
    /// # 参数
    /// * `user_id` - 用户ID
    /// 
    /// # 返回值
    /// 用户的所有会话令牌列表
    pub fn get_user_tokens(&self, user_id: &str) -> Vec<String> {
        let user_sessions = match self.user_sessions.read() {
            Ok(us) => us,
            Err(_) => return Vec::new(),
        };
        
        user_sessions.get(user_id).cloned().unwrap_or_default()
    }
    
    /// 清理过期会话
    /// 
    /// # 返回值
    /// 清理的会话数量
    pub fn cleanup_expired_sessions(&self) -> usize {
        let expired_tokens: Vec<String> = {
            let sessions = match self.sessions.read() {
                Ok(s) => s,
                Err(_) => return 0,
            };
            
            sessions.iter()
                .filter(|(_, session)| session.is_expired())
                .map(|(token, _)| token.clone())
                .collect()
        };
        
        let count = expired_tokens.len();
        
        // 移除过期会话
        for token in expired_tokens {
            self.remove_session(&token);
        }
        
        count
    }
}

/// 默认认证提供者
pub struct DefaultAuthProvider {
    /// 会话管理器
    session_manager: Arc<SessionManager>,
}

impl DefaultAuthProvider {
    /// 创建新的默认认证提供者
    /// 
    /// # 参数
    /// * `session_expires_in` - 会话过期时间（毫秒）
    pub fn new(session_expires_in: u64) -> Self {
        Self {
            session_manager: Arc::new(SessionManager::new(session_expires_in)),
        }
    }
    
    /// 获取会话管理器
    pub fn session_manager(&self) -> &Arc<SessionManager> {
        &self.session_manager
    }
}

impl AuthProvider for DefaultAuthProvider {
    fn validate_token(&self, token: &str) -> Result<String, FlareError> {
        self.session_manager.validate_session(token)
    }
    
    fn generate_token(&self, user_id: &str) -> Result<String, FlareError> {
        // 简单的令牌生成，实际应用中应使用更安全的方法
        let token = format!("token_{}_{}", user_id, 
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis());
        self.session_manager.create_session(user_id.to_string(), token.clone(), None)?;
        Ok(token)
    }
}

impl Default for DefaultAuthProvider {
    fn default() -> Self {
        // 默认会话过期时间：24小时
        Self::new(24 * 60 * 60 * 1000)
    }
}