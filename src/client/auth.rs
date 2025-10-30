/// 客户端认证上下文（客户端专有逻辑）
#[derive(Debug, Clone, Default)]
pub struct ClientAuthContext {
    pub user_id: Option<String>,
    pub platform: Option<String>,
    pub token: Option<String>,
}

impl ClientAuthContext {
    pub fn is_valid(&self) -> bool {
        self.token.as_ref().map(|t| !t.is_empty()).unwrap_or(false)
    }
}
