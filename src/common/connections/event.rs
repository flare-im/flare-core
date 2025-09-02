//! 连接事件处理
//!
//! 定义连接事件处理相关的 trait 与默认实现

use async_trait::async_trait;

use crate::common::protocol::Frame;

/// 连接事件处理器
/// 
/// 处理连接生命周期中的各种事件
#[async_trait]
pub trait ConnectionEventHandler: Send + Sync {
    /// 连接建立事件
    async fn on_connected(&self, connection_id: &str);
    
    /// 连接断开事件
    async fn on_disconnected(&self, connection_id: &str, reason: &str);
    
    /// 连接错误事件
    async fn on_error(&self, connection_id: &str, error: &str);
    
    /// 消息接收事件
    async fn on_message_received(&self, connection_id: &str, message: &Frame);
    
    /// 心跳超时事件
    async fn on_heartbeat_timeout(&self, connection_id: &str);
    
    /// 连接质量变化事件
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8);
}

/// 默认连接事件处理器
/// 
/// 提供基本的日志记录功能
pub struct DefaultConnectionEventHandler;

#[async_trait]
impl ConnectionEventHandler for DefaultConnectionEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        tracing::info!("连接已建立: {}", connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        tracing::info!("连接已断开: {} - 原因: {}", connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        tracing::error!("连接错误: {} - 错误: {}", connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        tracing::debug!("收到消息: {} - 类型: {:?}", connection_id, message.get_message_type());
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        tracing::warn!("心跳超时: {}", connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        tracing::info!("连接质量变化: {} - 评分: {}", connection_id, quality_score);
    }
}

impl Default for DefaultConnectionEventHandler {
    fn default() -> Self {
        Self
    }
}

/// 回显事件处理器
/// 
/// 在服务端使用，会自动回显收到的消息
pub struct EchoConnectionEventHandler {
    /// 连接实例（用于发送回显消息）
    connection: std::sync::Arc<tokio::sync::RwLock<Option<std::sync::Arc<tokio::sync::Mutex<Box<dyn crate::common::connections::traits::ServerConnection>>>>>>,
}

impl EchoConnectionEventHandler {
    /// 创建新的回显事件处理器
    pub fn new() -> Self {
        Self {
            connection: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
        }
    }
    
    /// 设置连接实例
    pub async fn set_connection(&self, connection: std::sync::Arc<tokio::sync::Mutex<Box<dyn crate::common::connections::traits::ServerConnection>>>) {
        *self.connection.write().await = Some(connection);
    }
}

#[async_trait]
impl ConnectionEventHandler for EchoConnectionEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        tracing::info!("回显服务 - 连接已建立: {}", connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        tracing::info!("回显服务 - 连接已断开: {} - 原因: {}", connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        tracing::error!("回显服务 - 连接错误: {} - 错误: {}", connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        let payload = message.get_payload();
        
        // 记录收到的消息
        if let Ok(text) = String::from_utf8(payload.to_vec()) {
            tracing::info!("回显服务 - 收到文本消息: {} - 内容: {}", connection_id, text);
        } else {
            tracing::info!("回显服务 - 收到二进制消息: {} - 长度: {}", connection_id, payload.len());
        }
        
        // 准备回显消息
        if let Some(conn_arc) = &*self.connection.read().await {
            // 先检查连接状态，如果连接不活跃就不发送
            {
                let connection = conn_arc.lock().await;
                if !connection.is_active().await {
                    tracing::warn!("连接不活跃，跳过回显消息发送");
                    return;
                }
            }
            
            let conn = std::sync::Arc::clone(conn_arc);
            let echo_message = message.clone();
            tracing::info!("正在发送回显消息...");
            
            // 在单独的任务中发送回显消息
            tokio::spawn(async move {
                let mut connection = conn.lock().await;
                
                // 再次检查连接状态
                if !connection.is_active().await {
                    tracing::warn!("连接不活跃，无法发送回显消息");
                    return;
                }
                
                if let Err(e) = connection.send_message(echo_message).await {
                    tracing::error!("回显消息发送失败: {}", e);
                } else {
                    tracing::info!("回显消息已发送");
                }
            });
        } else {
            tracing::warn!("无法发送回显消息，连接不可用");
        }
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        tracing::warn!("回显服务 - 心跳超时: {}", connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        tracing::info!("回显服务 - 连接质量变化: {} - 评分: {}", connection_id, quality_score);
    }
}

impl Default for EchoConnectionEventHandler {
    fn default() -> Self {
        Self::new()
    }
}

/// 心跳事件处理器
/// 
/// 专门用于处理心跳消息，自动回复心跳确认
pub struct HeartbeatConnectionEventHandler {
    /// 连接实例（用于发送心跳确认消息）
    connection: std::sync::Arc<tokio::sync::RwLock<Option<std::sync::Arc<tokio::sync::Mutex<Box<dyn crate::common::connections::traits::ServerConnection>>>>>>,
}

impl HeartbeatConnectionEventHandler {
    /// 创建新的心跳事件处理器
    pub fn new() -> Self {
        Self {
            connection: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
        }
    }
    
    /// 设置连接实例
    pub async fn set_connection(&self, connection: std::sync::Arc<tokio::sync::Mutex<Box<dyn crate::common::connections::traits::ServerConnection>>>) {
        *self.connection.write().await = Some(connection);
    }
}

#[async_trait]
impl ConnectionEventHandler for HeartbeatConnectionEventHandler {
    async fn on_connected(&self, connection_id: &str) {
        tracing::info!("心跳服务 - 连接已建立: {}", connection_id);
    }

    async fn on_disconnected(&self, connection_id: &str, reason: &str) {
        tracing::info!("心跳服务 - 连接已断开: {} - 原因: {}", connection_id, reason);
    }

    async fn on_error(&self, connection_id: &str, error: &str) {
        tracing::error!("心跳服务 - 连接错误: {} - 错误: {}", connection_id, error);
    }

    async fn on_message_received(&self, connection_id: &str, message: &Frame) {
        // 检查是否为心跳消息
        if message.is_heartbeat() {
            let message_type = message.get_message_type();
            tracing::info!("心跳服务 - 收到心跳消息: {} - 类型: {:?}", connection_id, message_type);
            
            // 根据心跳类型处理
            match message_type {
                crate::common::protocol::MessageType::Heartbeat => {
                    // 收到客户端心跳，发送心跳确认
                    if let Some(conn_arc) = &*self.connection.read().await {
                        // 先检查连接状态
                        {
                            let connection = conn_arc.lock().await;
                            if !connection.is_active().await {
                                tracing::warn!("连接不活跃，跳过心跳确认发送");
                                return;
                            }
                        }
                        
                        let conn = std::sync::Arc::clone(conn_arc);
                        let heartbeat_ack = Frame::heartbeat_ack();
                        tracing::info!("正在发送心跳确认...");
                        
                        tokio::spawn(async move {
                            let mut connection = conn.lock().await;
                            
                            if !connection.is_active().await {
                                tracing::warn!("连接不活跃，无法发送心跳确认");
                                return;
                            }
                            
                            if let Err(e) = connection.send_message(heartbeat_ack).await {
                                tracing::error!("心跳确认发送失败: {}", e);
                            } else {
                                tracing::info!("心跳确认已发送");
                            }
                        });
                    } else {
                        tracing::warn!("无法发送心跳确认，连接不可用");
                    }
                }
                crate::common::protocol::MessageType::HeartbeatAck => {
                    // 收到心跳确认，记录日志
                    tracing::info!("心跳服务 - 收到心跳确认: {}", connection_id);
                }
                _ => {
                    // 其他心跳相关消息
                    tracing::debug!("心跳服务 - 收到其他心跳消息: {} - 类型: {:?}", connection_id, message_type);
                }
            }
        } else {
            // 非心跳消息，记录但不处理
            let payload = message.get_payload();
            if let Ok(text) = String::from_utf8(payload.to_vec()) {
                tracing::info!("心跳服务 - 收到普通消息: {} - 内容: {}", connection_id, text);
            } else {
                tracing::info!("心跳服务 - 收到二进制消息: {} - 长度: {}", connection_id, payload.len());
            }
        }
    }

    async fn on_heartbeat_timeout(&self, connection_id: &str) {
        tracing::warn!("心跳服务 - 心跳超时: {}", connection_id);
    }
    
    async fn on_quality_changed(&self, connection_id: &str, quality_score: u8) {
        tracing::info!("心跳服务 - 连接质量变化: {} - 评分: {}", connection_id, quality_score);
    }
}

impl Default for HeartbeatConnectionEventHandler {
    fn default() -> Self {
        Self::new()
    }
}

