//! 统一消息处理
//!
//! 将消息发送、接收与 ConnectionEvent 紧密结合

use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Mutex, oneshot};
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::common::{
    error::{Result, FlareError},
    protocol::{
        Frame, 
        commands::{Command, ControlCmd, NotificationCmd, EventCmd},
        Reliability,
    },
    connections::event::ConnectionEvent,
};

/// 消息发送函数类型
pub type SendFunction = Arc<dyn Fn(Frame) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> + Send + Sync>;


/// 消息处理器
/// 
/// 结合消息发送、接收和 ConnectionEvent 处理
pub struct MessageHandler {
    /// 等待响应的请求
    pending_requests: Arc<Mutex<HashMap<String, (oneshot::Sender<Frame>, Instant)>>>,
    /// 默认超时时间
    default_timeout: Duration,
    /// 消息ID计数器
    message_id_counter: Arc<RwLock<u64>>,
    /// 连接事件处理器
    connection_event_handler: Arc<RwLock<Option<Arc<dyn ConnectionEvent>>>>,
    /// 消息发送函数
    send_function: Arc<RwLock<Option<SendFunction>>>,
}

impl MessageHandler {
    /// 创建新的消息处理器
    pub fn new(default_timeout: Duration) -> Self {
        Self {
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            default_timeout,
            message_id_counter: Arc::new(RwLock::new(0)),
            connection_event_handler: Arc::new(RwLock::new(None)),
            send_function: Arc::new(RwLock::new(None)),
        }
    }
    
    /// 设置连接事件处理器
    pub async fn set_connection_event_handler(&self, handler: Arc<dyn ConnectionEvent>) {
        *self.connection_event_handler.write().await = Some(handler);
    }
    
    /// 设置消息发送函数
    pub async fn set_send_function(&self, send_fn: SendFunction) {
        *self.send_function.write().await = Some(send_fn);
    }
    
    /// 生成唯一消息ID
    async fn generate_message_id(&self) -> String {
        let mut counter = self.message_id_counter.write().await;
        *counter += 1;
        format!("msg_{}_{}", 
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(), 
            *counter
        )
    }
    
    /// 创建消息帧
    async fn create_frame(&self, command: Command, reliability: Reliability) -> Result<Frame> {
        let message_id = self.generate_message_id().await;
        
        Ok(Frame::new(command, message_id, reliability))
    }
    
    /// 发送消息（内部实现）
    async fn send_internal<F>(&self, create_frame: F) -> Result<Frame>
    where
        F: FnOnce() -> Result<Frame>,
    {
        let frame = create_frame()?;
        
        // 触发消息发送事件
        if let Some(handler) = &*self.connection_event_handler.read().await {
            handler.on_message_sent("client", &frame).await;
        }
        
        // 使用设置的发送函数发送消息
        if let Some(send_fn) = &*self.send_function.read().await {
            debug!("发送消息: {}", frame.get_message_id());
            send_fn(frame.clone()).await?;
        } else {
            return Err(FlareError::general_error("发送函数未设置".to_string()));
        }
        
        Ok(frame)
    }
    
    /// 发送等待响应的消息
    pub async fn send_request<F>(
        &self,
        create_command: F,
        reliability: Reliability,
        custom_timeout: Option<Duration>,
    ) -> Result<Frame>
    where
        F: FnOnce(String) -> Result<Command>,
    {
        let message_id = self.generate_message_id().await;
        let command = create_command(message_id.clone())?;
        let frame = self.create_frame(command, reliability).await?;
        
        // 设置超时时间
        let timeout_duration = custom_timeout.unwrap_or(self.default_timeout);
        
        // 注册响应等待
        let (response_tx, response_rx) = oneshot::channel();
        {
            let mut pending = self.pending_requests.lock().await;
            pending.insert(message_id.clone(), (response_tx, Instant::now()));
        }
        
        // 发送消息
        match self.send_internal(|| Ok(frame.clone())).await {
            Ok(_) => {
                debug!("消息发送成功，等待响应: {}", message_id);
                
                // 等待响应
                match timeout(timeout_duration, response_rx).await {
                    Ok(Ok(response)) => {
                        debug!("收到响应: {}", message_id);
                        Ok(response)
                    }
                    Ok(Err(e)) => {
                        warn!("响应通道错误: {}", e);
                        Err(FlareError::general_error(format!("响应通道错误: {}", e)))
                    }
                    Err(_) => {
                        warn!("等待响应超时: {}", message_id);
                        self.unregister_request(&message_id).await;
                        Err(FlareError::timeout("等待响应超时".to_string()))
                    }
                }
            }
            Err(e) => {
                warn!("发送消息失败: {}", e);
                self.unregister_request(&message_id).await;
                Err(e)
            }
        }
    }
    
    /// 发送无需等待响应的消息
    pub async fn send_fire_and_forget<F>(
        &self,
        create_command: F,
        reliability: Reliability,
    ) -> Result<()>
    where
        F: FnOnce(String) -> Result<Command>,
    {
        let message_id = self.generate_message_id().await;
        let command = create_command(message_id)?;
        let frame = self.create_frame(command, reliability).await?;
        
        match self.send_internal(|| Ok(frame)).await {
            Ok(_) => {
                debug!("消息发送成功（无需等待响应）");
                Ok(())
            }
            Err(e) => {
                warn!("发送消息失败: {}", e);
                Err(e)
            }
        }
    }
    
    /// 发送控制消息
    pub async fn send_control(&self, control_cmd: ControlCmd) -> Result<()> {
        let command = Command::Control(control_cmd);
        let frame = self.create_frame(command, Reliability::BestEffort).await?;
        
        match self.send_internal(|| Ok(frame)).await {
            Ok(_) => {
                debug!("控制消息发送成功");
                Ok(())
            }
            Err(e) => {
                warn!("发送控制消息失败: {}", e);
                Err(e)
            }
        }
    }
    
    /// 发送通知消息
    pub async fn send_notification(&self, notification_cmd: NotificationCmd) -> Result<()> {
        let command = Command::Notification(notification_cmd);
        let frame = self.create_frame(command, Reliability::BestEffort).await?;
        
        match self.send_internal(|| Ok(frame)).await {
            Ok(_) => {
                debug!("通知消息发送成功");
                Ok(())
            }
            Err(e) => {
                warn!("发送通知消息失败: {}", e);
                Err(e)
            }
        }
    }
    
    /// 发送事件消息
    pub async fn send_event(&self, event_cmd: EventCmd) -> Result<()> {
        let command = Command::Event(event_cmd);
        let frame = self.create_frame(command, Reliability::BestEffort).await?;
        
        match self.send_internal(|| Ok(frame)).await {
            Ok(_) => {
                debug!("事件消息发送成功");
                Ok(())
            }
            Err(e) => {
                warn!("发送事件消息失败: {}", e);
                Err(e)
            }
        }
    }
    
    /// 发送消息（通用方法）
    pub async fn send_message(&self, frame: Frame) -> Result<()> {
        match self.send_internal(|| Ok(frame)).await {
            Ok(_) => {
                debug!("消息发送成功");
                Ok(())
            }
            Err(e) => {
                warn!("发送消息失败: {}", e);
                Err(e)
            }
        }
    }
    
    /// 处理接收到的消息
    pub async fn handle_message(&self, frame: Frame) -> Result<()> {
        let message_id = frame.get_message_id();
        debug!("收到消息: ID={}, 类型={}", message_id, frame.get_command_type_str());
        
        // 触发消息接收事件
        if let Some(handler) = &*self.connection_event_handler.read().await {
            handler.on_message_received("client", &frame).await;
        }
        
        // 处理响应匹配
        match self.handle_response(frame).await {
            Ok(_) => {
                debug!("消息处理成功: {}", message_id);
                Ok(())
            }
            Err(e) => {
                warn!("消息处理失败: {} - 错误: {}", message_id, e);
                Err(e)
            }
        }
    }
    
    /// 处理响应消息
    async fn handle_response(&self, response: Frame) -> Result<()> {
        let message_id = response.get_message_id();
        let mut pending = self.pending_requests.lock().await;
        
        if let Some((sender, _)) = pending.remove(&message_id) {
            match sender.send(response) {
                Ok(_) => {
                    debug!("响应发送成功: {}", message_id);
                    Ok(())
                }
                Err(_) => {
                    warn!("响应通道已关闭: {}", message_id);
                    Err(FlareError::general_error("响应通道已关闭".to_string()))
                }
            }
        } else {
            debug!("收到未请求的响应: {}", message_id);
            Ok(())
        }
    }
    
    /// 取消注册等待响应的请求
    async fn unregister_request(&self, message_id: &str) {
        let mut pending = self.pending_requests.lock().await;
        if pending.remove(message_id).is_some() {
            debug!("取消注册等待响应的请求: {}", message_id);
        }
    }
    
    /// 清理超时的请求
    pub async fn cleanup_timeout_requests(&self) {
        let mut pending = self.pending_requests.lock().await;
        let now = Instant::now();
        let timeout_duration = self.default_timeout;
        
        let mut timeout_requests = Vec::new();
        
        for (message_id, (_, timestamp)) in pending.iter() {
            if now.duration_since(*timestamp) > timeout_duration {
                timeout_requests.push(message_id.clone());
            }
        }
        
        for message_id in timeout_requests {
            if let Some((sender, _)) = pending.remove(&message_id) {
                let _ = sender.send(Frame::new(
                    Command::Control(ControlCmd::Error(
                        crate::common::protocol::commands::ErrorCommand::new(
                            408,
                            "请求超时".to_string()
                        )
                    )),
                    message_id.clone(),
                    Reliability::BestEffort,
                ));
                warn!("清理超时请求: {}", message_id);
            }
        }
    }
    
    /// 获取等待中的请求数量
    pub async fn get_pending_count(&self) -> usize {
        let pending = self.pending_requests.lock().await;
        pending.len()
    }
    
    /// 清空所有等待的请求
    pub async fn clear_all_requests(&self) {
        let mut pending = self.pending_requests.lock().await;
        let count = pending.len();
        pending.clear();
        debug!("清空所有等待的请求，数量: {}", count);
    }
    
    /// 设置默认超时时间
    pub fn set_default_timeout(&mut self, timeout: Duration) {
        self.default_timeout = timeout;
    }
    
    /// 获取默认超时时间
    pub fn get_default_timeout(&self) -> Duration {
        self.default_timeout
    }
    
    /// 批量发送消息
    /// 
    /// # 参数
    /// * `frames` - 要发送的消息帧列表
    /// 
    /// # 返回值
    /// 返回发送结果，包含成功和失败的消息数量
    pub async fn send_batch(&self, frames: Vec<Frame>) -> Result<(usize, usize)> {
        let mut success_count = 0;
        let mut failure_count = 0;
        
        for frame in frames {
            match self.send_message(frame).await {
                Ok(_) => success_count += 1,
                Err(e) => {
                    failure_count += 1;
                    warn!("批量发送消息失败: {}", e);
                }
            }
        }
        
        debug!("批量发送完成: 成功={}, 失败={}", success_count, failure_count);
        Ok((success_count, failure_count))
    }
    
    /// 获取消息处理器状态信息
    /// 
    /// # 返回值
    /// 返回包含状态信息的字符串
    pub async fn get_status_info(&self) -> String {
        let pending_count = self.get_pending_count().await;
        let timeout_duration = self.get_default_timeout();
        
        format!(
            "MessageHandler状态: 等待响应={}, 默认超时={}ms",
            pending_count,
            timeout_duration.as_millis()
        )
    }
}

impl Clone for MessageHandler {
    fn clone(&self) -> Self {
        Self {
            pending_requests: Arc::clone(&self.pending_requests),
            default_timeout: self.default_timeout,
            message_id_counter: Arc::clone(&self.message_id_counter),
            connection_event_handler: Arc::clone(&self.connection_event_handler),
            send_function: Arc::clone(&self.send_function),
        }
    }
}
