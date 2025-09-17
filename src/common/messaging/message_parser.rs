//! 消息解析器
//! 
//! 用于统一处理来自不同协议（QUIC/WebSocket）的消息数据，
//! 并触发相应的连接事件。

use std::sync::Arc;
use tracing::{debug, error};

use crate::common::{
    protocol::Frame,
    connections::{
        event::ConnectionEvent,
        traits::ConnectionStats,
        config::ConnectionConfig,
    },
    serialization::FrameSerializer,
};
use crate::common::protocol::commands::{Command, ControlCmd};

/// 消息解析器
/// 
/// 负责解析从不同协议接收到的原始数据，并触发相应的连接事件。
/// 每个连接应创建一个独立的实例以确保状态隔离。
pub struct MessageParser {
    /// 连接ID
    connection_id: String,
    /// 事件处理器
    event_handler: Arc<dyn ConnectionEvent>,
    /// 统计信息
    stats: Arc<tokio::sync::RwLock<ConnectionStats>>,
    /// 序列化器
    serializer: Arc<Box<dyn FrameSerializer>>,
    /// 连接配置
    config: ConnectionConfig,
    /// 消息发送通道（可选）
    message_sender: Option<tokio::sync::mpsc::UnboundedSender<Vec<u8>>>,
}

impl MessageParser {
    /// 创建新的消息解析器
    /// 
    /// 每个连接应创建一个独立的实例以确保状态隔离。
    pub fn new(
        connection_id: String,
        event_handler: Arc<dyn ConnectionEvent>,
        stats: Arc<tokio::sync::RwLock<ConnectionStats>>,
        serializer: Arc<Box<dyn FrameSerializer>>,
        config: ConnectionConfig,
    ) -> Self {
        Self {
            connection_id,
            event_handler,
            stats,
            serializer,
            config,
            message_sender: None,
        }
    }

    /// 设置消息发送通道
    /// 
    /// 用于直接发送消息，提高处理效率
    pub fn set_message_sender(&mut self, sender: tokio::sync::mpsc::UnboundedSender<Vec<u8>>) {
        self.message_sender = Some(sender);
    }

    /// 获取消息发送通道的引用（如果存在）
    pub fn get_message_sender(&self) -> Option<&tokio::sync::mpsc::UnboundedSender<Vec<u8>>> {
        self.message_sender.as_ref()
    }

    /// 直接发送消息数据
    /// 
    /// 通过消息发送通道直接发送已序列化的数据
    pub fn send_message_data(&self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(sender) = &self.message_sender {
            sender.send(data)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        } else {
            Err("消息发送通道未设置".into())
        }
    }

    /// 发送消息帧
    pub async fn send_message(&self, frame: Frame) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 使用序列化器序列化消息帧
        let data = self.serializer.serialize(&frame).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        if let Some(sender) = &self.message_sender {
            if let Err(e) = sender.send(data) {
                error!("发送消息失败: {}", e);
                // 触发错误事件
                self.event_handler.on_error(&self.connection_id, &format!("发送消息失败: {}", e)).await;
                Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            } else {
                // 触发消息发送事件
                self.event_handler.on_message_sent(&self.connection_id, &frame).await;
                Ok(())
            }
        } else {
            let err_msg = "消息发送通道未设置";
            error!("{}", err_msg);
            // 触发错误事件
            self.event_handler.on_error(&self.connection_id, err_msg).await;
            Err(err_msg.into())
        }
    }
    /// 解析并处理原始数据
    ///
    /// 该方法负责：
    /// 1. 使用序列化器解析原始数据
    /// 2. 如果解析失败，发送错误通知并返回错误
    /// 3. 如果解析成功，继续处理消息
    pub async fn parse_and_handle(&self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 使用序列化器解析消息
        match self.serializer.deserialize(&data).await {
            Ok(frame) => {
                debug!("成功解析消息: {}", frame.get_command_type_str());
                // 处理解析后的帧
                self.handle_frame(frame).await;
                Ok(())
            },
            Err(e) => {
                error!("消息反序列化失败: {}", e);
                // 发送错误通知给发送方
                let message_id = crate::common::protocol::factory::FrameFactory::generate_message_id();
                let error_frame = crate::common::protocol::Frame::error(message_id, format!("消息反序列化失败: {}", e));
                match self.serializer.serialize(&error_frame).await {
                    Ok(error_data) => {
                        // 如果有消息发送通道，直接发送错误消息
                        if let Some(sender) = &self.message_sender {
                            if let Err(send_err) = sender.send(error_data) {
                                error!("发送错误通知失败: {}", send_err);
                            }
                        } else {
                            // 没有消息发送通道，触发消息发送事件，让连接层实际发送错误消息
                            self.event_handler.on_message_sent(&self.connection_id, &error_frame).await;
                        }
                    }
                    Err(serialize_err) => {
                        error!("序列化错误通知失败: {}", serialize_err);
                    }
                }

                // 返回错误，不再继续处理
                Err(Box::new(e))
            }
        }
    }
    /// 自动回复心跳响应
    /// 
    /// 统一处理心跳响应的发送逻辑
    async fn auto_respond_heartbeat(&self) {
        // 创建心跳响应帧
        let message_id = crate::common::protocol::factory::FrameFactory::generate_message_id();
        let heartbeat_ack_frame = crate::common::protocol::Frame::new(
            crate::common::protocol::commands::Command::Control(crate::common::protocol::commands::ControlCmd::Pong),
            message_id.clone(),
            crate::common::protocol::Reliability::BestEffort
        );
        
        // 使用send_message方法发送心跳响应
        if let Err(e) = self.send_message(heartbeat_ack_frame).await {
            error!("发送心跳响应失败: {}", e);
        }
    }

    /// 处理解析后的Frame
    /// 
    /// 该方法负责：
    /// 1. 更新统计信息
    /// 2. 根据消息类型触发相应的连接事件
    /// 3. 自动回复心跳消息（如果配置启用）
    /// 4. 处理认证请求和响应消息
    pub async fn handle_frame(&self, frame: Frame) {
        // 更新统计信息
        {
            let mut stats = self.stats.write().await;
            stats.messages_received += 1;
        }
        
        // 克隆需要的变量
        let handler = Arc::clone(&self.event_handler);
        let id = self.connection_id.clone();
        let frame_clone = frame.clone();
        
        // 根据消息类型触发特定的事件处理方法
        match &frame_clone.command {
            Command::Control(control_cmd) => {
                match control_cmd {
                    ControlCmd::Ping => {
                        // 触发心跳请求事件
                        handler.on_heartbeat_ping(&id).await;
                        
                        // 如果启用了自动回复心跳，则自动回复
                        if self.config.is_auto_heartbeat_response_enabled() {
                            self.auto_respond_heartbeat().await;
                        }
                    },
                    ControlCmd::Pong => {
                        // 触发心跳响应事件
                        handler.on_heartbeat_pong(&id).await;
                    },
                    ControlCmd::Error(error_cmd) => {
                        // 触发错误事件
                        handler.on_error(&id, &format!("远程错误 {} - {}", error_cmd.status, error_cmd.message)).await;
                    },
                    ControlCmd::Connect(_) => {
                        // 触发连接事件
                        handler.on_connected(&id).await;
                    },
                    ControlCmd::Disconnect(_) => {
                        // 触发断开连接事件
                        handler.on_disconnected(&id, "收到断开连接消息").await;
                    },
                    ControlCmd::AuthRequest(_) => {
                        // 认证请求事件在on_message_received中处理
                        handler.on_message_received(&id, &frame_clone).await;
                    },
                    ControlCmd::AuthResponse(_) => {
                        // 认证响应事件在on_message_received中处理
                        handler.on_message_received(&id, &frame_clone).await;
                    },
                    ControlCmd::ConnectAck(_) => {
                        // 连接确认事件在on_message_received中处理
                        handler.on_message_received(&id, &frame_clone).await;
                    },
                    ControlCmd::Custom(_) => {
                        // 自定义命令事件在on_message_received中处理
                        handler.on_message_received(&id, &frame_clone).await;
                    }
                }
            },
            Command::Message(_) | Command::Notification(_) | Command::Event(_) => {
                // 消息、通知和事件类命令统一触发消息接收事件
                handler.on_message_received(&id, &frame_clone).await;
            }
        }
    }
}